"""Exercise local tooling path selection and ownership guards without Docker."""
import base64
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parent
KEYS = ('ORDER_DB_PASSWORD', 'INVENTORY_DB_PASSWORD', 'FULFILLMENT_DB_PASSWORD',
        'KEYCLOAK_DB_PASSWORD', 'KC_BOOTSTRAP_ADMIN_CLIENT_SECRET')
MOCK = '''#!/usr/bin/env python3
import json, os, pathlib, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ['WAREXIS_TEST_CALLS'], 'a') as output:
    output.write(json.dumps([name, *args]) + '\\n')
if name == 'kind':
    if args == ['version']: print('kind v0.33.0')
elif name == 'docker':
    if args[0] == 'inspect': print(os.environ.get('WAREXIS_TEST_NODE_ID', 'a' * 64) + ' warexis-phase11')
elif 'version' in args:
    print(json.dumps({'clientVersion': {'gitVersion': 'v1.37.0'}}))
elif 'current-context' in args: print('kind-warexis-phase11')
elif 'view' in args:
    print(json.dumps({'clusters': [{'name': 'kind-warexis-phase11', 'cluster': {'server': 'https://127.0.0.1:6443'}}]}))
elif 'get' in args and 'secret' in args:
    print(os.environ.get('WAREXIS_TEST_SECRET', ''))
elif 'get' in args and 'pvc' in args:
    if os.environ.get('WAREXIS_TEST_RETAINED_PVC'): print('persistentvolumeclaim/retained')
elif 'create' in args: print('{}')
elif 'apply' in args and '-' in args: sys.stdin.read()
'''


class StatePathsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='warexis-state-paths-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.tools = self.root / 'tools'
        self.tools.mkdir()
        for name in ('kind', 'kubectl', 'docker'):
            executable = self.tools / name
            executable.write_text(MOCK)
            executable.chmod(0o700)
        self.calls = self.root / 'calls.jsonl'
        self.env = {key: value for key, value in os.environ.items()
                    if key not in ('WAREXIS_K8S_STATE_DIR', 'XDG_STATE_HOME')}
        self.env.update(HOME=str(self.root / 'home'), XDG_STATE_HOME=str(self.root / 'xdg'),
                        WAREXIS_K8S_TOOLS_DIR=str(self.tools), WAREXIS_TEST_CALLS=str(self.calls),
                        PATH=str(self.tools) + ':' + os.environ['PATH'])

    def state(self):
        return Path(self.env.get('WAREXIS_K8S_STATE_DIR',
                    str(Path(self.env.get('XDG_STATE_HOME', self.env['HOME'] + '/.local/state')) / 'warexis/kubernetes')))

    def prepare(self):
        state = self.state()
        state.mkdir(parents=True, mode=0o700)
        (state / 'kubeconfig').write_text('synthetic test kubeconfig')
        (state / 'kubeconfig').chmod(0o600)
        (state / 'owned-node-id').write_text('a' * 64 + '\n')
        private = state / 'secrets'
        private.mkdir(mode=0o700)
        values = {key: 'A' * 48 for key in KEYS}
        (private / 'runtime.env').write_text(''.join(key + '=' + value + '\n' for key, value in values.items()))
        (private / 'runtime.env').chmod(0o600)
        self.env['WAREXIS_TEST_SECRET'] = json.dumps({'data': {
            key: base64.b64encode(value.encode()).decode() for key, value in values.items()}})
        return state

    def run_script(self, name, *args, success=True):
        result = subprocess.run(['bash', str(SCRIPTS / name), *args], env=self.env,
                                capture_output=True, text=True, timeout=15, check=False)
        self.assertEqual(result.returncode == 0, success, result.stderr)
        return result

    def calls_recorded(self):
        return [json.loads(line) for line in self.calls.read_text().splitlines()]

    def exercise_paths(self):
        state = self.prepare()
        before = {name: (state / name).read_bytes() for name in ('owned-node-id', 'kubeconfig', 'secrets/runtime.env')}
        self.run_script('cluster.sh', 'status')
        self.run_script('images.sh', 'load', '--service', 'order')
        self.run_script('deploy.sh', 'apply')
        configs = [call[call.index('--kubeconfig') + 1] for call in self.calls_recorded() if '--kubeconfig' in call]
        self.assertTrue(configs)
        self.assertEqual(set(configs), {str(state / 'kubeconfig')})
        self.assertTrue(any(call[:3] == ['kind', 'load', 'docker-image'] for call in self.calls_recorded()))
        for name, original in before.items():
            self.assertEqual((state / name).read_bytes(), original)

    def test_xdg_default_across_lifecycle_image_load_and_deploy(self):
        self.exercise_paths()

    def test_home_fallback_across_lifecycle_image_load_and_deploy(self):
        self.env.pop('XDG_STATE_HOME')
        self.exercise_paths()

    def test_explicit_state_override_reuses_existing_records(self):
        self.env['WAREXIS_K8S_STATE_DIR'] = str(self.root / 'existing-cluster')
        self.exercise_paths()

    def test_missing_owner_still_prevents_deployment(self):
        state = self.prepare()
        (state / 'owned-node-id').unlink()
        self.run_script('deploy.sh', 'apply', success=False)
        self.assertFalse(any('apply' in call for call in self.calls_recorded()))

    def test_wrong_owner_still_prevents_deployment(self):
        self.prepare()
        self.env['WAREXIS_TEST_NODE_ID'] = 'b' * 64
        self.run_script('deploy.sh', 'apply', success=False)
        self.assertFalse(any('apply' in call for call in self.calls_recorded()))

    def test_retained_volumes_without_private_inputs_still_prevent_rotation(self):
        state = self.prepare()
        (state / 'secrets/runtime.env').unlink()
        self.env['WAREXIS_TEST_RETAINED_PVC'] = '1'
        self.run_script('deploy.sh', 'apply', success=False)
        self.assertFalse((state / 'secrets/runtime.env').exists())
        self.assertFalse(any('apply' in call for call in self.calls_recorded()))

    def test_acceptance_defaults_and_explicit_cli_overrides(self):
        spec = importlib.util.spec_from_file_location('state_paths_acceptance', SCRIPTS / 'acceptance.py')
        acceptance = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(acceptance)
        for home_fallback, explicit in ((False, False), (True, False), (False, True)):
            with self.subTest(home_fallback=home_fallback, explicit=explicit):
                environ = dict(self.env)
                if home_fallback:
                    environ.pop('XDG_STATE_HOME')
                state_home = Path(environ.get('XDG_STATE_HOME', environ['HOME'] + '/.local/state'))
                config = state_home / 'warexis/kubernetes/kubeconfig'
                tool = self.tools / 'kubectl'
                arguments = ['acceptance.py', 'infrastructure', '--runtime-dir', str(self.root / 'runtime'), '--kubectl', str(tool)]
                if explicit:
                    config = self.root / 'existing-cluster/kubeconfig'
                    arguments += ['--kubeconfig', str(config)]
                with patch.dict(os.environ, environ, clear=True), patch('sys.argv', arguments), \
                        patch.object(acceptance, 'runtime_directory', return_value=self.root), \
                        patch.object(acceptance, 'private_file') as private_file, \
                        patch.object(acceptance.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0)) as guard, \
                        patch.object(acceptance, 'infrastructure', return_value={'status': 'PASS'}) as infrastructure, \
                        patch.object(acceptance, 'save'), contextlib.redirect_stdout(io.StringIO()):
                    acceptance.main()
                self.assertEqual(private_file.call_args.args, (config,))
                self.assertEqual(infrastructure.call_args.args[0].kubeconfig, str(config))
                self.assertEqual(guard.call_args.kwargs['env']['WAREXIS_K8S_STATE_DIR'], str(config.parent))
                self.assertEqual(guard.call_args.kwargs['env']['WAREXIS_K8S_TOOLS_DIR'], str(tool.parent))


if __name__ == '__main__':
    unittest.main()
