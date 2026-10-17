#!/usr/bin/env python3
"""Render and validate the local Kubernetes contract; optionally ask the API server."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

import yaml

BASE = Path(__file__).resolve().parents[1]
APPLICATIONS = {'order': 8081, 'inventory': 8082, 'fulfillment': 8083, 'frontend': 8080}
SECRET_KEYS = {'ORDER_DB_PASSWORD', 'INVENTORY_DB_PASSWORD', 'FULFILLMENT_DB_PASSWORD',
               'KEYCLOAK_DB_PASSWORD', 'KC_BOOTSTRAP_ADMIN_CLIENT_SECRET'}


def require(value, message):
    if not value:
        raise ValueError(message)


def quantity(value, memory=False):
    value = str(value)
    if memory:
        for suffix, multiplier in [('Gi', 1024 ** 3), ('Mi', 1024 ** 2), ('Ki', 1024)]:
            if value.endswith(suffix):
                return int(value[:-len(suffix)]) * multiplier
        return int(value)
    return int(value[:-1]) if value.endswith('m') else int(float(value) * 1000)


def validate(documents, base):
    keys = [(d['kind'], d['metadata']['name']) for d in documents]
    require(len(keys) == len(set(keys)), 'Duplicate resource identity')
    objects = dict(zip(keys, documents))
    require(('Namespace', 'warexis') in objects, 'Dedicated namespace required')
    require(not any(d['kind'] == 'Secret' for d in documents), 'Runtime Secrets must not be committed/rendered')
    configmaps = {name: d for (kind, name), d in objects.items() if kind == 'ConfigMap'}
    require('warexis-config' in configmaps and 'frontend-nginx' in configmaps, 'Expected ConfigMaps missing')
    for configmap in configmaps.values():
        require(not any(k in SECRET_KEYS for k in configmap.get('data', {})), 'Secret key in ConfigMap')
    # The shared deploy entry point creates the public realm ConfigMap before apply.
    realm = json.loads((base.parent / 'auth/warexis-realm.json').read_text())
    require(realm['realm'] == 'warexis' and not realm.get('users'), 'Realm import must contain no user credentials')
    browser = next(c for c in realm['clients'] if c['clientId'] == 'warexis-operations')
    require(browser['publicClient'] is True and browser['attributes']['pkce.code.challenge.method'] == 'S256', 'Preserve public PKCE client')
    require(browser['redirectUris'] == ['http://127.0.0.1:5173/'], 'Preserve exact browser root redirect')
    shared = configmaps['warexis-config']['data']
    require(shared.get('WAREXIS_OAUTH2_ISSUER_URI') == 'http://127.0.0.1:18080/realms/warexis', 'Public issuer mismatch')
    require(shared.get('WAREXIS_OAUTH2_AUDIENCE') == 'warexis-api', 'Audience validation must be preserved')
    require(shared.get('SPRING_SECURITY_OAUTH2_RESOURCESERVER_JWT_JWK_SET_URI') ==
            'http://keycloak:8080/realms/warexis/protocol/openid-connect/certs', 'Internal JWKS mismatch')
    require(shared.get('KAFKA_BOOTSTRAP_SERVERS') == 'kafka:29092' and shared.get('WAREXIS_MESSAGING_ENABLED') == 'true', 'Messaging contract mismatch')
    cpu_request = memory_limit = 0
    refs = set()
    workloads = [d for d in documents if d['kind'] in ('Deployment', 'StatefulSet')]
    require(len(workloads) == 10, 'Expected exactly ten local workloads')
    for resource in documents:
        if resource['kind'] != 'Namespace':
            require(resource['metadata'].get('namespace') == 'warexis', 'Unexpected resource namespace')
    for workload in workloads:
        name = workload['metadata']['name']
        require(workload['spec']['replicas'] == 1, 'Only one initial replica is validated')
        pod = workload['spec']['template']['spec']
        require(pod.get('automountServiceAccountToken') is False, 'Unneeded service account token mounted')
        require(not any(pod.get(k) for k in ('hostNetwork', 'hostPID', 'hostIPC')), 'Host namespace access is unnecessary')
        for volume in pod.get('volumes', []):
            require('hostPath' not in volume, 'Do not mount workstation paths')
            if 'configMap' in volume:
                require(volume['configMap']['name'] in configmaps or volume['configMap']['name'] == 'warexis-realm', 'ConfigMap volume does not resolve')
        for container in pod.get('containers', []) + pod.get('initContainers', []):
            image = container['image']
            require(':' in image and not image.endswith(':latest'), 'Specific image version required')
            require(container.get('securityContext', {}).get('allowPrivilegeEscalation') is False, 'Privilege escalation must be disabled')
            require(container.get('securityContext', {}).get('privileged') is not True, 'Privileged workload forbidden')
            resources = container['resources']
            require(all(k in resources for k in ('requests', 'limits')), 'Resource bounds required')
            if container in pod['containers']:
                cpu_request += quantity(resources['requests']['cpu'])
                memory_limit += quantity(resources['limits']['memory'], memory=True)
                require(all(probe in container for probe in ('startupProbe', 'readinessProbe', 'livenessProbe')), 'Three probe roles required')
            for env in container.get('env', []):
                ref = env.get('valueFrom', {}).get('secretKeyRef')
                if ref:
                    require(ref['name'] == 'warexis-runtime' and ref['key'] in SECRET_KEYS, 'Unexpected secret reference')
                    refs.add(ref['key'])
                require(not (env['name'] in SECRET_KEYS and 'value' in env), 'Literal credential in manifest')
            for source in container.get('envFrom', []):
                require(source.get('configMapRef', {}).get('name') in configmaps, 'envFrom ConfigMap does not resolve')
        if name in APPLICATIONS:
            container = pod['containers'][0]
            require(container['image'] == 'warexis/' + name + ':p11-14fa97e6' and container['imagePullPolicy'] == 'Never', 'Application image must be loaded locally')
            require(container['ports'][0]['containerPort'] == APPLICATIONS[name], 'Application port mismatch')
            require(workload['spec']['strategy']['rollingUpdate'] == {'maxUnavailable': 0, 'maxSurge': 1}, 'Readiness-aware rolling policy required')
            require(workload['spec']['minReadySeconds'] >= 5, 'Rollout stability interval required')
            require(pod['securityContext']['runAsNonRoot'] is True, 'Application images must run as non-root')
            require(container['securityContext']['readOnlyRootFilesystem'] is True, 'Application filesystem must be read-only')
            require(container['livenessProbe'].get('tcpSocket', {}).get('port') == 'http', 'Liveness must not depend on databases/Kafka')
            expected_health = '/healthz' if name == 'frontend' else '/actuator/health'
            for probe in ('startupProbe', 'readinessProbe'):
                require(container[probe]['httpGet']['path'] == expected_health, 'Use actual unauthenticated health path')
            if name != 'frontend':
                env = {item['name']: item for item in container['env']}
                require(env[name.upper() + '_DB_URL']['value'] ==
                        f'jdbc:postgresql://{name}-postgres:5432/warexis_{name}', 'Database ownership/DNS mismatch')
        if workload['kind'] == 'StatefulSet':
            claims = workload['spec']['volumeClaimTemplates']
            require(len(claims) == 1 and claims[0]['spec']['storageClassName'] == 'standard', 'Each stateful workload needs its own standard PVC')
            require(claims[0]['spec']['accessModes'] == ['ReadWriteOnce'], 'Single-node persistent access required')
    require(refs == SECRET_KEYS, 'Missing expected runtime secret references')
    require(cpu_request <= 2300 and memory_limit <= 8 * 1024 ** 3, 'Local steady-state resource budget exceeded')
    services = {name: d for (kind, name), d in objects.items() if kind == 'Service'}
    require({'order', 'inventory', 'fulfillment', 'frontend', 'kafka', 'keycloak', 'order-postgres',
             'inventory-postgres', 'fulfillment-postgres', 'keycloak-postgres'} <= services.keys(), 'Required Service missing')
    for name, service in services.items():
        spec = service['spec']
        expected = 'NodePort' if name in ('frontend', 'keycloak') else 'ClusterIP'
        require(spec.get('type', 'ClusterIP') == expected, 'Only frontend and identity may be externally routed')
        require(not spec.get('externalIPs'), 'External service IPs forbidden')
        if name in ('frontend', 'keycloak'):
            require(len(spec['ports']) == 1 and spec['ports'][0]['nodePort'] == (30173 if name == 'frontend' else 30080), 'Unexpected NodePort exposure')
    kind = yaml.safe_load((base / 'kind.yaml').read_text())
    require(kind['networking']['apiServerAddress'] == '127.0.0.1' and len(kind['nodes']) == 1, 'Single-node loopback cluster required')
    mappings = kind['nodes'][0]['extraPortMappings']
    require({(m['containerPort'], m['hostPort']) for m in mappings} == {(30173, 5173), (30080, 18080)}, 'Unexpected host port mappings')
    require(all(m.get('listenAddress') == '127.0.0.1' for m in mappings), 'Host endpoints must be loopback-only')
    return {'status': 'PASS', 'resources': len(documents), 'workloads': len(workloads),
            'cpuRequestMillicores': cpu_request, 'memoryLimitMiB': memory_limit // 1024 ** 2,
            'secretValuesRendered': False, 'deployGeneratedConfigMaps': ['warexis-realm']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, default=BASE)
    tools = Path(os.environ.get('XDG_DATA_HOME', Path.home() / '.local/share')) / 'warexis/kubernetes/bin'
    parser.add_argument('--kubectl', default=str(tools / 'kubectl'))
    parser.add_argument('--server-dry-run', action='store_true')
    parser.add_argument('--kubeconfig')
    args = parser.parse_args()
    try:
        rendered = subprocess.run([args.kubectl, 'kustomize', str(args.directory)],
                                  capture_output=True, text=True, timeout=30, check=False)
        require(rendered.returncode == 0, 'Kustomize rendering failed')
        result = validate([d for d in yaml.safe_load_all(rendered.stdout) if d], args.directory)
        if args.server_dry_run:
            require(bool(args.kubeconfig), 'Server dry-run requires the dedicated --kubeconfig')
            checked = subprocess.run([args.kubectl, '--kubeconfig', args.kubeconfig, '--context', 'kind-warexis-phase11',
                                      'apply', '--dry-run=server', '-k', str(args.directory)],
                                     capture_output=True, text=True, timeout=90, check=False)
            require(checked.returncode == 0, 'Kubernetes server dry-run failed')
            result['serverDryRun'] = 'PASS'
        print(json.dumps(result))
    except (ValueError, KeyError, OSError, subprocess.TimeoutExpired, yaml.YAMLError) as error:
        print('Validation failed: ' + (str(error) if isinstance(error, ValueError) else type(error).__name__), file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
