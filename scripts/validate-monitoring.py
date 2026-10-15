#!/usr/bin/env python3
"""Check provisioned dashboards; optionally parse every query with pinned promtool.

--runtime-dir also validates the generated scrape configuration and Compose file.
Docker is used only when --promtool or --runtime-dir is explicitly selected.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
IMAGE = 'prom/prometheus:v3.15.0'
METRICS = {
    'up', 'http_server_requests_seconds_count', 'http_server_requests_seconds_bucket',
    'jvm_memory_used_bytes', 'process_cpu_usage', 'hikaricp_connections_active',
    'hikaricp_connections_pending', 'process_uptime_seconds', 'warexis_outbox_pending',
    'warexis_outbox_oldest_age_seconds', 'warexis_outbox_snapshot_available',
    'warexis_outbox_publications_total', 'warexis_outbox_retries_total',
    'warexis_messaging_deliveries_total', 'warexis_messaging_failures_total',
    'warexis_messaging_retries_total', 'warexis_messaging_pauses_total',
}


def validate_dashboards():
    rules = []
    dashboards = sorted((ROOT / 'infrastructure/monitoring/grafana/dashboards').glob('*.json'))
    assert len(dashboards) == 3, 'Expected overview, services and messaging dashboards'
    seen = set()
    for path in dashboards:
        dashboard = json.loads(path.read_text())
        assert dashboard['uid'] not in seen and dashboard['editable'] is False
        seen.add(dashboard['uid'])
        assert 'unknown' in dashboard['description'], 'Explain absent data'
        assert dashboard['templating']['list'][0]['name'] == 'service'
        assert dashboard['templating']['list'][0]['query'] == 'label_values(up{job=~"warexis-.*"}, service)'
        for panel in dashboard['panels']:
            assert panel['description'] is not None
            assert panel['datasource']['uid'] == 'warexis-prometheus'
            for target in panel['targets']:
                expression = target['expr']
                assert not re.search(r'\bor\s+(?:vector\s*\(\s*0|0\b)', expression), 'Do not hide missing series as zero'
                assert not re.search(r'(eventId|orderId|reservationId|sku|email|token|exception)', expression, re.I)
                names = set(re.findall(r'\b([a-z][a-z0-9_]+)\s*\{', expression))
                assert names and names <= METRICS, f'Uncontracted metric in {path.name}'
                assert '$service' in expression
                rules.append({'record': f"warexis_dashboard_check_{len(rules)}", 'expr': expression.replace('$service', '.*')})
    return rules


def check_grafana(runtime, environment):
    """Prove the provisioned bundled plugin executes a real Prometheus query.

    This API check is a regression gate, not browser rendering acceptance.
    """
    origin = 'http://127.0.0.1:' + environment['WAREXIS_GRAFANA_PORT']
    password = (runtime / 'grafana-admin-password').read_text().strip()
    authorization = 'Basic ' + base64.b64encode(('warexis-admin:' + password).encode()).decode()

    def request(path, authenticated=True, body=None):
        headers = {'Authorization': authorization} if authenticated else {}
        if body is not None:
            headers['Content-Type'] = 'application/json'
        payload = json.dumps(body).encode() if body is not None else None
        with urllib.request.urlopen(urllib.request.Request(origin + path, headers=headers, data=payload), timeout=10) as response:
            return json.load(response)

    dashboards = request('/api/search?type=dash-db')
    expected = {'warexis-overview', 'warexis-services', 'warexis-messaging'}
    assert {item['uid'] for item in dashboards} >= expected, 'Missing provisioned dashboards'
    for uid in expected:
        detail = request('/api/dashboards/uid/' + uid)
        assert detail['meta']['provisioned'] is True and detail['dashboard']['editable'] is False
    now = int(time.time() * 1000)
    result = request('/api/ds/query', body={
        'from': str(now - 60000), 'to': str(now), 'queries': [{
            'refId': 'A', 'datasource': {'uid': 'warexis-prometheus', 'type': 'prometheus'},
            'expr': 'up{job=~"warexis-.*"}', 'instant': True, 'range': False, 'format': 'time_series'}]})
    assert result['results']['A']['status'] == 200, 'Provisioned Prometheus plugin query failed'
    observed = {field.get('labels', {}).get('service')
                for frame in result['results']['A']['frames']
                for field in frame['schema']['fields'] if field.get('type') == 'number'}
    assert observed == {'order', 'inventory', 'fulfillment'}, 'Expected all three configured scrape observations; wait for their first scrape'
    try:
        request('/api/search?type=dash-db', authenticated=False)
    except urllib.error.HTTPError as error:
        assert error.code == 401, 'Anonymous Grafana API must require authentication'
    else:
        raise AssertionError('Anonymous Grafana API unexpectedly accessible')
    print('PASS: three provisioned read-only dashboards, real Grafana-to-Prometheus up query, anonymous API401 (not browser QA)')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--promtool', action='store_true')
    parser.add_argument('--runtime-dir', type=Path)
    parser.add_argument('--grafana-api-check', action='store_true', help='Require a running runtime and check real datasource queries (not browser QA)')
    args = parser.parse_args()
    if args.grafana_api_check and not args.runtime_dir:
        parser.error('--grafana-api-check requires --runtime-dir')
    rules = validate_dashboards()
    if args.promtool:
        with tempfile.TemporaryDirectory(prefix='warexis-monitoring-query-check-') as directory:
            path = Path(directory) / 'rules.json'
            path.write_text(json.dumps({'groups': [{'name': 'dashboard-syntax', 'rules': rules}]}))
            path.chmod(0o600)
            subprocess.run(['docker', 'run', '--rm', '--network', 'none', '--user', f'{os.getuid()}:{os.getgid()}',
                            '--entrypoint', '/bin/promtool', '-v', f'{path}:/rules.json:ro', IMAGE,
                            'check', 'rules', '/rules.json'], check=True)
    if args.runtime_dir:
        runtime = args.runtime_dir.resolve(strict=True)
        environment = {}
        for line in (runtime / 'monitoring.env').read_text().splitlines():
            key, value = line.split('=', 1)
            environment[key] = value.strip("'")
        assert environment['WAREXIS_MONITORING_RUNTIME'] == str(runtime)
        subprocess.run(['docker', 'compose', '--env-file', str(runtime / 'monitoring.env'), '-f',
                        str(ROOT / 'compose.monitoring.yaml'), 'config', '--quiet'], check=True)
        subprocess.run(['docker', 'run', '--rm', '--network', 'none', '--user', f'{os.getuid()}:{os.getgid()}',
                        '--entrypoint', '/bin/promtool', '-v', f'{runtime / "prometheus.json"}:/etc/prometheus/prometheus.json:ro',
                        '-v', f'{environment["WAREXIS_COLLECTOR_SECRET_FILE"]}:/run/secrets/collector-secret:ro',
                        IMAGE, 'check', 'config', '/etc/prometheus/prometheus.json'], check=True)
        if args.grafana_api_check:
            check_grafana(runtime, environment)
    print(f'PASS: three provisioned dashboards; {len(rules)} contracted queries' + ('; promtool syntax checked' if args.promtool else ' (structural checks only)'))


if __name__ == '__main__':
    main()
