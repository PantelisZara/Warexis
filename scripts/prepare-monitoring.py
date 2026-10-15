#!/usr/bin/env python3
"""Prepare secret-free configuration and private local monitoring storage (Linux).

The runtime and collector secret must be outside every Git checkout. This creates
new runtime directories only; reuse monitoring.env to restart an existing stack.
"""
import argparse
import json
import os
from pathlib import Path
import re
import secrets
import stat
from urllib.parse import urlsplit

SERVICES = ('order', 'inventory', 'fulfillment')


def external_path(raw):
    path = Path(raw).expanduser().absolute()
    for item in (path, *path.parents):
        if item.is_symlink():
            raise ValueError('Runtime and secret paths must not contain symbolic links')
        if (item / '.git').exists():
            raise ValueError('Runtime and secret paths must be outside Git checkouts')
    if any(character in str(path) for character in "'\r\n\x00"):
        raise ValueError('Runtime and secret paths cannot contain quotes or newlines')
    return path


def loopback_url(raw, issuer=False):
    parsed = urlsplit(raw)
    try:
        valid_port = parsed.port is not None and 0 < parsed.port < 65536
    except ValueError:
        valid_port = False
    expected_path = bool(re.fullmatch(r'/realms/[A-Za-z0-9._-]+', parsed.path)) if issuer else parsed.path in ('', '/')
    if not (parsed.scheme == 'http' and parsed.hostname == '127.0.0.1' and valid_port
            and expected_path and not parsed.username and not parsed.password
            and not parsed.query and not parsed.fragment):
        raise ValueError('Use an explicit HTTP 127.0.0.1 port and a realm issuer or root service URL')
    return raw.rstrip('/')


def private_file(path, content):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        stream.write(content)


def prepare(args):
    if os.name != 'posix' or not Path('/proc/sys/kernel/ostype').read_text().startswith('Linux'):
        raise ValueError('This loopback host-network compose stack requires Linux')
    runtime = external_path(args.runtime_dir)
    secret = external_path(args.collector_secret_file)
    info = secret.stat()
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600 or info.st_uid != os.getuid() or info.st_nlink != 1:
        raise ValueError('Collector secret must be an owned regular mode0600 file with one link')
    if not 0 < info.st_size <= 4096:
        raise ValueError('Collector secret file must contain a nonempty bounded client secret')
    issuer = loopback_url(args.issuer, issuer=True)
    urls = {service: loopback_url(getattr(args, service + '_url')) for service in SERVICES}
    if not re.fullmatch(r'[A-Za-z0-9._-]{1,128}', args.collector_client_id):
        raise ValueError('Collector client ID must contain only bounded identifier characters')
    if not all(1024 <= port <= 65535 for port in (args.prometheus_port, args.grafana_port)) or args.prometheus_port == args.grafana_port:
        raise ValueError('Monitoring ports must be distinct unprivileged port numbers')
    if runtime.exists():
        raise ValueError('Runtime directory already exists; reuse its monitoring.env or choose a fresh directory')
    if not runtime.parent.is_dir():
        raise ValueError('Runtime parent must already exist')
    config = {
        'global': {'scrape_interval': '15s', 'scrape_timeout': '10s', 'evaluation_interval': '15s'},
        'scrape_configs': [
            {'job_name': 'warexis-' + service, 'metrics_path': '/actuator/prometheus', 'honor_labels': True,
             'static_configs': [{'targets': [urlsplit(urls[service]).netloc], 'labels': {'service': service}}],
             'oauth2': {'client_id': args.collector_client_id, 'client_secret_file': '/run/secrets/collector-secret',
                        'token_url': issuer + '/protocol/openid-connect/token'}} for service in SERVICES]
    }
    runtime.mkdir(mode=0o700)
    for name in ('prometheus-data', 'grafana-data'):
        (runtime / name).mkdir(mode=0o700)
    private_file(runtime / 'prometheus.json', json.dumps(config, indent=2) + '\n')
    private_file(runtime / 'grafana-admin-password', secrets.token_urlsafe(36) + '\n')
    environment = {
        'WAREXIS_MONITORING_RUNTIME': str(runtime), 'WAREXIS_COLLECTOR_SECRET_FILE': str(secret),
        'WAREXIS_MONITORING_UID': str(os.getuid()), 'WAREXIS_MONITORING_GID': str(os.getgid()),
        'WAREXIS_PROMETHEUS_PORT': str(args.prometheus_port), 'WAREXIS_GRAFANA_PORT': str(args.grafana_port)}
    private_file(runtime / 'monitoring.env', ''.join(f"{key}='{value}'\n" for key, value in environment.items()))
    print('Prepared private monitoring runtime. Start with compose.monitoring.yaml and its monitoring.env; no secret values are printed.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-dir', required=True)
    for service in SERVICES:
        parser.add_argument('--' + service + '-url', required=True)
    parser.add_argument('--issuer', required=True)
    parser.add_argument('--collector-client-id', default='warexis-metrics-collector')
    parser.add_argument('--collector-secret-file', required=True)
    parser.add_argument('--prometheus-port', type=int, default=9090)
    parser.add_argument('--grafana-port', type=int, default=3000)
    args = parser.parse_args()
    try:
        prepare(args)
    except (ValueError, OSError) as error:
        parser.exit(2, f'Monitoring preparation failed: {error}\n')


if __name__ == '__main__':
    main()
