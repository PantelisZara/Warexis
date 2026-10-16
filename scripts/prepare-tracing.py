#!/usr/bin/env python3
"""Prepare a fresh private Tempo runtime outside Git (Linux)."""
import argparse
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / 'infrastructure/tracing/tempo.yaml'


def external_path(raw):
    path = Path(raw).expanduser().absolute()
    # Check the original spelling before normalizing parent components.
    for item in (path, *path.parents):
        if item.is_symlink():
            raise ValueError('Runtime paths must not contain symbolic links')
    path = Path(os.path.abspath(path))
    for item in (path, *path.parents):
        if item.is_symlink():
            raise ValueError('Runtime paths must not contain symbolic links')
        if (item / '.git').exists():
            raise ValueError('Runtime paths must be outside Git checkouts')
    if any(character in str(path) for character in "'\r\n\x00"):
        raise ValueError('Runtime paths cannot contain quotes or newlines')
    return path


def private_file(path, content):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        stream.write(content)
        os.fchmod(stream.fileno(), 0o600)


def prepare(args):
    if os.name != 'posix' or not Path('/proc/sys/kernel/ostype').read_text().startswith('Linux'):
        raise ValueError('This local Compose stack requires Linux')
    runtime = external_path(args.runtime_dir)
    ports = (args.query_port, args.otlp_http_port)
    if not all(1024 <= port <= 65535 for port in ports) or len(set(ports)) != 2:
        raise ValueError('Tracing ports must be distinct unprivileged port numbers')
    if runtime.exists():
        raise ValueError('Runtime directory already exists; reuse tracing.env or choose a fresh directory')
    if not runtime.parent.is_dir():
        raise ValueError('Runtime parent must already exist')
    config = CONFIG.read_text()
    runtime.mkdir(mode=0o700)
    runtime.chmod(0o700)
    data = runtime / 'tempo-data'
    data.mkdir(mode=0o700)
    data.chmod(0o700)
    private_file(runtime / 'tempo.yaml', config)
    # The optional overlay replaces only Grafana's provisioning mount. Preserve
    # the accepted Prometheus datasource and dashboard provider byte-for-byte.
    provisioning = runtime / 'grafana-provisioning'
    provisioning.mkdir(mode=0o700)
    for relative in ('dashboards/warexis.yaml', 'datasources/prometheus.yaml'):
        destination = provisioning / relative
        destination.parent.mkdir(mode=0o700, exist_ok=True)
        private_file(destination, (ROOT / 'infrastructure/monitoring/grafana/provisioning' / relative).read_text())
    private_file(provisioning / 'datasources/tempo.yaml',
                 (ROOT / 'infrastructure/tracing/grafana/tempo.yaml').read_text())
    environment = {
        'WAREXIS_TRACING_RUNTIME': str(runtime),
        'WAREXIS_TRACING_UID': str(os.getuid()), 'WAREXIS_TRACING_GID': str(os.getgid()),
        'WAREXIS_TEMPO_QUERY_PORT': str(args.query_port),
        'WAREXIS_TEMPO_OTLP_HTTP_PORT': str(args.otlp_http_port)}
    private_file(runtime / 'tracing.env', ''.join(f"{key}='{value}'\n" for key, value in environment.items()))
    print('Prepared private tracing runtime. Start compose.tracing.yaml with its tracing.env.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-dir', required=True)
    parser.add_argument('--query-port', type=int, default=3200)
    parser.add_argument('--otlp-http-port', type=int, default=4318)
    args = parser.parse_args()
    try:
        prepare(args)
    except (ValueError, OSError) as error:
        parser.exit(2, f'Tracing preparation failed: {error}\n')


if __name__ == '__main__':
    main()
