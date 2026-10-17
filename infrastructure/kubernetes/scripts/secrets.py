#!/usr/bin/env python3
"""Generate a private local Kubernetes Secret input; never print credential values."""
import argparse
import os
from pathlib import Path
import base64
import stat

KEYS = ("ORDER_DB_PASSWORD", "INVENTORY_DB_PASSWORD", "FULFILLMENT_DB_PASSWORD",
        "KEYCLOAK_DB_PASSWORD", "KC_BOOTSTRAP_ADMIN_CLIENT_SECRET")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Private directory outside the Git checkout")
    args = parser.parse_args()
    directory = args.directory.expanduser().resolve()
    repo = Path(__file__).resolve().parents[3]
    if directory == repo or repo in directory.parents:
        parser.error("Secret inputs must be outside the repository")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.is_symlink() or stat.S_IMODE(directory.stat().st_mode) & 0o077:
        parser.error("Secret input directory must have mode 0700")
    target = directory / "runtime.env"
    # Reuse persistent credentials safely; rotating DB passwords independently breaks existing PVCs.
    if target.exists():
        if target.is_symlink() or stat.S_IMODE(target.stat().st_mode) & 0o077:
            parser.error("Existing input must be a private regular file")
        lines = target.read_text().splitlines()
        if set(line.split("=", 1)[0] for line in lines) != set(KEYS):
            parser.error("Existing input has unexpected keys; inspect it privately")
        if not all("=" in line and len(line.split("=", 1)[1]) >= 32 for line in lines):
            parser.error("Existing input is incomplete")
        print("Private runtime Secret input reused; values suppressed")
        return
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write("".join(key + "=" + base64.urlsafe_b64encode(os.urandom(36)).decode() + "\n" for key in KEYS))
    print("Private runtime Secret input generated; values suppressed")


if __name__ == "__main__":
    main()
