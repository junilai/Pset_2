"""Importa los tres flujos usando el usuario de la interfaz de Kestra."""

import argparse
import base64
import getpass
import os
from pathlib import Path
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.environ.get("KESTRA_URL", "http://localhost:8080"))
    parser.add_argument("--user", default=os.environ.get("KESTRA_USER"))
    args = parser.parse_args()
    username = args.user or input("Usuario de Kestra (correo): ").strip()
    password = os.environ.get("KESTRA_PASSWORD") or getpass.getpass("Contraseña de Kestra: ")
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    flows_dir = Path(__file__).resolve().parents[1] / "kestra"

    for name in ("load_raw", "load_raw_inec", "pipeline"):
        path = flows_dir / f"{name}.yml"
        boundary = uuid.uuid4().hex
        body = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="fileUpload"; filename="{path.name}"\r\n'
            "Content-Type: application/x-yaml\r\n\r\n"
        ).encode() + path.read_bytes() + f"\r\n--{boundary}--\r\n".encode()
        request = urllib.request.Request(
            f"{args.url.rstrip('/')}/api/v1/main/flows/import",
            data=body,
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                raise SystemExit("Kestra rechazó el usuario o la contraseña (HTTP 401).") from None
            raise SystemExit(f"No se pudo importar {name}: HTTP {exc.code}. {exc.read().decode()}") from None
        except urllib.error.URLError as exc:
            raise SystemExit(f"No se pudo conectar a Kestra: {exc.reason}") from None
        print(f"Importado: pset2.{name}")


if __name__ == "__main__":
    main()
