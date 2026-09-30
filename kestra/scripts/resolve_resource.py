"""
Busca en el portal de Datos Abiertos (API CKAN) el CSV del mes pedido.

Las URLs de descarga contienen IDs aleatorios del portal, así que no se pueden
construir: se consulta el dataset y se elige el recurso cuyo nombre termina en
_YYYYMM (p. ej. "ECU911_Base de Emergencias_202401") y cuya URL es un .csv.
Las etiquetas de formato del portal no son confiables (hay CSV marcados como XLSX),
por eso se decide por la extensión de la URL.

Salida para Kestra (outputs.<task>.vars.*): url, resource_name, resource_id.
Código de salida 2 = el mes todavía no está publicado.
"""
import argparse
import json
import re
import sys

import requests

API = "https://www.datosabiertos.gob.ec/api/3/action/package_show"
DATASET = "base-de-emergencias"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", required=True, help="YYYY-MM")
    args = ap.parse_args()
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", args.period):
        sys.exit(f"Período inválido: {args.period}")
    compact = args.period.replace("-", "")

    resp = requests.get(API, params={"id": DATASET}, timeout=60)
    resp.raise_for_status()
    resources = resp.json()["result"]["resources"]

    candidates = [
        r for r in resources
        if re.search(rf"_{compact}(\D|$)", r.get("name") or "")
        and (r.get("url") or "").lower().endswith(".csv")
    ]
    if not candidates:
        print(f"El mes {args.period} no está publicado como CSV en el portal.", file=sys.stderr)
        sys.exit(2)
    # Si hubiera más de una versión, se usa la última modificada.
    best = max(candidates, key=lambda r: r.get("last_modified") or r.get("created") or "")
    if len(candidates) > 1:
        print(f"AVISO: {len(candidates)} recursos para {args.period}; se usa {best['name']}")

    print(f"{args.period} -> {best['name']} | {best['url']}")
    outputs = {"url": best["url"], "resource_name": best["name"].strip(), "resource_id": best["id"]}
    # Formato que Kestra interpreta como outputs de la tarea
    print("::" + json.dumps({"outputs": outputs}) + "::")


if __name__ == "__main__":
    main()
