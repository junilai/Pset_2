"""
Carga un CSV mensual del ECU 911 en BRONZE.EMERGENCIAS (Snowflake), de forma idempotente.

1. Detecta la codificación del archivo: la mayoría viene en UTF-8 con BOM, pero
   algunos meses vienen en Latin-1/CP1252 (p. ej. "CAÑAR" aparece corrupto si se
   lee como UTF-8). Se transcodifica a UTF-8 sin alterar el contenido.
2. Cuenta las filas de datos del archivo (para validar la carga completa).
3. PUT al stage interno y, en UNA transacción: DELETE de las filas previas de ese
   período + COPY INTO. Re-ejecutar un mes nunca duplica datos.
4. Verifica que las filas cargadas == filas del archivo y registra en BRONZE.LOAD_AUDIT.

Bronze conserva el dato original: todas las columnas como texto, sin limpieza.
"""
import argparse
import json
import sys
import tempfile
from pathlib import Path

from sf import connect

SCHEMA = "BRONZE"
TABLE = "EMERGENCIAS"
STAGE = "ECU911_STAGE"
SOURCE_COLUMNS = ["FECHA", "PROVINCIA", "CANTON", "COD_PARROQUIA", "PARROQUIA", "SERVICIO", "SUBTIPO"]


def decode(raw: bytes):
    for enc in ("utf-8-sig", "cp1252"):
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1"), "latin-1"


def bootstrap(cur, db):
    cols = ",\n  ".join(f"{c} VARCHAR" for c in SOURCE_COLUMNS)
    for sql in [
        f"create schema if not exists {db}.{SCHEMA}",
        f"use schema {db}.{SCHEMA}",
        """create file format if not exists ECU911_CSV
             type = csv field_delimiter = ';' skip_header = 1
             field_optionally_enclosed_by = '"' encoding = 'UTF8'
             empty_field_as_null = true trim_space = false skip_blank_lines = true
             error_on_column_count_mismatch = false""",
        f"create stage if not exists {STAGE} file_format = ECU911_CSV",
        f"""create table if not exists {TABLE} (
              {cols},
              _SOURCE_FILE VARCHAR,
              _SOURCE_URL VARCHAR,
              _SOURCE_PERIOD DATE,
              _SOURCE_ROW_NUMBER NUMBER,
              _SOURCE_ENCODING VARCHAR,
              _LOADED_AT TIMESTAMP_LTZ,
              _INGESTION_ID VARCHAR)""",
        """create table if not exists LOAD_AUDIT (
              source_period DATE, source_file VARCHAR, source_url VARCHAR,
              source_encoding VARCHAR, rows_in_file NUMBER, rows_loaded NUMBER,
              ingestion_id VARCHAR, loaded_at TIMESTAMP_LTZ, status VARCHAR)""",
    ]:
        cur.execute(sql)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", required=True, help="YYYY-MM")
    ap.add_argument("--file", required=True, help="CSV descargado")
    ap.add_argument("--url", required=True)
    ap.add_argument("--resource-name", default="")
    ap.add_argument("--ingestion-id", required=True)
    args = ap.parse_args()

    period_date = f"{args.period}-01"
    file_name = f"ecu911_{args.period.replace('-', '')}.csv"
    text, encoding = decode(Path(args.file).read_bytes())
    # Filas de datos = líneas no vacías menos el encabezado (misma regla que skip_blank_lines)
    rows_in_file = sum(1 for line in text.splitlines() if line.strip()) - 1
    print(f"{args.resource_name or args.url}: codificación {encoding}, {rows_in_file:,} filas de datos")

    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / file_name
        local.write_text(text, encoding="utf-8", newline="")

        conn = connect()
        cur = conn.cursor()
        db = conn.database
        bootstrap(cur, db)
        cur.execute(f"put 'file://{local.as_posix()}' @{STAGE}/{args.period} auto_compress = true overwrite = true")

        select = ", ".join(f"${i + 1}" for i in range(len(SOURCE_COLUMNS)))
        copy_sql = f"""
            copy into {TABLE} ({", ".join(SOURCE_COLUMNS)}, _SOURCE_FILE, _SOURCE_URL, _SOURCE_PERIOD,
                               _SOURCE_ROW_NUMBER, _SOURCE_ENCODING, _LOADED_AT, _INGESTION_ID)
            from (
                select {select}, %(file)s, %(url)s, %(period)s::date,
                       metadata$file_row_number, %(enc)s, current_timestamp(), %(ing)s
                from @{STAGE}/{args.period}/{file_name}.gz
            )
            file_format = (format_name = 'ECU911_CSV')
            force = true
            on_error = abort_statement
        """
        params = {"file": args.resource_name or file_name, "url": args.url, "period": period_date,
                  "enc": encoding, "ing": args.ingestion_id}
        cur.execute("begin")
        try:
            cur.execute(f"delete from {TABLE} where _SOURCE_PERIOD = %s", (period_date,))
            replaced = cur.rowcount
            cur.execute(copy_sql, params)
            rows_loaded = sum(r[3] for r in cur.fetchall())
            if rows_loaded != rows_in_file:
                raise RuntimeError(f"Carga incompleta: {rows_loaded:,} filas cargadas vs {rows_in_file:,} en el archivo")
            cur.execute(
                "insert into LOAD_AUDIT select %s::date, %s, %s, %s, %s, %s, %s, current_timestamp(), 'LOADED'",
                (period_date, params["file"], args.url, encoding, rows_in_file, rows_loaded, args.ingestion_id))
            cur.execute("commit")
        except Exception:
            cur.execute("rollback")
            raise
        finally:
            cur.execute(f"remove @{STAGE}/{args.period}")
        conn.close()

    print(f"BRONZE.{TABLE} {args.period}: {rows_loaded:,} filas cargadas ({replaced:,} filas previas reemplazadas)")
    print("::" + json.dumps({"outputs": {"rows_loaded": rows_loaded, "encoding": encoding}}) + "::")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
