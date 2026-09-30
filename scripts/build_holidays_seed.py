"""
Genera dbt/seeds/feriados_ecuador.csv con los feriados nacionales de Ecuador.

Fuente: librería `holidays` (python-holidays), que implementa el Código del Trabajo
y las reglas de traslado de la Ley Orgánica Reformatoria (2016) para Ecuador.
Limitación: no incluye feriados locales (fundaciones cantonales) ni los "puentes"
decretados de forma ad hoc por el Ejecutivo.

Uso:  pip install holidays && python scripts/build_holidays_seed.py
"""
import csv
from pathlib import Path

import holidays

YEARS = range(2023, 2027)
OUT = Path(__file__).resolve().parents[1] / "dbt" / "seeds" / "feriados_ecuador.csv"

rows = sorted(holidays.country_holidays("EC", years=YEARS, language="es").items())
with open(OUT, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["fecha", "nombre_feriado"])
    for fecha, nombre in rows:
        w.writerow([fecha.isoformat(), nombre])
print(f"{OUT.name}: {len(rows)} feriados {min(YEARS)}-{max(YEARS)} (holidays {holidays.__version__})")
