"""Emite a Kestra la lista de meses YYYY-MM entre dos períodos (inclusivo)."""
import json
import sys


def month_range(start, end):
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    while (y, m) <= (ey, em):
        yield f"{y:04d}-{m:02d}"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


if __name__ == "__main__":
    months = list(month_range(sys.argv[1], sys.argv[2]))
    if not months:
        sys.exit(f"Rango vacío: {sys.argv[1]} .. {sys.argv[2]}")
    print(f"{len(months)} meses: {months[0]} .. {months[-1]}")
    print("::" + json.dumps({"outputs": {"months": months}}) + "::")
