#!/usr/bin/env bash
# Tubería completa por CLI (sin la UI de Kestra), con los mismos scripts que usa Kestra:
#   ingesta mes a mes -> dbt build (Silver/Gold + tests) -> Spark (OBT)
# Uso: docker compose run --rm cli [DESDE YYYY-MM] [HASTA YYYY-MM]
set -euo pipefail

START="${1:-$START_PERIOD}"
END="${2:-$END_PERIOD}"
S=/pset2/kestra/scripts
TMP=$(mktemp -d)
INGESTION_ID="cli-$(date +%Y%m%dT%H%M%S)"

kestra_output() {  # extrae outputs.<clave> de la línea ::{"outputs": {...}}::
  sed -n 's/^::\(.*\)::$/\1/p' | "$PIPELINE_PY" -c "import json,sys; v=json.load(sys.stdin)['outputs']['$1']; print(' '.join(v) if isinstance(v, list) else v)"
}

echo "==> [1/3] Ingesta ${START} .. ${END} -> BRONZE"
for period in $("$PIPELINE_PY" "$S/month_range.py" "$START" "$END" | kestra_output months); do
  resolved=$("$PIPELINE_PY" "$S/resolve_resource.py" --period "$period")
  url=$(echo "$resolved" | kestra_output url)
  name=$(echo "$resolved" | kestra_output resource_name)
  curl -fsSL --retry 3 --retry-delay 10 -o "$TMP/source.csv" "$url"
  (cd "$S" && "$PIPELINE_PY" load_bronze.py --period "$period" --file "$TMP/source.csv" \
      --url "$url" --resource-name "$name" --ingestion-id "$INGESTION_ID")
done

echo "==> [2/3] dbt: Bronze -> Silver -> Gold + tests"
cd /pset2/dbt
"$DBT_BIN" deps --quiet
"$DBT_BIN" build --fail-fast

echo "==> [3/3] Spark: Gold -> OBT"
bash /pset2/spark/submit_obt.sh
echo "==> Tubería completada"
