#!/usr/bin/env bash
# Run the data-producing tasks of the granted_patent_updater DAG locally,
# landing everything as parquet via DuckDB under output/.
#
# Usage:
#   local_run/run_granted_patent_updater.sh                          # default run id
#   local_run/run_granted_patent_updater.sh 2026-01-20T09:00:00+00:00
#
# Each step is a real Airflow task execution (`airflow tasks test`) against the
# given logical date -- no scheduler, webserver, Celery or docker involved.
set -euo pipefail

LOGICAL_DATE="${1:-2026-01-20T09:00:00+00:00}"
DAG_ID="granted_patent_updater"

cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
source local_run/env.sh

# Tasks in DAG dependency order. The remaining ~24 tasks in this DAG are MySQL /
# Elasticsearch / AWS operations against the production databases and are not
# part of the local run -- see local_run/README.md.
TASKS=(
  download_xml         # USPTO bulk API  -> output/data/<db>/raw_data/
  process_xml          # wrap as valid   -> output/data/<db>/clean_data/
  parse_xml            # XML -> 26 TSVs  -> output/data/<db>/parsed_data/
  upload_current       # TSVs            -> DuckDB + parquet
  parse_xml_to_sql     # XML -> cpc/gi   -> DuckDB + parquet
  parse_text_data      # claims/descs    -> DuckDB + parquet
  withdrawn_processor  # USPTO withdrawn -> DuckDB + parquet
)

echo "=============================================================="
echo " DAG          : ${DAG_ID}"
echo " logical date : ${LOGICAL_DATE}"
echo " tasks        : ${#TASKS[@]}"
echo "=============================================================="

for task in "${TASKS[@]}"; do
  echo
  echo "--------------------------------------------------------------"
  echo ">>> ${task}"
  echo "--------------------------------------------------------------"
  start=$(date +%s)
  if airflow tasks test "${DAG_ID}" "${task}" "${LOGICAL_DATE}" 2>&1 \
       | grep -viE "font_manager|SyntaxWarning|if prompt" \
       | tail -n 40; then
    echo "    ${task} finished in $(( $(date +%s) - start ))s"
  else
    echo "!!! ${task} FAILED" >&2
    exit 1
  fi
done

echo
echo "=============================================================="
echo " done. parquet output:"
ls -la output/parquet/upload_*/ | tail -n 5
echo "=============================================================="
