#!/usr/bin/env bash
# Open an interactive SQL session against a run's DuckDB database.
#
#   local_run/duckdb_shell.sh                 # upload_20260127
#   local_run/duckdb_shell.sh upload_20260127
#
# Either way you get a plain SQL prompt: type statements the way you would in
# `mysql`, ending each one with `;`. Uses the `duckdb` CLI if installed
# (brew install duckdb), otherwise a Python prompt that behaves the same way.
set -euo pipefail
UPLOAD_DB="${1:-upload_20260127}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DB="${ROOT}/output/duckdb/${UPLOAD_DB}.duckdb"

if [ ! -f "${DB}" ]; then
  echo "No DuckDB database at ${DB}" >&2
  echo "Available:" >&2; ls -1 "${ROOT}/output/duckdb/" >&2 || true
  exit 1
fi

if command -v duckdb >/dev/null 2>&1; then
  echo "opening ${DB} with the duckdb CLI (.tables, .schema, .quit)"
  exec duckdb "${DB}"
fi

echo "duckdb CLI not found (brew install duckdb); using the Python SQL prompt."
exec "${ROOT}/.venv/bin/python" "${ROOT}/local_run/duckdb_repl.py" "${DB}"
