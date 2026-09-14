#!/usr/bin/env bash
# Drop the DuckDB database + parquet output for one run so landing tasks can be
# re-run cleanly. In production this is what upload_database_setup does when it
# DROPs and recreates the upload_<date> MySQL database; that task is MySQL-only,
# so local re-runs need this instead.
#
# Usage: local_run/reset_run.sh [upload_20260127]
set -euo pipefail
UPLOAD_DB="${1:-upload_20260127}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)/output"
rm -f  "${ROOT}/duckdb/${UPLOAD_DB}.duckdb" "${ROOT}/duckdb/${UPLOAD_DB}.duckdb.wal"
rm -rf "${ROOT}/parquet/${UPLOAD_DB}"
echo "reset ${UPLOAD_DB}: removed duckdb database and parquet folder"
