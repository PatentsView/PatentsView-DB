#!/usr/bin/env bash
# Environment for running the PatentsView DAGs locally (no docker, uv venv).
# Usage:  source local_run/env.sh
#
# Mirrors the env the docker image sets (see Dockerfile + airflow_pipeline_env.sh.template),
# but with repo-local paths and a SQLite/SequentialExecutor Airflow instead of
# MariaDB + RabbitMQ + Celery workers.

# Repo root, derived from this file's location so the script is portable.
if [ -n "${BASH_SOURCE:-}" ]; then
  _pv_env_file="${BASH_SOURCE[0]}"
else
  _pv_env_file="${(%):-%x}"   # zsh
fi
export PACKAGE_HOME="$(cd "$(dirname "${_pv_env_file}")/.." && pwd)"
unset _pv_env_file
export OUTPUT_ROOT="${PACKAGE_HOME}/output"

# Same three entries the Dockerfile puts on PYTHONPATH
export PYTHONPATH="${PACKAGE_HOME}:${PACKAGE_HOME}/updater/disambiguation/hierarchical_clustering_disambiguation:${PACKAGE_HOME}/lib"
export DISAMBIGUATION_ROOT="${PACKAGE_HOME}/updater/disambiguation/hierarchical_clustering_disambiguation"

# --- Airflow: single-process, SQLite metadata db, everything under output/ ---
export AIRFLOW_HOME="${OUTPUT_ROOT}/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="${PACKAGE_HOME}/airflow/dags"
export AIRFLOW__CORE__EXECUTOR="SequentialExecutor"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///${AIRFLOW_HOME}/airflow.db"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="${AIRFLOW_HOME}/logs"
# Keep Airflow from trying to phone home / check for updates while offline
export AIRFLOW__WEBSERVER__EXPOSE_CONFIG="False"
export AIRFLOW__CORE__UNIT_TEST_MODE="False"

# uv/pip need the system cert store on this machine (TLS is intercepted)
export UV_NATIVE_TLS=1

mkdir -p "${AIRFLOW_HOME}" "${OUTPUT_ROOT}/data" "${OUTPUT_ROOT}/duckdb" \
         "${OUTPUT_ROOT}/parquet" "${OUTPUT_ROOT}/text" "${OUTPUT_ROOT}/resources"

# Activate the uv venv unless the caller already did
if [ -z "${VIRTUAL_ENV:-}" ]; then
  # shellcheck disable=SC1091
  source "${PACKAGE_HOME}/.venv/bin/activate"
fi

echo "PACKAGE_HOME = ${PACKAGE_HOME}"
echo "AIRFLOW_HOME = ${AIRFLOW_HOME}"
echo "python       = $(command -v python)"
