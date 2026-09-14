# Running `granted_patent_updater` locally (no docker)

Runs the data-producing half of the weekly granted-patent DAG on a laptop, with
a uv virtualenv instead of the 6-container docker stack, and DuckDB/parquet
instead of the `upload_<date>` MySQL database.

## Quick start

```bash
local_run/run_granted_patent_updater.sh 2026-01-20T09:00:00+00:00
```

To start over from an empty landing zone:

```bash
local_run/reset_run.sh upload_20260127
```

## Setup (already done once)

```bash
uv venv --python 3.11 .venv
UV_NATIVE_TLS=1 uv pip install "apache-airflow==2.10.5" \
    --constraint <airflow 2.10.5 constraints-3.11.txt>
UV_NATIVE_TLS=1 uv pip install pandas==2.1.4 numpy==1.26.4 lxml==5.3.0 \
    elasticsearch==8.1.0 PyMySQL billiard boto3 beautifulsoup4 Unidecode \
    sqlparse tabulate slack_sdk requests tqdm mysql-connector-python clint \
    duckdb matplotlib seaborn scipy networkx scikit-learn thefuzz \
    pathos textdistance

source local_run/env.sh
airflow db migrate
airflow pools set high_memory_pool 2 local
airflow pools set elastic_search_pool 2 local
```

Notes on the pins:

* **Airflow 2.x is required.** The DAG uses `schedule_interval` and
  `provide_context`, both removed in Airflow 3.
* `UV_NATIVE_TLS=1` is needed on this machine; TLS is intercepted and uv's
  bundled cert store rejects the proxy certificate.
* `pandas==2.1.4` / `numpy==1.26.4` match Airflow's tested constraints. The
  latest pandas 3.x breaks this codebase (which targets pandas 1.x).
* `torch`, `sent2vec` and `grinch` from `requirements.txt` are **not** needed
  for these tasks.
* `pyarrow` is **not** needed either -- DuckDB's native parquet writer and its
  pandas scan cover everything the sink does (verified by removing it).

## No Airflow server

There is no webserver, scheduler, RabbitMQ, Celery worker or Flower. Tasks run
in-process via `airflow tasks test <dag> <task> <logical-date>` against a SQLite
metadata DB, which executes the real operator with the real run context.

The one thing this does not exercise is inter-task scheduling (queues, pools,
retries). Task *bodies* are identical to production.

## Run ID mapping

`scheduled__2026-01-20T09:00:00+00:00` is the logical date. `get_current_config`
derives:

| value | result |
|---|---|
| `START_DATE` | `20260121` (logical date + 1 day) |
| `END_DATE` | `20260127` (logical date + 7 days) |
| `TEMP_UPLOAD_DB` | `upload_20260127` |
| `WORKING_FOLDER` | `output/data/upload_20260127` |
| USPTO file | `ipg260127.zip` |

## Where output goes

Everything is under `output/` (already gitignored):

```
output/
  data/upload_20260127/raw_data/      ipg260127.xml        (807 MB)
                      clean_data/     ipg260127_clean.xml  (807 MB)
                      parsed_data/260127/  26 TSVs         (54 MB)
                      withdrawn/      withdrawn.txt
  duckdb/upload_20260127.duckdb       all tables           (~190 MB)
  parquet/upload_20260127/            one file per table   (125 MB)
  resources/text_parser_realized.json generated parser config
  airflow/                            airflow.db, logs
```

## config.ini

`config.ini` is gitignored and created by hand from `config_template.ini`. The
local copy points every container path (`/project`, `/data-volume`,
`/app-volume`) at the repo, plus:

```ini
[PARALLELISM]
parallelism = 1        # DuckDB is single-writer; queue_parsers() forks a pool

[LOCAL_SINK]
enabled = 1            # 0 restores normal MySQL writes
duckdb_folder  = .../output/duckdb
parquet_folder = .../output/parquet
skip_infra_qc  = 1     # skip AWS/ES-dependent QC assertions only
```

## What the DuckDB sink does

`lib/duckdb_sink.py` accumulates tables in one DuckDB file per week and exports
each to parquet. It is wired in at the two places the pipeline wrote to MySQL:

| production write | local behaviour |
|---|---|
| `upload_new.upload_table` (TSV → MySQL) | `append_csv` → DuckDB, same pandas read options |
| `parser.load_df_to_sql` (DataFrames → MySQL) | `append_dataframe` → DuckDB |

All columns are stored as `VARCHAR`: the upload tables are overwhelmingly
varchar in MySQL, the parsers produce Python strings, and fixing the type
prevents per-file inference conflicts (e.g. `D1109971` vs `11099710`).

Three pieces of MySQL behaviour are reproduced rather than skipped, because
otherwise the in-scope tasks would land incomplete data:

* **`apply_uuid_triggers`** — `main_cpc`, `further_cpc`, `rel_app_text` and the
  yearly text tables get their `uuid` from MySQL `BEFORE INSERT` triggers
  installed by `create_uuid_triggers` / `create_text_yearly_tables`. Those tasks
  are MySQL-only, so the sink fills `uuid` directly.
* **`updater/xml_to_sql/post_processing_duckdb.py`** — DuckDB ports of the three
  post-parse steps inside `parse_xml_to_sql`: `consolidate_granted_cpc`,
  `trim_whitespace`, `clean_rawlocation_plus_downstream`.
* **`update_withdrawn`** — in production this flags rows in the full production
  `patent` table; locally it flags this week's parsed `patent` table.

## Inspecting the output

Query the parquet directly -- no DuckDB database needed, and no import step:

```bash
# needs nothing but the venv
.venv/bin/python -c "
import duckdb
P='output/parquet/upload_20260127'
print(duckdb.sql(f\"SELECT id, date, title FROM '{P}/patent.parquet' LIMIT 5\"))
print(duckdb.sql(f\"SELECT category, count(*) FROM '{P}/cpc.parquet' GROUP BY 1\"))
"
```

Or open the accumulated database interactively:

```bash
local_run/duckdb_shell.sh upload_20260127
```

Either way you get a plain SQL prompt -- type statements the way you would in
`mysql`, ending each one with `;`. It uses the `duckdb` CLI when present
(`brew install duckdb`) and otherwise falls back to `local_run/duckdb_repl.py`,
which accepts the same SQL plus backtick-quoted identifiers, `#` comments, and
the `\dt` / `\d <table>` / `\q` shortcuts. Useful entry points:

```sql
SHOW TABLES;
DESCRIBE patent;
SELECT count(*) FROM claims_2026;
-- parquet is queryable by path too, and joinable across files
SELECT p.id, p.title, count(*) AS claims
FROM 'output/parquet/upload_20260127/patent.parquet' p
JOIN 'output/parquet/upload_20260127/claims_2026.parquet' c ON c.patent_id = p.id
GROUP BY 1, 2 ORDER BY claims DESC LIMIT 5;
```

## Comparing against MySQL

DuckDB's `mysql` extension can ATTACH a MySQL server, so both sides are
queryable in one engine -- nothing needs to be dumped or copied:

```bash
python local_run/compare_to_mysql.py --mysql-database upload_20260127
```

Output is a per-table row-count diff, plus which tables exist on only one side.
Two extra modes:

```bash
python local_run/compare_to_mysql.py --mysql-database upload_20260127 \
    --check-columns \
    --hash-tables patent,rawinventor,cpc
```

* `--check-columns` reports column sets that differ.
* `--hash-tables` compares actual content with an order-independent md5 over the
  shared columns, skipping `uuid`, `created_date` and `updated_date` -- those are
  generated per insert and can never match across two separate runs.

Connection details come from `[DATABASE_SETUP]` in `config.ini`, overridable
with `--host/--port/--user/--password` or `PV_MYSQL_PASSWORD`. Against a stock
Homebrew MySQL the working account is usually `--user root` with no password;
note the extension connects over TCP, so socket-only auth will not work.

To do the same thing by hand:

```sql
INSTALL mysql; LOAD mysql;
ATTACH 'host=127.0.0.1 port=3306 user=root database=upload_20260127'
  AS my (TYPE mysql, READ_ONLY);
SELECT (SELECT count(*) FROM patent)    AS duckdb_rows,
       (SELECT count(*) FROM my.patent) AS mysql_rows;
```

The extension also writes, which is the easiest way to push a local run into
MySQL for comparison with a downstream tool:

```sql
ATTACH 'host=127.0.0.1 port=3306 user=root database=scratch' AS my (TYPE mysql);
CREATE OR REPLACE TABLE my.patent AS SELECT * FROM patent;
```

Caveat when comparing: every column on the DuckDB side is `VARCHAR`, so compare
values as text (or cast) rather than expecting MySQL's `int`/`date` types.

## Tasks that are NOT part of the local run

Of the DAG's 30 tasks, 7 run locally. The other 23 are MySQL/Elasticsearch/AWS
operations, not data production — changing the output format does not make them
runnable:

| task | why it cannot run locally |
|---|---|
| `backup_oldest_database` | `mydumper` against prod MySQL + AWS |
| `upload_database_setup`, `qc_upload_database_setup` | builds the upload schema via `CREATE TABLE … LIKE patent.<t>`; **the repo has no DDL for those ~30 tables** — the schema exists only in the live prod DB |
| `create_uuid_triggers`, `create_text_yearly_tables`, `create_text_yearly_tables-upload`, `fix_patent_ids-upload` | MySQL DDL/trigger/`UPDATE` templates (effects reproduced in the sink where needed) |
| `qc_upload_new`, `qc_parse_text_data` | assertions issued as SQL against the upload DB |
| `geocode_rawlocations` | needs an Elasticsearch cluster with an OSM index |
| `loc_disambiguation`, `qc_loc_disambiguation` | reads/writes MySQL location tables |
| `gi_NER`, `postprocess_NER`, `simulate_manual_task`, `post_manual`, `GI_QC` | Stanford NER over data read from MySQL; `NER.py` also hardcodes `/project/persistent_files/...` |
| `check_prod_integrity`, `merge_db`, `merge_text_db`, `qc_merge_db`, `qc_merge_text_db` | `INSERT … ON DUPLICATE KEY UPDATE` into the production `patent` / `patent_text` databases (hundreds of GB of history not present locally) |
| `qc_withdrawn_processor` | SQL assertions against prod |

## Changes made to shared code

All are no-ops when `[LOCAL_SINK] enabled = 0`, except the Slack fix:

| file | change |
|---|---|
| `lib/duckdb_sink.py` | new — the sink |
| `updater/xml_to_sql/post_processing_duckdb.py` | new — DuckDB ports of 3 post-parse steps |
| `updater/create_databases/upload_new.py` | `upload_table` / `consolidate_cpc_classes` / `begin_upload` branch on the sink |
| `updater/xml_to_sql/parser.py` | `load_df_to_sql` branches on the sink |
| `updater/xml_to_sql/patent_parser.py` | `patent_sql_parser` uses the DuckDB post-processing |
| `updater/text_data_processor/text_table_parsing.py` | `begin_text_parsing` fills uuids + exports parquet |
| `updater/collect_supplemental_data/update_withdrawn.py` | `load_withdrawn` / `update_withdrawn` branch on the sink |
| `QA/xml_to_csv/ParserTest.py` | `test_aws_rds_space` honours `skip_infra_qc` |
| `local_run/*` | new — env, runner, reset, DuckDB shell, MySQL comparison |
| `requirements.txt` | added `duckdb` (lazily imported, so a prod image without it is unaffected while `enabled = 0`) |
| `.gitignore` | added `.venv/` and `*.code-workspace` |
| `lib/notifications.py` | **unconditional fix**: blank Slack token returned an unassigned `response`, raising `UnboundLocalError` in every task callback. Now logs and returns. |
