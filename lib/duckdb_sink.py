"""
Local (no-MySQL) landing zone for the weekly parsers.

The production DAG lands parsed data in a per-week MySQL database
(``upload_YYYYMMDD``) whose schema is cloned from the live ``patent`` database.
Off-container there is no such database, so this module provides a drop-in sink
that accumulates the same tables in a single DuckDB file and exports each one to
parquet.

Enabled via the ``[LOCAL_SINK]`` section of config.ini::

    [LOCAL_SINK]
    enabled = 1
    duckdb_folder = .../output/duckdb
    parquet_folder = .../output/parquet

All columns are stored as VARCHAR. The upload tables are overwhelmingly varchar
in MySQL, the parsers hand us Python strings, and fixing the type avoids
per-file type-inference conflicts (e.g. patent_id 'D1109971' vs 11099710).
"""
import logging
import os

logger = logging.getLogger("airflow.task")

_SENTINEL_NULLS = ('', 'NULL', 'null')


def sink_enabled(config):
    """True when table writes should go to DuckDB/parquet instead of MySQL."""
    if 'LOCAL_SINK' not in config:
        return False
    return config['LOCAL_SINK'].getboolean('enabled', fallback=False)


def skip_infra_qc(config):
    """True when QC assertions that depend on AWS/Elasticsearch should be skipped."""
    if 'LOCAL_SINK' not in config:
        return False
    return config['LOCAL_SINK'].getboolean('skip_infra_qc', fallback=False)


def _version(config):
    return config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB']


def duckdb_path(config):
    folder = config['LOCAL_SINK']['duckdb_folder']
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, '{}.duckdb'.format(_version(config)))


def parquet_dir(config):
    folder = os.path.join(config['LOCAL_SINK']['parquet_folder'], _version(config))
    os.makedirs(folder, exist_ok=True)
    return folder


def connect(config, read_only=False):
    """Open the week's DuckDB database. Caller is responsible for closing."""
    import duckdb
    return duckdb.connect(duckdb_path(config), read_only=read_only)


def _existing_columns(con, table):
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = ? AND table_schema = 'main'", [table]).fetchall()
    return [r[0] for r in rows]


def _ensure_table(con, table, columns):
    """Create the table if absent; widen it if a later file brings new columns."""
    existing = _existing_columns(con, table)
    if not existing:
        coldefs = ', '.join('"{}" VARCHAR'.format(c) for c in columns)
        con.execute('CREATE TABLE "{}" ({})'.format(table, coldefs))
        return
    for col in columns:
        if col not in existing:
            logger.info("duckdb_sink: adding new column %s.%s", table, col)
            con.execute('ALTER TABLE "{}" ADD COLUMN "{}" VARCHAR'.format(table, col))


def append_dataframe(config, table, df, con=None):
    """Append a pandas DataFrame to ``table``, creating/widening it as needed."""
    if df is None or len(df.columns) == 0:
        return 0
    own_con = con is None
    con = con or connect(config)
    try:
        columns = [str(c) for c in df.columns]
        _ensure_table(con, table, columns)
        con.register('_src_df', df)
        select_list = ', '.join(
            'CAST("{c}" AS VARCHAR) AS "{c}"'.format(c=c) for c in columns)
        con.execute(
            'INSERT INTO "{t}" BY NAME SELECT {s} FROM _src_df'.format(t=table, s=select_list))
        con.unregister('_src_df')
        rows = len(df.index)
        logger.info("duckdb_sink: +%d rows -> %s", rows, table)
        return rows
    finally:
        if own_con:
            con.close()


def append_csv(config, table, csv_path, version_indicator=None, con=None):
    """
    Append a parsed TSV to ``table``.

    Reads with the same pandas options the MySQL path used
    (updater/create_databases/upload_new.upload_table) so that NULL handling and
    quoting behaviour are unchanged.
    """
    import pandas as pd
    df = pd.read_csv(csv_path, delimiter='\t', index_col=False,
                     keep_default_na=False, na_values=list(_SENTINEL_NULLS),
                     dtype=str)
    if version_indicator is not None:
        df = df.assign(version_indicator=version_indicator)
    return append_dataframe(config, table, df, con=con)


def apply_uuid_triggers(config, tables, con=None):
    """
    Populate a ``uuid`` column for tables whose MySQL schema fills it with a
    BEFORE INSERT trigger (resources/granted_patent_database.sql and
    resources/text_tables.sql, installed by the create_uuid_triggers and
    create_text_yearly_tables tasks). Those tasks are MySQL-only, so the
    equivalent is done here.
    """
    own_con = con is None
    con = con or connect(config)
    try:
        present = {r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'").fetchall()}
        for table in tables:
            if table not in present:
                continue
            if 'uuid' not in _existing_columns(con, table):
                con.execute('ALTER TABLE "{}" ADD COLUMN "uuid" VARCHAR'.format(table))
            con.execute(
                'UPDATE "{t}" SET "uuid" = CAST(uuid() AS VARCHAR) '
                'WHERE "uuid" IS NULL'.format(t=table))
            logger.info("duckdb_sink: filled uuid for %s", table)
    finally:
        if own_con:
            con.close()


def execute_sql(config, statements, con=None):
    """Run a sequence of DuckDB statements against the week's database."""
    own_con = con is None
    con = con or connect(config)
    try:
        for statement in statements:
            logger.info("duckdb_sink: %s", ' '.join(statement.split())[:300])
            con.execute(statement)
    finally:
        if own_con:
            con.close()


def list_tables(config, con=None):
    own_con = con is None
    con = con or connect(config)
    try:
        return [r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' ORDER BY table_name").fetchall()]
    finally:
        if own_con:
            con.close()


def export_parquet(config, tables=None, con=None):
    """
    Write one parquet file per table into ``parquet_folder/<upload_db>/``.
    Returns {table: (row_count, path)}.
    """
    own_con = con is None
    con = con or connect(config)
    out_dir = parquet_dir(config)
    written = {}
    try:
        for table in (tables if tables is not None else list_tables(config, con=con)):
            path = os.path.join(out_dir, '{}.parquet'.format(table))
            con.execute(
                "COPY \"{t}\" TO '{p}' (FORMAT PARQUET, COMPRESSION ZSTD)".format(
                    t=table, p=path))
            count = con.execute('SELECT count(*) FROM "{}"'.format(table)).fetchone()[0]
            written[table] = (count, path)
            logger.info("duckdb_sink: exported %s rows -> %s", count, path)
    finally:
        if own_con:
            con.close()
    return written
