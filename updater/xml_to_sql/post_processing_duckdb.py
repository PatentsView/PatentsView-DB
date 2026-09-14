"""
DuckDB ports of the post-parse SQL steps that patent_sql_parser() runs against
the upload_ MySQL database.

Each function mirrors its MySQL counterpart in
updater/xml_to_sql/post_processing.py (and lib.utilities.trim_whitespace),
with MySQL-isms translated:
  IF(c, a, b)  -> CASE WHEN c THEN a ELSE b END
  CHAR_LENGTH  -> length
  INSERT IGNORE / UNIQUE-key dedupe -> explicit NOT IN / DISTINCT
"""
import json
import logging
import os

from lib.duckdb_sink import execute_sql, connect

logger = logging.getLogger("airflow.task")

# Column list taken from the INSERT in post_processing.consolidate_granted_cpc.
# In MySQL this table is cloned from patent.cpc by setup_database(); there is no
# prod database locally, so it is declared here.
_CPC_COLUMNS = [
    'uuid', 'patent_id', 'section_id', 'subsection_id', 'group_id', 'subgroup_id',
    'category', 'action_date', 'version', 'sequence', 'symbol_position',
    'version_indicator',
]


def consolidate_granted_cpc(config):
    """main_cpc + further_cpc -> cpc (see post_processing.consolidate_granted_cpc)."""
    logger.info('consolidating cpc from main and further cpc tables (duckdb)')
    coldefs = ', '.join('"{}" VARCHAR'.format(c) for c in _CPC_COLUMNS)
    collist = ', '.join('"{}"'.format(c) for c in _CPC_COLUMNS)

    def _select(source, sequence_expr):
        return """
SELECT "uuid",
       "patent_id",
       "section",
       concat("section", "class"),
       concat("section", "class", "subclass"),
       concat("section", "class", "subclass", "main_group", '/', "subgroup"),
       CASE WHEN "value" = 'I' THEN 'inventional' ELSE 'additional' END,
       "action_date",
       "version",
       {seq},
       "symbol_position",
       "version_indicator"
FROM "{src}"
""".format(src=source, seq=sequence_expr)

    statements = [
        'CREATE TABLE IF NOT EXISTS "cpc" ({})'.format(coldefs),
        'INSERT INTO "cpc" ({}) {}'.format(collist, _select('main_cpc', '"sequence"')),
        # further_cpc rows that are entirely empty are excluded, as in MySQL
        'INSERT INTO "cpc" ({}) {} WHERE NOT ('
        '"section" IS NULL AND "class" IS NULL AND "subclass" IS NULL '
        'AND "main_group" IS NULL AND "subgroup" IS NULL '
        'AND "symbol_position" IS NULL)'.format(
            collist, _select('further_cpc', 'CAST("sequence" AS BIGINT) + 1')),
    ]
    execute_sql(config, statements)


def trim_whitespace(config):
    """Trim configured columns (see lib.utilities.trim_whitespace)."""
    project_home = os.environ["PACKAGE_HOME"]
    db_type = config["PATENTSVIEW_DATABASES"]["TEMP_UPLOAD_DB"][:6]
    resources_file = "{root}/{resources}/columns_for_whitespace_trim.json".format(
        root=project_home, resources=config["FOLDERS"]["resources_folder"])
    cols_tables_whitespace = json.load(open(resources_file))

    con = connect(config)
    try:
        present = {r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'").fetchall()}
        for table in cols_tables_whitespace.keys():
            if db_type not in cols_tables_whitespace[table]["TestScripts"]:
                continue
            if table not in present:
                logger.info("trim_whitespace: table %s not present locally, skipping", table)
                continue
            columns = {r[0] for r in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = ? AND table_schema = 'main'", [table]).fetchall()}
            for column in cols_tables_whitespace[table]["fields"]:
                if column not in columns:
                    continue
                con.execute("""
                    UPDATE "{t}"
                    SET "{c}" = trim("{c}")
                    WHERE length("{c}") != length(trim("{c}"))
                """.format(t=table, c=column))
    finally:
        con.close()


def clean_rawlocation_plus_downstream(config, applicant_table="non_inventor_applicant"):
    """Null out FKs to empty rawlocations, then drop them (see post_processing)."""
    con = connect(config)
    try:
        present = {r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'").fetchall()}
        if 'rawlocation' not in present:
            logger.info("clean_rawlocation: no rawlocation table locally, skipping")
            return
        con.execute('DROP TABLE IF EXISTS "null_rawlocations"')
        con.execute("""
            CREATE TABLE "null_rawlocations" AS
            SELECT "id" FROM "rawlocation"
            WHERE "city" IS NULL AND "state" IS NULL AND "country" IS NULL
        """)
        for table in ['rawinventor', 'rawassignee', applicant_table]:
            if table not in present:
                logger.info("clean_rawlocation: %s not present locally, skipping", table)
                continue
            con.execute("""
                UPDATE "{t}"
                SET "rawlocation_id" = NULL
                WHERE "rawlocation_id" IN (SELECT "id" FROM "null_rawlocations")
            """.format(t=table))
        con.execute("""
            DELETE FROM "rawlocation"
            WHERE "city" IS NULL AND "state" IS NULL AND "country" IS NULL
        """)
    finally:
        con.close()
