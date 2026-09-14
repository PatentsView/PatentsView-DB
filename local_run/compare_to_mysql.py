#!/usr/bin/env python
"""
Compare a local DuckDB run against the equivalent MySQL upload database.

Uses DuckDB's mysql extension to ATTACH MySQL, so both sides are queryable in
one engine -- no dumping or intermediate files.

    # row counts for every shared table
    python local_run/compare_to_mysql.py --mysql-database upload_20260127

    # also compare per-table column sets, and content hashes for some tables
    python local_run/compare_to_mysql.py --mysql-database upload_20260127 \
        --check-columns --hash-tables patent,rawinventor,cpc

Connection details default to [DATABASE_SETUP] in config.ini and can be
overridden with --host/--port/--user/--password (or PV_MYSQL_PASSWORD).
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb  # noqa: E402


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--run-id', default='2026-01-20T09:00:00+00:00',
                   help='DAG logical date, used to locate the DuckDB file')
    p.add_argument('--duckdb-file', help='explicit path to the .duckdb file')
    p.add_argument('--mysql-database', help='MySQL schema to compare against '
                                            '(default: the run\'s TEMP_UPLOAD_DB)')
    p.add_argument('--host'), p.add_argument('--port'), p.add_argument('--user')
    p.add_argument('--password')
    p.add_argument('--check-columns', action='store_true',
                   help='report column sets that differ between the two sides')
    p.add_argument('--hash-tables', default='',
                   help='comma-separated tables to compare by content hash')
    return p.parse_args()


def resolve(args):
    """Fill in DuckDB path / MySQL DSN from config.ini where not given."""
    import pendulum
    from lib.configuration import get_current_config
    from lib.duckdb_sink import duckdb_path

    config = get_current_config(
        'granted_patent', **{'execution_date': pendulum.parse(args.run_id)})
    db_file = args.duckdb_file or duckdb_path(config)
    mysql_db = args.mysql_database or config['PATENTSVIEW_DATABASES']['TEMP_UPLOAD_DB']
    setup = config['DATABASE_SETUP']
    dsn = {
        'host': args.host or setup['HOST'] or '127.0.0.1',
        'port': args.port or setup['PORT'] or '3306',
        'user': args.user or setup['USERNAME'] or 'root',
        'password': (args.password or os.environ.get('PV_MYSQL_PASSWORD')
                     or setup['PASSWORD'] or ''),
        'database': mysql_db,
    }
    return db_file, dsn


def main():
    args = parse_args()
    db_file, dsn = resolve(args)
    if not os.path.exists(db_file):
        sys.exit("No DuckDB database at {}".format(db_file))

    print("duckdb : {}".format(db_file))
    print("mysql  : {user}@{host}:{port}/{database}".format(**dsn))

    con = duckdb.connect(db_file, read_only=True)
    con.execute("INSTALL mysql")
    con.execute("LOAD mysql")
    attach = ' '.join("{}={}".format(k, v) for k, v in dsn.items() if v != '')
    try:
        con.execute("ATTACH '{}' AS my (TYPE mysql, READ_ONLY)".format(attach))
    except Exception as exc:
        sys.exit("Could not ATTACH MySQL: {}\n"
                 "Pass --host/--user/--password or fill [DATABASE_SETUP] in "
                 "config.ini.".format(exc))

    duck_tables = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = current_database() AND table_schema = 'main'").fetchall()}
    my_tables = {r[0] for r in con.execute(
        "SELECT table_name FROM my.information_schema.tables "
        "WHERE table_schema = '{}'".format(dsn['database'])).fetchall()}

    shared = sorted(duck_tables & my_tables)
    print("\n{:32} {:>12} {:>12} {:>10}".format('table', 'duckdb', 'mysql', 'delta'))
    print('-' * 70)
    mismatched = []
    for t in shared:
        d = con.execute('SELECT count(*) FROM "{}"'.format(t)).fetchone()[0]
        m = con.execute('SELECT count(*) FROM my."{}"'.format(t)).fetchone()[0]
        flag = '' if d == m else '  <-- differs'
        if d != m:
            mismatched.append(t)
        print("{:32} {:>12,} {:>12,} {:>10}{}".format(t, d, m, d - m, flag))
    print('-' * 70)
    print("{} shared tables, {} with differing row counts".format(
        len(shared), len(mismatched)))

    only_duck = sorted(duck_tables - my_tables)
    only_my = sorted(my_tables - duck_tables)
    if only_duck:
        print("\nonly in duckdb ({}): {}".format(len(only_duck), ', '.join(only_duck)))
    if only_my:
        print("\nonly in mysql  ({}): {}".format(len(only_my), ', '.join(only_my)))

    if args.check_columns:
        print("\n=== column differences ===")
        any_diff = False
        for t in shared:
            dc = {r[0] for r in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = ? AND table_schema = 'main'", [t]).fetchall()}
            mc = {r[0] for r in con.execute(
                "SELECT column_name FROM my.information_schema.columns "
                "WHERE table_name = ? AND table_schema = ?",
                [t, dsn['database']]).fetchall()}
            if dc != mc:
                any_diff = True
                print("  {}: duckdb-only={} mysql-only={}".format(
                    t, sorted(dc - mc) or '-', sorted(mc - dc) or '-'))
        if not any_diff:
            print("  none")

    hash_tables = [t.strip() for t in args.hash_tables.split(',') if t.strip()]
    if hash_tables:
        print("\n=== content hashes (shared columns, NULL-normalised, order-independent) ===")
        for t in hash_tables:
            if t not in shared:
                print("  {}: not in both sides, skipping".format(t))
                continue
            dc = [r[0] for r in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = ? AND table_schema = 'main'", [t]).fetchall()]
            mc = {r[0] for r in con.execute(
                "SELECT column_name FROM my.information_schema.columns "
                "WHERE table_name = ? AND table_schema = ?",
                [t, dsn['database']]).fetchall()}
            # uuid/created_date/updated_date are generated per-insert, so they
            # can never match across the two runs.
            cols = [c for c in dc
                    if c in mc and c not in ('uuid', 'created_date', 'updated_date')]
            if not cols:
                print("  {}: no comparable columns".format(t))
                continue
            expr = " || '|' || ".join(
                "coalesce(CAST(\"{}\" AS VARCHAR), '~')".format(c) for c in cols)
            d = con.execute(
                'SELECT md5(string_agg(h, \'\' ORDER BY h)) FROM '
                '(SELECT {} AS h FROM "{}")'.format(expr, t)).fetchone()[0]
            m = con.execute(
                'SELECT md5(string_agg(h, \'\' ORDER BY h)) FROM '
                '(SELECT {} AS h FROM my."{}")'.format(expr, t)).fetchone()[0]
            status = 'MATCH' if d == m else 'DIFFER'
            print("  {:28} {}  ({} cols compared)".format(t, status, len(cols)))
            if d != m:
                print("      duckdb {}\n      mysql  {}".format(d, m))

    con.close()


if __name__ == '__main__':
    main()
