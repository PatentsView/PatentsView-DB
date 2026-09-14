#!/usr/bin/env python
"""A MySQL-flavoured SQL prompt for a run's DuckDB database.

Used by local_run/duckdb_shell.sh when the `duckdb` CLI is not installed. Type
SQL the way you would in `mysql`: statements end at `;`, backtick-quoted
identifiers work, and \\q / quit exits.
"""
import atexit
import os
import readline
import sys
import time

import duckdb

HISTORY = os.path.expanduser("~/.duckdb_shell_history")

HELP = """\
Commands (everything else is sent to DuckDB as SQL):
  \\q  quit  exit        leave the shell (Ctrl-D also works)
  \\dt .tables           list tables
  \\d <table>  .schema <table>
                        describe a table
  \\h  help  .help       this message
Statements run when you end them with `;`. Ctrl-C clears a half-typed one.
"""


def scan(sql):
    """Split `sql` into complete statements plus any unfinished trailing text.

    MySQL-isms are translated on the way through: backtick-quoted identifiers
    become double-quoted ones and `#` comments become `--` ones, both of which
    DuckDB understands. Statements left open by an unterminated quote or
    comment stay in the trailing text so the caller can ask for another line.
    """
    statements, out = [], []
    i, n = 0, len(sql)

    def split_here():
        done = [text for text, _ in statements if text]
        rest = sql[statements[-1][1]:] if statements else sql
        return done, rest

    while i < n:
        c = sql[i]
        if c == "`":
            close = sql.find("`", i + 1)
            if close < 0:
                return split_here()
            out.append('"' + sql[i + 1:close].replace('"', '""') + '"')
            i = close + 1
        elif c in "'\"":
            j = i + 1
            while j < n:
                if sql[j] == "\\":
                    j += 2
                elif sql[j] != c:
                    j += 1
                elif j + 1 < n and sql[j + 1] == c:  # doubled-quote escape
                    j += 2
                else:
                    break
            if j >= n:
                return split_here()
            out.append(sql[i:j + 1])
            i = j + 1
        elif c == "#" or sql.startswith("--", i):
            end = sql.find("\n", i)
            end = n if end < 0 else end
            out.append("--" + sql[i + 1:end] if c == "#" else sql[i:end])
            i = end
        elif sql.startswith("/*", i):
            close = sql.find("*/", i + 2)
            if close < 0:
                return split_here()
            out.append(sql[i:close + 2])
            i = close + 2
        elif c == ";":
            statements.append(("".join(out).strip(), i + 1))
            out = []
            i += 1
        else:
            out.append(c)
            i += 1
    return split_here()


def expand(line):
    """Translate a mysql/duckdb-CLI meta command to SQL.

    Returns SQL to run, "" for commands handled in place, or None when the line
    is ordinary SQL. Raises SystemExit for the quit commands.
    """
    head, _, arg = line.strip().rstrip(";").partition(" ")
    head, arg = head.lower(), arg.strip().strip('`"')
    if head in ("\\q", "quit", "exit", ".quit", ".exit"):
        raise SystemExit(0)
    if head in ("\\h", "help", ".help", "?"):
        print(HELP, end="")
        return ""
    if head in ("\\dt", ".tables") or (head in ("\\d", ".schema") and not arg):
        return "SHOW TABLES;"
    if head in ("\\d", ".schema") and arg:
        return 'DESCRIBE "%s";' % arg.replace('"', '""')
    return None


def run(con, sql):
    started = time.time()
    try:
        result = con.sql(sql)
    except (duckdb.Error, RuntimeError) as exc:
        print("ERROR: %s" % exc, file=sys.stderr)
        return
    elapsed = time.time() - started
    if result is None:  # DDL and friends have no result set
        print("OK (%.2f sec)" % elapsed)
    else:
        result.show()
        print("(%.2f sec)" % elapsed)


def main():
    db = sys.argv[1]
    con = duckdb.connect(db)
    try:
        readline.read_history_file(HISTORY)
    except OSError:
        pass
    readline.set_history_length(2000)
    atexit.register(save_history)

    print("Connected to %s (duckdb %s)." % (db, duckdb.__version__))
    print("Type SQL ending in `;`, \\h for help, \\q to quit.\n")
    run(con, "SHOW TABLES")

    buffer = ""
    while True:
        try:
            line = input("duckdb> " if not buffer else "     -> ")
        except EOFError:
            print()
            break
        except KeyboardInterrupt:
            print("\n(cancelled)")
            buffer = ""
            continue
        if not buffer:
            try:
                expanded = expand(line)
            except SystemExit:
                break
            if expanded == "":
                continue
            if expanded is not None:
                line = expanded
        statements, buffer = scan(buffer + line + "\n")
        if not buffer.strip():
            buffer = ""
        for statement in statements:
            run(con, statement)
    con.close()


def save_history():
    try:
        readline.write_history_file(HISTORY)
    except OSError:
        pass


if __name__ == "__main__":
    main()
