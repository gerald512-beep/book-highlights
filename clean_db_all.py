#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clean ALL tables in a Postgres schema by truncating them.

- Discovers base tables in the target schema (default: public)
- Shows row counts before/after
- Confirms before executing unless --yes is provided
- Uses TRUNCATE ... RESTART IDENTITY CASCADE
- Skips common migration tables by default (alembic_version, schema_migrations)

Usage examples:
  py clean_db_all.py --yes                       # truncate all tables in 'public' except migrations
  py clean_db_all.py --schema other --yes        # truncate all tables in 'other'
  py clean_db_all.py --include-migrations --yes  # include migration tables too
  py clean_db_all.py --exclude logs,audit --yes  # exclude specific tables
"""

import os
import sys
import argparse
from typing import Dict, List

from dotenv import load_dotenv
import psycopg
from psycopg import sql

DEFAULT_EXCLUDES = {"alembic_version", "schema_migrations"}


def confirm_or_exit(text: str, assume_yes: bool = False) -> None:
    if assume_yes:
        return
    ans = input(f"{text} Type 'yes' to continue: ").strip().lower()
    if ans != "yes":
        print("Aborted. Nothing changed.")
        sys.exit(1)


def list_tables(cur, schema: str) -> List[str]:
    cur.execute(
        """
        select table_name
        from information_schema.tables
        where table_schema=%s and table_type='BASE TABLE'
        order by table_name
        """,
        (schema,),
    )
    return [r[0] for r in cur.fetchall()]


def get_counts(cur, schema: str, tables: List[str]) -> Dict[str, int]:
    out = {}
    for t in tables:
        try:
            cur.execute(
                sql.SQL("select count(*) from {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(t)
                )
            )
            out[t] = cur.fetchone()[0]
        except Exception as e:
            out[t] = f"ERR: {e}"
    return out


def print_counts(label: str, counts: Dict[str, int]) -> None:
    print(f"\n{label}")
    for t in sorted(counts):
        print(f"  {t:30s} {counts[t]}")


def main():
    load_dotenv()
    load_dotenv(".env.local", override=True)

    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="public", help="target schema (default: public)")
    ap.add_argument("--yes", action="store_true", help="skip confirmation prompts")
    ap.add_argument(
        "--include-migrations",
        action="store_true",
        help="include migration tables (alembic_version, schema_migrations)",
    )
    ap.add_argument(
        "--exclude",
        default="",
        help="comma-separated table names to exclude (in addition to defaults)",
    )
    args = ap.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL missing. Put it in .env")
        sys.exit(1)

    excludes = set(filter(None, [x.strip() for x in args.exclude.split(",")]))
    if not args.include_migrations:
        excludes |= DEFAULT_EXCLUDES

    print(f"Connecting to Postgres (schema={args.schema}).")
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        try:
            cur.execute("set local statement_timeout = 15000")
        except Exception:
            pass

        all_tables = list_tables(cur, args.schema)
        if not all_tables:
            print(f"No base tables found in schema '{args.schema}'. Nothing to do.")
            return

        target_tables = [t for t in all_tables if t not in excludes]
        skipped_tables = sorted(set(all_tables) - set(target_tables))

        if not target_tables:
            print("No tables selected for truncation after applying exclusions.")
            if skipped_tables:
                print("Skipped:")
                for t in skipped_tables:
                    print(f"  - {t}")
            return

        pre = get_counts(cur, args.schema, target_tables)
        print_counts("Before:", pre)
        if skipped_tables:
            print("\nSkipping tables:")
            for t in skipped_tables:
                print(f"  - {t}")

        tbl_preview = ", ".join(target_tables)
        confirm_or_exit(
            f"This will TRUNCATE {len(target_tables)} table(s): {tbl_preview}",
            assume_yes=args.yes,
        )

        stmt = sql.SQL("TRUNCATE {} RESTART IDENTITY CASCADE;").format(
            sql.SQL(", ").join(
                [
                    sql.SQL("{}.{}").format(
                        sql.Identifier(args.schema), sql.Identifier(t)
                    )
                    for t in target_tables
                ]
            )
        )
        cur.execute(stmt)
        conn.commit()

        post = get_counts(cur, args.schema, target_tables)
        print_counts("After:", post)

    print("\nDone.")


if __name__ == "__main__":
    main()
