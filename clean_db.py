#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clean your project tables in Postgres.

Default (no flags):
  - Clears dynamic/posting tables only:
      platform_posts, publication_quotes, publications, compliance_checks, sources, scores, generated_images
  - Keeps books and quotes in place.

With --nuke:
  - Also clears quotes and books.

Safety:
  - Prints row counts before/after
  - Asks for interactive confirmation unless --yes is provided
  - Uses TRUNCATE ... RESTART IDENTITY CASCADE to reset ids safely

Usage:
  py clean_db.py                # clean posting/aux tables only
  py clean_db.py --yes          # same, no prompt
  py clean_db.py --nuke --yes   # wipe EVERYTHING (books, quotes included)
  py clean_db.py --reset-flags --yes  # keep data, just reset quote flags
"""

import os
import sys
import argparse
import psycopg
from dotenv import load_dotenv

AUX_TABLES = [
    # child tables first, but TRUNCATE CASCADE handles dependency anyway
    "platform_posts",
    "publication_quotes",
    "publications",
    "compliance_checks",
    "sources",
    "scores",
    "generated_images",
    "generated_captions",
]

CORE_TABLES = [
    "quotes",
    "books",
]


def get_counts(cur, tables):
    out = {}
    for t in tables:
        try:
            cur.execute(f"select count(*) from {t}")
            out[t] = cur.fetchone()[0]
        except Exception as e:
            out[t] = f"ERR: {e}"
    return out


def print_counts(label, counts):
    print(f"\n{label}")
    for t in counts:
        print(f"  {t:20s} {counts[t]}")


def confirm_or_exit(text, assume_yes=False):
    if assume_yes:
        return
    ans = input(f"{text} Type 'yes' to continue: ").strip().lower()
    if ans != "yes":
        print("Aborted. Nothing changed.")
        sys.exit(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nuke", action="store_true", help="Also delete books and quotes")
    ap.add_argument(
        "--reset-flags",
        action="store_true",
        help="Reset drafting/discard flags on quotes (keeps content)",
    )
    ap.add_argument("--yes", action="store_true", help="Skip interactive confirmation")
    args = ap.parse_args()

    load_dotenv()
    load_dotenv(".env.local", override=True)
    db_url = os.getenv("DATABASE_URL")
    assert db_url, "DATABASE_URL missing. Put it in .env"

    target_tables = AUX_TABLES.copy()
    if args.nuke:
        target_tables += CORE_TABLES

    print("Connecting to Postgres…")
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        try:
            cur.execute("set local statement_timeout = 15000")
        except Exception:
            pass

        # Pre counts and determine existing tables
        pre_counts = get_counts(cur, target_tables)
        print_counts("Before:", pre_counts)
        existing_tables = [t for t, v in pre_counts.items() if isinstance(v, int)]
        missing_tables = [t for t, v in pre_counts.items() if not isinstance(v, int)]
        if missing_tables:
            print("\nNote: the following tables were not found and will be skipped:")
            for t in missing_tables:
                print(f"  - {t}")

        if existing_tables:
            if args.nuke:
                confirm_or_exit(
                    "This will TRUNCATE books and quotes too (irreversible).",
                    assume_yes=args.yes,
                )
            else:
                confirm_or_exit(
                    "This will TRUNCATE posting/aux tables (keeps books & quotes).",
                    assume_yes=args.yes,
                )
        else:
            print("No target tables exist to TRUNCATE. Skipping table truncation.")

        # Build TRUNCATE command only for existing tables. Use CASCADE to satisfy FKs and
        # RESTART IDENTITY to reset serials.
        if existing_tables:
            truncate_list = ", ".join(existing_tables)
            cur.execute(f"TRUNCATE {truncate_list} RESTART IDENTITY CASCADE;")
            conn.commit()

        # Post counts
        post_counts = get_counts(cur, target_tables)
        print_counts("After:", post_counts)

        # Optional: reset flags on quotes (when not nuking)
        if args.reset_flags:
            if args.nuke:
                print("\n--reset-flags specified, but --nuke removed quotes; skipping flag reset.")
            else:
                confirm_or_exit(
                    "This will reset is_drafted/is_discarded flags on quotes.",
                    assume_yes=args.yes,
                )
                try:
                    cur.execute(
                        "update quotes set is_drafted=false, drafted_at=null, is_discarded=false, discarded_at=null"
                    )
                    conn.commit()
                    print("Reset drafting/discard flags on quotes.")
                except Exception as e:
                    print(f"Could not reset flags on quotes: {e}")

    print("\nDone. ✅")


if __name__ == "__main__":
    main()
