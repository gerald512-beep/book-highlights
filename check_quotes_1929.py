#!/usr/bin/env python3
"""Quick helper to check how many quotes a '1929' book has."""

from __future__ import annotations

import os
import sys

import psycopg

try:  # Optional; we just want .env loading if available
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore


def main() -> None:
    if load_dotenv:
        load_dotenv()
        load_dotenv(".env.local", override=True)

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not set; export it or add it to .env and rerun.")
        sys.exit(1)

    sql = """
    select b.book_id, b.title, count(q.quote_id) as quote_count
    from books b
    join quotes q on q.book_id = b.book_id
    where coalesce(q.is_discarded,false)=false
      and b.title ilike '%1929%'
    group by b.book_id, b.title
    order by quote_count desc
    """

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    if not rows:
        print("No books with title containing '1929' found.")
        return

    for book_id, title, count in rows:
        print(f"{title} (book_id={book_id}) has {count} quotes")


if __name__ == "__main__":
    main()
