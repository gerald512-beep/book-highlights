#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DB Smoke Tests — Books/Quotes/Publishing
Runs read-only checks plus one safe trigger test (rolled back).
Print outputs here so we can debug your data shape before rendering images.
"""

import os
import psycopg
from dotenv import load_dotenv

load_dotenv()
load_dotenv(".env.local", override=True)
DB = os.getenv("DATABASE_URL")
assert DB, "DATABASE_URL missing in .env"


def print_header(title: str):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def q_fetch(cur, sql, params=None):
    cur.execute(sql, params or ())
    return cur.fetchall()


def test_1_counts(cur):
    print_header("TEST 1 — Basic counts")
    rows = q_fetch(
        cur,
        """
        with q as (
          select
            count(*)                        as quotes_total,
            count(*) filter (where is_approved) as quotes_approved
          from quotes
        ),
        b as (select count(*) as books_total from books),
        p as (
          select
            count(*) as publications_total,
            count(*) filter (where status='draft')     as publications_draft,
            count(*) filter (where status='scheduled') as publications_scheduled,
            count(*) filter (where status='posted')    as publications_posted
          from publications
        )
        select * from b, q, p;
    """,
    )
    if rows:
        (
            books_total,
            quotes_total,
            quotes_approved,
            publications_total,
            publications_draft,
            publications_scheduled,
            publications_posted,
        ) = rows[0]
        print(f"Books: {books_total}")
        print(f"Quotes: {quotes_total} (approved: {quotes_approved})")
        print(
            f"Publications: {publications_total} | draft {publications_draft} | scheduled {publications_scheduled} | posted {publications_posted}"
        )
    else:
        print("No data returned. That’s… concerning.")


def test_2_sample_approved_quotes(cur):
    print_header("TEST 2 — Up to 3 approved quotes per book (sample)")
    rows = q_fetch(
        cur,
        """
        select title, author_primary as author, cover_url, quote_text
        from (
          select
            b.title, b.author_primary, b.cover_url,
            q.quote_text,
            row_number() over (partition by b.book_id order by q.created_at asc) as rn
          from books b
          join quotes q on q.book_id = b.book_id
          where q.is_approved = true
        ) t
        where rn <= 3
        order by title, rn;
    """,
    )
    if not rows:
        print(
            "No approved quotes found. Approve some rows in quotes.is_approved or attach quotes to books."
        )
        return
    cur_title = None
    for title, author, cover, qt in rows:
        if title != cur_title:
            print(f"\n• {title} — {author}")
            if cover:
                print(f"  cover: {cover[:80]}{'…' if len(cover)>80 else ''}")
            cur_title = title
        print(f"  - {qt[:120]}{'…' if len(qt)>120 else ''}")


def test_3_next_publication_with_ordered_quotes(cur):
    print_header("TEST 3 — Next draft/scheduled publication with ordered quotes")
    rows = q_fetch(
        cur,
        """
        select
          p.publication_id,
          p.status,
          p.planned_at,
          b.title,
          b.author_primary as author,
          b.cover_url,
          json_agg(json_build_object('position', pq.position, 'text', q.quote_text)
                   order by pq.position) as ordered_quotes
        from publications p
        join books b on b.book_id = p.book_id
        join publication_quotes pq on pq.publication_id = p.publication_id
        join quotes q on q.quote_id = pq.quote_id
        where p.status in ('draft','scheduled')
          and q.is_approved = true
        group by p.publication_id, p.status, p.planned_at, b.title, b.author_primary, b.cover_url
        order by coalesce(p.planned_at, now())
        limit 1;
    """,
    )
    if not rows:
        print(
            "No draft/scheduled publication with approved quotes found. Create a publication and attach approved quotes."
        )
        return
    pub_id, status, planned_at, title, author, cover, ordered = rows[0]
    print(f"Publication: {pub_id} | status={status} | planned_at={planned_at}")
    print(f"Book: {title} — {author}")
    if cover:
        print(f"Cover: {cover[:90]}{'…' if len(cover)>90 else ''}")
    for item in ordered or []:
        print(
            f"  [{item['position']}] {item['text'][:120]}{'…' if len(item['text'])>120 else ''}"
        )


def test_4_trigger_guard_dry_run(cur):
    print_header("TEST 4 — Trigger guard dry-run (should block missing commentary)")
    # Find any publication with attached quotes where at least one quote lacks commentary or approval.
    row = q_fetch(
        cur,
        """
        select p.publication_id
        from publications p
        join publication_quotes pq on pq.publication_id = p.publication_id
        join quotes q on q.quote_id = pq.quote_id
        where p.status = 'draft'
        group by p.publication_id
        having bool_or(q.is_approved = false) or bool_or(coalesce(length(trim(q.my_commentary)),0) < 30)
        limit 1;
    """,
    )
    if not row:
        print(
            "No publication suitable for a guard test. Either all your quotes are compliant (good) or you haven’t set up publications yet."
        )
        return
    pub_id = row[0][0]
    print(f"Testing publication_id={pub_id} in a savepoint (no changes will persist)…")
    try:
        cur.execute("savepoint sp_guard_test")
        cur.execute(
            "update publications set status='scheduled' where publication_id=%s",
            (pub_id,),
        )
        print(
            "Trigger did NOT fire. That means all quotes for this publication are compliant, or the trigger is missing."
        )
    except psycopg.Error as e:
        print("Trigger fired as expected. Message:")
        print(str(e).strip())
    finally:
        cur.execute("rollback to savepoint sp_guard_test")
        print("Rolled back to keep database unchanged.")


def test_5_missing_assets_for_instagram(cur):
    print_header(
        "TEST 5 — Publications missing Instagram assets (what you should render next)"
    )
    rows = q_fetch(
        cur,
        """
        select p.publication_id, b.title, b.author_primary as author,
               exists(select 1 from platform_posts pp where pp.publication_id=p.publication_id and platform='instagram') as has_ig_post
        from publications p
        join books b on b.book_id = p.book_id
        where p.status in ('draft','scheduled')
        order by coalesce(p.planned_at, now())
        limit 10;
    """,
    )
    if not rows:
        print("No publications found. Create at least one publication to plan a post.")
        return
    for pub_id, title, author, has_ig in rows:
        print(f"- {pub_id} | {title} — {author} | instagram_asset_exists={has_ig}")


def main():
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        # Set a gentle statement timeout in case Neon is sleepy
        try:
            cur.execute("set local statement_timeout = 15000")
        except Exception:
            pass

        test_1_counts(cur)
        test_2_sample_approved_quotes(cur)
        test_3_next_publication_with_ordered_quotes(cur)
        test_4_trigger_guard_dry_run(cur)
        test_5_missing_assets_for_instagram(cur)


if __name__ == "__main__":
    main()
