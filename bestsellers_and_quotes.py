#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bestsellers & Highlights Harvester (Postgres + quotes/search, ISBN-safe upsert)
-------------------------------------------------------------------------------
- Pull NYT bestsellers
- Enrich via Google Books
- Collect SHORT quotes from Goodreads quotes/search (polite, best-effort)
- Write CSV + JSON
- Persist to Postgres using DATABASE_URL in .env
- Neon pooler friendly (no startup options)
- ISBN-safe upsert: finds existing rows by isbn13 first, then updates

Install (inside your venv):
  py -m pip install requests beautifulsoup4 python-dotenv pandas psycopg[binary]

.env:
  NYT_API_KEY=your_key_here
  DATABASE_URL=postgresql://user:pass@host/db?sslmode=require

Run:
  py bestsellers_and_quotes.py
  # Menu option 1 harvests NYT "advice-how-to-and-miscellaneous" (limit 10, top quotes 20)
"""

import os
import re
import time
import json
from datetime import date, timedelta
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg
from dotenv import load_dotenv

GOODREADS_BASE = "https://www.goodreads.com"
NYT_BASE = "https://api.nytimes.com/svc/books/v3/lists"
GB_BASE = "https://www.googleapis.com/books/v1/volumes"

REQ_SLEEP = 1.2  # politeness between requests
MAX_QUOTE_WORDS = 100  # cap to stay on the safe side
DEFAULT_CATEGORY = "advice-how-to-and-miscellaneous"
COMBINED_NONFICTION_CATEGORY = "combined-print-and-e-book-nonfiction"
BUSINESS_BOOKS_CATEGORY = "business-books"
PAPERBACK_NONFICTION_CATEGORY = "paperback-nonfiction"
DEFAULT_LIMIT = 10
DEFAULT_TOP_QUOTES = 20
DEFAULT_WEEKS_AGO = 0
DEFAULT_OUT_PREFIX = "nyt_bestsellers"
MANUAL_CATEGORY = "manual-entry"
QUOTE_FETCH_RETRY_DELAY = 2.5
QUOTE_FETCH_ATTEMPTS = 3
GOOGLE_SEARCH_URL = "https://www.google.com/search"


def normalize_author_name(name: str) -> str:
    import unicodedata

    name = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def author_variants(author: str) -> set:
    variants = set()
    if not author:
        return variants
    splitter = re.compile(r"[;/&]|(?:\band\b)|,|(?:\bwith\b)", re.IGNORECASE)
    pieces = splitter.split(author)
    for piece in pieces:
        norm = normalize_author_name(piece)
        if norm:
            variants.add(norm)
    base = normalize_author_name(author)
    if base:
        variants.add(base)
    return variants


def normalize_quote_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip()).lower()


def deduplicate_quote_entries(quotes: list) -> list:
    seen = set()
    unique = []
    for q in quotes or []:
        norm = normalize_quote_text(q.get("quote_text"))
        if not norm or norm in seen:
            continue
        seen.add(norm)
        unique.append(q)
    return unique


def wcount(s: str) -> int:
    return len((s or "").split())


def slugify(s: str) -> str:
    return "-".join("".join(c.lower() if c.isalnum() else " " for c in s).split())


def short(s: str, n=160) -> str:
    s = s or ""
    return s if len(s) <= n else s[: n - 1] + "…"


def get_nyt_bestsellers(
    category: str, weeks_ago: int = 0, limit: int = 10, api_key: str = ""
) -> list:
    if not api_key:
        raise RuntimeError("NYT_API_KEY missing. Put it in a .env file or environment.")
    target_date = date.today() - timedelta(weeks=weeks_ago)
    endpoint = f"{NYT_BASE}/{target_date:%Y-%m-%d}/{category}.json"
    r = requests.get(endpoint, params={"api-key": api_key}, timeout=30)
    r.raise_for_status()
    data = r.json()
    out = []
    for b in data.get("results", {}).get("books", [])[:limit]:
        isbn13 = None
        for i in b.get("isbns", []):
            if i.get("isbn13"):
                isbn13 = i["isbn13"]
                break
        out.append(
            {
                "nyt_rank": b.get("rank"),
                "title": b.get("title"),
                "author": b.get("author"),
                "publisher": b.get("publisher"),
                "description": b.get("description"),
                "amazon_product_url": b.get("amazon_product_url"),
                "book_image": b.get("book_image"),
                "primary_isbn13": isbn13,
                "weeks_on_list": b.get("weeks_on_list"),
                "nyt_category": data.get("results", {}).get("list_name"),
                # Do not persist API keys in generated artifacts.
                "nyt_citation": endpoint,
            }
        )
    return out


def get_google_books_by_isbn(isbn13: str) -> dict:
    if not isbn13:
        return {}
    url = f"{GB_BASE}?q=isbn:{isbn13}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    j = r.json()
    if not j.get("items"):
        return {}
    v = j["items"][0]["volumeInfo"]
    return {
        "google_books_id": j["items"][0].get("id"),
        "gb_title": v.get("title"),
        "gb_subtitle": v.get("subtitle"),
        "gb_authors": ", ".join(v.get("authors", [])[:3]) if v.get("authors") else "",
        "gb_publisher": v.get("publisher"),
        "gb_publishedDate": v.get("publishedDate"),
        "gb_pageCount": v.get("pageCount"),
        "gb_categories": (
            ", ".join(v.get("categories", [])[:3]) if v.get("categories") else ""
        ),
        "gb_thumbnail": (v.get("imageLinks", {}) or {}).get("thumbnail"),
        "gb_previewLink": v.get("previewLink"),
        "google_books_citation": url,
    }


def fetch_goodreads_quotes(
    title: str, author: str, max_quotes: int = 6, verbose: bool = False
) -> list:
    """
    Prefer Goodreads' quotes search:
      https://www.goodreads.com/quotes/search?q=<query>
    Tries title first, then title+author. Parses .quoteText blocks.
    """

    def log(msg: str):
        if verbose:
            print(msg)

    headers = {"User-Agent": "Mozilla/5.0 (curation-bot; contact: you@example.com)"}

    queries = []
    t = (title or "").strip()
    a = (author or "").strip()
    if t:
        queries.append(t)
        if a:
            queries.append(f"{t} {a}")

    out, seen = [], set()
    acceptable_authors = author_variants(a)

    for q in queries:
        if len(out) >= max_quotes:
            break
        search_url = f"{GOODREADS_BASE}/quotes/search?q={quote_plus(q)}"
        ok = False
        for attempt in range(1, QUOTE_FETCH_ATTEMPTS + 1):
            try:
                r = requests.get(search_url, headers=headers, timeout=15)
                r.raise_for_status()
                time.sleep(REQ_SLEEP)
                ok = True
                break
            except Exception as e:
                log(f"  quotes search error for '{q}' (attempt {attempt}/{QUOTE_FETCH_ATTEMPTS}): {e}")
                if attempt < QUOTE_FETCH_ATTEMPTS:
                    time.sleep(QUOTE_FETCH_RETRY_DELAY)
                else:
                    log(f"  giving up on '{q}' after {QUOTE_FETCH_ATTEMPTS} attempts")
        if not ok:
            continue

        soup = BeautifulSoup(r.text, "html.parser")
        blocks = soup.select(".quoteText")
        if not blocks:
            blocks = soup.select(
                ".quotes .quoteText, .quote .quoteText, .quoteDetails .quoteText"
            )
        if verbose:
            log(f"  search '{q}' -> {len(blocks)} blocks")

        for qb in blocks:
            quote_author = ""
            author_node = qb.find("span", class_="authorOrTitle")
            if author_node:
                quote_author = author_node.get_text(" ", strip=True)
            if not quote_author:
                # Look for author link fallback
                link = qb.find("a", class_="authorOrTitle")
                if link:
                    quote_author = link.get_text(" ", strip=True)
            if not quote_author:
                # Goodreads often includes author right after an em dash in text
                text_with_author = qb.get_text(separator=" ", strip=True)
                parts = re.split(r"\s+—\s+", text_with_author)
                if len(parts) > 1:
                    quote_author = parts[1]

            if acceptable_authors:
                normalized_quote_author = normalize_author_name(
                    quote_author.split(",")[0] if quote_author else ""
                )
                matched = any(
                    normalized_quote_author
                    and (normalized_quote_author == au or normalized_quote_author in au or au in normalized_quote_author)
                    for au in acceptable_authors
                )
                if not matched:
                    continue

            text = qb.get_text(separator=" ", strip=True)
            text = text.replace("―", "—")
            text = re.sub(r"\s+—\s+.*$", "", text).strip()
            text = re.sub(r"\s+", " ", text)

            if not text or text in seen:
                continue
            if len(text.split()) > MAX_QUOTE_WORDS:
                continue

            out.append(
                {
                    "quote_text": text,
                    "source_name": "Goodreads (search)",
                    "source_url": search_url,
                }
            )
            seen.add(text)
            if len(out) >= max_quotes:
                break

    if verbose:
        print(f"  collected {len(out)} quotes via quotes/search")
    return out


def build_bundle(category: str, weeks_ago: int, limit: int, top_quotes: int) -> dict:
    nyt_key = os.getenv("NYT_API_KEY", "").strip()
    print(f"Fetching NYT list for '{category}' (weeks_ago={weeks_ago})…")
    bests = get_nyt_bestsellers(
        category=category, weeks_ago=weeks_ago, limit=limit, api_key=nyt_key
    )
    print(f"Got {len(bests)} books. Enriching metadata and collecting quotes…")

    bundle = []
    for idx, b in enumerate(bests, 1):
        title = b.get("title") or ""
        author = b.get("author") or ""
        print(f"[{idx}/{len(bests)}] {title} — {author}")

        gb = get_google_books_by_isbn(b.get("primary_isbn13"))
        if not title:
            title = gb.get("gb_title") or title
        if not author:
            author = gb.get("gb_authors") or author

        quotes = collect_quotes_for_book(title, author, top_quotes=top_quotes)

        entry = {
            "book_id": slugify(f'{title}-{author}-{b.get("primary_isbn13") or ""}'),
            "title": title,
            "author": author,
            "isbn13": b.get("primary_isbn13"),
            "nyt_rank": b.get("nyt_rank"),
            "weeks_on_list": b.get("weeks_on_list"),
            "nyt_category": b.get("nyt_category"),
            "publisher": b.get("publisher") or gb.get("gb_publisher"),
            "pub_year": (gb.get("gb_publishedDate") or "")[:4],
            "cover_url": b.get("book_image") or gb.get("gb_thumbnail"),
            "amazon_product_url": b.get("amazon_product_url"),
            "google_preview": gb.get("gb_previewLink"),
            "quotes": deduplicate_quote_entries(quotes),
            "citations": [
                {
                    "name": "NYT Books API",
                    "url": b.get("nyt_citation"),
                    "purpose": "bestseller rank",
                },
                {
                    "name": "Google Books API",
                    "url": gb.get("google_books_citation"),
                    "purpose": "metadata",
                },
            ],
        }
        # remove None citations
        entry["citations"] = [c for c in entry["citations"] if c["url"]]
        bundle.append(entry)

    bundle.sort(key=lambda x: (x.get("nyt_rank") or 1_000_000))
    return {
        "generated_at": str(date.today()),
        "category": category,
        "weeks_ago": weeks_ago,
        "items": bundle,
    }


def collect_quotes_for_book(title: str, author: str, top_quotes: int = DEFAULT_TOP_QUOTES) -> list:
    """Fetch Goodreads quotes with a quick retry to reduce occasional timeouts."""
    try:
        return fetch_goodreads_quotes(title, author, max_quotes=top_quotes, verbose=True)
    except Exception as exc:
        print(f"  quotes attempt failed ({exc}); retrying in {QUOTE_FETCH_RETRY_DELAY}s…")
        time.sleep(QUOTE_FETCH_RETRY_DELAY)
        try:
            return fetch_goodreads_quotes(title, author, max_quotes=top_quotes, verbose=True)
        except Exception as exc2:
            print(f"  second attempt failed: {exc2}")
            return []


def _find_goodreads_url_via_google(title: str, author: str, verbose: bool = False) -> str | None:
    query = f"site:goodreads.com/quotes {title} {author}"
    print(f"  Google search query: {query}")
    params = {"q": query, "num": "8", "hl": "en"}
    headers = {"User-Agent": "Mozilla/5.0 (quote-harvest/1.0)"}
    try:
        r = requests.get(GOOGLE_SEARCH_URL, params=params, headers=headers, timeout=20)
        if verbose:
            print(f"  google status: {r.status_code}, url: {r.url}")
        r.raise_for_status()
    except Exception as exc:
        if verbose:
            print(f"  google search failed: {exc}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    candidates = []
    for a in soup.select("a"):
        href = a.get("href") or ""
        target = ""
        if "/url?q=" in href:
            try:
                target = href.split("/url?q=", 1)[1].split("&", 1)[0]
            except Exception:
                target = ""
        elif href.startswith("http"):
            target = href
        if target and "goodreads.com" in target and "quotes" in target:
            candidates.append(target)
    if verbose:
        print("  google goodreads links found:", candidates or "<none>")
    return candidates[0] if candidates else None


def _parse_goodreads_quote_page(url: str, max_quotes: int, verbose: bool = False) -> list:
    headers = {"User-Agent": "Mozilla/5.0 (quote-harvest/1.0)"}
    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
    except Exception as exc:
        if verbose:
            print(f"  fetch failed for {url}: {exc}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    blocks = soup.select(".quoteText")
    if not blocks:
        blocks = soup.select(".quotes .quoteText, .quote .quoteText, .quoteDetails .quoteText")
    if verbose:
        print(f"  goodreads page -> {len(blocks)} blocks")

    out, seen = [], set()
    for qb in blocks:
        quote_author = ""
        author_node = qb.find("span", class_="authorOrTitle")
        if author_node:
            quote_author = author_node.get_text(" ", strip=True)
        if not quote_author:
            link = qb.find("a", class_="authorOrTitle")
            if link:
                quote_author = link.get_text(" ", strip=True)
        if not quote_author:
            text_with_author = qb.get_text(separator=" ", strip=True)
            parts = re.split(r"\s+—\s+", text_with_author)
            if len(parts) > 1:
                quote_author = parts[1]

        text = qb.get_text(separator=" ", strip=True)
        text = text.replace("―", "—")
        text = re.sub(r"\s+—\s+.*$", "", text).strip()
        text = re.sub(r"\s+", " ", text)

        if not text or text in seen:
            continue
        if len(text.split()) > MAX_QUOTE_WORDS:
            continue

        out.append(
            {
                "quote_text": text,
                "source_name": "Goodreads (google-assisted)",
                "source_url": url,
            }
        )
        seen.add(text)
        if len(out) >= max_quotes:
            break
    if verbose:
        print(f"  collected {len(out)} quotes via google-assisted page")
    return out


def collect_quotes_via_google_then_goodreads(title: str, author: str, top_quotes: int = DEFAULT_TOP_QUOTES) -> list:
    url = _find_goodreads_url_via_google(title, author, verbose=True)
    if not url:
        print("  No Goodreads link found via Google; skipping quote collection.")
        return []
    quotes = _parse_goodreads_quote_page(url, max_quotes=top_quotes, verbose=True)
    if not quotes:
        print("  Could not collect quotes from the discovered Goodreads page.")
    return quotes


def to_dataframe(bundle: dict) -> pd.DataFrame:
    rows = []
    for it in bundle["items"]:
        q_texts = [q["quote_text"] for q in it["quotes"][:2]]
        rows.append(
            {
                "nyt_rank": it["nyt_rank"],
                "title": it["title"],
                "author": it["author"],
                "isbn13": it["isbn13"],
                "weeks_on_list": it["weeks_on_list"],
                "publisher": it["publisher"],
                "pub_year": it["pub_year"],
                "cover_url": it["cover_url"],
                "amazon_product_url": it["amazon_product_url"],
                "google_preview": it["google_preview"],
                "quote_1": q_texts[0] if len(q_texts) > 0 else "",
                "quote_2": q_texts[1] if len(q_texts) > 1 else "",
            }
        )
    return pd.DataFrame(rows)


# ---------------- Postgres persistence (ISBN-safe) ---------------- #


def _to_int_or_none(v):
    try:
        return int(v)
    except Exception:
        return None


def upsert_book(cur, b: dict) -> str | None:
    """
    ISBN-safe upsert:
    - If a row with this isbn13 exists, UPDATE it and return its existing book_id.
    - Otherwise INSERT a new row (book_id is your slug) and return that book_id.
    """
    isbn = (b.get("isbn13") or "").strip()
    title = b["title"]
    author_primary = (b["author"] or "").split(",")[0][:255]
    publisher = b.get("publisher")
    pub_year = _to_int_or_none(b.get("pub_year"))
    category = b.get("nyt_category")
    cover_url = b.get("cover_url")

    if not isbn:
        cur.execute("select coalesce(is_deleted,false) from books where book_id=%s", (b["book_id"],))
        row = cur.fetchone()
        if row:
            if row[0]:
                print(f"  skipping '{title}' (book_id={b['book_id']}) because it was previously deleted.")
                return None
            cur.execute(
                """update books set title=%s, author_primary=%s, publisher=%s, pub_year=%s,
                          category=%s, cover_url=%s where book_id=%s""",
                (
                    title,
                    author_primary,
                    publisher,
                    pub_year,
                    category,
                    cover_url,
                    b["book_id"],
                ),
            )
            return b["book_id"]
        cur.execute(
            """insert into books (book_id, title, author_primary, isbn13, publisher, pub_year, category, cover_url, is_deleted)
               values (%s,%s,%s,%s,%s,%s,%s,%s,false)""",
            (
                b["book_id"],
                title,
                author_primary,
                None,
                publisher,
                pub_year,
                category,
                cover_url,
            ),
        )
        return b["book_id"]

    # First, see if isbn13 already exists
    cur.execute("select book_id, coalesce(is_deleted,false) from books where isbn13=%s", (isbn,))
    row = cur.fetchone()
    if row:
        existing_id, is_deleted = row
        if is_deleted:
            print(f"  skipping '{title}' (isbn={isbn}) because it was previously deleted (book_id={existing_id}).")
            return None
        # Update existing by its primary key, keep its original book_id
        cur.execute(
            """update books set title=%s, author_primary=%s, publisher=%s, pub_year=%s,
                      category=%s, cover_url=%s where book_id=%s""",
            (
                title,
                author_primary,
                publisher,
                pub_year,
                category,
                cover_url,
                existing_id,
            ),
        )
        # Ensure caller knows to use the existing id
        return existing_id

    # Else insert fresh
    cur.execute(
        """insert into books (book_id, title, author_primary, isbn13, publisher, pub_year, category, cover_url, is_deleted)
           values (%s,%s,%s,%s,%s,%s,%s,%s,false)""",
        (
            b["book_id"],
            title,
            author_primary,
            isbn,
            publisher,
            pub_year,
            category,
            cover_url,
        ),
    )
    return b["book_id"]


def insert_quote(cur, book_id: str, q: dict) -> None:
    cur.execute(
        """insert into quotes (book_id, quote_text, source_name, source_url, is_approved)
           values (%s,%s,%s,%s,false)
           on conflict do nothing""",
        (book_id, q["quote_text"], q.get("source_name"), q.get("source_url")),
    )


def persist_bundle(bundle: dict) -> None:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not set, skipping DB insert.")
        return
    print("Saving bundle into Postgres…")

    # Neon pooler-safe: connect, then set timeout via SQL
    with psycopg.connect(db_url, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute("set session statement_timeout = 15000")
            for b in bundle["items"]:
                # Upsert book by ISBN first; may return an existing book_id
                book_id_in_db = upsert_book(cur, b)
                if not book_id_in_db:
                    print("  -> skipped (deleted book).")
                    continue
                # Quotes handling: if the database already has fewer quotes
                # than the newly-collected set, delete existing quotes and
                # replace them with the new ones. This ensures we grow/refresh
                # quote sets but avoid erasing more comprehensive stored data
                # when the scraper returns fewer quotes (transient/no-results).
                new_qs = deduplicate_quote_entries(b.get("quotes") or [])
                existing_texts = set()
                if new_qs:
                    cur.execute(
                        "select quote_text from quotes where book_id=%s", (book_id_in_db,)
                    )
                    rows = cur.fetchall()
                    existing_texts = {
                        normalize_quote_text(row[0])
                        for row in rows
                        if row and row[0]
                    }
                    stored_count = len(existing_texts)
                    if stored_count < len(new_qs):
                        cur.execute(
                            "delete from quotes where book_id=%s", (book_id_in_db,)
                        )
                        existing_texts.clear()
                        print(
                            f"  replaced {stored_count} stored quotes with {len(new_qs)} for {book_id_in_db}"
                        )
                # Insert quotes against the resolved book_id
                for q in new_qs:
                    normalized = normalize_quote_text(q.get("quote_text"))
                    if not normalized or normalized in existing_texts:
                        continue
                    insert_quote(cur, book_id_in_db, q)
                    existing_texts.add(normalized)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                """
                delete from quotes q
                using quotes dup
                where q.ctid < dup.ctid
                  and q.book_id = dup.book_id
                  and trim(lower(q.quote_text)) = trim(lower(dup.quote_text))
                """
            )
        conn.commit()
    print("Saved bundle into Postgres and removed duplicate quotes. ✅")


def run_nyt_harvest(category: str):
    weeks_ago = DEFAULT_WEEKS_AGO
    limit = DEFAULT_LIMIT
    top_quotes = DEFAULT_TOP_QUOTES
    out_prefix = DEFAULT_OUT_PREFIX

    bundle = build_bundle(category, weeks_ago, limit, top_quotes)

    out_json = f"{out_prefix}_{slugify(category)}.json"
    out_csv = f"{out_prefix}_{slugify(category)}.csv"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(bundle, f, ensure_ascii=False, indent=2)

    df = to_dataframe(bundle)
    df.to_csv(out_csv, index=False, encoding="utf-8")

    print(f"\nGenerated: {out_json} and {out_csv}")
    print(f"Category: {bundle['category']} (weeks_ago={bundle['weeks_ago']})")
    print("\nTop 5 snapshot:")
    for it in bundle["items"][:5]:
        print(
            f"  #{it['nyt_rank']:>2}  {short(it['title'], 60)} — {short(it['author'], 40)}  | quotes: {len(it['quotes'])}"
        )

    persist_bundle(bundle)

    print(
        "\nAlternate sources for 'why-read' highlights (manual/optional):\n"
        " - Kindle Popular Highlights (owned/borrowed ebooks)\n"
        " - Amazon 'Most Helpful' reviews (quoted lines)\n"
        " - Publisher reading guides / press kits\n"
        " - Author interviews / newsletters\n"
        " - Open Library / LibraryThing (community notes)\n"
        " - Readwise (your highlights)\n"
    )


def manual_add_book():
    """Add a single book by ISBN, confirm metadata, and collect quotes."""
    load_dotenv()
    load_dotenv(".env.local", override=True)
    isbn = input("Enter ISBN13 (or ISBN): ").strip()
    if not isbn:
        print("No ISBN entered; cancelling.")
        return

    gb = {}
    try:
        gb = get_google_books_by_isbn(isbn)
    except Exception as exc:
        print(f"Google Books lookup failed: {exc}")

    default_title = gb.get("gb_title", "") or ""
    default_author = gb.get("gb_authors", "") or ""
    if default_title and default_author:
        title, author = default_title, default_author
    else:
        combined = input("Enter Title | Author: ").strip()
        if combined:
            if "|" in combined:
                title_part, author_part = [p.strip() for p in combined.split("|", 1)]
            elif " - " in combined:
                title_part, author_part = [p.strip() for p in combined.split(" - ", 1)]
            else:
                title_part, author_part = combined.strip(), ""
            title = title_part or default_title
            author = author_part or default_author
        else:
            title = default_title
            author = default_author

    if not title or not author:
        print("Title and author are required. Cancelling.")
        return

    confirm = input(f"\nTitle/Author: {title} — {author}\nProceed to fetch quotes? (y/n): ").strip().lower()
    if confirm not in {"y", "yes"}:
        print("Cancelled.")
        return

    quotes = collect_quotes_for_book(title, author, top_quotes=DEFAULT_TOP_QUOTES)

    entry = {
        "book_id": slugify(f"{title}-{author}-{isbn}"),
        "title": title,
        "author": author,
        "isbn13": isbn,
        "nyt_rank": None,
        "weeks_on_list": None,
        "nyt_category": MANUAL_CATEGORY,
        "publisher": gb.get("gb_publisher"),
        "pub_year": (gb.get("gb_publishedDate") or "")[:4],
        "cover_url": gb.get("gb_thumbnail"),
        "amazon_product_url": None,
        "google_preview": gb.get("gb_previewLink"),
        "quotes": deduplicate_quote_entries(quotes),
        "citations": [
            {
                "name": "Google Books API",
                "url": gb.get("google_books_citation"),
                "purpose": "metadata",
            },
        ],
    }
    bundle = {
        "generated_at": str(date.today()),
        "category": MANUAL_CATEGORY,
        "weeks_ago": 0,
        "items": [entry],
    }
    print(f"\nCollected {len(entry['quotes'])} quotes. Saving bundle…")
    persist_bundle(bundle)
    print("Done.")


def main():
    load_dotenv()
    load_dotenv(".env.local", override=True)

    while True:
        print("\nChoose an action:")
        print(" 1) Get NYT bestsellers (advice-how-to-and-miscellaneous)")
        print(" 2) Get NYT bestsellers (combined-print-and-e-book-nonfiction)")
        print(" 3) Get NYT bestsellers (business-books)")
        print(" 4) Get NYT bestsellers (paperback-nonfiction)")
        print(" 5) Add book manually by ISBN")
        print(" q) Quit")
        choice = input("Select an option: ").strip().lower()
        if choice == "q":
            return
        if choice == "1":
            run_nyt_harvest(DEFAULT_CATEGORY)
        elif choice == "2":
            run_nyt_harvest(COMBINED_NONFICTION_CATEGORY)
        elif choice == "3":
            run_nyt_harvest(BUSINESS_BOOKS_CATEGORY)
        elif choice == "4":
            run_nyt_harvest(PAPERBACK_NONFICTION_CATEGORY)
        elif choice == "5":
            manual_add_book()
        else:
            print("Invalid option.")


if __name__ == "__main__":
    main()
