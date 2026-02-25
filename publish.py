#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
publish.py
----------
Publish pre-generated quote images and captions to social accounts.

Current automation:
  - LinkedIn Page (via UGC posts)
  - X / Twitter account (via v1.1 media + statuses endpoints)

Facebook + Instagram:
  *Not automated yet*. Menu entries exist only to remind you to post
  manually; once you confirm, the run is marked as published.

Prerequisites:
  - DATABASE_URL (same one used by prepublish.py)
  - LINKEDIN_ACCESS_TOKEN  (Bearer token with rw_organization_admin + w_member_social)
  - LINKEDIN_ORG_URN       (e.g., urn:li:organization:123456)
  - TWITTER_API_KEY / TWITTER_API_SECRET
  - TWITTER_ACCESS_TOKEN / TWITTER_ACCESS_SECRET

Outputs:
  - Updates Postgres (`quotes.is_published`, `quotes.published_at`,
    `generated_captions.published_at`)
  - Appends per-run metadata under publish_out/<slug>/publish-log.json
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import psycopg
import requests
from dotenv import load_dotenv
from requests_oauthlib import OAuth1


BASE_DIR = Path(__file__).parent
PREPUBLISH_ROOT = BASE_DIR / "prepublish_out"
PUBLISH_ROOT = BASE_DIR / "publish_out"
PUBLISH_ROOT.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def slugify(value: str) -> str:
    import unicodedata

    value = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    value = "".join(c if c.isalnum() else "-" for c in value.lower())
    while "--" in value:
        value = value.replace("--", "-")
    return value.strip("-") or "post"


def _connect(db_url: str):
    return psycopg.connect(db_url)


def _fetch_ready_books(conn) -> List[Dict[str, str]]:
    """
    Return books with approved (but unpublished) quotes and confirmed captions.
    """
    sql = """
    with approved as (
        select book_id, count(*) as approved
        from quotes
        where coalesce(is_approved,false)=true
          and coalesce(is_published,false)=false
        group by book_id
    ),
    captions as (
        select distinct on (book_id) book_id, run_key, confirmed_at
        from generated_captions
        where confirmed_at is not null
        order by book_id, confirmed_at desc
    )
    select b.book_id,
           b.title,
           coalesce(b.author_primary,'') as author,
           coalesce(b.affiliate_url,'') as affiliate_url,
           a.approved,
           c.run_key
    from approved a
    join books b on b.book_id=a.book_id
    join captions c on c.book_id=b.book_id
    order by b.title
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [
        {
            "book_id": r[0],
            "title": r[1],
            "author": r[2],
            "affiliate_url": r[3] or "",
            "approved": int(r[4] or 0),
            "run_key": r[5],
        }
        for r in rows
    ]


def _captions_for_run(conn, book_id: str, run_key: str) -> Dict[str, Dict[str, str]]:
    sql = """
    select lower(coalesce(variant,'')) as variant, caption_path, caption_text, confirmed_at
    from generated_captions
    where book_id=%s and run_key=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (book_id, run_key))
        rows = cur.fetchall()
    data = {}
    for variant, path, text, confirmed_at in rows:
        data[variant] = {
            "path": path,
            "text": text or "",
            "confirmed_at": confirmed_at.isoformat() if confirmed_at else None,
        }
    return data


def _images_for_run(conn, book_id: str, run_key: str) -> List[Dict[str, Optional[str]]]:
    sql = """
    select quote_id, file_path, coalesce(is_cta,false) as is_cta
    from generated_images
    where book_id=%s and run_key=%s
    order by file_path
    """
    with conn.cursor() as cur:
        cur.execute(sql, (book_id, run_key))
        rows = cur.fetchall()
    images = []
    for quote_id, file_path, is_cta in rows:
        images.append(
            {
                "quote_id": quote_id,
                "file_path": file_path,
                "is_cta": bool(is_cta),
            }
        )
    return images


def _mark_quotes_published(conn, quote_ids: Sequence[str]):
    if not quote_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            "update quotes set is_published=true, published_at=now() where quote_id = any(%s)",
            (list(quote_ids),),
        )
    conn.commit()


def _mark_captions_published(conn, book_id: str, run_key: str, variants: Sequence[str]):
    if not variants:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            update generated_captions
            set published_at = now()
            where book_id=%s and run_key=%s and lower(coalesce(variant,'')) = any(%s)
            """,
            (book_id, run_key, [v.lower() for v in variants]),
        )
    conn.commit()


def _append_publish_log(slug: str, entry: Dict):
    out_dir = PUBLISH_ROOT / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "publish-log.json"
    history: List[Dict] = []
    if log_path.exists():
        try:
            history = json.loads(log_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            history = []
    history.append(entry)
    log_path.write_text(json.dumps(history, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# LinkedIn (manual until business verification completes)
# ---------------------------------------------------------------------------
def linkedin_ready() -> bool:
    return False


def linkedin_test_connection() -> bool:
    print("[linkedin] Automated posting disabled (awaiting business verification).")
    return False


# ---------------------------------------------------------------------------
# Twitter / X
# ---------------------------------------------------------------------------
def twitter_ready() -> bool:
    required = [
        "TWITTER_API_KEY",
        "TWITTER_API_SECRET",
        "TWITTER_ACCESS_TOKEN",
        "TWITTER_ACCESS_SECRET",
    ]
    return all(os.getenv(k) for k in required)


def twitter_oauth() -> OAuth1:
    return OAuth1(
        os.environ["TWITTER_API_KEY"].strip(),
        os.environ["TWITTER_API_SECRET"].strip(),
        os.environ["TWITTER_ACCESS_TOKEN"].strip(),
        os.environ["TWITTER_ACCESS_SECRET"].strip(),
    )


def twitter_test_connection() -> bool:
    if not twitter_ready():
        print("[twitter] Missing API credentials")
        return False
    try:
        resp = requests.get(
            "https://api.twitter.com/1.1/account/verify_credentials.json",
            auth=twitter_oauth(),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        print(f"[twitter] OK: @{data.get('screen_name')}")
        return True
    except Exception as exc:
        print("[twitter] connection error:", exc)
        return False


def _twitter_upload_media(image_path: Path) -> str:
    with image_path.open("rb") as fh:
        files = {"media": fh}
        resp = requests.post(
            "https://upload.twitter.com/1.1/media/upload.json",
            auth=twitter_oauth(),
            files=files,
            timeout=60,
        )
    resp.raise_for_status()
    data = resp.json()
    return data["media_id_string"]


def _twitter_post_status(text: str, media_id: str) -> str:
    payload = {"status": text, "media_ids": media_id}
    resp = requests.post(
        "https://api.twitter.com/1.1/statuses/update.json",
        auth=twitter_oauth(),
        data=payload,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return str(data.get("id"))


def truncate_for_twitter(text: str) -> str:
    limit = 280
    t = text.strip()
    if len(t) <= limit:
        return t
    return t[: limit - 1].rstrip() + "…"


def publish_to_twitter(images: List[Path], caption: str) -> List[Dict[str, str]]:
    if not images:
        return []
    results = []
    for img in images:
        print(f"[twitter] uploading {img.name} …")
        media_id = _twitter_upload_media(img)
        status = truncate_for_twitter(caption)
        tweet_id = _twitter_post_status(status, media_id)
        results.append({"image": img.name, "tweet_id": tweet_id})
        print(f"[twitter] tweeted {img.name} → {tweet_id}")
    return results


# ---------------------------------------------------------------------------
# Menus / Workflows
# ---------------------------------------------------------------------------
def ask(prompt: str) -> str:
    return input(prompt).strip()


def confirm(prompt: str) -> bool:
    return ask(prompt + " (y/n): ").lower() == "y"


def test_connections():
    print("\nTesting connectors …")
    linkedin_ok = linkedin_test_connection()
    twitter_ok = twitter_test_connection()
    print("[facebook] Manual posting required; no API test.")
    print("[instagram] Manual posting required; no API test.")
    if linkedin_ok and twitter_ok:
        print("All automated connectors responded successfully.")


def _resolve_image_paths(entries: List[Dict[str, Optional[str]]]) -> List[Path]:
    paths = []
    for item in entries:
        if not item.get("file_path"):
            continue
        path = Path(item["file_path"])
        if not path.is_absolute():
            path = BASE_DIR / path
        if path.exists():
            paths.append(path)
        else:
            print(f"[warn] Missing image file: {path}")
    return paths


def _load_publish_payload(conn, book: Dict) -> Optional[Dict]:
    run_key = book["run_key"]
    captions = _captions_for_run(conn, book["book_id"], run_key)
    fb_caption = captions.get("fb", {}).get("text") or ""
    ig_caption = captions.get("ig", {}).get("text") or ""
    if not fb_caption or not ig_caption:
        print("Captions missing; re-run review_draft to confirm them.")
        return None
    images = _images_for_run(conn, book["book_id"], run_key)
    if not images:
        print("No generated images found for this run.")
        return None
    resolved = _resolve_image_paths(images)
    if not resolved:
        print("Image files missing from disk.")
        return None
    slug = slugify(book["title"])
    return {
        "book": book,
        "run_key": run_key,
        "slug": slug,
        "images": resolved,
        "fb_caption": fb_caption,
        "ig_caption": ig_caption,
        "quote_ids": [item["quote_id"] for item in images if item["quote_id"]],
    }


def publish_flow(conn):
    books = _fetch_ready_books(conn)
    if not books:
        print("No ready books (need approved quotes + confirmed captions).")
        return
    print("\nBooks ready to publish:")
    for i, b in enumerate(books, 1):
        print(f" {i:2d}. {b['title']} - {b['author']} (approved quotes: {b['approved']})")
    choice = ask("Select a number (or 'q' to cancel): ").lower()
    if choice == "q":
        return
    if not choice.isdigit() or not (1 <= int(choice) <= len(books)):
        print("Invalid selection.")
        return
    book = books[int(choice) - 1]
    payload = _load_publish_payload(conn, book)
    if not payload:
        return
    images = payload["images"]
    slug = payload["slug"]
    fb_caption = payload["fb_caption"]
    ig_caption = payload["ig_caption"]

    log_entry = {
        "timestamp": dt.datetime.utcnow().isoformat() + "Z",
        "book_id": book["book_id"],
        "title": book["title"],
        "run_key": payload["run_key"],
        "results": {},
    }

    twitter_results: List[Dict[str, str]] = []
    linkedin_success = False
    twitter_success = False

    linkedin_manual = confirm("Have you manually posted to LinkedIn?") if fb_caption else False
    log_entry["results"]["linkedin_manual"] = linkedin_manual
    linkedin_success = linkedin_manual

    if twitter_ready():
        if confirm("Publish to Twitter (X) now?"):
            try:
                twitter_results = publish_to_twitter(images, fb_caption)
                twitter_success = True
                log_entry["results"]["twitter"] = twitter_results
            except Exception as exc:
                print("[twitter] publish error:", exc)
                log_entry["results"]["twitter_error"] = str(exc)
    else:
        print("[twitter] credentials missing; skipping.")

    fb_manual = confirm("Have you manually posted to Facebook?") if fb_caption else False
    ig_manual = confirm("Have you manually posted to Instagram?") if ig_caption else False
    log_entry["results"]["facebook_manual"] = fb_manual
    log_entry["results"]["instagram_manual"] = ig_manual

    all_done = linkedin_success and twitter_success and fb_manual and ig_manual
    if all_done:
        _mark_quotes_published(conn, payload["quote_ids"])
        _mark_captions_published(conn, book["book_id"], payload["run_key"], ["fb", "ig"])
        print("Statuses updated: quotes marked as published.")
    else:
        print("Not all platforms confirmed; DB status left unchanged.")

    _append_publish_log(slug, log_entry)
    print(f"Log written to publish_out/{slug}/publish-log.json")


def main():
    load_dotenv()
    load_dotenv(".env.local", override=True)
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is required.")
        sys.exit(1)
    try:
        conn = _connect(db_url)
    except Exception as exc:
        print("Could not connect to database:", exc)
        sys.exit(1)

    try:
        while True:
            print("\nPublish menu")
            print(" 1) Test connections")
            print(" 2) Publish book")
            print(" q) Quit")
            choice = ask("Choose an option: ").lower()
            if choice == "1":
                test_connections()
            elif choice == "2":
                publish_flow(conn)
            elif choice == "q":
                break
            else:
                print("Invalid option.")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
