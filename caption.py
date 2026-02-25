#!/usr/bin/env python3
"""Interactive helper to test the ChatGPT API and craft captions for drafted books."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Iterable, List

try:
    from dotenv import load_dotenv  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None  # type: ignore

import psycopg
from openai import OpenAI

DEFAULT_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
DEFAULT_GENERIC_PROMPT = "Say hello in an enthusiastic but concise way."
DEFAULT_TONE = "curious and hopeful"
DEFAULT_EXTRAS = "Highlight the theme of second chances."
DEFAULT_AFFILIATE_LINK = os.environ.get("AFFILIATE_LINK", "https://example.com/book")
DEFAULT_AFFILIATE_DISCLOSURE = os.environ.get(
    "AFFILIATE_DISCLOSURE",
    "Affiliate disclosure: I may earn a commission from qualifying purchases.",
)


def log_debug(enabled: bool, message: str) -> None:
    if enabled:
        print(f"[debug] {message}")


def load_environment(dotenv_path: Path = Path(".env"), debug: bool = False) -> None:
    """Populate environment variables from `.env` if python-dotenv is absent."""
    if load_dotenv:
        log_debug(debug, "Loading .env via python-dotenv.")
        load_dotenv(dotenv_path)
        load_dotenv(Path(".env.local"), override=True)
        return

    if not dotenv_path.exists():
        log_debug(debug, ".env file not found; skipping manual load.")
        return

    log_debug(debug, "Manually reading .env file.")
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)
    log_debug(debug, "Environment variables loaded from .env.")


def ensure_client(model: str, debug: bool = False) -> tuple[OpenAI, str]:
    log_debug(debug, f"Preparing OpenAI client for model '{model}'.")
    load_environment(debug=debug)
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY not found. Set it in the environment or .env file."
        )
    log_debug(debug, "API key detected; creating OpenAI client.")
    return OpenAI(api_key=api_key), model


def extract_text(chunks: Iterable) -> str:
    """Extract plain text from the response output structure."""
    texts: list[str] = []
    for item in chunks:
        for content in getattr(item, "content", []):
            if getattr(content, "type", None) == "output_text":
                texts.append(content.text)
    final_text = "\n".join(texts).strip()
    return final_text


def send_prompt(client: OpenAI, model: str, prompt: str, debug: bool = False) -> str:
    log_debug(debug, f"Dispatching prompt ({len(prompt)} chars) to '{model}'.")
    response = client.responses.create(
        model=model,
        input=prompt,
    )

    if hasattr(response, "output_text"):
        text = response.output_text.strip()
        log_debug(debug, f"Received {len(text)} chars via output_text.")
        return text

    extracted = extract_text(getattr(response, "output", []))
    log_debug(debug, f"Received {len(extracted)} chars via output[].")
    return extracted


def build_caption_prompt(title: str, author: str, tone: str, extras: str) -> str:
    base = (
        "You are a witty social-media copywriter. "
        f"Write a {tone} caption (max 60 words) for the book '{title}' by {author}. "
        "Make it compelling enough to stop someone mid-scroll and include at least one engaging hashtag."
    )
    if extras:
        base += f" Additional context: {extras}"
    return base


def format_facebook_post(caption: str, affiliate_link: str, disclosure: str) -> str:
    invitation = (
        f"Ready to read it? Grab your copy here: {affiliate_link}"
        if affiliate_link
        else "Ready to read it? Grab your copy today."
    )
    return f"{caption}\n\n{invitation}\n{disclosure}".strip()


def format_instagram_post(caption: str, disclosure: str) -> str:
    invite = "Ready to read it? Visit the link in bio to grab your copy."
    return f"{caption}\n\n{invite}\n{disclosure}".strip()


SHORT_HASHTAG_MAX = 20
PREPUBLISH_ROOT = Path("prepublish_out")
PUBLISH_ROOT = Path("publish_out")


def slugify(value: str) -> str:
    import unicodedata

    value = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    value = "".join(c if c.isalnum() else "-" for c in value.lower())
    while "--" in value:
        value = value.replace("--", "-")
    return value.strip("-") or "post"


def strip_outer_quotes(text: str) -> str:
    s = (text or "").strip()
    quote_pairs = [
        ('"', '"'),
        ("“", "”"),
        ("”", "“"),
        ("„", "“"),
        ("'", "'"),
        ("‘", "’"),
        ("’", "‘"),
        ("«", "»"),
        ("‹", "›"),
    ]
    changed = True
    while changed and len(s) >= 2:
        changed = False
        for left, right in quote_pairs:
            if s.startswith(left) and s.endswith(right):
                s = s[len(left) : len(s) - len(right)].strip()
                changed = True
                break
    return s


def hashtagify(token: str, max_len: int = SHORT_HASHTAG_MAX) -> str:
    cleaned = "".join(ch for ch in token.title() if ch.isalnum())
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len]
    return f"#{cleaned}" if cleaned else ""


def build_hashtag_line(title: str, author: str) -> str:
    tags = []
    author_tag = hashtagify(author)
    title_tag = hashtagify(title)
    if author_tag:
        tags.append(author_tag)
    if title_tag:
        tags.append(title_tag)
    tags.extend(["#Bookstagram", "#MissedPages", "#CommissionsEarned"])
    tags.append("@Missed.Pages.Books")
    return " ".join(tags).strip()


def append_hashtags(caption: str, title: str, author: str) -> str:
    line = build_hashtag_line(title, author)
    if not line:
        return caption.strip()
    base = caption.strip()
    if not base:
        return line
    return f"{base}\n{line}"


def ensure_database_connection(debug: bool = False):
    log_debug(debug, "Opening database connection.")
    load_environment(debug=debug)
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set. Add it to your environment or .env file.")
    try:
        return psycopg.connect(db_url)
    except Exception as exc:  # pragma: no cover - connection errors are runtime issues
        raise RuntimeError(f"Could not connect to database: {exc}") from exc


def fetch_drafted_books(conn) -> list[dict[str, object]]:
    sql = """
    select b.book_id,
           coalesce(b.title,'') as title,
           coalesce(b.author_primary,'') as author,
           coalesce(b.affiliate_url,'') as affiliate_url,
           count(*) as drafted_count
    from quotes q
    join books b on b.book_id=q.book_id
    where coalesce(q.is_drafted,false)=true
    group by b.book_id, b.title, b.author_primary, b.affiliate_url
    order by b.title
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    drafted = []
    for row in rows:
        drafted.append(
            {
                "book_id": row[0],
                "title": row[1],
                "author": row[2],
                "affiliate_url": row[3] or "",
                "drafted_count": int(row[4] or 0),
            }
        )
    return drafted


def fetch_all_books(conn) -> list[dict[str, object]]:
    sql = """
    select b.book_id,
           coalesce(b.title,'') as title,
           coalesce(b.author_primary,'') as author,
           coalesce(b.affiliate_url,'') as affiliate_url,
           count(q.quote_id) as quote_count,
           sum(case when coalesce(q.is_drafted,false) then 1 else 0 end) as drafted_count,
           sum(case when coalesce(q.is_published,false) then 1 else 0 end) as published_count
    from books b
    left join quotes q on q.book_id=b.book_id
    group by b.book_id, b.title, b.author_primary, b.affiliate_url
    order by b.title
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    books: list[dict[str, object]] = []
    for row in rows:
        books.append(
            {
                "book_id": row[0],
                "title": row[1] or "",
                "author": row[2] or "",
                "affiliate_url": row[3] or "",
                "quote_count": int(row[4] or 0),
                "drafted_count": int(row[5] or 0),
                "published_count": int(row[6] or 0),
            }
        )
    return books


def latest_run_key(conn, book_id: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "select run_key from generated_captions where book_id=%s order by created_at desc limit 1",
            (book_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def captions_for_run(conn, book_id: str, run_key: str) -> dict[str, dict[str, object]]:
    sql = """
    select lower(coalesce(variant,'')) as variant,
           caption_path,
           caption_text,
           confirmed_at
    from generated_captions
    where book_id=%s and run_key=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (book_id, run_key))
        rows = cur.fetchall()
    data: dict[str, dict[str, object]] = {}
    for variant, path, text, confirmed_at in rows:
        data[variant] = {
            "path": path,
            "text": text or "",
            "confirmed_at": confirmed_at,
        }
    return data


def update_caption_record(
    conn,
    book_id: str,
    run_key: str,
    variant: str,
    text: str,
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text or "", encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(
            "update generated_captions "
            "set caption_text=%s "
            "where book_id=%s and run_key=%s and lower(coalesce(variant,''))=lower(%s)",
            (text or "", book_id, run_key, variant),
        )
    conn.commit()


def prompt_debug_choice() -> bool:
    while True:
        print("Select debug option:")
        print(" 1) Activate debug")
        print(" 2) Do not activate debug")
        choice = input("Enter choice [1-2]: ").strip()
        if choice == "1":
            return True
        if choice == "2":
            return False
        print("Invalid option. Please choose 1 or 2.\n")


def prompt_primary_action() -> str:
    while True:
        print("\nWhat would you like to do next?")
        print(" 1) Test connection")
        print(" 2) Select book to generate caption")
        print(" 3) Regenerate captions for drafted books")
        print(" 4) Delete book (remove data + files)")
        print(" q) Quit")
        choice = input("Enter choice [1-4 or q]: ").strip().lower()
        if choice in {"1", "2", "3", "4", "q"}:
            return choice
        print("Invalid option. Please enter 1, 2, 3, 4, or q.\n")


def prompt_book_selection(books: list[dict[str, object]]) -> dict[str, object] | None:
    print("\nDrafted books:")
    for idx, book in enumerate(books, start=1):
        print(
            f" {idx:2d}) {book['title']} — {book['author']} "
            f"(drafted quotes: {book['drafted_count']})"
        )
    print(" b) Back to previous menu")
    while True:
        raw = input("Select a book number (or 'b' to go back): ").strip().lower()
        if raw in {"b", "back"}:
            return None
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(books):
                return books[idx - 1]
        print("Invalid selection. Try again.")


def prompt_delete_book_selection(books: list[dict[str, object]]) -> dict[str, object] | None:
    print("\nAll books:")
    for idx, book in enumerate(books, start=1):
        status = "[PB]" if book.get("published_count", 0) else ("[DR]" if book.get("drafted_count", 0) else "[--]")
        print(
            f" {idx:2d}) {status} {book.get('title', '')} — {book.get('author', '')} "
            f"(quotes: {book.get('quote_count', 0)})"
        )
    print(" b) Back to previous menu")
    while True:
        raw = input("Select a book number (or b to go back): ").strip().lower()
        if raw in {"b", "back"}:
            return None
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(books):
                return books[idx - 1]
        print("Invalid selection. Try again.")


def prompt_multi_book_selection(books: list[dict[str, object]]) -> list[dict[str, object]]:
    if not books:
        return []
    print("\nDrafted books:")
    for idx, book in enumerate(books, start=1):
        print(
            f" {idx:2d}) {book['title']} — {book['author']} "
            f"(drafted quotes: {book['drafted_count']})"
        )
    print("Enter comma-separated numbers, 'all' to process everything, or blank to cancel.")
    raw = input("Selection: ").strip().lower()
    if not raw:
        return []
    if raw in {"all", "a"}:
        return books
    picks: List[dict[str, object]] = []
    for token in raw.replace(";", ",").split(","):
        token = token.strip()
        if not token or not token.isdigit():
            continue
        idx = int(token)
        if 1 <= idx <= len(books):
            picks.append(books[idx - 1])
    return picks


def prompt_affiliate_link(initial_link: str | None) -> str:
    current = (initial_link or "").strip()
    while True:
        print("\nAffiliate link setup")
        if current:
            print(f"Current link: {current}")
        else:
            print("Current link: [none]")
        user_input = input("Enter affiliate link (leave blank to keep current): ").strip()
        if user_input:
            current = user_input
        confirmation = input(f"Use this link? [{current or 'none'}] (y/n): ").strip().lower()
        if confirmation in {"y", "yes"}:
            return current
        if confirmation in {"n", "no"}:
            print("Okay, let's try again.")
            continue
        print("Please respond with 'y' or 'n'.")


def resolve_affiliate_link(book: dict[str, object]) -> str:
    base_link = (book.get("affiliate_url") or "").strip()
    override_path = PREPUBLISH_ROOT / slugify(str(book.get("title", ""))) / "newAFlink.txt"
    if override_path.exists():
        override = override_path.read_text(encoding="utf-8").strip()
        if override:
            return override
    return base_link or DEFAULT_AFFILIATE_LINK


def delete_book_everything(conn, book: dict[str, object], debug: bool = False) -> None:
    book_id = str(book.get("book_id"))
    title = str(book.get("title", ""))
    slug = slugify(title)
    statements = [
        "delete from generated_images where book_id=%s",
        "delete from generated_captions where book_id=%s",
        "delete from publications where book_id=%s",
        "delete from quotes where book_id=%s",
        "delete from books where book_id=%s",
    ]
    with conn.cursor() as cur:
        for sql in statements:
            try:
                cur.execute(sql, (book_id,))
                log_debug(debug, f"Executed cleanup: {sql.split()[2]}")
            except Exception as exc:
                log_debug(debug, f"Skipping {sql}: {exc}")
        conn.commit()

    for root in [PREPUBLISH_ROOT / slug, PUBLISH_ROOT / slug]:
        if root.exists():
            shutil.rmtree(root, ignore_errors=True)
            log_debug(debug, f"Removed directory {root}")


def create_caption_bundle(
    title: str,
    author: str,
    *,
    client: OpenAI | None = None,
    model: str = DEFAULT_MODEL,
    tone: str = DEFAULT_TONE,
    extras: str = DEFAULT_EXTRAS,
    affiliate_link: str = DEFAULT_AFFILIATE_LINK,
    affiliate_disclosure: str = DEFAULT_AFFILIATE_DISCLOSURE,
    debug: bool = False,
) -> dict[str, str]:
    """
    Build a caption plus FB/IG variants for the given title/author pair.

    Designed so prepublish.py (or other scripts) can call it directly when the
    book metadata is already known.
    """

    active_client = client
    active_model = model
    if active_client is None:
        active_client, active_model = ensure_client(model, debug)

    log_debug(debug, f"Generating posts for '{title}' by {author}.")
    caption_prompt = build_caption_prompt(title, author, tone, extras)
    raw_caption = strip_outer_quotes(send_prompt(active_client, active_model, caption_prompt, debug))
    caption_text = strip_outer_quotes(append_hashtags(raw_caption, title, author))
    fb_text = strip_outer_quotes(format_facebook_post(caption_text, affiliate_link, affiliate_disclosure))
    ig_text = strip_outer_quotes(format_instagram_post(caption_text, affiliate_disclosure))

    return {
        "prompt": caption_prompt,
        "raw_caption": raw_caption,
        "caption": caption_text,
        "facebook": fb_text,
        "instagram": ig_text,
    }


def generate_posts_for(*args, **kwargs):
    """
    Backwards-compatible wrapper for older callers expecting generate_posts_for.
    """
    return create_caption_bundle(*args, **kwargs)


def handle_test_connection(debug: bool) -> None:
    try:
        client, model = ensure_client(DEFAULT_MODEL, debug)
    except RuntimeError as exc:
        print(exc)
        return

    print(f"\nTesting ChatGPT API via model: {model}")
    try:
        reply = send_prompt(client, model, DEFAULT_GENERIC_PROMPT, debug)
    except Exception as exc:  # pragma: no cover - network errors
        print("Connection test failed:", exc)
        return

    print("\nPrompt:")
    print(DEFAULT_GENERIC_PROMPT)
    print("\nResponse:")
    print(reply or "[No text returned]")


def handle_caption_generation(debug: bool) -> None:
    try:
        conn = ensure_database_connection(debug)
    except RuntimeError as exc:
        print(exc)
        return

    try:
        drafted_books = fetch_drafted_books(conn)
    finally:
        conn.close()

    if not drafted_books:
        print("No drafts available.")
        return

    selection = prompt_book_selection(drafted_books)
    if not selection:
        return

    try:
        client, model = ensure_client(DEFAULT_MODEL, debug)
    except RuntimeError as exc:
        print(exc)
        return

    base_link = (selection.get("affiliate_url") or DEFAULT_AFFILIATE_LINK).strip()
    affiliate_link = prompt_affiliate_link(base_link) or DEFAULT_AFFILIATE_LINK

    post_bundle = create_caption_bundle(
        str(selection.get("title", "")),
        str(selection.get("author", "")),
        client=client,
        model=model,
        tone=DEFAULT_TONE,
        extras=DEFAULT_EXTRAS,
        affiliate_link=affiliate_link,
        affiliate_disclosure=DEFAULT_AFFILIATE_DISCLOSURE,
        debug=debug,
    )

    print("\n--- Caption Prompt ---")
    print(post_bundle["prompt"])
    print("\nCaption Suggestion:")
    print(post_bundle["caption"] or "[No text returned]")
    print("\n--- FB Style ---")
    print(post_bundle["facebook"] or "[No text returned]")
    print("\n--- IG Style ---")
    print(post_bundle["instagram"] or "[No text returned]")


def handle_batch_regeneration(debug: bool) -> None:
    try:
        conn = ensure_database_connection(debug)
    except RuntimeError as exc:
        print(exc)
        return

    try:
        drafted_books = fetch_drafted_books(conn)
        if not drafted_books:
            print("No drafts available.")
            return

        selections = prompt_multi_book_selection(drafted_books)
        if not selections:
            print("No books selected.")
            return

        try:
            client, model = ensure_client(DEFAULT_MODEL, debug)
        except RuntimeError as exc:
            print(exc)
            return

        for book in selections:
            title = str(book.get("title", ""))
            author = str(book.get("author", ""))
            book_id = str(book.get("book_id"))
            slug = slugify(title)
            out_dir = PREPUBLISH_ROOT / slug
            print(f"\nRegenerating captions for {title} — {author}")
            try:
                run_key = latest_run_key(conn, book_id)
            except Exception as exc:
                print("  Could not fetch run key:", exc)
                continue
            if not run_key:
                print("  No caption run found; skipping.")
                continue
            try:
                bundle = create_caption_bundle(
                    title,
                    author,
                    client=client,
                    model=model,
                    tone=DEFAULT_TONE,
                    extras=DEFAULT_EXTRAS,
                    affiliate_link=resolve_affiliate_link(book),
                    affiliate_disclosure=DEFAULT_AFFILIATE_DISCLOSURE,
                    debug=debug,
                )
            except Exception as exc:
                print("  Caption generation failed:", exc)
                continue

            ig_text = strip_outer_quotes(bundle.get("instagram", "")).strip()
            fb_text = strip_outer_quotes(bundle.get("facebook", "")).strip()
            if not ig_text or not fb_text:
                print("  Generated captions were empty; skipping.")
                continue

            try:
                caps = captions_for_run(conn, book_id, run_key)
            except Exception as exc:
                print("  Could not fetch caption records:", exc)
                continue

            ig_path = Path(caps.get("ig", {}).get("path") or out_dir / "IGcaption.txt")
            fb_path = Path(caps.get("fb", {}).get("path") or out_dir / "FBcaption.txt")

            try:
                update_caption_record(conn, book_id, run_key, "ig", ig_text, ig_path)
                update_caption_record(conn, book_id, run_key, "fb", fb_text, fb_path)
            except Exception as exc:
                print("  Failed to persist captions:", exc)
                continue

            print("  Captions regenerated and saved.")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def handle_delete_book(debug: bool) -> None:
    try:
        conn = ensure_database_connection(debug)
    except RuntimeError as exc:
        print(exc)
        return

    try:
        books = fetch_all_books(conn)
        if not books:
            print("No books found in database.")
            return
        selection = prompt_delete_book_selection(books)
        if not selection:
            return
        title = selection.get("title") or "(untitled)"
        author = selection.get("author") or ""
        confirm = input(
            f"Are you sure you want to delete {title} by {author}? This cannot be undone. (y/n): "
        ).strip().lower()
        if confirm not in {"y", "yes"}:
            print("Deletion cancelled.")
            return
        delete_book_everything(conn, selection, debug)
        print(f"Deleted book {title} and associated assets.")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    debug_enabled = prompt_debug_choice()
    while True:
        action = prompt_primary_action()
        if action == "1":
            handle_test_connection(debug_enabled)
        elif action == "2":
            handle_caption_generation(debug_enabled)
        elif action == "3":
            handle_batch_regeneration(debug_enabled)
        elif action == "4":
            handle_delete_book(debug_enabled)
        elif action == "q":
            print("Goodbye!")
            break


if __name__ == "__main__":
    main()
