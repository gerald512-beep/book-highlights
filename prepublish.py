#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
prepublish.py (rebuilt, focused)
--------------------------------
Prepare Instagram/Facebook-ready quote images and one caption file per run.

What this version does (clean and minimal):
- Lets you pick a book from Postgres (books + quotes tables).
- Allows selecting which quotes to generate (by numbers).
- Renders images using PIL with blurred-cover background.
- Creates two CTA images (IG and FB) with rotated texts.
- Generates a single caption file ending with "-caption.txt" using a fixed local template
  (never calls external APIs; leaves it simple but editable).
- Records generated images in `generated_images` if the table exists.

Environment:
  DATABASE_URL
"""

from __future__ import annotations

import os
import io
import re
import time
import hashlib as _h
from pathlib import Path
from typing import List, Tuple, Optional
import shutil

from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont, ImageFilter
import requests

try:
    from caption import (
        create_caption_bundle,
        DEFAULT_AFFILIATE_DISCLOSURE as CAPTION_DEFAULT_DISCLOSURE,
        DEFAULT_AFFILIATE_LINK as CAPTION_DEFAULT_AFFILIATE_LINK,
    )
except Exception:  # pragma: no cover - optional dependency
    create_caption_bundle = None  # type: ignore
    CAPTION_DEFAULT_DISCLOSURE = "Disclosure: I may earn a commission from qualifying purchases."
    CAPTION_DEFAULT_AFFILIATE_LINK = "https://example.com/book"


# ---------------- UI + Layout constants ----------------
PAGE_SIZE = 10
CANVAS_W, CANVAS_H = 1080, 1350
MARGIN = 64
LINE_SPACING = 0.94
QUOTE_COLOR = (20, 20, 20)
META_COLOR = (40, 40, 40)
BRAND_COLOR = (90, 90, 90)
CARD_BG = (255, 255, 255)
OVERLAY_TINT = (255, 255, 255, 210)
DEFAULT_HASHTAGS = "#Bookstagram #MissedPages"
DEFAULT_IMAGES_PER_BOOK = 4
BRAND_HANDLE = "@Missed.Pages.Books"
TWITTER_CAPTION_TEMPLATES = [
    "Small read, big upgrade. Grab your copy: {link}\n\nDisclosure: affiliate link.",
    "Fuel your brain a little. It's been through enough. Grab your copy: {link}\n\nDisclosure: affiliate link.",
    "If you needed a sign to start a new book, this is it. Grab your copy: {link}\n\nDisclosure: affiliate link.",
    "A page a day won't fix your life, but it won't hurt either. Grab your copy: {link}\n\nDisclosure: affiliate link.",
]

DEFAULT_TEXT_FONT = r"C:\\Windows\\Fonts\\georgia.ttf"
DEFAULT_TITLE_FONT = r"C:\\Windows\\Fonts\\georgiab.ttf"
DEFAULT_QUOTE_FONT = r"C:\\Windows\\Fonts\\georgia.ttf"

# Holds the most recent raw caption body returned by the LLM (for debugging)
LLM_LAST_OUTPUT: List[str] = []
PREPUBLISH_ROOT = Path("prepublish_out")
PUBLISH_ROOT = Path("publish_out")


# ---------------- Utilities ----------------
def _pick(seq, seed: int, label: str):
    """Deterministically select an element from a list based on a seed."""
    if not seq:
        return None
    h = _h.md5(f"{seed}:{label}".encode("utf-8")).hexdigest()
    idx = int(h[:8], 16) % len(seq)
    return seq[idx]


def slugify(s: str) -> str:
    import unicodedata
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    s = "".join(c if c.isalnum() else "-" for c in s.lower())
    while "--" in s:
        s = s.replace("--", "-")
    return s.strip("-") or "post"


def shortkey(text: str) -> str:
    return _h.md5((text or "").encode("utf-8")).hexdigest()[:8]


def new_run_key(book_id: str) -> str:
    """Return a per-run key so outputs don't collide across batches."""
    return shortkey(f"{book_id}-{int(time.time() * 1000)}")


def load_font(path: Optional[str], size: int) -> ImageFont.FreeTypeFont:
    try:
        if path and os.path.exists(path):
            return ImageFont.truetype(path, size)
        candidates = [
            path,
            "C:\\Windows\\Fonts\\arial.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "arial.ttf",
        ]
        for p in candidates:
            try:
                if p and os.path.exists(p):
                    return ImageFont.truetype(p, size)
            except Exception:
                continue
    except Exception:
        pass
    return ImageFont.load_default()


def load_image_from_url(url: str, timeout=20) -> Optional[Image.Image]:
    if not url:
        return None
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "prepublish/1.0"})
        r.raise_for_status()
        return Image.open(io.BytesIO(r.content)).convert("RGB")
    except Exception:
        return None


def wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont, max_width: int) -> str:
    words = (text or "").strip().split()
    if not words:
        return ""
    lines, line = [], []
    for w in words:
        t = (" ".join(line + [w])) if line else w
        if draw.textlength(t, font=font) <= max_width:
            line.append(w)
        else:
            if line:
                lines.append(" ".join(line))
            line = [w]
    if line:
        lines.append(" ".join(line))
    return "\n".join(lines)


def autosize_wrap(draw, text, font_path, min_size, max_size, max_width, max_lines=None):
    size = max_size
    while size >= min_size:
        f = load_font(font_path, size)
        wrapped = wrap_text(draw, text, f, max_width)
        lines = [ln for ln in wrapped.split("\n") if ln]
        ok = all(draw.textlength(ln, font=f) <= max_width for ln in lines)
        if ok and (max_lines is None or len(lines) <= max_lines):
            return f, wrapped
        size -= 2
    f = load_font(font_path, min_size)
    return f, wrap_text(draw, text, f, max_width)


def compose_background(cover: Optional[Image.Image]) -> Image.Image:
    canvas = Image.new("RGB", (CANVAS_W, CANVAS_H), CARD_BG)
    if cover is None:
        return canvas
    aspect = cover.width / max(1, cover.height)
    target = CANVAS_W / CANVAS_H
    if aspect > target:
        new_h = CANVAS_H
        new_w = int(aspect * new_h)
    else:
        new_w = CANVAS_W
        new_h = int(new_w / max(aspect, 1e-6))
    resized = cover.resize((new_w, new_h), Image.LANCZOS)
    x = (CANVAS_W - new_w) // 2
    y = (CANVAS_H - new_h) // 2
    canvas.paste(resized, (x, y))
    canvas = canvas.filter(ImageFilter.GaussianBlur(12))
    overlay = Image.new("RGBA", (CANVAS_W, CANVAS_H), OVERLAY_TINT)
    return Image.alpha_composite(canvas.convert("RGBA"), overlay).convert("RGB")


def strip_outer_double_quotes(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
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


def make_image(quote_text: str, title: str, author: str, cover_url: Optional[str], out_path: str) -> str:
    cover = load_image_from_url(cover_url) if cover_url else None
    canvas = compose_background(cover)
    draw = ImageDraw.Draw(canvas)

    x0, y0 = MARGIN, MARGIN
    x1, y1 = CANVAS_W - MARGIN, CANVAS_H - MARGIN
    footer_h = 60
    meta_block_h = 140

    quote_box = (x0, y0, x1, y1 - meta_block_h - footer_h - 24)
    meta_box = (x0, y1 - meta_block_h - footer_h, x1, y1 - footer_h)

    clean = strip_outer_double_quotes(quote_text or "")
    wrapped_q = f'"{clean}"' if clean else ""
    qfont, wrapped = autosize_wrap(draw, wrapped_q, DEFAULT_QUOTE_FONT, 34, 64, quote_box[2] - quote_box[0], 10)
    lines = [ln for ln in wrapped.split("\n") if ln]
    if lines:
        total_h = len(lines) * int(qfont.size * LINE_SPACING)
        y = quote_box[1] + ((quote_box[3] - quote_box[1]) - total_h) / 2
        for ln in lines:
            w = draw.textlength(ln, font=qfont)
            x = quote_box[0] + ((quote_box[2] - quote_box[0]) - w) / 2
            draw.text((x, y), ln, font=qfont, fill=QUOTE_COLOR)
            y += int(qfont.size * LINE_SPACING)

    # Meta
    tfont, twrapped = autosize_wrap(draw, (title or "").strip(), DEFAULT_TEXT_FONT, 24, 36, meta_box[2] - meta_box[0], 2)
    afont = load_font(DEFAULT_TITLE_FONT, max(20, int(tfont.size * 0.9)))
    tx, ty = meta_box[0], meta_box[1]
    for ln in [ln for ln in twrapped.split("\n") if ln]:
        draw.text((tx, ty), ln, font=tfont, fill=META_COLOR)
        ty += int(tfont.size * LINE_SPACING)
    author_text = (author or "").strip()
    if author_text:
        for ln in wrap_text(draw, author_text, afont, meta_box[2] - meta_box[0]).split("\n"):
            draw.text((tx, ty), ln, font=afont, fill=BRAND_COLOR)
            ty += int(afont.size * LINE_SPACING)

    ffont = load_font(DEFAULT_TEXT_FONT, 28)
    brand_text = BRAND_HANDLE
    w = draw.textlength(brand_text, font=ffont)
    draw.text((CANVAS_W - MARGIN - w, CANVAS_H - MARGIN - 34), brand_text, font=ffont, fill=BRAND_COLOR)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    canvas.save(out_path, format="PNG", optimize=True)
    return out_path


def make_cta_image(cta_text: str, cover_url: Optional[str], out_path: str) -> str:
    cover = load_image_from_url(cover_url) if cover_url else None
    canvas = compose_background(cover)
    draw = ImageDraw.Draw(canvas)
    x0, y0 = MARGIN, MARGIN
    x1, y1 = CANVAS_W - MARGIN, CANVAS_H - MARGIN
    footer_h = 60
    cta_box = (x0, y0, x1, y1 - footer_h)
    qfont, wrapped = autosize_wrap(draw, (cta_text or "").strip(), DEFAULT_QUOTE_FONT, 34, 64, cta_box[2] - cta_box[0], 10)
    lines = [ln for ln in wrapped.split("\n") if ln]
    if lines:
        total_h = len(lines) * int(qfont.size * LINE_SPACING)
        y = cta_box[1] + ((cta_box[3] - cta_box[1]) - total_h) / 2
        for ln in lines:
            w = draw.textlength(ln, font=qfont)
            x = cta_box[0] + ((cta_box[2] - cta_box[0]) - w) / 2
            draw.text((x, y), ln, font=qfont, fill=QUOTE_COLOR)
            y += int(qfont.size * LINE_SPACING)
    ffont = load_font(DEFAULT_TEXT_FONT, 28)
    brand_text = BRAND_HANDLE
    bw = draw.textlength(brand_text, font=ffont)
    draw.text((CANVAS_W - MARGIN - bw, CANVAS_H - MARGIN - 34), brand_text, font=ffont, fill=BRAND_COLOR)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    canvas.save(out_path, format="PNG", optimize=True)
    return out_path


def export_images_to_pdf(image_paths: List[str], pdf_path: str) -> Optional[str]:
    valid_images = []
    for path in image_paths:
        try:
            img = Image.open(path).convert("RGB")
            new_size = (
                max(1, int(img.width * 0.75)),
                max(1, int(img.height * 0.75)),
            )
            img = img.resize(new_size, Image.LANCZOS)
            valid_images.append(img)
        except Exception:
            continue
    if not valid_images:
        return None
    head, *tail = valid_images
    try:
        head.save(pdf_path, save_all=True, append_images=tail)
        return pdf_path
    finally:
        for img in valid_images:
            try:
                img.close()
            except Exception:
                pass


# ---------------- Caption helpers (no external API) ----------------
def sanitize_body(text: str) -> str:
    """Clean caption body: remove dashes and normalize spaces."""
    t = str(text or "")
    t = t.replace("—", ", ").replace("–", ", ").replace("-", ", ")
    t = re.sub(r"\s+,\s+", ", ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def generate_caption_body_llm_simple(title: str, author: str, out_dir: str, isbn: str = "") -> Optional[str]:
    """Return a deterministic, local caption paragraph (no external API)."""
    title_clean = (title or "").strip() or "this book"
    author_clean = (author or "").strip()
    isbn_note = f" (ISBN {isbn})" if isbn else ""
    hook_options = [
        "Slow down with a book that rewards your full attention",
        "Give yourself a thoughtful pause in the middle of the week",
        "Set aside a quiet moment"
        " and explore a new point of view",
        "Refill your curiosity before the next busy stretch",
    ]
    payoff_options = [
        "Page after page it offers ideas you can revisit whenever you need a reset",
        "It keeps circling back to questions that make great journal prompts later on",
        "It folds practical insights into storytelling that sticks with you",
        "You finish each chapter feeling a little more grounded",
    ]
    hook_seed = int(_h.md5(title_clean.encode("utf-8")).hexdigest(), 16)
    payoff_seed = int(_h.md5((author_clean or title_clean).encode("utf-8")).hexdigest(), 16)
    hook = _pick(hook_options, hook_seed, "hook") or hook_options[0]
    payoff = _pick(payoff_options, payoff_seed, "payoff") or payoff_options[0]
    author_piece = f" by {author_clean}" if author_clean else ""
    body = (
        f"{hook}. {title_clean}{author_piece}{isbn_note} is the one I keep coming back to lately. "
        f"{payoff}. Add it to your stack if you want an intentional read."
    )
    clean = sanitize_body(body)
    LLM_LAST_OUTPUT.clear()
    LLM_LAST_OUTPUT.append(clean)
    return clean


def build_twitter_caption(title: str, author: str, affiliate_link: Optional[str], seed: str) -> str:
    link = (affiliate_link or "{affiliate link}").strip() or "{affiliate link}"
    title_clean = (title or "").strip()
    author_clean = (author or "").strip()
    subject = f"{title_clean} by {author_clean}".strip()
    if subject:
        subject = f" {subject} —"
    try:
        base_seed = int(seed, 16)
    except Exception:
        base_seed = int(_h.md5(seed.encode("utf-8")).hexdigest(), 16) if seed else 0
    template = _pick(TWITTER_CAPTION_TEMPLATES, base_seed, "tw-caption") or TWITTER_CAPTION_TEMPLATES[0]
    text = template.replace("{link}", link)
    return f"{subject} {text}" if subject else text


def _swap_affiliate_line(text: str, new_link: str) -> str:
    if not text:
        return new_link
    lines = text.splitlines()
    link_line = f"Ready to read it? Grab your copy here: {new_link}"
    replaced = False
    for idx, line in enumerate(lines):
        if "Ready to read it? Grab your copy here:" in line:
            lines[idx] = link_line
            replaced = True
            break
        if line.strip().startswith("http") or line.strip().startswith("www"):
            lines[idx] = new_link
            replaced = True
            break
    if not replaced:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(new_link)
    return "\n".join(lines).rstrip()


def _latest_run_key_global(conn, book_id: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            "select run_key from generated_captions where book_id=%s order by created_at desc limit 1",
            (book_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _captions_for_run_global(conn, book_id: str, run_key: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "select lower(coalesce(variant,'')), caption_path, caption_text from generated_captions where book_id=%s and run_key=%s",
            (book_id, run_key),
        )
        rows = cur.fetchall()
    data = {}
    for variant, path, text in rows:
        data[variant] = {"path": path, "text": text or ""}
    return data


def _persist_caption_variant(conn, book_id: str, run_key: str, variant: str, text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text or "", encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(
            "update generated_captions set caption_text=%s, caption_path=%s where book_id=%s and run_key=%s and lower(coalesce(variant,''))=lower(%s)",
            (text or "", str(path), book_id, run_key, variant),
        )
        if cur.rowcount == 0:
            cur.execute(
                "insert into generated_captions (book_id, run_key, variant, caption_path, caption_text) values (%s,%s,%s,%s,%s)",
                (book_id, run_key, variant, str(path), text or ""),
            )
    conn.commit()


def _refresh_affiliate_captions(conn, book_id: str, out_dir: str, title: str, author: str, affiliate_url: str) -> None:
    run_key = _latest_run_key_global(conn, book_id)
    if not run_key:
        print("No caption run found; skipping caption refresh.")
        return
    caps = _captions_for_run_global(conn, book_id, run_key)
    fb = caps.get('fb', {"path": os.path.join(out_dir, "FBcaption.txt"), "text": ""})
    tw = caps.get('tw', {"path": os.path.join(out_dir, "TWcaption.txt"), "text": ""})
    fb_path = Path(fb.get("path") or os.path.join(out_dir, "FBcaption.txt"))
    tw_path = Path(tw.get("path") or os.path.join(out_dir, "TWcaption.txt"))
    new_link = (affiliate_url or CAPTION_DEFAULT_AFFILIATE_LINK).strip()
    new_text = ""
    if create_caption_bundle:
        try:
            bundle = create_caption_bundle(
                title,
                author,
                affiliate_link=new_link or CAPTION_DEFAULT_AFFILIATE_LINK,
                affiliate_disclosure=CAPTION_DEFAULT_DISCLOSURE,
            )
            new_text = (bundle.get("facebook") or "").strip()
        except Exception as exc:
            print("Could not regenerate caption via API; falling back to local update.")
            print("Reason:", exc)
    if not new_text:
        existing = fb.get("text") or ""
        if not existing and fb_path.exists():
            try:
                existing = fb_path.read_text(encoding="utf-8")
            except Exception:
                existing = ""
        if existing:
            new_text = _swap_affiliate_line(existing, new_link or CAPTION_DEFAULT_AFFILIATE_LINK)
    if not new_text:
        print("Unable to refresh FB caption automatically.")
        return
    new_text = strip_outer_double_quotes(new_text).strip()
    _persist_caption_variant(conn, book_id, run_key, 'fb', new_text, fb_path)
    tw_text = strip_outer_double_quotes(build_twitter_caption(title, author, new_link, run_key)).strip()
    _persist_caption_variant(conn, book_id, run_key, 'tw', tw_text, tw_path)
    print("FB + TW captions updated with new affiliate link.")


def ensure_book_delete_columns(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("alter table books add column if not exists is_deleted boolean default false")
        cur.execute("alter table books add column if not exists deleted_at timestamptz")
    conn.commit()


def delete_book_everything(conn, book_id: str, title: str):
    ensure_book_delete_columns(conn)
    statements = [
        "delete from generated_images where book_id=%s",
        "delete from generated_captions where book_id=%s",
        "delete from publications where book_id=%s",
        "delete from quotes where book_id=%s",
        "update books set is_deleted=true, deleted_at=now() where book_id=%s",
    ]
    with conn.cursor() as cur:
        for sql in statements:
            try:
                cur.execute(sql, (book_id,))
            except Exception:
                continue
        conn.commit()
    slug = slugify(title)
    for root in (PREPUBLISH_ROOT / slug, PUBLISH_ROOT / slug):
        try:
            if root.exists():
                shutil.rmtree(root, ignore_errors=True)
        except Exception:
            pass


# ---------------- DB helpers ----------------
def _connect(db_url: str):
    import psycopg
    return psycopg.connect(db_url)


def list_books(conn) -> List[Tuple[str, str, str, str, int, int, int, int]]:
    """Return [(book_id, title, author, cover_url, quotes_count, drafted_count, published_count, available_quotes)]"""
    ensure_book_delete_columns(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            select b.book_id,
                   b.title,
                   b.author_primary,
                   b.cover_url,
                   count(q.*) as quotes,
                   sum(case when coalesce(q.is_drafted,false) then 1 else 0 end) as drafted_count,
                   sum(case when coalesce(q.is_published,false) then 1 else 0 end) as published_count
            from books b
            left join quotes q on q.book_id=b.book_id
            where coalesce(b.is_deleted,false)=false
            group by b.book_id, b.title, b.author_primary, b.cover_url
            having count(q.*) > 0
            order by b.title
            limit 100
            """
        )
        rows = cur.fetchall()

    generated_counts = {}
    available_counts = {}
    try:
        with conn.cursor() as cur:
            cur.execute("select book_id, count(*) from generated_images group by book_id")
            generated_counts = {r[0]: int(r[1] or 0) for r in cur.fetchall()}
    except Exception:
        generated_counts = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select q.book_id, count(*) as available
                from quotes q
                where coalesce(q.is_discarded,false)=false
                  and not exists (
                      select 1 from generated_images gi
                      where gi.quote_id = q.quote_id
                  )
                group by q.book_id
                """
            )
            available_counts = {r[0]: int(r[1] or 0) for r in cur.fetchall()}
    except Exception:
        available_counts = {}

    out = []
    for r in rows:
        book_id = r[0]
        draft_count = int(r[5] or 0)
        draft_count = max(draft_count, generated_counts.get(book_id, 0))
        available = available_counts.get(book_id)
        if available is None:
            available = int(r[4] or 0)
        out.append(
            (
                book_id,
                r[1] or "",
                r[2] or "",
                r[3] or "",
                int(r[4] or 0),
                draft_count,
                int(r[6] or 0),
                int(available or 0),
            )
        )
    return out


def fetch_quotes_for_book(conn, book_id: str) -> List[Tuple[str, str]]:
    with conn.cursor() as cur:
        try:
            cur.execute("select quote_id, quote_text from quotes where book_id=%s and coalesce(is_discarded,false)=false order by created_at asc", (book_id,))
        except Exception:
            cur.execute("select quote_id, quote_text from quotes where book_id=%s order by created_at asc", (book_id,))
        return [(r[0], r[1] or "") for r in cur.fetchall()]


def fetch_quotes_without_images(conn, book_id: str) -> List[Tuple[str, str]]:
    """Return only quotes with no generated images yet."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select q.quote_id, q.quote_text
                from quotes q
                where q.book_id=%s
                  and coalesce(q.is_discarded,false)=false
                  and not exists (
                      select 1 from generated_images gi
                      where gi.quote_id = q.quote_id
                  )
                order by q.created_at asc
                """,
                (book_id,),
            )
            return [(r[0], r[1] or "") for r in cur.fetchall()]
    except Exception:
        # Fallback to previous behavior if the generated_images table is missing or query fails
        quotes = fetch_quotes_for_book(conn, book_id)
        if not quotes:
            return []
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "select distinct quote_id from generated_images where book_id=%s and quote_id is not null",
                    (book_id,),
                )
                seen = {r[0] for r in cur.fetchall() if r[0]}
        except Exception:
            # If table missing or query fails, fall back to all quotes
            return quotes
        return [(qid, qt) for (qid, qt) in quotes if qid not in seen]


def mark_flags(conn, gens: List[str], discs: List[str]):
    with conn.cursor() as cur:
        for qid in gens:
            try:
                cur.execute("update quotes set is_drafted=true, drafted_at=now() where quote_id=%s", (qid,))
            except Exception:
                pass
        for qid in discs:
            try:
                cur.execute("update quotes set is_discarded=true, discarded_at=now() where quote_id=%s", (qid,))
            except Exception:
                pass
        conn.commit()


def record_generated_image(conn, book_id: str, quote_id: Optional[str], run_key: str, file_path: str):
    try:
        with conn.cursor() as cur:
            fhash = None
            try:
                with open(file_path, 'rb') as fh:
                    fhash = _h.md5(fh.read()).hexdigest()
            except Exception:
                pass
            cur.execute(
                """
                insert into generated_images (book_id, quote_id, is_cta, run_key, file_path, width, height, file_hash)
                values (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (book_id, quote_id, quote_id is None, run_key, file_path, CANVAS_W, CANVAS_H, fhash),
            )
            conn.commit()
    except Exception:
        pass


def record_generated_caption(conn, book_id: str, run_key: str, variant: str, caption_path: str, caption_text: str):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into generated_captions (book_id, run_key, variant, caption_path, caption_text)
                values (%s,%s,%s,%s,%s)
                """,
                (book_id, run_key, variant, caption_path, caption_text),
            )
            conn.commit()
    except Exception:
        pass


# ---------------- CLI helpers ----------------
def _status_label(drafted_n: int, published_n: int) -> str:
    if published_n > 0:
        return "[PB]"
    if drafted_n > 0:
        return "[DR]"
    return "[--]"


def pick_from_list(items: List[Tuple[str, str, str, str, int, int, int, int]]) -> Tuple[str, str, str, str]:
    print("Available books (first 100 with quotes):")
    for i, (_, title, author, _, qn, drafted_n, published_n, available_n) in enumerate(items, start=1):
        status_label = _status_label(drafted_n, published_n)
        print(f" {i:2d}. {status_label} {title} - {author} | Quotes: {available_n} of {qn}")
    while True:
        val = input("Select a number (or 'q' to quit): ").strip().lower()
        if val == 'q':
            raise SystemExit(0)
        if val.isdigit():
            n = int(val)
            if 1 <= n <= len(items):
                return items[n-1][0], items[n-1][1], items[n-1][2], items[n-1][3]
        print("Invalid selection.")


def select_quotes_simple(quotes: List[Tuple[str, str]], limit: int) -> Tuple[List[str], List[str]]:
    print("\nQuotes:")
    for i, (_, qt) in enumerate(quotes, start=1):
        text = qt.replace('\n', ' ')
        print(f" [{i:2d}] {text}")
    raw = input(f"Enter numbers to GENERATE (comma separated, up to {limit}). Leave blank to cancel: ").strip()
    if not raw:
        return [], []
    picks = []
    for tok in re.split(r"[,\s]+", raw):
        if tok.isdigit():
            n = int(tok)
            if 1 <= n <= len(quotes):
                picks.append(n)
    picks = picks[:limit]
    gens = [quotes[i-1][0] for i in picks]
    discs = []
    return gens, discs


def handle_delete_book(conn) -> None:
    books = list_books(conn)
    if not books:
        print("No books found.")
        return
    print("\nBooks available for deletion:")
    for i, (book_id, title, author, _, qn, drafted_n, published_n, available_n) in enumerate(books, start=1):
        status_label = _status_label(drafted_n, published_n)
        print(f" {i:2d}. {status_label} {title} - {author} | Quotes: {available_n} of {qn}")
    print(" q) Cancel")
    while True:
        val = input("Select a number to delete (or 'q' to cancel): ").strip().lower()
        if val == 'q':
            return
        if val.isdigit():
            n = int(val)
            if 1 <= n <= len(books):
                book_id, title, author, *_ = books[n-1]
                confirm = input(
                    f"Are you sure you want to delete '{title}' by {author}? This cannot be undone. (y/n): "
                ).strip().lower()
                if confirm == 'y':
                    delete_book_everything(conn, book_id, title)
                    print("Book deleted.")
                else:
                    print("Deletion cancelled.")
                return
        print("Invalid selection.")


def handle_affiliate_refresh(conn) -> None:
    books = list_books(conn)
    drafted = [(b_id, title, author) for (b_id, title, author, _, _, drafted_n, _, _) in books if drafted_n > 0]
    if not drafted:
        print("No drafted books available.")
        return
    print("\nBooks with drafts:")
    for i, (_, title, author) in enumerate(drafted, start=1):
        print(f" {i:2d}. {title} - {author}")
    print(" q) Cancel")
    while True:
        val = input("Select a number to update (or 'q' to cancel): ").strip().lower()
        if val == 'q':
            return
        if val.isdigit():
            n = int(val)
            if 1 <= n <= len(drafted):
                book_id, title, author = drafted[n - 1]
                out_dir = PREPUBLISH_ROOT / slugify(title)
                afl_path = out_dir / "newAFlink.txt"
                if not afl_path.exists():
                    print("No newAFlink.txt found; skipping.")
                    return
                new_link = afl_path.read_text(encoding="utf-8").strip()
                if not new_link:
                    print("Affiliate link file is empty; skipping.")
                    return
                _refresh_affiliate_captions(conn, book_id, str(out_dir), title, author, new_link)
                return
        print("Invalid selection.")


def main():
    load_dotenv()
    load_dotenv(".env.local", override=True)
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL is not set in environment.")
        raise SystemExit(1)
    try:
        conn = _connect(db_url)
    except Exception as e:
        print("Could not connect to database:", e)
        raise SystemExit(1)

    try:
        while True:
            print("\nChoose an action:")
            print(" 1) Generate images + captions")
            print(" 2) Delete a book")
            print(" 3) Refresh FB caption with new affiliate link")
            print(" q) Quit")
            action = input("Select an option: ").strip().lower()
            if action == 'q':
                return
            if action == '2':
                handle_delete_book(conn)
                continue
            if action == '3':
                handle_affiliate_refresh(conn)
                continue
            if action != '1':
                print("Invalid option.")
                continue

            books = list_books(conn)
            if not books:
                print("No books with quotes found.")
                return
            book_id, title, author, cover_url = pick_from_list(books)
            images_n = input(f"How many images/quotes to create [{DEFAULT_IMAGES_PER_BOOK}]: ").strip()
            break
        try:
            images_n = int(images_n) if images_n else DEFAULT_IMAGES_PER_BOOK
        except Exception:
            images_n = DEFAULT_IMAGES_PER_BOOK

        out_dir = os.path.join("prepublish_out", slugify(title))
        os.makedirs(out_dir, exist_ok=True)
        run_key = new_run_key(book_id)

        quotes = fetch_quotes_without_images(conn, book_id)
        if not quotes:
            print("No quotes without images available for this book.")
            return
        gens, discs = select_quotes_simple(quotes, images_n)
        if not gens and not discs:
            print("No selection.")
            return
        mark_flags(conn, gens, discs)

        # Generate images
        generated_files = []
        first_quote_text = None
        for i, (qid, qtext) in enumerate([(qid, qt) for (qid, qt) in quotes if qid in gens], start=1):
            fname = f"{slugify(title)}-{run_key}-{i}.png"
            out_path = os.path.join(out_dir, fname)
            make_image(qtext, title, author, cover_url, out_path)
            generated_files.append(out_path)
            record_generated_image(conn, book_id, qid, run_key, out_path)
            if first_quote_text is None:
                first_quote_text = qtext

        # CTA images
        cta_ig = _pick([
            "If this resonated, read the next chapter. Link in bio.",
            "If this stuck with you, turn the page. Link in bio.",
        ], int(run_key, 16), "ig") or "If this resonated, read the next chapter. Link in bio."
        cta_fb = _pick([
            "If this resonated, read the next chapter. Link in post",
            "If this stuck with you, turn the page. Link in post",
        ], int(run_key, 16), "fb") or "If this resonated, read the next chapter. Link in post"
        ig_path = os.path.join(out_dir, f"{slugify(title)}-{run_key}-cta-IG.png")
        fb_path = os.path.join(out_dir, f"{slugify(title)}-{run_key}-cta-FB.png")
        make_cta_image(cta_ig, cover_url, ig_path)
        generated_files.append(ig_path)
        record_generated_image(conn, book_id, None, run_key, ig_path)
        make_cta_image(cta_fb, cover_url, fb_path)
        generated_files.append(fb_path)
        record_generated_image(conn, book_id, None, run_key, fb_path)

        pdf_path = os.path.join(out_dir, f"{slugify(title)}-{run_key}-images.pdf")
        pdf_sources = [
            path
            for path in generated_files
            if not os.path.basename(path).endswith("-cta-IG.png")
        ]
        pdf_created = export_images_to_pdf(pdf_sources, pdf_path)
        if pdf_created:
            generated_files.append(pdf_created)

        # Hashtags (basic fallback)
        hashtags_line = DEFAULT_HASHTAGS

        # Fetch affiliate link (if any) to include in FB/Other caption
        affiliate_url = None
        try:
            with conn.cursor() as cur:
                cur.execute("select affiliate_url from books where book_id=%s", (book_id,))
                row = cur.fetchone()
                affiliate_url = (row[0] or None) if row else None
        except Exception:
            affiliate_url = None

        caption_bundle = None
        caption_error = None
        if create_caption_bundle:
            try:
                caption_bundle = create_caption_bundle(
                    title,
                    author,
                    affiliate_link=affiliate_url or CAPTION_DEFAULT_AFFILIATE_LINK,
                    affiliate_disclosure=CAPTION_DEFAULT_DISCLOSURE,
                )
            except Exception as exc:
                caption_error = f"Caption generation error: {exc}"
                print("Could not generate ChatGPT caption bundle.")
                print("Reason:", exc)

        if caption_bundle:
            ig_caption_text = caption_bundle.get("instagram", "").strip()
            fb_caption_text = caption_bundle.get("facebook", "").strip()
            if not ig_caption_text or not fb_caption_text:
                caption_bundle = None  # force fallback if incomplete

        if not caption_bundle and caption_error:
            ig_caption_text = caption_error
            fb_caption_text = caption_error
        elif not caption_bundle:
            # Caption bodies (local fallback)
            body = generate_caption_body_llm_simple(title, author, out_dir) or ""
            ig_caption_text = f"{title} - {author}\n{body}\n\n{BRAND_HANDLE} {hashtags_line}".rstrip()
            fb_caption_text = f"{title} - {author}\n{body}\n\n{affiliate_url or '{affiliate link}'}\n\n{BRAND_HANDLE} {hashtags_line}".rstrip()

        tw_caption_text = build_twitter_caption(title, author, affiliate_url, run_key)

        ig_caption_text = strip_outer_double_quotes(ig_caption_text).strip()
        fb_caption_text = strip_outer_double_quotes(fb_caption_text).strip()
        tw_caption_text = strip_outer_double_quotes(tw_caption_text).strip()

        ig_caption_path = os.path.join(out_dir, "IGcaption.txt")
        fb_caption_path = os.path.join(out_dir, "FBcaption.txt")
        tw_caption_path = os.path.join(out_dir, "TWcaption.txt")
        with open(ig_caption_path, "w", encoding="utf-8") as f:
            f.write(ig_caption_text)
        with open(fb_caption_path, "w", encoding="utf-8") as f:
            f.write(fb_caption_text)
        with open(tw_caption_path, "w", encoding="utf-8") as f:
            f.write(tw_caption_text)

        # Create editable placeholders for user-provided overrides if missing
        ig_new_path = os.path.join(out_dir, "IGnewcaption.txt")
        fb_new_path = os.path.join(out_dir, "FBnewcaption.txt")
        if not os.path.exists(ig_new_path):
            with open(ig_new_path, "w", encoding="utf-8") as f:
                f.write("")
        if not os.path.exists(fb_new_path):
            with open(fb_new_path, "w", encoding="utf-8") as f:
                f.write("")
        # Affiliate link editable placeholder file
        afl_path = os.path.join(out_dir, "newAFlink.txt")
        if not os.path.exists(afl_path):
            with open(afl_path, "w", encoding="utf-8") as f:
                f.write(affiliate_url or "")

        # Record captions in DB
        record_generated_caption(conn, book_id, run_key, "ig", ig_caption_path, ig_caption_text)
        record_generated_caption(conn, book_id, run_key, "fb", fb_caption_path, fb_caption_text)
        record_generated_caption(conn, book_id, run_key, "tw", tw_caption_path, tw_caption_text)
        # Captions recorded above for IG and FB variants

        # Print outputs
        print("\nGenerated files:")
        for p in generated_files:
            print(" -", p)
        print("Captions files:\n -", ig_caption_path, "\n -", fb_caption_path, "\n -", tw_caption_path)
        print("\nCaption preview (IG):\n")
        print(ig_caption_text)
        print("\nCaption preview (FB):\n")
        print(fb_caption_text)
        print("\nCaption preview (TW):\n")
        print(tw_caption_text)
        if LLM_LAST_OUTPUT:
            print("\nLLM simple caption body (400-500 chars):\n")
            print(LLM_LAST_OUTPUT[-1])
        print("\nDone. Nothing was posted or scheduled. Assets are ready.")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1].strip().lower() == "review_draft":
        from datetime import datetime

        def _books_with_drafts(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select b.book_id, b.title, b.author_primary, count(*) as drafted
                    from quotes q
                    join books b on b.book_id=q.book_id
                    where coalesce(q.is_drafted,false)=true
                    group by b.book_id, b.title, b.author_primary
                    order by b.title
                    """
                )
                return [(r[0], r[1] or "", r[2] or "", int(r[3] or 0)) for r in cur.fetchall()]

        def _book_info(conn, book_id):
            with conn.cursor() as cur:
                cur.execute("select title, author_primary, cover_url, coalesce(affiliate_url,'') from books where book_id=%s", (book_id,))
                r = cur.fetchone()
                if not r:
                    return None
                return {"title": r[0] or "", "author": r[1] or "", "cover_url": r[2] or "", "affiliate_url": r[3] or ""}

        def _drafted_quotes(conn, book_id):
            with conn.cursor() as cur:
                cur.execute("select quote_id, quote_text from quotes where book_id=%s and coalesce(is_drafted,false)=true order by created_at asc", (book_id,))
                return [(r[0], r[1] or "") for r in cur.fetchall()]

        def _images_for_quote(conn, quote_id):
            with conn.cursor() as cur:
                cur.execute("select image_id, file_path from generated_images where quote_id=%s order by created_at asc", (quote_id,))
                return [(r[0], r[1]) for r in cur.fetchall()]

        def _latest_run_key(conn, book_id):
            with conn.cursor() as cur:
                cur.execute("select run_key from generated_captions where book_id=%s order by created_at desc limit 1", (book_id,))
                r = cur.fetchone()
                return r[0] if r else None

        def _captions_for_run(conn, book_id, run_key):
            with conn.cursor() as cur:
                cur.execute("select variant, caption_path, caption_text, confirmed_at from generated_captions where book_id=%s and run_key=%s", (book_id, run_key))
                out = {}
                for v, p, t, c in cur.fetchall():
                    out[(v or "").lower()] = {"path": p, "text": t, "confirmed_at": c}
                return out

        def _confirm_captions(conn, book_id, run_key):
            with conn.cursor() as cur:
                cur.execute("update generated_captions set confirmed_at=now() where book_id=%s and run_key=%s", (book_id, run_key))
                conn.commit()

        def _set_quotes_approved(conn, book_id):
            with conn.cursor() as cur:
                cur.execute("update quotes set is_approved=true where book_id=%s and coalesce(is_drafted,false)=true", (book_id,))
                conn.commit()

        def _delete_image(conn, image_id, file_path):
            try:
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
            except Exception:
                pass
            try:
                with conn.cursor() as cur:
                    cur.execute("delete from generated_images where image_id=%s", (image_id,))
                    conn.commit()
            except Exception:
                pass

        def _update_caption(conn, book_id, run_key, variant, new_text, path):
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
            except Exception:
                pass
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(new_text or "")
            except Exception:
                pass
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "update generated_captions set caption_text=%s where book_id=%s and run_key=%s and lower(coalesce(variant,''))=lower(%s)",
                        (new_text or "", book_id, run_key, variant),
                    )
                    if cur.rowcount == 0:
                        cur.execute(
                            """
                            insert into generated_captions (book_id, run_key, variant, caption_path, caption_text)
                            values (%s,%s,%s,%s,%s)
                            """,
                            (book_id, run_key, variant, path, new_text or ""),
                        )
                    conn.commit()
            except Exception:
                pass

        def _update_affiliate(conn, book_id, new_url):
            with conn.cursor() as cur:
                cur.execute("update books set affiliate_url=%s where book_id=%s", (new_url, book_id))
                conn.commit()

        def _regenerate_captions(conn, book_id, run_key, title, author, out_dir, affiliate_link, ig_path, fb_path, tw_path):
            if not create_caption_bundle:
                print("Caption module not available; cannot regenerate.")
                return False
            link = (affiliate_link or "").strip() or CAPTION_DEFAULT_AFFILIATE_LINK
            try:
                bundle = create_caption_bundle(
                    title,
                    author,
                    affiliate_link=link,
                    affiliate_disclosure=CAPTION_DEFAULT_DISCLOSURE,
                )
            except Exception as exc:
                print("Caption regeneration failed:", exc)
                return False
            ig_text = strip_outer_double_quotes(bundle.get("instagram", "")).strip()
            fb_text = strip_outer_double_quotes(bundle.get("facebook", "")).strip()
            if not ig_text or not fb_text:
                print("Caption regeneration returned empty text.")
                return False
            ig_text = strip_outer_double_quotes(ig_text).strip()
            fb_text = strip_outer_double_quotes(fb_text).strip()
            _update_caption(conn, book_id, run_key, 'ig', ig_text, ig_path)
            _update_caption(conn, book_id, run_key, 'fb', fb_text, fb_path)
            tw_text = strip_outer_double_quotes(build_twitter_caption(title, author, link, run_key)).strip()
            _update_caption(conn, book_id, run_key, 'tw', tw_text, tw_path)
            print("Captions regenerated.")
            return True

        def _generate_additional_image(conn, book_id, quote_id, out_dir):
            info = _book_info(conn, book_id)
            if not info:
                return None
            qtext = None
            with conn.cursor() as cur:
                cur.execute("select quote_text from quotes where quote_id=%s", (quote_id,))
                r = cur.fetchone()
                qtext = (r[0] or "") if r else ""
            if not qtext:
                return None
            run_key = new_run_key(book_id)
            fname = f"{slugify(info['title'])}-{run_key}-{int(datetime.now().timestamp())}.png"
            out_path = os.path.join(out_dir, fname)
            make_image(qtext, info['title'], info['author'], info['cover_url'], out_path)
            record_generated_image(conn, book_id, quote_id, run_key, out_path)
            return out_path

        # Run the review UI
        load_dotenv()
        load_dotenv(".env.local", override=True)
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            print("DATABASE_URL is not set in environment.")
            raise SystemExit(1)
        try:
            conn = _connect(db_url)
        except Exception as e:
            print("Could not connect to database:", e)
            raise SystemExit(1)
        try:
            books = _books_with_drafts(conn)
            if not books:
                print("No drafts available")
                raise SystemExit(0)
            print("Books with drafts:")
            for i, (_, t, a, n) in enumerate(books, 1):
                print(f" {i:2d}. {t} - {a} | drafted: {n}")
            while True:
                sel = input("Select a number (or 'q' to quit): ").strip().lower()
                if sel == 'q':
                    raise SystemExit(0)
                if sel.isdigit() and 1 <= int(sel) <= len(books):
                    break
                print("Invalid selection.")
            book_id, title, author, _ = books[int(sel)-1]
            out_dir = os.path.join("prepublish_out", slugify(title))
            os.makedirs(out_dir, exist_ok=True)

            while True:
                print("\nReview menu for:", title, "-", author)
                print(" a) Quotes")
                print(" b) Captions")
                print(" c) Affiliate link")
                print(" q) Quit")
                choice = input("Choose an option: ").strip().lower()
                if choice == 'q':
                    break
                elif choice == 'a':
                    # Quotes submenu
                    while True:
                        dq = _drafted_quotes(conn, book_id)
                        if not dq:
                            print("No drafted quotes.")
                            break
                        print("\nDrafted quotes:")
                        img_map = {}
                        for i, (qid, qt) in enumerate(dq, 1):
                            imgs = _images_for_quote(conn, qid)
                            img_map[qid] = imgs
                            files = [os.path.basename(p) for (_, p) in imgs]
                            print(f" [{i:2d}] {qt.replace('\n',' ')}")
                            print("      images:", ", ".join(files) if files else "<none>")
                        print("\n  1) Confirm current selection (approve drafted quotes)")
                        print("  2) Generate an additional image")
                        print("  3) Eliminate an image")
                        print("  4) Back")
                        act = input("Pick: ").strip()
                        if act == '4':
                            break
                        elif act == '1':
                            yn = input("Approve all drafted quotes? (y/n): ").strip().lower()
                            if yn == 'y':
                                _set_quotes_approved(conn, book_id)
                                print("Approved.")
                        elif act == '2':
                            missing = []
                            for idx, (qid, qt) in enumerate(dq, 1):
                                if not img_map.get(qid):
                                    missing.append((idx, qid, qt))
                            if not missing:
                                print("All drafted quotes already have at least one image.")
                                continue
                            print("\nQuotes without images:")
                            for i, (orig_idx, _, qt) in enumerate(missing, 1):
                                print(f" {i:2d}. [#{orig_idx}] {qt.replace('\n', ' ')}")
                            sel = input("Select quote number to render (or blank to cancel): ").strip()
                            if not sel:
                                continue
                            if sel.isdigit() and 1 <= int(sel) <= len(missing):
                                _, qid, _ = missing[int(sel)-1]
                                path = _generate_additional_image(conn, book_id, qid, out_dir)
                                print("Generated:", path or "<failed>")
                                continue
                            print("Invalid selection.")
                        elif act == '3':
                            # collect all images across drafted quotes
                            all_imgs = []
                            for (qid, _) in dq:
                                for (img_id, fp) in img_map.get(qid, []):
                                    all_imgs.append((img_id, fp))
                            if not all_imgs:
                                print("No images to delete.")
                            else:
                                for i, (iid, fp) in enumerate(all_imgs, 1):
                                    print(f" {i:2d}. {fp}")
                                ix = input("Select image number to delete (or blank to cancel): ").strip()
                                if ix and ix.isdigit() and 1 <= int(ix) <= len(all_imgs):
                                    iid, fp = all_imgs[int(ix)-1]
                                    yn = input(f"Delete file and DB row for\n  {fp}\nAre you sure? (y/n): ").strip().lower()
                                    if yn == 'y':
                                        _delete_image(conn, iid, fp)
                                        print("Deleted.")
                        else:
                            print("Invalid option.")
                elif choice == 'b':
                    # Captions submenu
                    run_key = _latest_run_key(conn, book_id)
                    if not run_key:
                        print("No captions found for this book.")
                        continue
                    caps = _captions_for_run(conn, book_id, run_key)
                    ig = caps.get('ig', {"path": os.path.join(out_dir, "IGcaption.txt"), "text": ""})
                    fb = caps.get('fb', {"path": os.path.join(out_dir, "FBcaption.txt"), "text": ""})
                    tw = caps.get('tw', {"path": os.path.join(out_dir, "TWcaption.txt"), "text": ""})
                    print("\nCurrent captions (latest run):")
                    print(" IG:\n" + (ig.get('text') or "<empty>") + "\n")
                    print(" FB:\n" + (fb.get('text') or "<empty>") + "\n")
                    print(" TW:\n" + (tw.get('text') or "<empty>") + "\n")
                    while True:
                        print("  1) Confirm captions")
                        print("  2) Load FBnewcaption.txt")
                        print("  3) Load IGnewcaption.txt")
                        print("  4) Regenerate via ChatGPT (uses newAFlink)")
                        print("  5) Back")
                        act = input("Pick: ").strip()
                        if act == '5':
                            break
                        elif act == '1':
                            yn = input("Mark latest captions as confirmed? (y/n): ").strip().lower()
                            if yn == 'y':
                                _confirm_captions(conn, book_id, run_key)
                                print("Confirmed.")
                        elif act == '2':
                            path = os.path.join(out_dir, "FBnewcaption.txt")
                            try:
                                with open(path, "r", encoding="utf-8") as f:
                                    text = f.read()
                            except Exception:
                                text = ""
                            print("\nNew FB caption candidate:\n")
                            print(text or "<empty>")
                            yn = input("Replace FBcaption.txt and DB text with this? (y/n): ").strip().lower()
                            if yn == 'y':
                                _update_caption(conn, book_id, run_key, 'fb', text, fb.get('path'))
                                print("FB caption updated.")
                        elif act == '3':
                            path = os.path.join(out_dir, "IGnewcaption.txt")
                            try:
                                with open(path, "r", encoding="utf-8") as f:
                                    text = f.read()
                            except Exception:
                                text = ""
                            print("\nNew IG caption candidate:\n")
                            print(text or "<empty>")
                            yn = input("Replace IGcaption.txt and DB text with this? (y/n): ").strip().lower()
                            if yn == 'y':
                                _update_caption(conn, book_id, run_key, 'ig', text, ig.get('path'))
                                print("IG caption updated.")
                        elif act == '4':
                            afl_path = os.path.join(out_dir, "newAFlink.txt")
                            try:
                                with open(afl_path, "r", encoding="utf-8") as f:
                                    override_link = f.read().strip()
                            except Exception:
                                override_link = ""
                            info = _book_info(conn, book_id) or {}
                            link = override_link or info.get('affiliate_url') or ""
                            success = _regenerate_captions(
                                conn,
                                book_id,
                                run_key,
                                info.get('title') or title,
                                info.get('author') or author,
                                out_dir,
                                link,
                                ig.get('path') or os.path.join(out_dir, "IGcaption.txt"),
                                fb.get('path') or os.path.join(out_dir, "FBcaption.txt"),
                                tw.get('path') or os.path.join(out_dir, "TWcaption.txt"),
                            )
                            if success:
                                caps = _captions_for_run(conn, book_id, run_key)
                                ig = caps.get('ig', {"path": os.path.join(out_dir, "IGcaption.txt"), "text": ""})
                                fb = caps.get('fb', {"path": os.path.join(out_dir, "FBcaption.txt"), "text": ""})
                                print("\nUpdated captions:")
                                print(" IG:\n" + (ig.get('text') or "<empty>") + "\n")
                                print(" FB:\n" + (fb.get('text') or "<empty>") + "\n")
                                print(" TW:\n" + (tw.get('text') or "<empty>") + "\n")
                        else:
                            print("Invalid option.")
                elif choice == 'c':
                    # Affiliate link submenu
                    info = _book_info(conn, book_id)
                    afl = (info.get('affiliate_url') or '').strip()
                    print("\nAffiliate link:", afl if afl else "Affiliate link not found")
                    while True:
                        print("  1) Update AFL (read newAFlink.txt)")
                        print("  2) Delete AFL")
                        print("  3) Back")
                        act = input("Pick: ").strip()
                        if act == '3':
                            break
                        elif act == '1':
                            path = os.path.join(out_dir, "newAFlink.txt")
                            try:
                                with open(path, "r", encoding="utf-8") as f:
                                    new_url = f.read().strip()
                            except Exception:
                                new_url = ""
                            print("Candidate affiliate URL:", new_url or "<empty>")
                            yn = input("Update DB with this value? (y/n): ").strip().lower()
                            if yn == 'y':
                                _update_affiliate(conn, book_id, new_url or None)
                                print("Updated.")
                                afl = new_url
                                if info:
                                    info['affiliate_url'] = new_url
                                _refresh_affiliate_captions(
                                    conn,
                                    book_id,
                                    out_dir,
                                    info.get('title') or title,
                                    info.get('author') or author,
                                        new_url,
                                    )
                        elif act == '2':
                            yn = input("Clear affiliate link in DB? (y/n): ").strip().lower()
                            if yn == 'y':
                                _update_affiliate(conn, book_id, None)
                                print("Deleted.")
                        else:
                            print("Invalid option.")
                else:
                    print("Invalid option.")
        finally:
            try:
                conn.close()
            except Exception:
                pass
    else:
        main()
