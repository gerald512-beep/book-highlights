#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sampler.py
-----------
Generate a preview image using the same layout variables and helpers
as prepublish.py, with easy overrides for fonts, sizes, and canvas.

Usage (examples):
  py sampler.py
  py sampler.py --quote "Small choices compound" --title "Atomic Habits" --author "James Clear"
  py sampler.py --cover-url "https://.../cover.jpg" --width 1080 --height 1350 --margin 64
  py sampler.py --quote-size 52 --title-size 30 --author-size 26

Outputs a PNG in the project root and prints the absolute path.
"""

from __future__ import annotations

import os
import argparse
from typing import Optional, Tuple, List

from PIL import Image, ImageDraw, ImageFilter

# Reuse constants and helpers from prepublish.py for consistency
import prepublish as pp


def compose_background_local(
    cover: Optional[Image.Image],
    width: int,
    height: int,
    card_bg: Tuple[int, int, int],
    overlay_tint: Tuple[int, int, int, int],
    blur_radius: int = 12,
) -> Image.Image:
    """Compose a legible background, mirroring prepublish.compose_background,
    but parameterized by target width/height.
    """
    canvas = Image.new("RGB", (width, height), card_bg)
    if cover is None:
        return canvas

    cover_aspect = cover.width / max(1, cover.height)
    target_aspect = width / max(1, height)
    if cover_aspect > target_aspect:
        new_h = height
        new_w = int(cover_aspect * new_h)
    else:
        new_w = width
        new_h = int(new_w / max(cover_aspect, 1e-6))
    cover_resized = cover.resize((new_w, new_h), Image.LANCZOS)

    x = (width - new_w) // 2
    y = (height - new_h) // 2
    canvas.paste(cover_resized, (x, y))
    canvas = canvas.filter(ImageFilter.GaussianBlur(blur_radius))
    overlay = Image.new("RGBA", (width, height), overlay_tint)
    return Image.alpha_composite(canvas.convert("RGBA"), overlay).convert("RGB")


def draw_centered_lines(
    draw: ImageDraw.ImageDraw,
    box: Tuple[int, int, int, int],
    lines: list[str],
    font,
    fill: Tuple[int, int, int],
    line_spacing: float,
):
    x0, y0, x1, y1 = box
    if not lines:
        return
    line_h = int(font.size * line_spacing)
    total_h = len(lines) * line_h
    y = y0 + ((y1 - y0) - total_h) / 2
    for ln in lines:
        w = draw.textlength(ln, font=font)
        x = x0 + ((x1 - x0) - w) / 2
        draw.text((x, y), ln, font=font, fill=fill)
        y += line_h


def make_sample_image(
    out_path: str,
    quote_text: str,
    title: str,
    author: str,
    cover_url: Optional[str],
    width: int,
    height: int,
    margin: int,
    line_spacing: float,
    quote_font_path: Optional[str],
    title_font_path: Optional[str],
    author_font_path: Optional[str],
    brand_handle: Optional[str],
    brand_font_path: Optional[str] = None,
    label_text: Optional[str] = None,
    quote_size: int = 0,
    title_size: int = 0,
    author_size: int = 0,
) -> str:
    # Background
    cover_img = pp.load_image_from_url(cover_url) if cover_url else None
    bg = compose_background_local(
        cover=cover_img,
        width=width,
        height=height,
        card_bg=pp.CARD_BG,
        overlay_tint=pp.OVERLAY_TINT,
    )
    draw = ImageDraw.Draw(bg)

    # Layout boxes
    x0, y0 = margin, margin
    x1, y1 = width - margin, height - margin
    footer_h = 60
    meta_gap = 24
    meta_block_h = 140
    quote_box = (x0, y0, x1, y1 - meta_block_h - footer_h - meta_gap)
    meta_box = (x0, y1 - meta_block_h - footer_h, x1, y1 - footer_h)

    # Quote text
    clean_text = pp.strip_outer_double_quotes(quote_text or "").strip()
    wrapped_quote = f'"{clean_text}"' if clean_text else ""

    # Quote font (autosize unless explicit size provided)
    if quote_size and quote_size > 0:
        qfont = pp.load_font(quote_font_path, quote_size)
        wrapped = pp.wrap_text(draw, wrapped_quote, qfont, max_width=(quote_box[2] - quote_box[0]))
    else:
        qfont, wrapped = pp.autosize_wrap(
            draw,
            wrapped_quote,
            quote_font_path,
            min_size=34,
            max_size=64,
            max_width=(quote_box[2] - quote_box[0]),
            max_lines=10,
        )
    q_lines = [ln for ln in (wrapped or "").split("\n") if ln]
    draw_centered_lines(draw, quote_box, q_lines, qfont, pp.QUOTE_COLOR, line_spacing)

    # Title font (autosize ~34..50 unless explicit) — ~40% larger than prepublish defaults
    if title_size and title_size > 0:
        tfont = pp.load_font(title_font_path, title_size)
        twrapped = pp.wrap_text(draw, title or "", tfont, meta_box[2] - meta_box[0])
    else:
        tfont, twrapped = pp.autosize_wrap(
            draw,
            title or "",
            title_font_path,
            min_size=34,
            max_size=50,
            max_width=(meta_box[2] - meta_box[0]),
            max_lines=2,
        )
    # Author font (explicit or ~0.9 of title font) — match prepublish defaults
    afont = (
        pp.load_font(author_font_path, author_size)
        if author_size and author_size > 0
        else pp.load_font(author_font_path, max(20, int(tfont.size * 0.9)))
    )

    # Draw title & author (top-left of meta_box)
    tx, ty = meta_box[0], meta_box[1]
    for ln in [ln for ln in (twrapped or "").split("\n") if ln]:
        draw.text((tx, ty), ln, font=tfont, fill=pp.META_COLOR)
        ty += int(tfont.size * line_spacing)
    if author:
        awrapped = pp.wrap_text(draw, author, afont, meta_box[2] - meta_box[0])
        for ln in [ln for ln in (awrapped or "").split("\n") if ln]:
            draw.text((tx, ty), ln, font=afont, fill=pp.BRAND_COLOR)
            ty += int(afont.size * line_spacing)

    # Optional label of the font choice (top-left inside margin)
    if label_text:
        lfont = pp.load_font(title_font_path or pp.DEFAULT_TEXT_FONT, 22)
        draw.text((x0, y0 - 4 + 0), label_text, font=lfont, fill=pp.BRAND_COLOR)

    # Brand handle at footer right
    if brand_handle:
        ffont = pp.load_font(brand_font_path or pp.DEFAULT_TEXT_FONT, 28)
        w = draw.textlength(brand_handle, font=ffont)
        draw.text(
            (width - margin - w, height - margin - 34),
            brand_handle,
            font=ffont,
            fill=pp.BRAND_COLOR,
        )

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    bg.save(out_path, format="PNG", optimize=True)
    return os.path.abspath(out_path)


def _resolve_first(paths: List[str]) -> Optional[str]:
    for p in paths:
        if p and os.path.exists(p):
            return p
    return None


def generate_font_variants(
    base_out_prefix: str,
    quote_text: str,
    title: str,
    author: str,
    cover_url: Optional[str],
    width: int,
    height: int,
    margin: int,
    line_spacing: float,
    brand_handle: Optional[str],
    limit: int = 5,
) -> List[str]:
    """Generate up to `limit` images with different serif-oriented font pairs.
    Regular font is applied to quote/title/brand; bold to author.
    Labels each image with the font family name.
    """
    families = [
        {
            "name": "Georgia",
            "regular": [r"C:\\Windows\\Fonts\\georgia.ttf"],
            "bold": [r"C:\\Windows\\Fonts\\georgiab.ttf"],
        },
        {
            "name": "Garamond",
            "regular": [r"C:\\Windows\\Fonts\\GARA.TTF", r"C:\\Windows\\Fonts\\garamond.ttf"],
            "bold": [r"C:\\Windows\\Fonts\\GARABD.TTF", r"C:\\Windows\\Fonts\\garamondbd.ttf"],
        },
        {
            "name": "Palatino Linotype",
            "regular": [r"C:\\Windows\\Fonts\\pala.ttf"],
            "bold": [r"C:\\Windows\\Fonts\\palab.ttf"],
        },
        {
            "name": "Constantia",
            "regular": [r"C:\\Windows\\Fonts\\constan.ttf"],
            "bold": [r"C:\\Windows\\Fonts\\constanb.ttf"],
        },
        {
            "name": "Calibri",
            "regular": [r"C:\\Windows\\Fonts\\calibri.ttf"],
            "bold": [r"C:\\Windows\\Fonts\\calibrib.ttf"],
        },
        {
            "name": "Times New Roman",
            "regular": [
                r"C:\\Windows\\Fonts\\times.ttf",
                r"C:\\Windows\\Fonts\\times.ttf",
                r"C:\\Windows\\Fonts\\times new roman.ttf",
            ],
            "bold": [
                r"C:\\Windows\\Fonts\\timesbd.ttf",
                r"C:\\Windows\\Fonts\\times new roman bold.ttf",
            ],
        },
        # Linux fallbacks (if you run this outside Windows)
        {
            "name": "DejaVu Serif",
            "regular": [r"/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf"],
            "bold": [r"/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf"],
        },
        {
            "name": "Liberation Serif",
            "regular": [r"/usr/share/fonts/truetype/liberation/LiberationSerif-Regular.ttf"],
            "bold": [r"/usr/share/fonts/truetype/liberation/LiberationSerif-Bold.ttf"],
        },
    ]

    generated: List[str] = []
    for fam in families:
        if len(generated) >= limit:
            break
        reg = _resolve_first(fam.get("regular", []))
        bold = _resolve_first(fam.get("bold", []))
        if not (reg and bold):
            continue
        slug = pp.slugify(fam["name"]) or "font"
        out_path = f"{base_out_prefix}-{slug}.png"
        abs_path = make_sample_image(
            out_path=out_path,
            quote_text=quote_text,
            title=title,
            author=author,
            cover_url=cover_url,
            width=width,
            height=height,
            margin=margin,
            line_spacing=line_spacing,
            quote_font_path=reg,
            title_font_path=reg,
            author_font_path=bold,
            brand_handle=brand_handle,
            brand_font_path=reg,
            label_text=f"Font: {fam['name']}",
        )
        print(f"Generated variant: {abs_path}  (family: {fam['name']})")
        generated.append(abs_path)

    if len(generated) < limit:
        print(
            f"Note: only {len(generated)} font variants were generated because fewer usable font pairs were found."
        )
    return generated


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Generate a sample image and, by default, additional variants "
            "with curated reading-friendly fonts for quotations."
        )
    )
    parser.add_argument("--quote", default="Habit is the intersection of knowledge and action.")
    parser.add_argument("--title", default="Sample Book Title")
    parser.add_argument("--author", default="Sample Author")
    parser.add_argument("--cover-url", default="")
    parser.add_argument("--width", type=int, default=pp.CANVAS_W)
    parser.add_argument("--height", type=int, default=pp.CANVAS_H)
    parser.add_argument("--margin", type=int, default=pp.MARGIN)
    parser.add_argument("--line-spacing", type=float, default=pp.LINE_SPACING)
    parser.add_argument("--quote-font", default=pp.DEFAULT_QUOTE_FONT)
    parser.add_argument("--title-font", default=pp.DEFAULT_TEXT_FONT)
    parser.add_argument("--author-font", default=pp.DEFAULT_TITLE_FONT)
    parser.add_argument("--brand-handle", default=pp.BRAND_HANDLE)
    parser.add_argument("--brand-font", default=None)
    parser.add_argument("--quote-size", type=int, default=0, help="0=auto")
    parser.add_argument("--title-size", type=int, default=0, help="0=auto")
    parser.add_argument("--author-size", type=int, default=0, help="0=auto")
    parser.add_argument("--output", default="sampler_output.png")
    parser.add_argument("--variants", type=int, default=5, help="How many font variants to try (default 5)")
    parser.add_argument("--out-prefix", default="sampler_fonts", help="Prefix for variant files (default sampler_fonts)")
    args = parser.parse_args()

    # Print current variable values
    print("Sampler configuration:")
    print(f"  canvas: {args.width}x{args.height}")
    print(f"  margin: {args.margin}")
    print(f"  line_spacing: {args.line_spacing}")
    print(f"  quote_font: {args.quote_font}  size: {args.quote_size or 'auto(34..64)'}")
    print(f"  title_font: {args.title_font}  size: {args.title_size or 'auto(24..36)'}")
    print(f"  author_font: {args.author_font} size: {args.author_size or '~0.9*title'}")
    print(f"  brand_handle: {args.brand_handle!r}")
    print(f"  colors: quote={pp.QUOTE_COLOR}, meta={pp.META_COLOR}, brand={pp.BRAND_COLOR}, bg={pp.CARD_BG}")
    print(f"  overlay_tint: {pp.OVERLAY_TINT}")
    if args.cover_url:
        print(f"  cover_url: {args.cover_url}")

    out_abs = make_sample_image(
        out_path=args.output,
        quote_text=args.quote,
        title=args.title,
        author=args.author,
        cover_url=args.cover_url.strip() or None,
        width=args.width,
        height=args.height,
        margin=args.margin,
        line_spacing=args.line_spacing,
        quote_font_path=args.quote_font,
        title_font_path=args.title_font,
        author_font_path=args.author_font,
        brand_handle=args.brand_handle,
        brand_font_path=args.brand_font,
        quote_size=args.quote_size,
        title_size=args.title_size,
        author_size=args.author_size,
    )

    print("\nImage generated:")
    print(out_abs)

    # Always generate curated font variants by default (best for quotations)
    print("\nGenerating additional curated font variants...")
    generated = generate_font_variants(
        base_out_prefix=args.out_prefix,
        quote_text=args.quote,
        title=args.title,
        author=args.author,
        cover_url=args.cover_url.strip() or None,
        width=args.width,
        height=args.height,
        margin=args.margin,
        line_spacing=args.line_spacing,
        brand_handle=args.brand_handle,
        limit=max(1, int(args.variants or 5)),
    )
    print("\nVariant images:")
    for p in generated:
        print(" -", p)


if __name__ == "__main__":
    main()
