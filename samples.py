#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
samples.py
----------
Tiny helper that can generate one sample image using the same rendering
helpers from prepublish.py, or delegate to prepublish's interactive CLI.

Usage:
  py samples.py --sample   # create one sample image into ./samples/
  py samples.py            # run prepublish CLI (same as `py prepublish.py`)
"""

import os
import sys
import prepublish as pp


def generate_example_sample(out_dir: str = "samples"):
    """
    Create one example image using pp.make_image() and save it under
    a folder named `samples` (relative to repo root).
    """
    try:
        os.makedirs(out_dir, exist_ok=True)
        sample_quote = '"Habit is the intersection of knowledge and action."'
        sample_title = "Sample Book Title"
        sample_author = "Sample Author"
        # no cover URL -> background will be composed from CARD_BG
        sample_fname = f"sample-{pp.slugify(sample_title)}.png"
        out_path = os.path.join(out_dir, sample_fname)
        pp.make_image(sample_quote, sample_title, sample_author, None, out_path)
        print(f"Sample image created: {out_path}")
        return out_path
    except Exception as e:
        print(f"[error] Failed to create sample image: {e}")
        return None


if __name__ == "__main__":
    # quick CLI: use --sample to create a sample image and exit
    if len(sys.argv) > 1 and sys.argv[1] == "--sample":
        generate_example_sample("samples")
        sys.exit(0)
    # otherwise delegate to prepublish's CLI
    pp.main()

