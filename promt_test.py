#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simple script to call OpenAI Chat Completions with a fixed prompt and
print the returned content. Expects OPENAI_API_KEY in the environment.
"""

from __future__ import annotations

import os
import sys
import json
from typing import Optional, Dict, Any

import requests

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # Optional dependency
from datetime import datetime, timedelta, timezone


PROMPT = "In a text between 400 and 500 characters, write a text to explain why reading the book Atomic Habits by James Clear"


def _load_env() -> None:
    """Load environment variables from a .env file if python-dotenv is available.
    Tries current working directory, then the script's directory.
    """
    if not load_dotenv:
        return
    try:
        # 1) Default search from CWD upwards
        load_dotenv()
        # 1b) Prefer local untracked overrides if present
        load_dotenv(".env.local", override=True)
        # 2) Explicitly try a .env next to this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        env_path = os.path.join(script_dir, ".env")
        if os.path.exists(env_path):
            load_dotenv(env_path)
        env_local_path = os.path.join(script_dir, ".env.local")
        if os.path.exists(env_local_path):
            load_dotenv(env_local_path, override=True)
    except Exception:
        # Best-effort only
        pass


def _api_key() -> Optional[str]:
    # Primary: OPENAI_API_KEY. Fallback: AZURE_OPENAI_KEY if present.
    return os.getenv("OPENAI_API_KEY") or os.getenv("AZURE_OPENAI_KEY")


def _org_header() -> Dict[str, str]:
    """Optional organization header for multi-org accounts.
    Honors OPENAI_ORG_ID / OPENAI_ORGANIZATION / OPENAI_ORG if present.
    """
    org = (
        os.getenv("OPENAI_ORG_ID")
        or os.getenv("OPENAI_ORGANIZATION")
        or os.getenv("OPENAI_ORG")
        or ""
    ).strip()
    return {"OpenAI-Organization": org} if org else {}


def call_openai_chat(
    prompt: str, *, model: str = "gpt-4o", timeout: int = 30
) -> Optional[str]:
    api_key = _api_key()
    if not api_key:
        print("ERROR: OPENAI_API_KEY is not set in the environment.", file=sys.stderr)
        return None

    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "User-Agent": "promt_test/1.0",
    }
    headers.update(_org_header())
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.7,
        "max_tokens": 600,
        "n": 1,
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        status = resp.status_code
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            snippet = (resp.text or "")[:400]
            print(f"HTTP error {status}: {snippet}", file=sys.stderr)
            return None
        data = resp.json()
        # Defensive parsing
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        text = (content or "").strip()
        return text
    except requests.RequestException as e:
        print(f"Request failed: {e}", file=sys.stderr)
        return None
    except (ValueError, json.JSONDecodeError) as e:
        print(f"Invalid JSON response: {e}", file=sys.stderr)
        return None


def _fmt_usd(amount: float) -> str:
    return f"$ {amount:,.2f}"


def _get_json(url: str, *, timeout: int = 20) -> Optional[Dict[str, Any]]:
    api_key = _api_key()
    if not api_key:
        return None
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "User-Agent": "promt_test/1.0",
    }
    headers.update(_org_header())
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Some accounts do not have access to these legacy endpoints.
            return None
        return r.json()
    except Exception:
        return None


def fetch_billing_status() -> Dict[str, Any]:
    """Fetch subscription, credit grants, and last-30-days usage (USD).

    Returns a dict with optional fields; missing values are None.
    """
    today = datetime.now(timezone.utc).date()
    start_30 = (today - timedelta(days=30)).isoformat()
    end_today = today.isoformat()

    sub = _get_json("https://api.openai.com/v1/dashboard/billing/subscription")
    credits = _get_json("https://api.openai.com/v1/dashboard/billing/credit_grants")
    usage_30 = _get_json(
        f"https://api.openai.com/v1/dashboard/billing/usage?start_date={start_30}&end_date={end_today}"
    )

    hard_limit = None
    if sub is not None:
        hard_limit = sub.get("hard_limit_usd")

    total_granted = total_used = total_available = None
    expires_at = None
    if credits is not None:
        total_granted = credits.get("total_granted", credits.get("grant_total"))
        total_used = credits.get("total_used")
        total_available = credits.get("total_available")
        expires_at = credits.get("grant_expiry_date") or credits.get("expires_at")
        if isinstance(expires_at, (int, float)):
            try:
                expires_at = datetime.fromtimestamp(expires_at, tz=timezone.utc).date().isoformat()
            except Exception:
                pass

    usage_last_30_usd = None
    if usage_30 is not None:
        tu = usage_30.get("total_usage")
        if isinstance(tu, (int, float)):
            usage_last_30_usd = float(tu) / 100.0

    return {
        "hard_limit_usd": hard_limit,
        "total_granted_usd": total_granted,
        "total_used_usd": total_used,
        "total_available_usd": total_available,
        "expires_at": expires_at,
        "usage_last_30_usd": usage_last_30_usd,
        "start_30": start_30,
        "end_today": end_today,
    }


def print_billing_status() -> None:
    print("Checking OpenAI usage/billing status...\n")
    st = fetch_billing_status()
    if not st:
        print("[billing] Unable to retrieve billing info (no API key?)\n")
        return
    if st.get("hard_limit_usd") is not None:
        print("Subscription hard limit:", _fmt_usd(float(st["hard_limit_usd"])) )
    else:
        print("Subscription hard limit: <unknown>")
    tg = st.get("total_granted_usd")
    tu = st.get("total_used_usd")
    ta = st.get("total_available_usd")
    exp = st.get("expires_at")
    if any(v is not None for v in (tg, tu, ta)):
        if tg is not None:
            print("Credit grants total:", _fmt_usd(float(tg)))
        if tu is not None:
            print("Credit grants used:", _fmt_usd(float(tu)))
        if ta is not None:
            print("Credit grants remaining:", _fmt_usd(float(ta)))
        if exp:
            print("Credit grants expire:", exp)
    else:
        print("Credit grants: <unavailable>")
    u30 = st.get("usage_last_30_usd")
    if u30 is not None:
        print(
            f"Usage last 30 days ({st['start_30']} → {st['end_today']}):",
            _fmt_usd(float(u30)),
        )
    else:
        print("Usage last 30 days: <unavailable>")
    print()


def main() -> int:
    _load_env()
    print_billing_status()
    try:
        ans = input("Proceed with the chat request now? [Y/n]: ").strip().lower()
    except EOFError:
        ans = "y"
    if ans and ans.startswith("n"):
        print("Aborted by user.")
        return 0
    print("\nCalling OpenAI with the fixed prompt...\n")
    text = call_openai_chat(PROMPT)
    if text is None:
        return 1
    print(text)
    # Helpful footer: show character count to check the 400-500 range
    print("\n---\nLength:", len(text), "characters")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
