#!/usr/bin/env python3
"""Simple launcher to jump between project menus."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


def base_dir() -> Path:
    """Return the directory that holds the scripts when frozen or not."""
    if getattr(sys, "frozen", False):
        # PyInstaller onefile extracts to a temp dir, but the exe location is the repo root if copied there.
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


MENU = [
    ("Bestsellers & quotes", "bestsellers_and_quotes.py", []),
    ("Prepublish (images + captions)", "prepublish.py", []),
    ("Caption helper", "caption.py", []),
]


def resolve_python_cmd() -> list[str] | None:
    """Pick a Python interpreter to launch child scripts without recursion."""
    if not getattr(sys, "frozen", False) and sys.executable:
        return [sys.executable]
    candidates: list[list[str]] = []
    env_py = os.environ.get("PYTHON")
    if env_py:
        candidates.append([env_py])
    candidates.extend([[name] for name in ("python", "python3")])
    candidates.append(["py", "-3"])
    candidates.append(["py"])
    for cmd in candidates:
        exe = shutil.which(cmd[0])
        if exe:
            cmd[0] = exe
            return cmd
    return None


def run_script(label: str, script_name: str, extra_args: list[str] | None = None) -> None:
    root = base_dir()
    script_path = root / script_name
    if not script_path.exists():
        print(f"Script not found: {script_path}")
        return

    python_cmd = resolve_python_cmd()
    if not python_cmd:
        print("Could not find a Python interpreter to launch the script.")
        return

    cmd = python_cmd + [str(script_path)]
    if extra_args:
        cmd.extend(extra_args)

    print(f"\n--- Launching {label} ---\n")
    try:
        subprocess.run(cmd, check=False)
    except FileNotFoundError as exc:
        print(f"Could not start {script_path.name}: {exc}")
    print(f"\n--- Returned from {label} ---\n")


def main() -> None:
    while True:
        print("\nMain menu:")
        for idx, (label, _, _) in enumerate(MENU, start=1):
            print(f" {idx}) {label}")
        print(" 4) Quit")
        choice = input("Select an option: ").strip().lower()

        if choice in {"4", "q", "quit", "exit"}:
            print("Goodbye!")
            return
        if choice.isdigit() and 1 <= int(choice) <= len(MENU):
            label, script_name, args = MENU[int(choice) - 1]
            run_script(label, script_name, args)
        else:
            print("Invalid selection. Try again.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted. Exiting.")
