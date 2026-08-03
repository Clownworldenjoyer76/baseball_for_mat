#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
from datetime import datetime
import os
import logging
from zoneinfo import ZoneInfo

AUTO_PUSH = False
FILES_TO_COMMIT = [
    "data/adjusted/batters_home.csv",
    "data/adjusted/batters_away.csv",
]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def clean_output(text_bytes):
    return text_bytes.decode().strip() if text_bytes else ""

def run_git_command(parts, success, error, log_output=False):
    try:
        result = subprocess.run(parts, check=True, capture_output=True, text=True)
        logger.info(success)
        if log_output:
            if result.stdout:
                logger.debug(f"Git stdout:\n{clean_output(result.stdout.encode())}")
            if result.stderr:
                logger.debug(f"Git stderr:\n{clean_output(result.stderr.encode())}")
        return True
    except FileNotFoundError:
        logger.critical("❌ 'git' not found in PATH.")
        exit(1)
    except subprocess.CalledProcessError as e:
        logger.error(f"{error}: {' '.join(parts)}")
        if e.stdout:
            logger.error(f"Git stdout:\n{clean_output(e.stdout.encode())}")
        if e.stderr:
            logger.error(f"Git stderr:\n{clean_output(e.stderr.encode())}")
        return False

def has_staged_changes():
    result = subprocess.run(["git", "diff", "--cached", "--exit-code"],
                            capture_output=True)
    return result.returncode == 1

def main():
    ts = datetime.now(ZoneInfo("America/New_York")).strftime(
        "%Y-%m-%d %I:%M:%S %p %Z"
    )

    run_git_command(["git", "config", "user.name", "github-actions"],
                    "✅ Git user.name set", "❌ Failed to set user.name")
    run_git_command(["git", "config", "user.email",
                     "github-actions@github.com"],
                    "✅ Git user.email set", "❌ Failed to set user.email")

    missing = [f for f in FILES_TO_COMMIT if not os.path.exists(f)]
    if missing:
        for f in missing:
            logger.error(f"❌ Missing required file: {f}")
        logger.critical("🚫 Aborting Git commit due to missing inputs.")
        exit(1)

    logger.info(f"🔁 Staging files: {', '.join(FILES_TO_COMMIT)}")
    if not run_git_command(["git", "add"] + FILES_TO_COMMIT,
                           "✅ Git add successful", "❌ Git add failed"):
        return

    if not has_staged_changes():
        logger.warning("⚠️ No changes detected. Nothing to commit.")
        return

    msg = f"🔄 Split batters home/away @ {ts}"
    logger.info(f"📝 Committing: {msg}")
    if not run_git_command(["git", "commit", "-m", msg],
                           "✅ Git commit successful",
                           "❌ Git commit failed",
                           log_output=True):
        return

    if AUTO_PUSH:
        logger.info("🚀 Pushing changes to remote...")
        run_git_command(["git", "push"],
                        "✅ Git push successful", "❌ Git push failed")

if __name__ == "__main__":
    main()
