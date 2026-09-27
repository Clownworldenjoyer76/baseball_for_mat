from __future__ import annotations

import re
from datetime import UTC, datetime

import pandas as pd


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def sanitize_log_message(message: str) -> str:
    sanitized = str(message)
    sanitized = re.sub(
        r"(?i)\blat\s*=\s*[^,\s|]+",
        "lat=<redacted>",
        sanitized,
    )
    sanitized = re.sub(
        r"(?i)\blon\s*=\s*[^,\s|]+",
        "lon=<redacted>",
        sanitized,
    )
    return re.sub(
        r"\b-?\d{1,3}(?:\.\d+)?\s*,\s*-?\d{1,3}(?:\.\d+)?\b",
        "<redacted-coordinates>",
        sanitized,
    )


def clean_value(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def to_float(value):
    try:
        text = clean_value(value)
        return None if not text else float(text)
    except (TypeError, ValueError):
        return None
