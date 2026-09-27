from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Callable


def make_logger(log_file: Path, script_name: str) -> Callable[[str], None]:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.write_text(
        f"=== {script_name} RUN {datetime.now().isoformat()} ===\n",
        encoding="utf-8",
    )

    def log(message: str) -> None:
        with log_file.open("a", encoding="utf-8") as handle:
            handle.write(f"{datetime.now().isoformat()} | {message}\n")

    return log
