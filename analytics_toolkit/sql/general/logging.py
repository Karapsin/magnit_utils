from __future__ import annotations

from datetime import datetime


def time_print(message: str) -> None:
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] {message}")
