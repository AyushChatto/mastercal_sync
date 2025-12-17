import json
import sys
from datetime import datetime
from typing import Any

def _ts() -> str:
    return datetime.now().isoformat(timespec="seconds")

def log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)

def jdump(obj: Any, *, prefix: str = "", max_chars: int = 8000) -> None:
    try:
        s = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=False, default=str)
    except Exception as e:
        s = f"<jdump failed: {e!r}> {obj!r}"
    if len(s) > max_chars:
        s = s[:max_chars] + f"\n... (truncated, {len(s)} chars total)"
    if prefix:
        log(prefix)
    print(s, flush=True)

def die(msg: str, *, code: int = 1) -> None:
    log("FATAL: " + msg)
    sys.exit(code)
