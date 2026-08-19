"""What every ingest step shares: the day, the landing root, and the record.

THE DAY IS WRITTEN DOWN, not recomputed. Four ingest steps and bronze all have
to agree on one date partition, and `date.today()` called five times can return
two different answers -- once per run, around midnight, on the run nobody is
watching. The first step to land decides; the rest read what it decided.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from .target import host_delta

STATE = Path("state.json")


def _state() -> dict:
    try:
        return json.loads(STATE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def day() -> str:
    """The landing date partition, decided once and reused."""
    st = _state()
    existing = st.get("landing_day")
    if existing:
        return existing
    chosen = dt.date.today().isoformat()
    record(landing_day=chosen)
    return chosen


def record(**fields) -> None:
    """Merge facts into state.json, which provision.py also writes."""
    st = _state()
    st.update(fields)
    STATE.write_text(json.dumps(st, indent=2) + "\n", encoding="utf-8")


def root(vendor: str) -> Path:
    """Host-side landing directory for one vendor's date partition.

    The engine sees the same bytes at `landing_path()`; this is the operator's
    side of the mount. Ingest writes files, not Delta -- bronze's job is to be
    the bytes as they arrived.
    """
    dest = host_delta() / "landing" / vendor / day()
    dest.mkdir(parents=True, exist_ok=True)
    return dest
