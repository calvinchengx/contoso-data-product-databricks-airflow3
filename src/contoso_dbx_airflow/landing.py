"""What every ingest step shares: the day, the landing root, and the record.

THE DAY IS WRITTEN DOWN, not recomputed. Four ingest steps and bronze all have
to agree on one date partition, and `date.today()` called five times can return
two different answers -- once per run, around midnight, on the run nobody is
watching. The first step to land decides; the rest read what it decided.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path

from .target import host_delta

# WHERE THE RUN'S SHARED FACTS LIVE -- absolute here, relative in the Jobs leaf,
# and this is the one line where the two copies diverge on purpose.
#
# `Path("state.json")` is correct for a job: one process, one working directory,
# and the file is beside it. Under an orchestrator it is not. Tasks do not share
# a working directory, so `provision` would write the warehouse id somewhere
# `land` never looks, and `day()` -- which exists precisely so every vendor
# lands in ONE date partition -- would decide a fresh day per task and quietly
# scatter the run across partitions that bronze then reads as empty.
#
# So the platform names a path on the volume every task already shares. The
# default keeps a checkout behaving exactly as the Jobs leaf does.
STATE = Path(os.environ.get("CONTOSO_STATE", "state.json"))


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
