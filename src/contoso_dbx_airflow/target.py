"""This leaf's policy on top of the published databricks-target contract.

THE CONTRACT IS NOT WRITTEN HERE. It is `databricks-target`, published from
databricks-emulator's release and installed by this repo. This file adds only
the decisions that are this leaf's: warehouse name, catalog name, where landing
lives, and how a credential is found.

WHY THIS DIFFERS FROM THE JOBS LEAF'S COPY, in exactly one respect. There, the
defaults are the Jobs platform's PUBLISHED PORTS, because a Databricks Job runs
on the operator's host and reaches the stack through them. Here every task runs
inside the platform's compose network, so the defaults are SERVICE NAMES. That
is the whole of the difference between the two leaves' bindings, and it is a
difference in address rather than in behaviour -- which is the property that
makes comparing the two cells' gold meaningful. Both are overridable, so this
same file drives a real workspace with nothing but environment.
"""

from __future__ import annotations

import os
from pathlib import Path

import databricks_target

WORKSPACE = "contoso-analytics"
WAREHOUSE = "contoso_warehouse"
CATALOG = "contoso"
LANDING_NAME = "landing"
TABLES_NAME = "tables"
# WHERE THE PRODUCT'S OWN FILES ARE, asked of the environment rather than
# derived from this file's location.
#
# `Path(__file__).parent.parent.parent` is what the Jobs leaf uses and it is
# correct THERE, because its steps run from the repository. Here the package is
# INSTALLED: in the worker image that expression resolved to
# `/home/airflow/.local/lib/python3.13`, and the token lookup went looking for
# `.../python3.13/data/admin.pat`. It failed loudly, which is the only reason
# this was a five-minute diagnosis rather than a silent read of the wrong file.
#
# The platform mounts the product and knows where; the product asks. The
# fallback keeps this working from a checkout, where the derivation is right.
ROOT = Path(
    os.environ.get("CONTOSO_PRODUCT_DIR", Path(__file__).resolve().parent.parent.parent)
)


def T():
    """The target, addressed the way a task inside the compose network sees it.

    `setdefault` throughout, never assignment: a real deployment exports these
    and must win. The emulator addresses are a convenience for the local cell,
    not a decision this product is making about where Databricks is.
    """
    os.environ.setdefault("DATABRICKS_EMULATOR_URL", "http://databricks:8447")
    os.environ.setdefault("DATABRICKS_DATA_DIR", str(ROOT / "data"))
    os.environ.setdefault("DATABRICKS_SPARK_CONNECT_URL", "http://spark-agent:8099")
    os.environ.setdefault("DATABRICKS_UC_URL", "http://uc:8080")
    os.environ.setdefault("DATABRICKS_WAREHOUSE", WAREHOUSE)
    os.environ.setdefault("OM_URL", "http://openmetadata:8585/api/v1")
    if not os.environ.get("DATABRICKS_TOKEN"):
        tok = _token()
        if tok:
            os.environ["DATABRICKS_TOKEN"] = tok
    return databricks_target.target()


def _token() -> str:
    """The workspace token, placed here by whichever platform is running us.

    A PRODUCT DOES NOT REACH INTO A PLATFORM -- it is handed a credential. The
    platform's `token` step copies one out of the emulator into the mounted
    product directory, and this reads it. On a real workspace DATABRICKS_TOKEN
    is already exported and `T()` never calls this.

    AN AIRFLOW CONNECTION WOULD BE THE MORE AIRFLOW-SHAPED ANSWER, and the
    Fabric Airflow leaf uses one (`Target.from_connection("fabric")`). It is
    deliberately not used here yet: the Jobs leaf and this one must produce the
    same numbers before anything else about this cell is worth believing, and
    changing how the credential is found at the same time as changing the
    orchestrator would leave two candidate explanations for any difference.
    Recorded as its own step rather than done quietly.
    """
    # The platform names the file; it is the platform that put it there.
    pat = Path(os.environ.get("DATABRICKS_TOKEN_FILE", ROOT / "data" / "admin.pat"))
    try:
        return pat.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise SystemExit(
            f"no workspace token at {pat} ({exc}).\n\n"
            f"The platform places it there -- run this product through the "
            f"platform (`make verify PRODUCT=...`), which depends on its "
            f"`token` step. On a real workspace, export DATABRICKS_TOKEN."
        ) from exc


def landing_path() -> str:
    """Engine-visible landing directory. Name-based; the scheme is the target's."""
    root = os.environ.get("CONTOSO_DELTA", "/data/delta")
    return f"{root}/{LANDING_NAME}"


def tables_path() -> str:
    root = os.environ.get("CONTOSO_DELTA", "/data/delta")
    return f"{root}/{TABLES_NAME}"


def host_delta() -> Path:
    """The same volume, as the code writing landing files sees it.

    The Jobs leaf defaults this to the operator's host path. Here the worker
    container mounts the same volume the engine does, so writer and reader
    agree by mounting rather than by translation.
    """
    return Path(os.environ.get("DELTA_DATA", "/data/delta"))
