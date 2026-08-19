"""Render the product's gold project to a dbt manifest, for cosmos to load.

WHY A MANIFEST AND NOT `dbt ls`. Cosmos renders the DAG at PARSE time, and its
default way of doing that is to run `dbt ls` -- which needs dbt in whatever
environment the dag-processor runs in. Here it cannot be there: dbt-databricks
1.12.4 pins databricks-sdk<0.118, databricks-connect 19.1 needs >=0.122, and uv
proves the two unsatisfiable. So dbt lives in its own virtualenv that only the
gold TASKS enter, and parse time has no dbt at all.

WHAT COSMOS DOES WHEN dbt IS MISSING is the reason this file exists rather than
being left to the default. `LoadMode.AUTOMATIC` catches the FileNotFoundError
and silently falls back to a custom parser -- deprecated, documented as "the
least accurate way", and a subset of what dbt knows. The DAG would still
appear, still run, and still be missing whatever the subset dropped. A graph
that renders successfully while omitting the contracts is precisely the failure
this product keeps meeting: a guard that reports success while doing nothing.

So the manifest is built explicitly, by dbt itself, and the DAG loads it with
`LoadMode.DBT_MANIFEST` -- no fallback, no subset.

STAMPED WITH THE PRODUCT VERSION IT WAS BUILT FROM. A manifest is a generated
artifact describing a pinned package's SQL; if the pin moves and this does not,
cosmos renders yesterday's models against today's code and says nothing. The
DAG refuses to render a manifest whose stamp disagrees with the installed
package -- a false green from a stale artifact has cost this family a day
before.
"""

from __future__ import annotations

import json
import os
import subprocess
from importlib.metadata import version
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROFILES = ROOT / "dbt" / "gold"
TARGET = PROFILES / "target"

# The dbt environment, named here and in pyproject.toml's `dbt` group. Kept
# equal to it deliberately: this runs `--isolated`, so it reads no lockfile and
# a drift between the two would mean the manifest was rendered by a different
# dbt than the tasks run.
DBT_REQUIREMENTS = ("dbt-core>=1.9,<2", "dbt-databricks==1.12.4")


def main() -> int:
    from contoso_product import gold_dir

    project = gold_dir()
    core = version("contoso-data-product")

    cmd = ["uv", "run", "--isolated", "--no-project"]
    for req in DBT_REQUIREMENTS:
        cmd += ["--with", req]
    cmd += [
        "dbt",
        "parse",
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(PROFILES),
        "--target-path",
        str(TARGET),
    ]
    # GOLD'S sources.yml DEMANDS THESE AT PARSE TIME, and not because the shared
    # project is careless. It reads:
    #
    #     database: "{{ env_var('CONTOSO_SILVER_DATABASE', env_var('LAKEHOUSE_ID')) }}"
    #
    # The outer call has a fallback, but the fallback is ITSELF an env_var with
    # no default, and Jinja evaluates it eagerly -- so LAKEHOUSE_ID is required
    # even when CONTOSO_SILVER_DATABASE is set.
    #
    # Satisfied here rather than fixed there: contoso-data-product is consumed
    # by four platforms, and changing a shared project to suit one consumer's
    # renderer is the wrong direction. These values name the catalog this
    # product uses; `dbt parse` does not connect, and the real values reach the
    # gold tasks through the run environment.
    env = os.environ.copy()
    env.setdefault("CONTOSO_SILVER_DATABASE", "contoso")
    env.setdefault("CONTOSO_SILVER_SCHEMA", "silver")
    env.setdefault("LAKEHOUSE_ID", "contoso")
    print("==> " + " ".join(cmd), flush=True)
    # `dbt parse` resolves the profile, so the placeholders in profiles.yml are
    # exercised here too -- if one of them lost its default, this fails at build
    # time rather than at DAG-parse time inside the scheduler.
    subprocess.check_call(cmd, env=env)

    manifest = TARGET / "manifest.json"
    if not manifest.exists():
        raise SystemExit(
            f"dbt parse succeeded but wrote no {manifest} -- refusing to stamp a "
            f"manifest that is not there."
        )

    # PROVE IT CONTAINS THE CONTRACTS. The whole reason for building a manifest
    # rather than accepting the fallback parser is that the fallback drops
    # things quietly; a manifest that had dropped them would be no better.
    nodes = json.loads(manifest.read_text(encoding="utf-8")).get("nodes", {})
    tests = sorted(k for k in nodes if k.startswith("test."))
    models = sorted(k for k in nodes if k.startswith("model."))
    expect = sorted(p.stem for p in (project / "tests").glob("*.sql"))
    # dbt names a SINGULAR test `test.<project>.<name>` and a GENERIC one
    # `test.<project>.<name>.<hash>`. Matching on `.<name>.` alone finds only
    # the generic ones, so every contract -- all of which are singular -- reads
    # as missing. Both shapes, explicitly.
    missing = [
        c
        for c in expect
        if not any(t.endswith(f".{c}") or f".{c}." in t for t in tests)
    ]
    if missing:
        raise SystemExit(
            "the manifest is missing contracts that exist as files, so the DAG "
            "would render without them and every run would look clean: "
            + ", ".join(missing)
        )

    (TARGET / "manifest.stamp.json").write_text(
        json.dumps(
            {"contoso_data_product": core, "models": len(models), "tests": len(tests)},
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"manifest: {len(models)} models, {len(tests)} tests, product {core}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
