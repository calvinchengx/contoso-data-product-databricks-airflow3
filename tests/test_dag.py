"""What the DAG must still be true about, checked without a stack.

These are the properties that were WRONG at some point while building this cell
and gave no sign of it. A DAG that renders is not a DAG that does the work.
"""

from __future__ import annotations

import json
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "dbt" / "gold" / "target" / "manifest.json"


@pytest.fixture(scope="module")
def dag():
    from airflow.models.dagbag import DagBag

    bag = DagBag(dag_folder=str(ROOT / "dags"))
    assert not bag.import_errors, bag.import_errors
    return bag.dags["contoso_daily"]


def _tasks(dag) -> list[str]:
    return sorted(t.task_id for t in dag.tasks)


def test_the_medallion_is_a_graph_not_a_list(dag):
    """The eight-position STEPS list became tasks, and the vendors fan out."""
    ids = set(_tasks(dag))
    for step in (
        "provision",
        "seed_secrets",
        "land",
        "to_bronze",
        "to_silver",
        "register",
        "publish",
    ):
        assert step in ids, f"{step} is not a task: {sorted(ids)}"


def test_gold_renders_every_model(dag):
    from contoso_product import gold_dir

    models = sorted(p.stem for p in (gold_dir() / "models").glob("*.sql"))
    rendered = {
        t.split(".")[-1].removesuffix("_run")
        for t in _tasks(dag)
        if t.startswith("gold.") and t.endswith("_run")
    }
    assert set(models) == rendered, (
        f"gold models on disk {models} but rendered {sorted(rendered)}"
    )


def test_the_contracts_are_actually_in_the_run(dag):
    """THE REGRESSION THIS FILE EXISTS FOR.

    `TestBehavior.AFTER_EACH` -- the obvious, better-looking choice -- renders a
    test task per model and silently drops all five ODCS contracts, because they
    are SINGULAR tests attached to no model. The DAG still renders, still runs,
    and never evaluates a guarantee the product publishes.

    There is no per-contract task to assert on under AFTER_ALL, so this asserts
    the two things that actually make the contracts run: a whole-suite test task
    exists, and the manifest cosmos renders from contains them.
    """
    assert "gold.gold_test" in _tasks(dag), (
        "no whole-suite test task -- if this was changed to AFTER_EACH, the "
        "singular ODCS contracts are no longer executed by any task"
    )

    from contoso_product import gold_dir

    contracts = sorted(p.stem for p in (gold_dir() / "tests").glob("*.sql"))
    assert contracts, "the product ships no singular contracts -- has gold moved?"
    nodes = json.loads(MANIFEST.read_text(encoding="utf-8"))["nodes"]
    tests = [k for k in nodes if k.startswith("test.")]
    missing = [
        c
        for c in contracts
        if not any(t.endswith(f".{c}") or f".{c}." in t for t in tests)
    ]
    assert not missing, f"manifest omits contracts: {missing}"


def test_the_manifest_matches_the_installed_product():
    """A stale manifest renders yesterday's graph against today's code."""
    from importlib.metadata import version

    stamp = json.loads(
        (MANIFEST.parent / "manifest.stamp.json").read_text(encoding="utf-8")
    )
    assert stamp["contoso_data_product"] == version("contoso-data-product")


def test_dbt_pins_agree_between_the_dag_and_the_project():
    """The venv is built from a list, the tasks from a lockfile. Keep them equal."""
    import tomllib

    import sys

    sys.path.insert(0, str(ROOT / "dags"))
    import contoso_daily

    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    group = project["dependency-groups"]["dbt"]
    assert sorted(contoso_daily.DBT_REQUIREMENTS) == sorted(group), (
        "the dbt virtualenv the gold tasks build no longer matches the `dbt` "
        "dependency group -- the manifest and the run would use different dbts"
    )


def test_the_engine_and_dbt_are_declared_incompatible():
    """Not a style choice: uv proves them unsatisfiable together."""
    import tomllib

    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    conflicts = project["tool"]["uv"]["conflicts"]
    groups = [{c.get("group") for c in pair} for pair in conflicts]
    assert {"engine", "dbt"} in groups, (
        "engine and dbt must stay declared as conflicting -- databricks-connect "
        "19.1 needs databricks-sdk>=0.122 and dbt-databricks 1.12.4 needs <0.118"
    )
