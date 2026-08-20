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


def test_dbt_pins_agree_between_the_dag_and_the_manifest():
    """The manifest and the gold tasks must be built by the SAME dbt.

    dbt is not a dependency of this product -- it cannot be (see below), so no
    lockfile holds these together. Two literal lists do, and this is what keeps
    them equal: a manifest rendered by one dbt and executed by another is a
    graph describing something the run will not do.
    """
    import sys

    sys.path.insert(0, str(ROOT / "dags"))
    sys.path.insert(0, str(ROOT / "scripts"))
    import contoso_daily
    import manifest

    assert sorted(contoso_daily.DBT_REQUIREMENTS) == sorted(manifest.DBT_REQUIREMENTS)


def test_dbt_is_not_a_dependency_of_this_product():
    """Not an oversight, and not a style choice: it cannot be one.

    `databricks-connect` 19.1 needs `databricks-sdk>=0.122`; `dbt-databricks`
    1.12.4 needs `<0.118`. uv resolves dependency groups together with the base
    dependencies, so declaring dbt anywhere in this file makes the whole project
    unresolvable -- and `[tool.uv] conflicts` can only express group-against-
    group, not group-against-base.

    The engine, by contrast, MUST be a base dependency: the worker image
    installs the project and no groups, and as a group it was simply absent at
    run time -- the first task died with `workspace_client() needs
    databricks-sdk` after everything else about the cell was right.
    """
    import tomllib

    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    declared = " ".join(project["project"]["dependencies"])
    for group in project.get("dependency-groups", {}).values():
        declared += " " + " ".join(group)
    assert "dbt-databricks" not in declared, (
        "dbt-databricks is declared as a dependency -- this project cannot "
        "resolve with it and databricks-connect together"
    )
    assert "databricks-connect" in " ".join(project["project"]["dependencies"]), (
        "the engine must be a BASE dependency -- the worker image installs the "
        "project and no groups"
    )


def test_no_dbt_task_emits_an_asset_cosmos_invented(monkeypatch):
    """Cosmos must emit no assets; this DAG declares its own.

    G37, MET IN THE FABRIC AIRFLOW CELL AND FIXED HERE BEFORE IT WAS MET.
    With emission left on -- the default -- cosmos assigns each model task an
    outlet at RUN TIME, and there it assigned three concurrent gold tasks the
    SAME one (`dbo/fct_orders`, claimed six times in one run's log). They raced
    to create one AssetModel row; one won, and the API server answered the
    others with "Error updating Task Instance state. Setting the task to
    failed." while their payload said SUCCESS. One run in two.

    This cell had the same configuration and the same nine concurrent gold
    models. It had simply never been observed failing -- two clean runs against
    a one-in-two race is roughly a one-in-four coincidence, which is not
    evidence. "Not yet seen" and "cannot happen" are different claims and this
    file should only ever make the second one.

    IT ASSERTS `emit_datasets`, NOT `outlets`. Cosmos's own parameter doc says
    emission happens "during task execution", so a rendered task carries no
    outlets either way and `assert not task.outlets` passes identically with
    emission on and off -- the mistake the first version of the Fabric leaf's
    guard made, where it appeared to work only because emission-on crashed the
    render against a missing metadata table.
    """
    pytest.importorskip("airflow.sdk")
    pytest.importorskip("cosmos")
    # Hermetic: cosmos caches its rendered graph in an Airflow Variable, which
    # needs a metadata database. Rendering still happens; only the cache is off.
    monkeypatch.setenv("AIRFLOW__COSMOS__ENABLE_CACHE", "False")
    import sys

    sys.path.insert(0, str(ROOT / "dags"))
    import contoso_daily

    dag = contoso_daily.contoso_daily()
    dbt_tasks = [t for t in dag.tasks if t.task_id.startswith("gold.")]
    assert dbt_tasks, "no dbt tasks rendered -- the scan proved nothing"
    emitting = {t.task_id for t in dbt_tasks if getattr(t, "emit_datasets", True)}
    assert not emitting, (
        f"cosmos will emit assets for these tasks at run time; the G37 race is "
        f"live for them: {sorted(emitting)}"
    )


def test_the_readme_inventory_matches_the_pinned_core():
    """The README's product list must be what this leaf's pin actually contains.

    A generated list that falls behind is worse than none: a reader trusts it
    BECAUSE it looks generated. The check lives in the core, so all seven leaves
    ask the same question of their own pin, and it fails here, in the repository
    that has to fix it.

    Regenerate with:  python -m contoso_product.show --markdown
    """
    from pathlib import Path

    from contoso_product import show

    ok, message = show.check(Path(__file__).resolve().parent.parent / "README.md")
    assert ok, message
