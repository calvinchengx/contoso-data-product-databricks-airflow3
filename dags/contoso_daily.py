"""The Contoso daily pipeline on Databricks, orchestrated by Airflow 3.

WHAT THIS CELL IS FOR. Two claims already stand on their own: the Fabric
Airflow cell shows an external Airflow 3 can drive a medallion end to end, and
`contoso-data-product-databricks-jobs` shows these steps produce the family's
numbers on Databricks. Neither shows that the ORCHESTRATOR is portable. This
cell is where that is either true or not: the same steps, against the same
engine, at the same pins, driven by a different orchestrator. If its gold
differs from the Jobs cell's, the orchestrator is the only remaining candidate,
and that is the entire point of building it.

SO NOTHING ELSE MOVES. Silver runs through the product's PySpark runner here,
exactly as it does in the Jobs cell, even though core also ships silver as a
dbt project and `dbt-databricks` is already installed -- rendering silver as one
Airflow task per model would be a better DAG and is recorded as its own step.
Doing it in the same change as the orchestrator swap would leave two candidate
explanations for any difference in the numbers, which would cost this cell the
one thing it exists to establish.

WHAT DOES CHANGE, deliberately: the eight-position `STEPS` list in the Jobs
leaf's `pipeline.py` was always a graph. Four vendors are genuinely independent
-- a wrong key, a mangled binary body, a short change stream are three separate
failures -- so they are four mapped tasks that retry alone, rather than four
positions in a sequence where the second failing means the third never runs.

TASK SDK ONLY (`airflow.sdk`). That is Airflow 3's boundary between task code
and the scheduler's internals, and it is what lets this same file run on a
managed Airflow in production without edits.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import pathlib

import pendulum
from airflow.sdk import Asset, dag, task
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode, TestBehavior

from contoso_product import gold_dir, silver_dir

# The profile lives here; the MODELS come from the installed product package.
DBT_DIR = pathlib.Path(__file__).resolve().parent.parent / "dbt"
# dbt IS NOT INSTALLED IN THE WORKER, and cannot be. dbt-databricks 1.12.4 pins
# databricks-sdk<0.118 while databricks-connect 19.1 -- which bronze and silver
# import -- needs 0.129; the two are unsatisfiable together, and pyproject.toml
# declares them as conflicting groups rather than letting one silently win.
#
# So the gold tasks build their own virtualenv from these requirements. It is
# the same two-environment split the Jobs cell gets by running each step as a
# separate job, drawn at a task boundary instead. The pins are literal here
# because a venv is built from a list, not from a lockfile -- keep them equal
# to the `dbt` group in pyproject.toml.
DBT_REQUIREMENTS = [
    "dbt-core>=1.9,<2",
    "dbt-databricks==1.12.4",
]
# An operator who has already provisioned a dbt environment can name it and skip
# the per-run build; unset, cosmos creates one.
DBT_VENV = os.environ.get("COSMOS_DBT_VENV")

# THE GRAPH COMES FROM A MANIFEST, built by `scripts/manifest.py` before the
# stack comes up. Cosmos's default is to run `dbt ls` at parse time, and when it
# cannot find dbt it falls back -- silently -- to a deprecated custom parser
# that knows a subset of what dbt knows. A DAG that renders without the
# contracts would look exactly like a DAG that passes them.
MANIFEST = DBT_DIR / "gold" / "target" / "manifest.json"
STAMP = DBT_DIR / "gold" / "target" / "manifest.stamp.json"


def _manifest() -> pathlib.Path:
    """The manifest, having proved it describes the product that is installed.

    A manifest is a generated artifact describing a PINNED package's SQL. If the
    pin moves and this does not, cosmos renders yesterday's models against
    today's code and reports nothing wrong. Raising here shows up as a DAG
    import error, which is loud; rendering a stale graph is silent, and this
    family has already lost a day to a stale artifact that looked green.
    """
    if not MANIFEST.exists():
        raise RuntimeError(
            f"no dbt manifest at {MANIFEST}. Build it with "
            f"`python scripts/manifest.py` (the platform's `make up` does this) "
            f"-- without it cosmos would fall back to a parser that drops the "
            f"contracts and the DAG would look healthy anyway."
        )
    stamp = json.loads(STAMP.read_text(encoding="utf-8")) if STAMP.exists() else {}
    built_from = stamp.get("contoso_data_product")
    installed = importlib.metadata.version("contoso-data-product")
    if built_from != installed:
        raise RuntimeError(
            f"the dbt manifest was built from contoso-data-product {built_from} "
            f"but {installed} is installed. Rebuild it with "
            f"`python scripts/manifest.py`; rendering it would run the pinned "
            f"package's code against a graph describing a different version."
        )
    return MANIFEST


# DERIVED FROM THE PRODUCT, not listed here. A hand-kept list is a second
# place the set of tables lives, and it is wrong on the day core adds one --
# quietly, by registering everything it knows about and reporting success.
SILVER_TABLES = sorted(p.stem for p in (silver_dir() / "models").glob("*.sql"))
GOLD_MODELS = sorted(p.stem for p in (gold_dir() / "models").glob("*.sql"))
GOLD_ASSETS = [Asset(f"contoso://gold/{m}") for m in GOLD_MODELS]

# The four vendors, as the product's ingest modules name them. Each is its own
# module because each is its own failure; a single function would report all
# four as "ingest failed".
VENDORS = [
    {"name": "Contoso POS", "module": "ingest_pos"},
    {"name": "Contoso Web", "module": "ingest_web"},
    {"name": "Contoso Reference", "module": "ingest_reference"},
    {"name": "Contoso ERP", "module": "ingest_erp_cdc"},
]


@dag(
    dag_id="contoso_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["contoso", "databricks", "medallion"],
)
def contoso_daily():
    @task
    def provision() -> dict:
        """Warehouse, catalog, schemas, secret scope. Ids resolved, never stored."""
        from contoso_dbx_airflow import landing
        from contoso_dbx_airflow.target import CATALOG, T, WAREHOUSE, WORKSPACE

        t = T()
        w = t.workspace_client()
        try:
            existing = {wh.name: wh for wh in w.warehouses.list()}
        except TypeError:
            existing = {}
        wh = existing.get(WAREHOUSE) or w.warehouses.create(name=WAREHOUSE).result()

        for create, what in (
            (lambda: w.catalogs.create(name=CATALOG), f"catalog {CATALOG}"),
            *[
                (
                    lambda s=s: w.schemas.create(name=s, catalog_name=CATALOG),
                    f"schema {s}",
                )
                for s in ("landing", "silver", "gold")
            ],
            (lambda: w.secrets.create_scope(scope=t.secret_scope), "secret scope"),
        ):
            try:
                create()
            except Exception as exc:  # noqa: BLE001 -- already-exists is the norm
                text = str(exc).lower()
                if "already" not in text and "409" not in text:
                    print(f"{what}: {exc}")

        # `record` MERGES. The state file also carries the landing day, and
        # replacing it wholesale is a bug the Jobs leaf already fixed once.
        landing.record(
            workspace=WORKSPACE,
            warehouse=WAREHOUSE,
            warehouse_id=wh.id,
            http_path=f"/sql/1.0/endpoints/{wh.id}",
        )
        return {"warehouse_id": wh.id, "http_path": f"/sql/1.0/endpoints/{wh.id}"}

    @task
    def seed_secrets(ctx: dict) -> dict:
        """Put each vendor's PUBLISHED key in the scope, under the name bronze asks for.

        Never a literal: this reads what the vendor actually published. A hand
        written credential here was wrong once already, and it failed as an
        authentication error against a vendor rather than as a typo here.
        """
        from contoso_dbx_airflow import seed_secrets as step

        step.main()
        return ctx

    @task
    def land(vendor: dict, ctx: dict) -> dict:
        """One vendor, landed. Four transports, four separate failures."""
        import importlib

        from contoso_dbx_airflow import landing

        module = importlib.import_module(f"contoso_dbx_airflow.{vendor['module']}")
        rc = module.main()
        if rc != 0:
            # The module already said what went wrong, on its own terms. This
            # only ensures a non-zero return cannot be mistaken for a landing.
            raise RuntimeError(f"{vendor['name']} did not land (rc={rc})")
        return {"vendor": vendor["name"], "day": landing.day()}

    @task(outlets=[Asset("contoso://bronze")])
    def to_bronze(landed: list[dict]) -> dict:
        """The product's bronze, against paths this target resolved.

        Takes the whole landed list rather than mapping: bronze reads the day
        partition as one unit, and a per-vendor bronze would have to agree with
        the others about a partition none of them owns.
        """
        from contoso_dbx_airflow import landing
        from contoso_dbx_airflow.spark_session import connect
        from contoso_dbx_airflow.target import landing_path, tables_path
        from contoso_product import run_bronze

        # The web vendor's JSON needs its shape declared; Spark will not infer a
        # struct it has not seen. These are the product's schema, not a guess.
        metrics = run_bronze(
            connect(),
            landing=landing_path(),
            tables=tables_path(),
            day=landing.day(),
            web_customer_ddl="array<struct<email:STRING,country:STRING>>",
            web_product_ddl="array<struct<product_id:STRING,name:STRING>>",
            web_order_ddl=(
                "array<struct<web_order_id:STRING,email:STRING,placed_at:STRING,"
                "status:STRING,lines:array<struct<line_no:STRING,product_id:STRING,"
                "quantity:STRING,unit_price:STRING>>>>"
            ),
            web_customer_fields=["email", "country"],
            web_product_fields=["product_id", "name"],
            web_order_fields=["web_order_id", "email", "lines"],
        )
        print(
            f"bronze: {metrics['bronze_customers']} POS customers, "
            f"{metrics['bronze_orders']} orders"
        )
        return {k: v for k, v in metrics.items()}

    @task(outlets=[Asset("contoso://silver")])
    def to_silver(bronze: dict) -> dict:
        """The product's silver runner -- the SAME one the Jobs cell runs."""
        from contoso_dbx_airflow.spark_session import connect
        from contoso_dbx_airflow.target import tables_path
        from contoso_product import run_silver

        metrics = run_silver(connect(), tables=tables_path())
        print(
            f"silver: {metrics['silver_customers']} customers, "
            f"{metrics['silver_party']} parties, {metrics['party_matched']} matched"
        )
        return {k: v for k, v in metrics.items()}

    @task
    def register(silver: dict) -> dict:
        """Register silver Delta paths as UC EXTERNAL tables.

        THIS RAISES. It once printed FAILED eight times and returned zero, so
        gold built against an empty catalog and published a snapshot of nothing.
        """
        from contoso_dbx_airflow.target import CATALOG, T, tables_path

        t = T()
        w = t.workspace_client()
        wh = t.warehouse()
        root = tables_path()
        failed = []
        for name in SILVER_TABLES:
            stmt = w.statement_execution.execute_statement(
                warehouse_id=wh.id,
                statement=(
                    f"CREATE TABLE IF NOT EXISTS {CATALOG}.silver.{name} "
                    f"USING delta LOCATION '{root}/{name}'"
                ),
            )
            state = (
                stmt.status.state.value if stmt.status and stmt.status.state else None
            )
            print(f"  {name}: {state}")
            if state != "SUCCEEDED":
                message = ""
                if stmt.status and stmt.status.error:
                    message = (stmt.status.error.message or "")[:300]
                failed.append(f"{name}: {state} {message}")
        if failed:
            raise RuntimeError(
                "silver tables did not register, so gold would build against an "
                "empty catalog:\n  " + "\n  ".join(failed)
            )
        return {"registered": True}

    @task
    def dbt_env(ctx: dict) -> dict:
        """The connection gold's dbt tasks run with, resolved at RUN time.

        Not at parse time, and not from the profile's placeholders: the token is
        minted per run and the http_path names a warehouse that did not exist
        when the DAG was rendered.
        """
        from contoso_dbx_airflow.target import CATALOG, T, WAREHOUSE

        t = T()
        wh = t.warehouse(WAREHOUSE)
        host = t.host
        return {
            "DATABRICKS_HOST": host.replace("https://", "").replace("http://", ""),
            "DATABRICKS_TOKEN": t.token,
            "DATABRICKS_HTTP_PATH": wh.http_path,
            "DATABRICKS_CONNECTION_URI": f"{host}{wh.http_path}",
            "DATABRICKS_CATALOG": CATALOG,
            # DBT_-PREFIXED SINCE CORE v0.6.0, and the reason is not Databricks'.
            # Snowflake's dbt Projects refuse any env var key that is not
            # UPPERCASE and DBT_-prefixed, so the names this used to set could
            # not be supplied there at all -- gold ran on every engine in this
            # family except the one named for running dbt as a first-class
            # object. The rename made the product portable.
            #
            # LAKEHOUSE_ID IS GONE, not renamed: it was read only because gold's
            # default was `env_var('CONTOSO_SILVER_DATABASE',
            # env_var('LAKEHOUSE_ID'))` and Jinja evaluates a default EAGERLY,
            # so a Fabric-only name was mandatory here. Core stopped nesting it.
            "DBT_SILVER_DATABASE": CATALOG,
            "DBT_SILVER_SCHEMA": "silver",
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        }

    ctx = provision()
    seeded = seed_secrets(ctx)
    landed = land.partial(ctx=seeded).expand(vendor=VENDORS)
    bronze = to_bronze(landed)
    silver = to_silver(bronze)
    registered = register(silver)
    env = dbt_env(ctx)

    # THE CONTRACTS RUN, and that is what `AFTER_ALL` buys. The obvious choice
    # is AFTER_EACH -- one test task per model, which reads better in the UI --
    # and it was measured here: it renders 9 models x (run, test) and DROPS ALL
    # FIVE ODCS CONTRACTS. They are SINGULAR tests, attached to no model, so a
    # per-model test task has nowhere to hang them; the DAG renders clean, runs
    # clean, and never evaluates a single guarantee the product publishes.
    #
    # AFTER_ALL renders one `gold_test` running the whole suite -- 52 tests,
    # the contracts among them. Less granular, and correct. The Jobs cell makes
    # the same call for the same reason, though there it is a subprocess whose
    # verdict has to be read back out of run_results.json, which in turn
    # required asserting that the last invocation really was `dbt test` (dbt run
    # overwrites the same file). Here it is a task with its own state and there
    # is no shared file to misread.
    gold = DbtTaskGroup(
        group_id="gold",
        project_config=ProjectConfig(
            dbt_project_path=gold_dir(),
            manifest_path=_manifest(),
        ),
        profile_config=ProfileConfig(
            profile_name="contoso_gold",
            target_name="dev",
            profiles_yml_filepath=DBT_DIR / "gold" / "profiles.yml",
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.VIRTUALENV,
            virtualenv_dir=DBT_VENV,
        ),
        operator_args={
            "env": env,
            "append_env": True,
            "py_requirements": DBT_REQUIREMENTS,
        },
        render_config=RenderConfig(
            # EXPLICIT, never AUTOMATIC: automatic falls back to the custom
            # parser when dbt is absent, which it always is here.
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_ALL,
            # NO COSMOS ASSETS. Left on, cosmos assigns each model task an
            # outlet of its own devising at RUN TIME -- and in the Fabric
            # Airflow cell it assigned three concurrent gold tasks the SAME
            # one, `dbo/fct_orders`. They raced to create one AssetModel row;
            # one won, and the API server answered the others with "Error
            # updating Task Instance state. Setting the task to failed." WHILE
            # THEIR PAYLOAD SAID SUCCESS. A model that built correctly, and
            # said so, recorded as failed. One run in two.
            #
            # THIS CELL WAS NEVER SHOWN SAFE, only unobserved. It had emission
            # on -- the default -- and its nine gold models run concurrently,
            # exactly the shape that failed there. Two clean runs are about a
            # one-in-four coincidence against a one-in-two race, which is not
            # evidence of anything. Turning it off costs this product nothing:
            # it declares its own target-neutral assets (`contoso://...`) and
            # emits them from `publish`, the task that COUNTS the rows rather
            # than the one that wrote them.
            emit_datasets=False,
        ),
        default_args={"retries": 0},
    )

    @task(outlets=GOLD_ASSETS)
    def publish(ctx: dict) -> dict:
        """Read gold at money's own grain and write the snapshot.

        CAST IN THE ENGINE rather than rounding in Python. Money columns in this
        catalog READ as binary floats -- the emulator registers decimal columns
        in Unity Catalog as `type_name: DOUBLE` while the Delta log, the Parquet
        physical type and `DESCRIBE` all still say `decimal(19,4)`, and the
        planner trusts UC (databricks-emulator#46). The cast recovers the value
        because money is defined to four places and the error is eight orders of
        magnitude below that. It does not repair the column and is not meant to.

        THE SAME FIELDS THE JOBS CELL PUBLISHES, so `compare_products` can put
        the two side by side. If they differ, the orchestrator is the only thing
        left that could have made them differ, which is what this cell is for.
        """
        from contoso_dbx_airflow.sql import query
        from contoso_dbx_airflow.target import CATALOG, T, WAREHOUSE

        t = T()
        w = t.workspace_client()
        wh = t.warehouse(WAREHOUSE)

        money = "CAST(CAST(coalesce(sum({}),0) AS DECIMAL(19,4)) AS STRING)"
        data = query(
            w,
            wh.id,
            f"SELECT {money.format('revenue_usd')}, "
            f"{money.format('cancelled_revenue_usd')}, "
            f"coalesce(sum(sale_lines),0) FROM {CATALOG}.gold.fct_revenue_summary",
        )
        if not data:
            # "COULD NOT READ" IS NOT "ZERO". Defaulting to 0 here would publish
            # a snapshot claiming this runtime built nothing while nine dbt
            # tasks had just gone green.
            raise RuntimeError(
                "gold built, but its aggregates came back with no rows -- "
                "refusing to publish a snapshot of zeros."
            )

        counts = {}
        for model in GOLD_MODELS:
            rows = query(w, wh.id, f"SELECT count(*) FROM {CATALOG}.gold.{model}")
            counts[model] = int(rows[0][0]) if rows else 0
        empty = [m for m, n in counts.items() if n == 0]
        if empty:
            raise RuntimeError(
                "gold models built but are empty, so a snapshot of them would "
                f"record nothing as a result: {', '.join(empty)}"
            )

        # THE CONTRACTS THIS RUN ACTUALLY RENDERED, read from the manifest the
        # graph was built from -- not globbed off disk. A name on disk that no
        # task executed is exactly the stale-evidence defect the Jobs leaf was
        # fixed for.
        nodes = json.loads(MANIFEST.read_text(encoding="utf-8"))["nodes"]
        from contoso_product import gold_dir

        singular = sorted(p.stem for p in (gold_dir() / "tests").glob("*.sql"))
        contracts = sorted(
            c
            for c in singular
            if any(
                k.startswith("test.") and (k.endswith(f".{c}") or f".{c}." in k)
                for k in nodes
            )
        )

        snapshot = {
            "revenue_usd": str(data[0][0]),
            "cancelled_revenue_usd": str(data[0][1]),
            "sale_lines": str(data[0][2]),
            "contracts": contracts,
            "runtime": "databricks-airflow3",
            "catalog": CATALOG,
            "gold": counts,
        }
        out = pathlib.Path(os.environ.get("CONTOSO_SNAPSHOT", "product_snapshot.json"))
        out.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(snapshot, indent=2))
        return snapshot

    registered >> env >> gold >> publish(ctx)


contoso_daily()
