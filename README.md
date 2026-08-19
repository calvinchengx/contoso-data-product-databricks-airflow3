# contoso-data-product-databricks-airflow3

The Contoso data product for **Databricks, orchestrated by Apache Airflow 3**.

This repository holds a data product. It holds no platform: no compose file, no
emulator, no ports. It is mounted by
[`databricks-platform-airflow3`](https://github.com/calvinchengx/databricks-platform-airflow3),
which supplies the Airflow, the Databricks target and the credential.

## What this cell is for

Two claims already stand on their own:

- `contoso-data-product-fabric-airflow3` shows an external Airflow 3 can drive a
  medallion end to end.
- `contoso-data-product-databricks-jobs` shows these steps produce the family's
  numbers on Databricks.

Neither shows that the **orchestrator** is portable. This cell is where that is
either true or not: the same steps, against the same engine, at the same pins,
driven by a different orchestrator. If its gold differs from the Jobs cell's,
the orchestrator is the only remaining candidate.

So nothing else moves. Silver runs through the product's PySpark runner here,
exactly as it does in the Jobs cell, even though core also ships silver as a dbt
project — rendering silver as one task per model would be a better DAG, and
doing it in the same change as the orchestrator swap would leave two candidate
explanations for any difference in the numbers.

## Two environments, not one

`dbt-databricks` 1.12.4 pins `databricks-sdk<0.118`; `databricks-connect` 19.1
needs `>=0.122`. uv proves them unsatisfiable together — this is a real
incompatibility, not a self-imposed pin.

The Jobs cell sidesteps it by running each step as its own job, so the two never
meet. An Airflow **worker** is a single environment, so that answer is not
available here: the worker installs the `engine` group, and the gold tasks build
their own dbt virtualenv from the `dbt` group. Two environments either way; the
boundary is a task boundary instead of a job boundary.

## The manifest is not optional

Cosmos renders the DAG at parse time by running `dbt ls` — which needs dbt in
the dag-processor's environment, where it cannot be. When it cannot find dbt,
cosmos falls back **silently** to a deprecated parser that knows a subset of
what dbt knows.

So the graph is built from a manifest instead, explicitly
(`LoadMode.DBT_MANIFEST`), and `scripts/manifest.py` refuses to stamp one that
does not contain the contracts. Run it before the stack comes up:

```bash
make manifest
```

The DAG refuses to render a manifest whose stamp disagrees with the installed
`contoso-data-product` version. A stale generated artifact that still looks
green has cost this family a day before.

### The contracts run because of `AFTER_ALL`

`TestBehavior.AFTER_EACH` reads better — one test task per model — and it was
measured here: it renders 9 models × (run, test) and **drops all five ODCS
contracts**, which are singular tests attached to no model. The DAG renders
clean, runs clean, and evaluates none of them. `AFTER_ALL` renders one
whole-suite task instead. Less granular, and correct.

## Layout

| path | what |
| --- | --- |
| `dags/contoso_daily.py` | the pipeline as a graph |
| `src/contoso_dbx_airflow/` | this leaf's binding: target policy, landing, vendor ingest |
| `dbt/gold/profiles.yml` | the profile only — the **models** come from `contoso-data-product` |
| `scripts/manifest.py` | builds and verifies the manifest cosmos renders from |

Bronze, silver and every line of gold SQL come from `contoso-data-product` by
release and are not restated here.
