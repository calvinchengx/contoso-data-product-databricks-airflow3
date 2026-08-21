# contoso-data-product-databricks-airflow3

The Contoso data product for **Databricks, orchestrated by Apache Airflow 3**.

This repository holds a data product. It holds no platform: no compose file, no
emulator, no ports. It is mounted by
[`databricks-platform-airflow3`](https://github.com/calvinchengx/databricks-platform-airflow3),
which supplies the Airflow, the Databricks target and the credential.

## What the product contains

The SQL is not here. It lives in the core so seven leaves cannot drift into
seven versions of it, and that costs you a click, so this list gives it back.
`make show-product` copies the same files into `product/` where you can open
them; the block below is generated from the pinned package and a test fails
when it falls behind.

<!-- BEGIN product inventory: python -m contoso_product.show --markdown -->

The product is [`contoso-data-product`](https://github.com/calvinchengx/contoso-data-product/tree/v0.6.0) at **v0.6.0**, the version this repository pins. It is not vendored here: these files live there and are staged locally by `make show-product`.

**silver**: 8 models, 1 singular test

- [`silver_customers`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_customers.sql)
- [`silver_fx_daily`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_fx_daily.sql)
- [`silver_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_orders.sql)
- [`silver_party`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_party.sql)
- [`silver_product_hierarchy`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_product_hierarchy.sql)
- [`silver_quarantine_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_quarantine_orders.sql)
- [`silver_web_customers`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_web_customers.sql)
- [`silver_web_order_lines`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/models/silver_web_order_lines.sql)

Assertions over silver, each failing the build on its own:

- [`silver_orders_never_holds_a_non_positive_quantity`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/silver/tests/silver_orders_never_holds_a_non_positive_quantity.sql)

**gold**: 9 models, 5 singular tests

- [`dim_country`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/dim_country.sql)
- [`dim_customer`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/dim_customer.sql)
- [`dim_date`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/dim_date.sql)
- [`dim_party`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/dim_party.sql)
- [`dim_product`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/dim_product.sql)
- [`fct_daily_revenue`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/fct_daily_revenue.sql)
- [`fct_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/fct_orders.sql)
- [`fct_revenue_summary`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/fct_revenue_summary.sql)
- [`fct_sales`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/models/fct_sales.sql)

Assertions over gold, each failing the build on its own:

- [`both_selling_systems_reach_the_pack`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/tests/both_selling_systems_reach_the_pack.sql)
- [`every_country_resolves_to_the_dimension`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/tests/every_country_resolves_to_the_dimension.sql)
- [`fiscal_year_is_not_the_calendar_year`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/tests/fiscal_year_is_not_the_calendar_year.sql)
- [`money_is_never_stored_as_float`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/tests/money_is_never_stored_as_float.sql)
- [`revenue_summary_loses_no_revenue`](https://github.com/calvinchengx/contoso-data-product/blob/v0.6.0/src/contoso_product/gold/tests/revenue_summary_loses_no_revenue.sql)

<!-- END product inventory -->

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
