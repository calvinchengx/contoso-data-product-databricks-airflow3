# The product's own targets. The PLATFORM runs the pipeline; these are what a
# developer runs on this repo alone.
UV ?= uv

.PHONY: help lock manifest prepare test lint check

help:
	@echo "manifest  build the dbt manifest cosmos renders the gold graph from"
	@echo "test      pytest (requires the manifest)"
	@echo "lint      ruff check + format --check"
	@echo "check     manifest, lint, test"

lock:
	$(UV) lock

# MUST RUN BEFORE THE STACK COMES UP. Cosmos renders the DAG at parse time and
# dbt cannot be installed in the worker (see pyproject.toml); without this the
# DAG fails to import, loudly, which is the intended behaviour -- the
# alternative cosmos offers is to render a graph missing the contracts and say
# nothing.
manifest:
	$(UV) sync --quiet
	$(UV) run --no-sync python scripts/manifest.py

test:
	$(UV) sync --quiet --group dev
	$(UV) run --no-sync pytest -q tests/

lint:
	$(UV) run --no-sync ruff check .
	$(UV) run --no-sync ruff format --check .

# WHAT THE PLATFORM CALLS. The platform asks whether this product declares a
# `prepare` target and runs it before building the worker; it never learns that
# preparing means a dbt manifest here.
prepare: manifest

check: manifest lint test
