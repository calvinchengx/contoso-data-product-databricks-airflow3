"""Run a statement against the warehouse and get its rows back.

NOT `statement_execution.execute_statement`. That returns a typed `ResultData`
whose model carries `data_array` and no `text` -- so when this warehouse answers
with `result.text` (the payload as a nested JSON string) the SDK drops the field
on the floor and every read looks like an empty table.

Measured here, not inherited: `publish` used the typed call, every one of the
nine gold models came back as zero rows, and the task refused to publish a
snapshot of zeros. The refusal was right and the diagnosis it invited -- "gold
is empty" -- was wrong; `SHOW TABLES IN contoso.gold` came back with
`result = None` too, which no real empty catalog does. The Jobs leaf met the
same thing and its comment is the reason this took minutes: the star held four
rows the whole time and the read was blind.

`api_client.do` is the same transport and the same auth, minus the model that
discards the field. BOTH SHAPES ARE ACCEPTED rather than one being declared
correct: real Databricks returns `data_array`, and a fix that only understood
the emulator would break against the thing this platform exists to rehearse.
"""

from __future__ import annotations

import json


def query(w, warehouse_id: str, statement: str) -> list:
    payload = w.api_client.do(
        "POST",
        "/api/2.0/sql/statements",
        body={
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": "30s",
        },
    )
    state = (payload.get("status") or {}).get("state")
    if state != "SUCCEEDED":
        message = ((payload.get("status") or {}).get("error") or {}).get("message", "")
        raise RuntimeError(f"statement did not succeed ({state}): {message[:300]}")
    result = payload.get("result") or {}
    if "data_array" in result:
        return result["data_array"] or []
    if "text" in result:
        return json.loads(result["text"]).get("data") or []
    return []
