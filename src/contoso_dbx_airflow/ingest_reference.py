"""Pull Contoso Reference over HTTP and land it verbatim.

THE FOURTH VENDOR, and the first that is not an operational system. POS, Web
and ERP each record things that happened; this one publishes the definitions
they are all reported against. It is a vendor rather than a table maintained
inside the platform because that is what it is in the business: the group data
office owns it, issues a credential for it, and changes it on its own schedule.

WHAT THIS VENDOR SENDS, none of it smoothed over here:

  * PARQUET -- binary and columnar, where POS ships delimited text and JSON
    Lines and Web ships JSON arrays. Three vendors, three dialects.
  * NOT PAGED, because the whole export is about four kilobytes.
  * FX RATES WITH GAPS. Rates are published for trading days only, so weekends
    are absent. Carrying the last rate forward is the consumer's decision and
    is made downstream, where it is visible.

WHY THIS STEP VERIFIES A CHECKSUM WHEN NO OTHER INGEST DOES. Every other feed
is text, so damage in transit announces itself -- a truncated CSV or a mangled
JSON array fails to parse. Parquet does not: it keeps its `PAR1` magic and its
`PAR1` footer through byte-level corruption, so a ruined file passes every
cheap check and fails much later inside a Parquet reader, naming neither the
transport nor the cause.

That is not hypothetical. mokapi's ordinary response path cannot carry binary
at all: `read()` returns a Go string, goja decodes it as UTF-8, and every byte
that is not valid UTF-8 becomes U+FFFD -- measured against these exact files,
inflating fx_rates.parquet from 2,268 bytes to 3,301 with both `PAR1` markers
still in place. serve.js avoids it by putting raw bytes on `response.data`, and
this step verifies the digest so that if it ever silently reverts, the failure
lands here, at the boundary, naming the cause.
"""

from __future__ import annotations

import hashlib

from . import landing
import requests
from .credentials import resolve

from .sources import REFERENCE_API, REFERENCE_KEY_SECRET

FEEDS = [
    ("/reference/v1/product-hierarchy", "product_hierarchy.parquet"),
    ("/reference/v1/fx-rates", "fx_rates.parquet"),
]


def fetch(path: str, key: str) -> requests.Response:
    return requests.get(
        f"{REFERENCE_API}{path}", headers={"X-Api-Key": key}, timeout=600
    )


def main() -> int:
    root = landing.root("contoso_reference")

    api_key = resolve(REFERENCE_KEY_SECRET)
    refused = fetch(FEEDS[0][0], "wrong-key")
    assert refused.status_code == 401, (
        f"Contoso Reference accepted a bad API key: {refused.status_code}"
    )

    landed: dict[str, dict[str, str]] = {}
    total = 0
    for path, filename in FEEDS:
        r = fetch(path, api_key)
        assert r.status_code == 200, (path, r.status_code, r.text[:200])
        blob = r.content
        assert blob, f"{path} returned an empty body"

        # NECESSARY AND NOT SUFFICIENT, which is exactly why the checksum below
        # exists. Both markers survive the corruption this step guards against,
        # so passing this pair proves only that something Parquet-shaped
        # arrived -- worth asserting to catch a 200 carrying an HTML error page,
        # and worth nothing at all against a mangled body.
        assert blob[:4] == b"PAR1" and blob[-4:] == b"PAR1", (
            f"{path} is not a Parquet file: starts {blob[:4]!r}, ends {blob[-4:]!r}"
        )

        # THE REAL CHECK. The vendor publishes the digest of what it sent; if
        # what arrived hashes differently, the transport changed the bytes.
        publishedsum = r.headers.get("X-Content-SHA256", "")
        assert publishedsum, (
            f"{path} served no X-Content-SHA256 — this vendor's whole format "
            f"corrupts quietly, so an unverifiable body is not usable"
        )
        got = hashlib.sha256(blob).hexdigest()
        assert got == publishedsum, (
            f"{path} arrived corrupted: the vendor sent sha256 {publishedsum} and "
            f"{len(blob):,} bytes hashing to {got}. Parquet keeps its PAR1 "
            f"markers through this, so nothing downstream would have noticed. "
            f"The usual cause is mokapi's text response path mangling binary — "
            f"serve.js must put raw bytes on `response.data`, never `body`."
        )

        (root / filename).write_bytes(blob)
        landed[filename] = {"bytes": str(len(blob)), "sha256": got}
        total += len(blob)
        print(f"landed {filename} — {len(blob):,} bytes, sha256 verified")

    assert len(landed) == 2, sorted(landed)
    landing.record(reference_landed=landed)
    print(f"Contoso Reference: {len(landed)} feed(s), {total:,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
