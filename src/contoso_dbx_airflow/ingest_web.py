"""Pull Contoso Web over HTTP and land it verbatim.

THE SECOND VENDOR, and the reason there is one. A platform that ingests a
single source proves it can ingest a single source. Two vendors is where the
work actually starts: two credentials that rotate separately, two formats that
agree about nothing, and two customer lists describing overlapping people
without either system knowing the other exists.

WHAT THIS VENDOR SENDS, and none of it is smoothed over here:

  * JSON arrays, not the delimited text and JSON Lines the POS system ships
  * ORDERS ARE NESTED -- one order carries its own `lines` array, because the
    storefront thinks in baskets. Flattening is a decision, and it belongs
    downstream where it is visible, not in the step that records what arrived
  * NO CUSTOMER ID -- accounts are keyed on email, which is what makes joining
    this to the POS system a resolution problem rather than a join
  * `country` as the shopper typed it -- "United States", not "US"
"""

from __future__ import annotations

from . import landing
import requests
from .credentials import resolve

from .sources import WEB_API, WEB_KEY_SECRET

FEEDS = [
    ("/api/v2/export/customers", "customers", "json"),
    ("/api/v2/export/products", "products", "json"),
    ("/api/v2/export/orders", "orders", "json"),
]


def fetch(path: str, key: str, page: int | None = None) -> requests.Response:
    params = {} if page is None else {"page": page}
    return requests.get(
        f"{WEB_API}{path}", headers={"X-Api-Key": key}, params=params, timeout=600
    )


def main() -> int:
    root = landing.root("contoso_web")

    # This vendor's own key, from this vendor's own secret. Using the POS key
    # here would still land bytes -- they are separate processes with separate
    # keys -- but it would prove nothing about either.
    api_key = resolve(WEB_KEY_SECRET)
    refused = fetch(FEEDS[0][0], "wrong-key", 1)
    assert refused.status_code == 401, (
        f"Contoso Web accepted a bad API key: {refused.status_code}"
    )

    landed = {}
    for path, subdir, ext in FEEDS:
        dest = root / subdir
        dest.mkdir(parents=True, exist_ok=True)
        first = fetch(path, api_key, 1)
        assert first.status_code == 200, (path, first.status_code, first.text[:200])
        total_pages = int(first.headers["X-Total-Pages"])
        assert total_pages >= 1, (path, total_pages)

        written_total, parts = 0, 0
        for page in range(1, total_pages + 1):
            r = first if page == 1 else fetch(path, api_key, page)
            assert r.status_code == 200, (path, page, r.status_code, r.text[:200])
            assert int(r.headers["X-Page"]) == page, (r.headers.get("X-Page"), page)
            blob = r.content
            assert blob, f"{path} page {page} returned an empty body"
            # Each page must be a COMPLETE array, not a fragment. A vendor that
            # split on bytes would hand back something no reader could parse
            # alone, and the failure would surface in bronze as an engine error
            # naming neither the vendor nor the page.
            assert blob[:1] == b"[" and blob[-1:] == b"]", (
                f"{path} page {page} is not a self-contained JSON array"
            )
            # ONE LINE PER PAGE, which is a constraint the ENGINE imposes rather
            # than a rule the vendor agreed to. Bronze reads these pages as text
            # -- one row per line -- and parses each with from_json. A
            # pretty-printed page would be split across rows, every fragment
            # would fail to parse, and the column would arrive full of NULLs.
            # Caught here the message names the cause; caught in bronze it looks
            # like an engine problem.
            assert b"\n" not in blob.strip(), (
                f"{path} page {page} contains newlines — bronze reads these "
                f"pages a line at a time, so a pretty-printed page parses to "
                f"NULLs rather than failing"
            )
            (dest / f"part-{page:04d}.{ext}").write_bytes(blob)
            written_total += len(blob)
            parts += 1

        over = fetch(path, api_key, total_pages + 1)
        assert over.status_code == 404, (
            f"{path} served page {total_pages + 1} of {total_pages}: {over.status_code}"
        )

        landed[subdir] = {"bytes": written_total, "parts": parts}
        print(f"landed {subdir}/ — {parts} part(s), {written_total:,} bytes")

    landing.record(web_landed=landed)
    total = sum(v["bytes"] for v in landed.values())
    n_parts = sum(v["parts"] for v in landed.values())
    print(
        f"Contoso Web: {n_parts} part(s) across {len(landed)} feed(s), {total:,} bytes"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
