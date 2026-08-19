"""Emulator-only. Real mode must already hold the customer's values."""

from __future__ import annotations

from .credentials import VENDOR_OF, published
from .target import T


def main() -> int:
    t = T()
    if not t.seed_secrets_allowed:
        print("seed_secrets skipped — DATABRICKS_TARGET=real uses the customer's scope")
        return 0
    w = t.workspace_client()
    for secret in sorted(VENDOR_OF):
        # THE VENDOR'S ACTUAL KEY, read from the vendor. This used to be a
        # literal `"pos-dev-key"` here, which was both a credential in the
        # source tree and the WRONG value -- the vendor issues
        # `pos-key-8843-dev`, so anything reading the scope would have been
        # refused 401 by the very vendor this seeded a key for.
        w.secrets.put_secret(
            scope=t.secret_scope, key=secret, string_value=published(secret)
        )
        print(f"seeded {t.secret_scope}/{secret}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
