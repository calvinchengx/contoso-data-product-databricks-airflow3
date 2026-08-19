"""Where the source systems live.

Contoso POS, Web, Reference and ERP are NOT Databricks. They are the vendors a
pipeline pulls from, and in production they are a real REST endpoint, a real
Postgres and a real Kafka broker. Keeping their addresses out of target.py is
the same discipline as keeping the emulator out of the platform: a Databricks
client should not know what a vendor's DSN looks like.

Every value is overridable, because that is the only thing that changes when
this platform is pointed at real vendors. The DEFAULTS are this platform's
published ports (see scripts/sources.py) rather than the vendor's own, so two
platforms' stacks can run side by side on one machine.
"""

from __future__ import annotations

import os

# Contoso POS -- the vendor's export API. Delimited text and JSON Lines, paged.
POS_API = os.environ.get("POS_API_URL", "http://localhost:18190")
# The NAME of the secret, never its value. Resolved from the scope at use.
POS_KEY_SECRET = os.environ.get("POS_KEY_SECRET", "contoso-pos-api-key")

# Contoso Web -- the storefront's export API. A SECOND VENDOR with its own
# endpoint and its own key: the POS credential must not open this door, which
# is what having two vendors means rather than two routes on one.
WEB_API = os.environ.get("WEB_API_URL", "http://localhost:18191")
WEB_KEY_SECRET = os.environ.get("WEB_KEY_SECRET", "contoso-web-api-key")

# Contoso Reference -- the group data office's master data. NOT an operational
# system: it publishes the definitions the other three are reported against,
# which is why it is a vendor in its own right rather than a table someone
# maintains inside the platform. Binary Parquet, and its own key.
REFERENCE_API = os.environ.get("REFERENCE_API_URL", "http://localhost:18192")
REFERENCE_KEY_SECRET = os.environ.get(
    "REFERENCE_KEY_SECRET", "contoso-reference-api-key"
)

# Contoso ERP -- a relational source, captured by CDC. The consumer reads the
# BROKER, not the database: what makes this vendor worth having is that its
# history arrives as a change stream, and a direct read would be a snapshot.
ERP_HOST = os.environ.get("ERP_HOST", "localhost")
ERP_PORT = os.environ.get("ERP_PORT", "55434")
ERP_DB = os.environ.get("ERP_DB", "erp")
ERP_USER = os.environ.get("ERP_USER", "contoso")
REDPANDA = os.environ.get("REDPANDA_BOOTSTRAP", "localhost:19094")
ERP_TOPIC = os.environ.get("ERP_TOPIC", "contoso.erp.customer")
