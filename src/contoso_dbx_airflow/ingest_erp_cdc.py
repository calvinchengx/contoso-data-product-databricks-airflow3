"""Consume the ERP change stream and land it.

This is the boundary. Everything upstream -- Postgres, Debezium, Redpanda -- is
the world outside the lakehouse; everything downstream is inside it. The
consumer is the only thing that touches both, which is exactly where a real
ingestion job sits.

WHY A STREAM AND NOT A TABLE READ. The ERP is the one vendor whose value is its
history. Reading `erp.customer` directly would produce rows -- possibly even a
plausible count -- while testing something else entirely: a snapshot cannot
tell you that a customer was in Germany before they were in France, and SCD2
over a snapshot is SCD1 wearing a longer name.

WHAT IS PRESERVED, AND WHAT IS NOT. Counts survive real CDC: the same DML
produces the same events. LSNs, commit timestamps and Kafka offsets do not --
they differ every run, and nothing here asserts on them. `effective_date`
travels as DATA, which keeps the fixture's deliberate disagreement between
capture order and business order intact: a pipeline that sorts by the wrong one
still gets the wrong answer, which is the lesson this source exists to teach.
"""

from __future__ import annotations

import io
import json
import os
import time

from . import landing
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from .sources import ERP_DB, ERP_HOST, ERP_PORT, ERP_TOPIC, ERP_USER, REDPANDA

TOPIC = ERP_TOPIC

# Debezium's op codes, in the vocabulary the change log uses. `r` is a snapshot
# read: it must not appear here, because the connector is registered before any
# DML -- and if it does, that is a finding about the ordering, not a row to
# quietly relabel.
OPS = {"c": "I", "u": "U", "d": "D"}

COLUMNS = [
    "erp_customer_id",
    "phone",
    "legal_name",
    "account_tier",
    "segment",
    "credit_band",
    "account_status",
    "payment_terms_days",
    "country",
    "effective_date",
]


def watermark(consumer: Consumer) -> int | None:
    """The topic's high offset, or None while the topic does not exist yet.

    A MISSING TOPIC IS NOT AN ERROR HERE, it is a vendor that has not finished
    becoming real. The platform starts the seeder as a one-shot container and
    `compose up --wait` does not wait for it -- so on a genuinely cold stack
    this step can reach the broker before Debezium has created the topic, and
    librdkafka answers `_UNKNOWN_PARTITION`. That surfaced as a stack trace
    naming a partition, which reads like a broker fault rather than a race.
    """
    try:
        _, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=30)
    except KafkaException as exc:
        if (
            exc.args
            and getattr(exc.args[0], "code", None) == KafkaError._UNKNOWN_PARTITION
        ):
            return None
        raise
    return high


def settled(
    consumer: Consumer, polls: int = 3, gap: float = 5.0, appear: float = 300.0
) -> int:
    """The high watermark, once it has stopped moving.

    THE ALTERNATIVE IS A SLEEP, and a fixed wait is a flake generator: it
    passes on an idle machine and fails on a loaded one, and -- worse -- passes
    with a PARTIAL stream, landing a shorter file that every count stated as a
    minimum would still accept. This waits for the replay to stop producing
    instead of guessing how long it takes.

    Stability is necessary and not sufficient, which is why `main` also
    reconciles the stream's net effect against the source table. A connector
    that died mid-replay is perfectly stable too.
    """
    # WAIT FOR THE TOPIC TO EXIST FIRST, then for it to stop growing. These are
    # two different waits: the vendor has to become real before its stream can
    # settle, and conflating them would make a cold start indistinguishable
    # from a connector that captured nothing.
    waited = 0.0
    while watermark(consumer) is None:
        if waited >= appear:
            raise SystemExit(
                f"topic {TOPIC!r} does not exist after {appear:.0f}s. The ERP "
                f"vendor never finished being seeded -- check the seeder "
                f"container: it registers the Debezium connector and replays "
                f"the vendor's history, and it is a one-shot that compose does "
                f"not wait for."
            )
        time.sleep(gap)
        waited += gap

    stable, last = 0, -1
    while stable < polls:
        high = watermark(consumer)
        stable = stable + 1 if high == last and high and high > 0 else 0
        last = high
        if stable < polls:
            time.sleep(gap)
    return last


def surviving_customers() -> int:
    """How many rows the ERP actually holds now, asked of the ERP.

    The reconciliation this exists for: the captured stream's inserts minus its
    deletes must equal what the table holds. A stream that stopped early is
    stable, well-formed and short -- and this is the only check here that
    notices.
    """
    import psycopg

    # The ERP password is the VENDOR'S, declared in its own sources.yaml and
    # handed to the containers by the generated compose fragment. It is not a
    # Contoso secret this platform holds, which is why it does not go through
    # credentials.resolve: reading this count is a consumer verifying what it
    # consumed, over the dev credential the vendor published for the purpose.
    password = os.environ.get("ERP_PASSWORD", "contoso-erp-dev")
    dsn = (
        f"host={ERP_HOST} port={ERP_PORT} dbname={ERP_DB} "
        f"user={ERP_USER} password={password}"
    )
    with psycopg.connect(dsn, connect_timeout=30) as conn:
        return conn.execute("SELECT count(*) FROM erp.customer").fetchone()[0]


def main() -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq

    root = landing.root("contoso_erp")

    consumer = Consumer(
        {
            "bootstrap.servers": REDPANDA,
            "group.id": "contoso-erp-ingest",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.assign([TopicPartition(TOPIC, 0, 0)])

    high = settled(consumer)
    assert high > 0, (
        f"the change stream {TOPIC!r} is empty — Debezium captured nothing, "
        f"which usually means the connector was registered after the replay "
        f"or wal_level is not logical"
    )

    rows = []
    while len(rows) < high:
        msg = consumer.poll(30.0)
        assert msg is not None, f"stream stalled at {len(rows):,}/{high:,}"
        assert not msg.error(), msg.error()
        # A null value is a TOMBSTONE, which the connector is configured not to
        # emit (`tombstones.on.delete: false`). One appearing here would mean
        # the connector config drifted, so it fails rather than being skipped --
        # skipping would silently shorten the stream by exactly the deletes.
        raw = msg.value()
        assert raw is not None, (
            f"tombstone at offset {msg.offset()} — tombstones.on.delete drifted"
        )
        env = json.loads(raw)
        op = env["op"]
        assert op in OPS, (
            f"unexpected Debezium op {op!r} at offset {msg.offset()} — 'r' means "
            f"a snapshot read, which means the connector started after the DML"
        )
        # A delete carries its row in `before`; an insert and an update in
        # `after`. REPLICA IDENTITY FULL is what makes the delete's before-image
        # complete -- without it an SCD2 build cannot close the version it
        # belonged to, and the past is silently erased.
        image = env["before"] if op == "d" else env["after"]
        assert image, f"{op} at offset {msg.offset()} carried no row image"
        rows.append(
            {
                "op": OPS[op],
                "capture_offset": msg.offset(),
                **{c: image[c] for c in COLUMNS},
            }
        )
    consumer.close()

    by_op = {o: sum(1 for r in rows if r["op"] == o) for o in ("I", "U", "D")}
    # ALL THREE, because a stream carrying only inserts is a snapshot that
    # arrived over Kafka. The updates are what SCD2 is built from and the
    # deletes are what closes a version; a fixture that lost either would still
    # produce a green pipeline over a materially easier problem.
    assert all(by_op[o] > 0 for o in ("I", "U", "D")), (
        f"the stream carries {by_op} — a change log missing an op class is not "
        f"a change log, it is a snapshot with extra steps"
    )

    # THE RECONCILIATION. Everything above proves the stream is well-formed and
    # that we read all of it; only this proves it is COMPLETE. If Debezium
    # stopped early the watermark still settles, every message still parses,
    # and the landed file is simply short — and the arithmetic below is what
    # notices, because the ERP's own row count cannot be short with it.
    surviving = surviving_customers()
    net = by_op["I"] - by_op["D"]
    # DIAGNOSE THE DIRECTION. This originally said only "the stream is short",
    # which is one of two ways the arithmetic can fail and was the wrong one
    # the first time it fired: a second `make verify` replayed the vendor's
    # history into a broker that still held the first run's events, so the
    # stream carried exactly twice the inserts while the ERP table -- which the
    # seeder truncates -- held one replay's worth. A guard that names the wrong
    # cause sends you looking at Debezium when the fault is a topic that was
    # never cleared.
    if net != surviving:
        if net > surviving:
            raise SystemExit(
                f"the captured stream implies {net:,} surviving customers "
                f"({by_op['I']:,} inserted − {by_op['D']:,} deleted) but the ERP "
                f"holds {surviving:,}. The stream is LONGER than the source: the "
                f"vendor's history has been replayed into a topic that still held "
                f"an earlier run. The seeder truncates the TABLE; it does not "
                f"clear the BROKER. Take the vendor stack down with its volumes "
                f"(`make down` removes them) before re-running."
            )
        raise SystemExit(
            f"the captured stream implies {net:,} surviving customers "
            f"({by_op['I']:,} inserted − {by_op['D']:,} deleted) but the ERP holds "
            f"{surviving:,}. The stream is SHORT: Debezium did not capture the "
            f"whole replay."
        )

    # Parquet, because that is what a CDC sink lands and what a columnar read
    # downstream expects.
    table = pa.table({c: pa.array([r[c] for r in rows]) for c in rows[0]})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    blob = buf.getvalue()
    (root / "changes.parquet").write_bytes(blob)

    landing.record(erp_landed=len(blob), erp_change_events=len(rows), erp_ops=by_op)
    print(
        f"Contoso ERP: {len(rows):,} change events consumed from Kafka "
        f"({by_op['I']:,} I / {by_op['U']:,} U / {by_op['D']:,} D), "
        f"reconciled against {surviving:,} surviving rows → "
        f"changes.parquet, {len(blob):,} bytes"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
