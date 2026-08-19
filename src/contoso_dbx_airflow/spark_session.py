"""Ambient Spark for this consumer: Databricks Connect onto the attached Sail."""

from __future__ import annotations

import os

from .target import T


def connect():
    from databricks.connect import DatabricksSession
    from databricks.sdk.service.compute import State

    t = T()
    w = t.workspace_client()
    running = [
        c for c in w.clusters.list() if getattr(c, "state", None) == State.RUNNING
    ]
    if running:
        cluster_id = running[0].cluster_id
    else:
        created = w.clusters.create(
            cluster_name="contoso", spark_version="13.3.x-scala2.12"
        ).result()
        cluster_id = created.cluster_id
    host = t.host.replace("http://", "").replace("https://", "")
    # pyspark ChannelBuilder skips TLS only for the name localhost.
    remote = (
        f"sc://localhost:{host.split(':')[-1]}/;"
        f"use_ssl=false;token={t.token};x-databricks-cluster-id={cluster_id}"
    )
    os.environ.setdefault("SPARK_CONNECT_GRPC_MESSAGE_MAX_SIZE", "134217728")
    spark = DatabricksSession.builder.remote(remote).getOrCreate()
    # Sail 0.24 advertises 3GB. databricks-connect 19.1 does int() on that
    # string inside createDataFrame, so bronze never starts.
    spark.conf.set("spark.sql.session.localRelationSizeLimit", "3221225472")
    return spark
