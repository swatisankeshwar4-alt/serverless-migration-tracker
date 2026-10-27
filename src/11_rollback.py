# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - Rollback: undo a change the tracker made
# MAGIC Every real change is logged in `actions_log` with the exact undo in `rollback_json`
# MAGIC (paused schedule, reverted drift, created split jobs, written notebook copies).
# MAGIC
# MAGIC * Leave `action_id` empty to list what can be rolled back.
# MAGIC * Set `action_id` (or `latest`) and `dry_run=false` to undo it (workspace admin only). The undo is
# MAGIC   itself logged as `ROLLED_BACK`.

# COMMAND ----------

import json
import os
import sys
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "serverless_migration")
dbutils.widgets.text("action_id", "")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
action_id = dbutils.widgets.get("action_id").strip()
dry_run = dbutils.widgets.get("dry_run") == "true"
fq = f"`{catalog}`.`{schema}`"

sys.path.insert(0, os.getcwd())
from tracker import approvals, rollback  # noqa: E402

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
is_admin, who = approvals.workspace_admin(w)
who = who or spark.sql("SELECT current_user()").first()[0]

actions = [r.asDict() for r in spark.sql(f"SELECT * FROM {fq}.actions_log").collect()]
open_items = rollback.candidates(actions)
display(spark.createDataFrame(
    [(a["action_id"], a["workload_id"], a["action"], a["detail"], str(a["performed_at"])) for a in open_items]
    or [("-", "-", "-", "nothing to roll back", "-")],
    "action_id STRING, workload_id STRING, action STRING, detail STRING, performed_at STRING"))

# COMMAND ----------

if action_id:
    target = open_items[0] if action_id == "latest" and open_items else \
        next((a for a in open_items if a["action_id"] == action_id), None)
    if target is None:
        raise ValueError(f"{action_id!r} is not an open action with an undo (see the list above)")
    ops = rollback.operations(target["rollback_json"])
    for op in ops:
        print(("would call " if dry_run else "calling ") + op["call"], json.dumps(op["body"]))
    if not dry_run:
        if not is_admin:
            raise PermissionError("Only a workspace admin can roll back.")
        for op in ops:
            method, path = op["call"].split(" ", 1)
            w.api_client.do(method, path, body=op["body"])
        spark.createDataFrame(
            [(str(uuid.uuid4()), target["workload_id"], "ROLLED_BACK",
              f"undo of {target['action_id']} ({target['action']})", False, who, datetime.now(timezone.utc), None)],
            "action_id STRING, workload_id STRING, action STRING, detail STRING, dry_run BOOLEAN, "
            "performed_by STRING, performed_at TIMESTAMP, rollback_json STRING",
        ).write.mode("append").saveAsTable(f"{fq}.actions_log")
        print(f"rolled back {target['action']} on {target['workload_id']}")
