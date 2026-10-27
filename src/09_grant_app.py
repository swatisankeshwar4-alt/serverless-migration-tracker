# Databricks notebook source
# MAGIC %md
# MAGIC # 09 - Give the tracking app access to the tracker tables
# MAGIC The app runs as its own service principal. This notebook looks that principal up and grants it
# MAGIC the minimum it needs:
# MAGIC * `SELECT` on the whole tracker schema (the app reads every table)
# MAGIC * `MODIFY` on `approvals` and `migration_status` only (the approval gate writes there)
# MAGIC
# MAGIC Run it once after `bundle deploy`, as a user who owns the schema (or a metastore admin).
# MAGIC It is safe to re-run.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "serverless_migration")
dbutils.widgets.text("app_name", "serverless-migration-tracker")
catalog, schema, app_name = (dbutils.widgets.get(k) for k in ("catalog", "schema", "app_name"))
for v in (catalog, schema, app_name):
    if not re.fullmatch(r"[A-Za-z0-9_\-]{1,128}", v):
        raise ValueError(f"invalid name: {v!r}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

app = WorkspaceClient().apps.get(app_name)
principal = app.service_principal_client_id   # the application id is the UC principal name
if not principal:
    raise RuntimeError(f"App {app_name!r} has no service principal yet. Deploy and start the app first.")
print(f"granting to app service principal {principal}")

# COMMAND ----------

fq = f"`{catalog}`.`{schema}`"
statements = [
    f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{principal}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {fq} TO `{principal}`",
    f"GRANT MODIFY ON TABLE {fq}.approvals TO `{principal}`",
    f"GRANT MODIFY ON TABLE {fq}.migration_status TO `{principal}`",
]
for s in statements:
    spark.sql(s)
    print("ok:", s)
