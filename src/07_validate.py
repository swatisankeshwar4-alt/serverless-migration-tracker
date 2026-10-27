# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - A/B output validation (L8)
# MAGIC Compares what a workload wrote on **classic** with what its migrated version wrote on **serverless**,
# MAGIC and records pass or fail per table in `validation_results` (shown on the app's Validation tab).
# MAGIC
# MAGIC **How a real migration uses it:** run the original job and the migrated clone so they write to two
# MAGIC different tables, then point this notebook at the pair (`mode = TABLES`).
# MAGIC
# MAGIC **What it checks:** same columns and types, same row count, and every row equal (keys optional;
# MAGIC floats rounded; ignored columns skipped). See `tracker/validator.py`.
# MAGIC
# MAGIC **Status workflow:** with `update_status = true` an admin can move an APPROVED / MIGRATING / VALIDATING job
# MAGIC to `DONE` (every recorded table passed) or `ROLLED_BACK` (a table failed: the job is not promoted and stays
# MAGIC on classic). Jobs that were never approved are never touched.
# MAGIC
# MAGIC **`mode = DEMO`** builds sample tables and runs one passing and one failing comparison. Free Edition has no
# MAGIC classic compute, so in the demo **both sides are produced on serverless**; the failing side has seeded
# MAGIC differences. The comparison logic is the same one used for real classic-vs-serverless output.
# MAGIC
# MAGIC For several tables of one workload: comma-separate the tables, and `;`-separate the key lists in the same order.

# COMMAND ----------

import json
import os
import sys
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "serverless_migration")
dbutils.widgets.dropdown("mode", "DEMO", ["DEMO", "TABLES"])
dbutils.widgets.text("workload_id", "")
dbutils.widgets.text("classic_table", "")
dbutils.widgets.text("serverless_table", "")
dbutils.widgets.text("key_columns", "")
dbutils.widgets.text("ignore_columns", "")
dbutils.widgets.text("decimals", "6")
dbutils.widgets.dropdown("update_status", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
mode = dbutils.widgets.get("mode")
fq = f"`{catalog}`.`{schema}`"

sys.path.insert(0, os.getcwd())
from tracker import approvals, tables, validator  # noqa: E402

# Tables from earlier versions of step 1 lack the new columns.
for col, typ in [("only_in_classic", "BIGINT"), ("only_in_serverless", "BIGINT"), ("changed_rows", "BIGINT"),
                 ("detail", "STRING"), ("note", "STRING")]:
    tables.ensure_column(spark, f"{fq}.validation_results", col, typ)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq}.actions_log (action_id STRING, workload_id STRING, action STRING,
  detail STRING, dry_run BOOLEAN, performed_by STRING, performed_at TIMESTAMP, rollback_json STRING) USING DELTA""")


def qualify(name):
    name = name.strip()
    return name if "." in name else f"{catalog}.{schema}.{name}"


def compare(workload_id, classic, serverless, keys, ignore, decimals, note):
    """Run one table comparison and return the row to store."""
    a, b = qualify(classic), qualify(serverless)
    problems, common = validator.compare_schemas(spark.table(a).dtypes, spark.table(b).dtypes, ignore)
    counts, samples = None, []
    if not problems:
        if not common:
            problems = ["no comparable columns left after ignoring"]
        else:
            counts = spark.sql(validator.build_diff_sql(a, b, common, keys, decimals)).first().asDict()
            bad = sum(int(counts[k] or 0) for k in ("only_in_classic", "only_in_serverless", "changed_rows"))
            if bad and keys:
                s = validator.build_sample_sql(a, b, common, keys, decimals)
                samples = [r.asDict() for r in spark.sql(s).collect()]
    passed, reason = validator.verdict(problems, counts)
    c = counts or {}
    n = lambda k: int(c.get(k) or 0)  # noqa: E731
    print(f"{'PASS' if passed else 'FAIL'}  {workload_id}  {serverless}: {reason}")
    return dict(workload_id=workload_id, table_name=serverless.split(".")[-1], classic_rows=n("classic_rows"),
                serverless_rows=n("serverless_rows"),
                mismatched_rows=n("only_in_classic") + n("only_in_serverless") + n("changed_rows"),
                passed=passed, only_in_classic=n("only_in_classic"), only_in_serverless=n("only_in_serverless"),
                changed_rows=n("changed_rows"), note=note,
                detail=json.dumps({"reason": reason, "samples": samples}, default=str))


def store(rows):
    now = datetime.now(timezone.utc)
    spark.createDataFrame([(r["workload_id"], r["table_name"]) for r in rows], "w STRING, t STRING") \
        .createOrReplaceTempView("_val_keys")
    # Keep the latest result per workload and table; actions_log keeps the history of decisions.
    spark.sql(f"DELETE FROM {fq}.validation_results WHERE concat(workload_id, '|', table_name) "
              f"IN (SELECT concat(w, '|', t) FROM _val_keys)")
    spark.createDataFrame(
        [(r["workload_id"], r["table_name"], r["classic_rows"], r["serverless_rows"], r["mismatched_rows"],
          r["passed"], now, r["only_in_classic"], r["only_in_serverless"], r["changed_rows"], r["detail"], r["note"])
         for r in rows],
        "workload_id STRING, table_name STRING, classic_rows BIGINT, serverless_rows BIGINT, mismatched_rows BIGINT, "
        "passed BOOLEAN, validated_at TIMESTAMP, only_in_classic BIGINT, only_in_serverless BIGINT, "
        "changed_rows BIGINT, detail STRING, note STRING").write.mode("append").saveAsTable(f"{fq}.validation_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DEMO mode: sample tables, one passing and one failing comparison

# COMMAND ----------

if mode == "DEMO":
    t = lambda n: f"{fq}.`{n}`"  # noqa: E731
    spark.sql(f"""CREATE OR REPLACE TABLE {t('ab_demo_classic_orders')} AS
      SELECT CAST(id AS BIGINT) AS order_id, concat('cust_', CAST(id % 7 AS STRING)) AS customer, CAST(id * 10.25 AS DOUBLE) AS amount,
             date_add(DATE'2026-01-01', CAST(id % 30 AS INT)) AS order_date, current_timestamp() AS loaded_at
      FROM range(1, 201)""")
    # Good migration: float noise below the tolerance and a different load time (ignored). Must PASS.
    spark.sql(f"""CREATE OR REPLACE TABLE {t('ab_demo_serverless_orders')} AS
      SELECT order_id, customer, amount + 1e-10 AS amount, order_date, current_timestamp() AS loaded_at
      FROM {t('ab_demo_classic_orders')}""")
    # Bad migration: one changed amount, one NULL customer, one missing order, one extra order. Must FAIL.
    spark.sql(f"""CREATE OR REPLACE TABLE {t('ab_demo_serverless_orders_bad')} AS
      SELECT order_id, CASE WHEN order_id = 7 THEN NULL ELSE customer END AS customer,
             CASE WHEN order_id = 17 THEN amount + 1.5 ELSE amount END AS amount, order_date, loaded_at
      FROM {t('ab_demo_classic_orders')} WHERE order_id <> 42
      UNION ALL SELECT CAST(999 AS BIGINT), 'cust_x', CAST(1.0 AS DOUBLE), DATE'2026-02-01', current_timestamp()""")
    note = "DEMO: both sides ran on serverless (Free Edition has no classic compute)"
    rows = [
        compare("demo-ab-pass", "ab_demo_classic_orders", "ab_demo_serverless_orders", ["order_id"], ["loaded_at"], 6, note),
        compare("demo-ab-fail", "ab_demo_classic_orders", "ab_demo_serverless_orders_bad", ["order_id"], ["loaded_at"], 6, note),
    ]
    store(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABLES mode: your real classic and serverless outputs

# COMMAND ----------

if mode == "TABLES":
    wid = dbutils.widgets.get("workload_id").strip()
    classic = [x for x in dbutils.widgets.get("classic_table").split(",") if x.strip()]
    serverless = [x for x in dbutils.widgets.get("serverless_table").split(",") if x.strip()]
    key_groups = dbutils.widgets.get("key_columns").split(";") if dbutils.widgets.get("key_columns").strip() else []
    ignore = [x.strip() for x in dbutils.widgets.get("ignore_columns").split(",") if x.strip()]
    decimals = int(dbutils.widgets.get("decimals"))
    if not wid or not classic or len(classic) != len(serverless):
        raise ValueError("TABLES mode needs workload_id and the same number of classic_table and serverless_table names")
    if not 0 <= decimals <= 15:
        raise ValueError("decimals must be between 0 and 15")
    rows = []
    for i, (a, b) in enumerate(zip(classic, serverless)):
        keys = [k.strip() for k in key_groups[i].split(",") if k.strip()] if i < len(key_groups) else []
        rows.append(compare(wid, a, b, keys, ignore, decimals, "TABLES: classic output vs serverless output"))
    store(rows)

    if dbutils.widgets.get("update_status") == "true":
        from databricks.sdk import WorkspaceClient
        is_admin, who = approvals.workspace_admin(WorkspaceClient())
        if not is_admin:
            print("update_status ignored: only workspace admins can change a migration status.")
        else:
            all_ok = spark.sql(f"SELECT min(passed) FROM {fq}.validation_results WHERE workload_id = '{wid.replace(chr(39), '')}'").first()[0]
            cur = spark.sql(f"SELECT status FROM {fq}.migration_status WHERE workload_id = '{wid.replace(chr(39), '')}'").first()
            new = validator.next_status(cur.status if cur else None, bool(all_ok))
            if new is None:
                print(f"status unchanged: {cur.status if cur else 'unknown workload'} is not APPROVED, MIGRATING or VALIDATING")
            else:
                spark.createDataFrame([(wid, new)], "workload_id STRING, s STRING").createOrReplaceTempView("_new_status")
                spark.sql(f"""MERGE INTO {fq}.migration_status t USING _new_status n ON t.workload_id = n.workload_id
                              WHEN MATCHED THEN UPDATE SET t.status = n.s, t.updated_at = current_timestamp()""")
                action = "VALIDATION_PASSED" if new == "DONE" else "VALIDATION_FAILED"
                detail = "; ".join(f"{r['table_name']}: {json.loads(r['detail'])['reason']}" for r in rows)
                spark.createDataFrame([(str(uuid.uuid4()), wid, action, detail[:2000], False, who, datetime.now(timezone.utc), None)],
                                      "action_id STRING, workload_id STRING, action STRING, detail STRING, dry_run BOOLEAN, "
                                      "performed_by STRING, performed_at TIMESTAMP, rollback_json STRING"
                                      ).write.mode("append").saveAsTable(f"{fq}.actions_log")
                print(f"status of {wid}: {cur.status} -> {new}")

# COMMAND ----------

display(spark.sql(f"""SELECT workload_id, table_name, passed, classic_rows, serverless_rows, only_in_classic,
  only_in_serverless, changed_rows, get_json_object(detail, '$.reason') AS reason, validated_at
  FROM {fq}.validation_results ORDER BY validated_at DESC"""))
