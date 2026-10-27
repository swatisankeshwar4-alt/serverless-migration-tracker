# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Safety: large-job split plans (L2) and duplicate-schedule guard (L10)
# MAGIC By default this notebook **only plans and logs**. It changes a workspace only when asked, and only for
# MAGIC an admin in LIVE mode.
# MAGIC
# MAGIC * **L2**: for jobs with more than 10 migratable tasks it generates a split plan (child jobs plus a
# MAGIC   *paused* orchestrator), checks the plan against the original, and stores it in `split_plans`.
# MAGIC   `apply_splits=true` creates the jobs for **approved** jobs only. The original job is never edited.
# MAGIC * **L10**: finds clones that fire on the same schedule as the original. `enforce=true` pauses the
# MAGIC   extra trigger and records the undo settings in `actions_log`.
# MAGIC
# MAGIC Real job definitions are re-fetched from the API at apply time, because stored definitions have
# MAGIC secret values removed.

# COMMAND ----------

import json
import os
import sys
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "serverless_migration")
dbutils.widgets.dropdown("mode", "FIXTURE", ["FIXTURE", "LIVE"])
dbutils.widgets.dropdown("enforce", "false", ["false", "true"])
dbutils.widgets.dropdown("apply_splits", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
mode = dbutils.widgets.get("mode")
enforce = dbutils.widgets.get("enforce") == "true"
apply_splits = dbutils.widgets.get("apply_splits") == "true"
fq = f"`{catalog}`.`{schema}`"

sys.path.insert(0, os.getcwd())
from tracker import approvals, schedules, splitter  # noqa: E402

# Tables from an earlier version of step 1 may be missing: create them here too.
spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq}.split_plans (workload_id STRING, plan_json STRING,
  verified BOOLEAN, applied BOOLEAN, created_at TIMESTAMP) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq}.actions_log (action_id STRING, workload_id STRING, action STRING,
  detail STRING, dry_run BOOLEAN, performed_by STRING, performed_at TIMESTAMP, rollback_json STRING) USING DELTA""")

rows = spark.sql(f"SELECT definition_json FROM {fq}.raw_definitions "
                 f"WHERE source = '{mode}' AND workload_type = 'JOB'").collect()
if not rows:
    raise RuntimeError(f"No job definitions for mode {mode}. Run the discover job first.")
jobs = [json.loads(r.definition_json) for r in rows]
by_id = {str(j["job_id"]): j for j in jobs}

# Changing a workspace needs LIVE mode and an admin. Everything else is plan-and-log.
w, is_admin, who = None, False, ""
if mode == "LIVE":
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    is_admin, who = approvals.workspace_admin(w)
can_change = mode == "LIVE" and is_admin
if (enforce or apply_splits) and not can_change:
    print("enforce/apply_splits ignored: they need LIVE mode and a workspace admin. Planning and logging only.")
    enforce = apply_splits = False

now = datetime.now(timezone.utc)
# Planned (dry-run) actions are logged once: a re-run must not repeat an identical line.
already_logged = {(r.workload_id, r.action, r.detail) for r in spark.sql(
    f"SELECT workload_id, action, detail FROM {fq}.actions_log WHERE dry_run = true").collect()}
log = []   # (action_id, workload_id, action, detail, dry_run, performed_by, performed_at, rollback_json)


def record(wid, action, detail, dry_run, rollback=None):
    if dry_run and (wid, action, detail) in already_logged:
        return
    log.append((str(uuid.uuid4()), wid, action, detail, dry_run, who or "tracker", now,
                json.dumps(rollback) if rollback else None))


def api(method, path, body=None, query=None):
    return w.api_client.do(method, path, query=query, body=body)

# COMMAND ----------

# MAGIC %md
# MAGIC ## L2: split plans

# COMMAND ----------

approved = {r.workload_id for r in spark.sql(
    f"SELECT workload_id FROM {fq}.migration_status WHERE status = 'APPROVED'").collect()}
already_applied = {r.workload_id for r in spark.sql(
    f"SELECT workload_id FROM {fq}.split_plans WHERE applied = true").collect()}

plans = []   # (workload_id, plan_json, verified, applied, created_at)
for wid, job in by_id.items():
    if wid in already_applied:
        continue
    try:
        plan = splitter.plan_split(job)
    except ValueError as e:
        record(wid, "SPLIT_NOT_POSSIBLE", str(e), True)
        continue
    if plan is None:
        continue
    problems = splitter.verify_split(job, plan)
    applied = False
    if not problems:
        record(wid, "PLANNED_SPLIT", f"{len(plan['children'])} child jobs + paused orchestrator", True)
        if apply_splits and wid in approved:
            # Re-fetch the real definition (stored ones are redacted), re-plan, re-verify, then create.
            real = api("GET", "/api/2.1/jobs/get", query={"job_id": int(wid)})
            real_plan = splitter.plan_split(real)
            if real_plan and not splitter.verify_split(real, real_plan):
                actions = splitter.apply_split(lambda m, p, b: api(m, p, body=b), real_plan, dry_run=False)
                record(wid, "SPLIT_APPLIED", json.dumps([a.get("job_id") for a in actions]), False,
                       rollback={"delete_jobs": [a["job_id"] for a in actions if a.get("job_id")]})
                applied = True
    else:
        record(wid, "SPLIT_PLAN_REJECTED", "; ".join(problems), True)
    plans.append((wid, json.dumps(plan), not problems, applied, now))

spark.createDataFrame([(p[0],) for p in plans], "workload_id STRING").createOrReplaceTempView("_plan_ids")
spark.sql(f"DELETE FROM {fq}.split_plans WHERE applied = false AND workload_id IN (SELECT workload_id FROM _plan_ids)")
spark.createDataFrame(plans, "workload_id STRING, plan_json STRING, verified BOOLEAN, applied BOOLEAN, created_at TIMESTAMP"
                      ).write.mode("append").saveAsTable(f"{fq}.split_plans")
print(f"{len(plans)} split plans stored")

# COMMAND ----------

# MAGIC %md
# MAGIC ## L10: duplicate schedules

# COMMAND ----------

dups = schedules.find_duplicates(jobs)
for d in dups:
    extra = d["extra"]
    frag = schedules.pause_settings(by_id[extra])
    detail = f"job {extra} duplicates the schedule ({d['schedule']}) of job {d['keep']}"
    if enforce and frag:
        real = api("GET", "/api/2.1/jobs/get", query={"job_id": int(extra)})
        real_frag = schedules.pause_settings(real)
        if real_frag:
            api("POST", "/api/2.1/jobs/update", body={"job_id": int(extra), "new_settings": real_frag})
            undo = {k: {**v, "pause_status": "UNPAUSED"} for k, v in real_frag.items()}
            record(extra, "PAUSED", detail, False, rollback={"job_id": int(extra), "new_settings": undo})
    else:
        record(extra, "PLANNED_PAUSE", detail, True)
print(f"{len(dups)} duplicate schedule(s); enforce={enforce}")

# COMMAND ----------

if log:
    spark.createDataFrame(
        log, "action_id STRING, workload_id STRING, action STRING, detail STRING, dry_run BOOLEAN, "
             "performed_by STRING, performed_at TIMESTAMP, rollback_json STRING"
    ).write.mode("append").saveAsTable(f"{fq}.actions_log")

display(spark.sql(f"""
SELECT performed_at, workload_id, action, detail, dry_run
FROM {fq}.actions_log ORDER BY performed_at DESC LIMIT 50"""))
