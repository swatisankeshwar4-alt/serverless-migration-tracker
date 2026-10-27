# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Drift check: Git (Asset Bundle) vs the workspace (L9)
# MAGIC The bundle files in Git are the source of truth. This notebook reads every job in `resources/*.yml`
# MAGIC (as deployed with this bundle) and compares it with the same job as it is now in the workspace.
# MAGIC Anything changed by hand in the UI shows up in `drift_findings`: a schedule added, a timeout or tag
# MAGIC changed, a parameter edited, a task moved back to a classic cluster.
# MAGIC
# MAGIC * `enforce=false` (default): report only.
# MAGIC * `enforce=true` (admin): top-level settings are put back to the Git values with `jobs/update`; the
# MAGIC   values found are stored in `actions_log.rollback_json`, so `11_rollback` can undo the revert.
# MAGIC   Task-level drift is not edited here: redeploy the bundle (`databricks bundle deploy`) to fix it.

# COMMAND ----------

import glob
import json
import os
import sys
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "serverless_migration")
dbutils.widgets.text("discovery_mode", "FIXTURE")
dbutils.widgets.text("cost_source", "SAMPLE")
dbutils.widgets.dropdown("enforce", "false", ["false", "true"])
variables = {k: dbutils.widgets.get(k) for k in ("catalog", "schema", "discovery_mode", "cost_source")}
enforce = dbutils.widgets.get("enforce") == "true"
fq = f"`{variables['catalog']}`.`{variables['schema']}`"

here = os.getcwd()
sys.path.insert(0, here)
from tracker import approvals, drift  # noqa: E402

try:
    import yaml
except ImportError:  # not in every serverless environment
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pyyaml"])
    import yaml

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
is_admin, who = approvals.workspace_admin(w)
who = who or spark.sql("SELECT current_user()").first()[0]
if enforce and not is_admin:
    raise PermissionError("Only a workspace admin can revert drift. Re-run with enforce=false to report only.")

# COMMAND ----------

bundle_root = os.path.dirname(here)
docs = []
for p in sorted(glob.glob(os.path.join(bundle_root, "resources", "*.yml"))):
    with open(p, encoding="utf-8") as f:
        docs.append(yaml.safe_load(f))
in_git = drift.bundle_jobs(docs, variables)
marker = "/.bundle/serverless-migration-tracker/"
live_jobs = [{"job_id": j.job_id, "settings": j.settings.as_dict() if j.settings else {}}
             for j in w.jobs.list(expand_tasks=True)]
print(f"{len(in_git)} jobs in Git, {len(live_jobs)} jobs in the workspace")

now = datetime.now(timezone.utc)
findings, log = [], []
for key, job_def in sorted(in_git.items()):
    live = drift.match_live_job(job_def["name"], marker, live_jobs)
    if live is None:
        findings.append((key, None, job_def["name"], "job", "defined", "not deployed", False, now))
        continue
    rows = drift.compare(job_def, live["settings"])
    for r in rows:
        findings.append((key, str(live["job_id"]), live["settings"].get("name"), r["field"],
                         json.dumps(r["bundle"]), json.dumps(r["live"]), r["auto_revert"], now))
    auto = [r for r in rows if r["auto_revert"]]
    if auto:
        revert, undo = drift.revert_payload(job_def, live["settings"], auto)
        detail = "reverted to Git: " + ", ".join(r["field"] for r in auto)
        if enforce:
            w.api_client.do("POST", "/api/2.1/jobs/update", body={"job_id": live["job_id"], **revert})
        log.append((str(uuid.uuid4()), str(live["job_id"]), "DRIFT_REVERTED" if enforce else "PLANNED_DRIFT_REVERT",
                    detail, not enforce, who, now,
                    json.dumps({"job_id": live["job_id"], **undo}) if enforce else None))

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq}.drift_findings (
  job_key STRING COMMENT 'resource key in the bundle', job_id STRING, job_name STRING, field STRING,
  in_git STRING, in_workspace STRING, auto_revert BOOLEAN, checked_at TIMESTAMP) USING DELTA""")
spark.sql(f"DELETE FROM {fq}.drift_findings")
if findings:
    spark.createDataFrame(findings, "job_key STRING, job_id STRING, job_name STRING, field STRING, in_git STRING, "
                                    "in_workspace STRING, auto_revert BOOLEAN, checked_at TIMESTAMP") \
        .write.mode("append").saveAsTable(f"{fq}.drift_findings")
if log:
    spark.createDataFrame(log, "action_id STRING, workload_id STRING, action STRING, detail STRING, dry_run BOOLEAN, "
                               "performed_by STRING, performed_at TIMESTAMP, rollback_json STRING") \
        .write.mode("append").saveAsTable(f"{fq}.actions_log")

print(f"{len(findings)} drift rows; enforce = {enforce}")
display(spark.sql(f"SELECT job_name, field, in_git, in_workspace, auto_revert FROM {fq}.drift_findings ORDER BY job_name"))
