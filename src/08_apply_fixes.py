# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Apply the automated fixes and prove they work (L6, L13)
# MAGIC 1. **Apply**: every automated rewrite in `proposed_rewrites` is written as a **new** notebook in
# MAGIC    `/Shared/serverless_migration_demo/` (`<name>_serverless`). The original is never edited.
# MAGIC 2. **Prove**: the runnable example `fixtures/notebooks/runnable_demo.py` (checkpoint, global temp view,
# MAGIC    ANSI `CAST` on bad input) is run on **serverless** twice, as written and as rewritten. The original
# MAGIC    fails, the rewrite succeeds; both results go to `rewrite_proofs`.
# MAGIC
# MAGIC Writing needs `dry_run=false` and a workspace admin. Every copy is logged in `actions_log` with
# MAGIC the undo (delete the copy), so `11_rollback` can remove it.

# COMMAND ----------

import base64
import json
import os
import sys
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "serverless_migration")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
dbutils.widgets.dropdown("prove", "true", ["true", "false"])
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
dry_run = dbutils.widgets.get("dry_run") == "true"
prove = dbutils.widgets.get("prove") == "true"
fq = f"`{catalog}`.`{schema}`"
FOLDER = "/Shared/serverless_migration_demo"

here = os.getcwd()
sys.path.insert(0, here)
from tracker import approvals, codescan, fixes, tables  # noqa: E402

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs as sdk_jobs
from databricks.sdk.service.workspace import ImportFormat, Language

w = WorkspaceClient()
is_admin, who = approvals.workspace_admin(w)
who = who or spark.sql("SELECT current_user()").first()[0]
print(f"signed in as {who}; admin = {is_admin}; dry_run = {dry_run}")
if not dry_run and not is_admin:
    raise PermissionError("Only a workspace admin can apply fixes. Re-run with dry_run=true to preview.")

# COMMAND ----------

def import_notebook(path, source, language):
    w.workspace.import_(path=path, format=ImportFormat.SOURCE, overwrite=True,
                        language=Language.SQL if language == "SQL" else Language.PYTHON,
                        content=base64.b64encode(source.encode("utf-8")).decode("ascii"))

now = datetime.now(timezone.utc)
log, applied = [], []
rows = spark.sql(f"""SELECT workload_id, language, proposed_source FROM {fq}.proposed_rewrites
                     WHERE decision = 'REWRITE_PROPOSED' AND proposed_source IS NOT NULL""").collect()
if not dry_run:
    w.workspace.mkdirs(FOLDER)
for r in rows:
    target = fixes.demo_target_path(FOLDER, r["workload_id"], "_serverless")
    if not dry_run:
        import_notebook(target, r["proposed_source"], r["language"])
        applied.append(r["workload_id"])
    log.append((str(uuid.uuid4()), r["workload_id"], "PLANNED_REWRITE" if dry_run else "REWRITE_APPLIED",
                f"rewritten copy -> {target} (original untouched)", dry_run, who, now,
                None if dry_run else json.dumps({"delete_path": target})))
    print(("would write " if dry_run else "wrote ") + target)

if applied:
    spark.createDataFrame([(a,) for a in applied], "workload_id STRING").createOrReplaceTempView("_applied")
    spark.sql(f"MERGE INTO {fq}.proposed_rewrites t USING _applied s ON t.workload_id = s.workload_id "
              f"WHEN MATCHED THEN UPDATE SET t.applied = true")

# COMMAND ----------

# MAGIC %md ## Proof: run the original and the rewrite on serverless

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq}.rewrite_proofs (
  notebook STRING, version STRING COMMENT 'ORIGINAL | REWRITTEN', source STRING, result STRING,
  error STRING, run_id BIGINT, proved_at TIMESTAMP) USING DELTA""")

proofs = []
if prove and not dry_run:
    with open(os.path.join(os.path.dirname(here), "fixtures", "notebooks", "runnable_demo.py"), encoding="utf-8") as f:
        original = f.read()
    rewritten, changes = codescan.rewrite_notebook(original, "PYTHON")
    print("changes:", changes)
    versions = {"ORIGINAL": (f"{FOLDER}/runnable_demo_original", original),
                "REWRITTEN": (f"{FOLDER}/runnable_demo_serverless", rewritten)}
    waits = {}
    for version, (path, src) in versions.items():
        import_notebook(path, src, "PYTHON")
        # No cluster in the task: a one-time run on serverless compute.
        waits[version] = w.jobs.submit(run_name=f"[tracker] proof {version.lower()}", tasks=[
            sdk_jobs.SubmitTask(task_key="proof", notebook_task=sdk_jobs.NotebookTask(notebook_path=path))])
    for version, wait in waits.items():
        run_id, error = wait.run_id, None
        try:
            run = wait.result()
            state = run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"
        except Exception as e:  # noqa: BLE001
            run, state, error = None, "FAILED", str(e)[:400]
        if state != "SUCCESS" and run is not None and run.tasks:
            try:
                out = w.jobs.get_run_output(run.tasks[0].run_id)
                error = (out.error or out.error_trace or error or "")[:400]
            except Exception:  # noqa: BLE001
                pass
        proofs.append(("runnable_demo", version, versions[version][1], state, error, run_id, datetime.now(timezone.utc)))
        print(version, state, (error or "")[:200])
    spark.sql(f"DELETE FROM {fq}.rewrite_proofs WHERE notebook = 'runnable_demo'")
    spark.createDataFrame(proofs, "notebook STRING, version STRING, source STRING, result STRING, error STRING, "
                                  "run_id BIGINT, proved_at TIMESTAMP").write.mode("append").saveAsTable(f"{fq}.rewrite_proofs")
    for version, (path, _) in versions.items():
        log.append((str(uuid.uuid4()), "runnable_demo", "REWRITE_PROOF", f"{version} run on serverless: "
                    f"{next(p[3] for p in proofs if p[1] == version)}", False, who, datetime.now(timezone.utc),
                    json.dumps({"delete_path": path})))

# COMMAND ----------

if log:
    spark.createDataFrame(log, "action_id STRING, workload_id STRING, action STRING, detail STRING, dry_run BOOLEAN, "
                               "performed_by STRING, performed_at TIMESTAMP, rollback_json STRING") \
        .write.mode("append").saveAsTable(f"{fq}.actions_log")
display(spark.sql(f"SELECT notebook, version, result, error FROM {fq}.rewrite_proofs ORDER BY version"))
