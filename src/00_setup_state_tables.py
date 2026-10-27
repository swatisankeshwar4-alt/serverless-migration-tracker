# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Create tracker state tables
# MAGIC Creates the Delta tables that hold the migration programme state.
# MAGIC Safe to re-run: every statement uses IF NOT EXISTS.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "serverless_migration")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fq = f"`{catalog}`.`{schema}`"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq}")

# COMMAND ----------

# One row per classic job / notebook / cluster found by discovery (L1, L11).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.inventory_workloads (
  workload_id     STRING  COMMENT 'job_id, notebook path or cluster_id',
  workload_type   STRING  COMMENT 'JOB | NOTEBOOK | CLUSTER',
  name            STRING,
  owner           STRING,
  task_count      INT     COMMENT 'active tasks, used for the >10 task rule (L2)',
  compute_kind    STRING  COMMENT 'CLASSIC | SERVERLESS | UNKNOWN',
  runtime_version STRING,
  source          STRING  COMMENT 'LIVE (SDK/system tables) or FIXTURE',
  discovered_at   TIMESTAMP
) USING DELTA
""")

# Blockers found per workload; one row per finding (L2-L7, L11, L13).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.readiness_findings (
  workload_id   STRING,
  limitation_id STRING COMMENT 'L2..L13',
  severity      STRING COMMENT 'BLOCKER | WARNING | INFO',
  detail        STRING,
  suggested_fix STRING,
  found_at      TIMESTAMP,
  auto_fixable  BOOLEAN COMMENT 'true when an automated rewrite removes this problem'
) USING DELTA
""")

# Latest score and status per workload (L1).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.migration_status (
  workload_id     STRING,
  readiness_score INT    COMMENT '0-100',
  status          STRING COMMENT 'DISCOVERED | READY | BLOCKED | APPROVED | MIGRATING | VALIDATING | DONE | ROLLED_BACK',
  wave            INT,
  updated_at      TIMESTAMP
) USING DELTA
""")

# Admin approval gate (L1).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.approvals (
  workload_id STRING,
  decision    STRING COMMENT 'APPROVED | REJECTED',
  decided_by  STRING,
  decided_at  TIMESTAMP,
  note        STRING
) USING DELTA
""")

# Classic vs serverless output comparison (L8).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.validation_results (
  workload_id   STRING,
  table_name    STRING,
  classic_rows  BIGINT,
  serverless_rows BIGINT,
  mismatched_rows BIGINT,
  passed        BOOLEAN,
  validated_at  TIMESTAMP,
  only_in_classic BIGINT,
  only_in_serverless BIGINT,
  changed_rows  BIGINT,
  detail        STRING COMMENT 'JSON: reason plus sample offending keys',
  note          STRING
) USING DELTA
""")

# DBU and cost before vs after (L12).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.cost_comparison (
  workload_id  STRING,
  period       STRING COMMENT 'BEFORE | AFTER',
  dbus         DOUBLE,
  list_cost_usd DOUBLE,
  team_tag     STRING,
  computed_at  TIMESTAMP
) USING DELTA
""")

# Generated split plans for jobs with more than 10 migratable tasks (L2).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.split_plans (
  workload_id STRING,
  plan_json   STRING COMMENT 'child jobs plus a paused orchestrator, built from the redacted definition',
  verified    BOOLEAN COMMENT 'true when verify_split found no problems',
  applied     BOOLEAN,
  created_at  TIMESTAMP
) USING DELTA
""")

# Audit log of every change the tracker makes (or would make in a dry run) to a workspace (L2, L10).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fq}.actions_log (
  action_id     STRING,
  workload_id   STRING,
  action        STRING COMMENT 'PLANNED_PAUSE | PAUSED | PLANNED_SPLIT | SPLIT_APPLIED | APPROVAL ...',
  detail        STRING,
  dry_run       BOOLEAN,
  performed_by  STRING,
  performed_at  TIMESTAMP,
  rollback_json STRING COMMENT 'the settings fragment that undoes the action'
) USING DELTA
""")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {fq}"))
