# Serverless Migration Tracker

A fleet-level tracker for Databricks classic-to-serverless migration. The `/compute migrate to serverless` agent moves
**one** notebook or job at a time. This project is the programme around it: it inventories every job, notebook and
cluster, scores readiness, lists the blockers per workload, plans waves, gates approval, checks that migrated output
is correct, and compares DBUs and cost before and after, in one app and one dashboard.

**It tracks, plans and verifies; it does not migrate.** Moving a workload is done by the agent or by hand. The tracker
tells you what to move first, what is blocked and why, and whether the result can be trusted.

## What it covers

| ID | Limitation | What is built | Depth |
|---|---|---|---|
| L1 | No fleet view, progress or approval | Inventory, readiness score, waves, admin approval gate, status workflow, Databricks App and AI/BI dashboard | Full |
| L2 | Jobs over 10 tasks | Split plan (child jobs plus a paused orchestrator), checked against the original, stored in `split_plans`. `apply_splits=true` creates the jobs for approved jobs | Automated, see gaps |
| L3 | Init scripts | Detected; a script is translated into a serverless environment spec (pip packages) | Detect + translate (fixture script) |
| L4 | ML runtime, custom image, DBR below 13, instance profiles | Detected from cluster specs, fix path in each finding | Detect |
| L5 | Eggs, JARs, Maven | Detected per job and task, with the wheel / volume / PyPI path | Detect |
| L6 | cache, checkpoint, global temp views, mounts | Static scan; automated rewrites are produced and proven by rescanning the rewritten source (`auto_fixable`) | Automated rewrite (proposed, not applied) |
| L7 | Scala and R | Detected and classified: PySpark/SQL rewrite effort or keep on classic | Classify |
| L8 | No output verification | A/B diff of the classic and serverless output tables with pass or fail, per-row reasons, status workflow | Automated |
| L10 | Duplicate schedules | Detected; `enforce=true` pauses the extra trigger and stores the undo settings | Automated, see gaps |
| L11 | Log delivery, SSH keys, mounts | Inventoried and reported as "lost on serverless" | Detect |
| L12 | Cost not tracked | DBUs and list cost per run before vs after, by job and team tag, from `system.billing` | Automated |
| L13 | Silent behaviour changes | Missing stream triggers and ANSI-risky SQL detected, with `try_cast` / `try_divide` rewrites | Automated rewrite (proposed) |

L9 (bundle and Git drift) is **not** addressed.

## Architecture

```mermaid
flowchart LR
  A[Jobs, clusters, notebooks<br/>SDK or fixtures] --> B[01 discover]
  B --> C[(inventory_workloads)]
  C --> D[03 code scan] --> E[(readiness_findings, proposed_rewrites)]
  C --> F[02 readiness + waves] --> G[(migration_status)]
  C --> H[05 safety: split plans, duplicate schedules] --> I[(split_plans, actions_log)]
  J[system.billing] --> K[06 cost] --> L[(cost_summary)]
  M[07 A/B validator] --> N[(validation_results)]
  G & E & I & L & N --> O[Databricks App + AI/BI dashboard]
  O -- admin approves --> P[(approvals)] --> G
```

* All state is Delta tables in one Unity Catalog schema (default `workspace.serverless_migration`).
* Jobs are plain notebooks, deployed as an Asset Bundle; logic lives in `src/tracker/` as pure Python and is unit-tested.
* The App reads through a SQL warehouse as its service principal and writes only `approvals` and `migration_status`.
  The approver is the signed-in user, checked as workspace admin; without that it is read-only.
* Planning is the default everywhere. Anything that changes the workspace (`enforce`, `apply_splits`, `update_status`)
  is opt-in, admin-only, restricted to approved jobs, and logged to `actions_log`.

### Readiness and waves

Score is 100 minus weighted findings. Wave 1: no blockers. Wave 2: every blocker has an automated fix. Wave 3: manual
work. Wave 4: needs an owner decision (ML runtime, custom image, Scala or R). Only classic jobs in wave 1 or 2 can enter
the approval gate.

## Deploy

Needs the Databricks CLI, a profile for your workspace, Unity Catalog, and a SQL warehouse.
The bundle looks up a warehouse named "Serverless Starter Warehouse"; if yours is named differently add
`--var="warehouse_id=<id>"` to the commands below. No workspace URL is stored in the repo.

```
databricks bundle validate -p <profile>
databricks bundle deploy   -p <profile>
databricks bundle run setup_state_tables    -p <profile>   # creates the tables
databricks bundle run pipeline              -p <profile>   # discover, scan, safety, readiness
databricks bundle run cost                  -p <profile>   # cost comparison (sample billing by default)
databricks bundle run validate              -p <profile>   # A/B validation demo
databricks bundle run migration_tracker_app -p <profile>   # start the app
databricks bundle run grant_app             -p <profile>   # give the app SELECT, and MODIFY on 2 tables
```

Then open the app (Compute, Apps) and the dashboard "[tracker] Serverless migration progress".
After changing app code, run `migration_tracker_app` again to redeploy it.

Variables (`databricks.yml`): `catalog`, `schema`, `discovery_mode` (FIXTURE or LIVE), `cost_source` (SAMPLE or SYSTEM),
`warehouse_id`. If you change catalog or schema, also change them in `src/app.yaml`.

### Modes

* **FIXTURE** (default): reads sample jobs, clusters and notebooks from `fixtures/`, so everything runs on a workspace
  with no classic compute (such as Free Edition).
* **LIVE**: reads the real workspace through the SDK. Secret-looking values are removed before anything is stored.
* **Cost SAMPLE** uses made-up billing rows and prices; **SYSTEM** reads `system.billing.usage`.

### Preview without a workspace

```
pip install streamlit pandas databricks-sdk
cd src && set TRACKER_LOCAL=1 && streamlit run app/main.py
```

## A/B validation (L8)

`databricks bundle run validate` runs a demo: one comparison that passes (float noise and a load timestamp are
tolerated) and one that fails (a changed value, a NULL, a missing row, an extra row). For a real migration, run the
original job and the migrated clone so they write two tables, then run the `[tracker] A/B validation` job with
"Run now with different parameters": `mode=TABLES`, `workload_id`, `classic_table`, `serverless_table`, `key_columns`,
`ignore_columns`, and `update_status=true` (admin only). An approved job then moves to `DONE` when every table passes,
or to `ROLLED_BACK` (not promoted) when one fails. Jobs that were never approved are never changed.

Row counts alone are not enough: in the demo the failing run has 200 rows on both sides (one missing, one extra).

## Tests

```
pip install pytest duckdb
pytest -q
```

108 tests cover inventory, readiness, scanning and rewrites, waves and approvals, splitting, schedules, cost,
the app's data layer and the validator. The validator's SQL is run on DuckDB, a real engine, for pass, changed value,
missing and extra rows, NULL vs value, duplicate keys and keyless comparison.

## Known gaps

* **Free Edition has no classic compute.** The demo runs in FIXTURE mode, and the A/B demo creates both sides on
  serverless with seeded differences. LIVE discovery, `enforce`, `apply_splits` and a real classic-vs-serverless
  validation are implemented but were **not** run against real classic workloads.
* **L9 (Git and bundle drift) is not built.** Nothing writes migrated config back to a bundle or repo.
* **No one-command rollback.** Pause actions store their undo settings in `actions_log.rollback_json`, but there is no
  notebook that replays them yet. A failed validation leaves the job on classic (`ROLLED_BACK`), it does not edit it.
* **Rewrites are proposals.** L6 and L13 rewrites are stored in `proposed_rewrites` and proven by rescanning; they are
  never applied to the original notebook.
* **L3** translation is shown on a sample script; fetching real scripts from S3 or DBFS needs cloud access and is not built.
* The validator compares outputs; it does not start the classic and serverless runs itself.
* `MIGRATING` and `VALIDATING` are not set automatically, because the tracker does not perform the migration.
* `cost_source=SYSTEM` was not verified against production billing, and prices are list prices, not contract rates.
* The dashboard file is hand-written JSON. It renders on Free Edition; other workspaces were not tried.
* The App needs Databricks Apps user authorization for approvals, and Free Edition allows one app per account.

## Next

Rollback notebook over `rollback_json`; L9 drift check against the bundle; run the A/B validator from a clone job;
verify `system.billing` mode on a paid workspace; Lakebase for approvals if row-level concurrency matters.

## Credits

Built with AI assistance (allowed by the challenge rules). No third-party code is included.
