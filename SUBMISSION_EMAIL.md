# Submission email (Option A: private GitHub repo)

**To:** nivethan.venkatachalam@diggibyte.com, anuj.sen@diggibyte.com, veerabadrachari.r@diggibyte.com, william.r@diggibyte.com
**Subject:** Serverless Challenge - Submission - Swati Konnuri

Fill the three <...> fields, check the name, then send **before Tuesday 27 Oct 2026, 11:59 PM IST** (extended deadline).

---

Serverless Migration Challenge - Submission

Name(s) and team: Swati Konnuri (individual)
Repo link and final commit SHA (Option A) or zip file name (Option B): <private repo URL>, commit <SHA>
Demo video link: <video link, access limited to the judges>
Cloud tested on (AWS / Azure / GCP): AWS (Databricks Free Edition)

Limitations addressed (IDs, e.g. L1, L3, L8): L1, L2, L3, L4, L5, L6, L7, L8, L9, L10, L11, L12, L13
For each one, 1-2 lines on how:
- L1: Inventory of jobs, notebooks and clusters; readiness score; four-wave plan; admin-only approval gate; status workflow; Databricks App and AI/BI dashboard on the same Delta tables.
- L2: Jobs over 10 tasks get a split plan (child jobs plus a paused orchestrator) that is verified against the original; applying it is opt-in, admin-only and for approved jobs.
- L3: Init scripts are detected and translated into the job's serverless environments block (pip packages) plus a manual follow-up list; shown before/after in the app on a sample script.
- L4: ML runtime, custom image, DBR below 13 and instance profiles are detected from cluster specs, each with a fix path.
- L5: Egg, JAR and Maven libraries are detected per job and task, with the wheel, volume or PyPI path.
- L6: Static scan for cache, checkpoint, global temp views and mounts; automated rewrites shown as diffs, written as new notebook copies (originals untouched), and proven on serverless: the original example fails, the rewrite succeeds.
- L7: Scala and R are detected and classified as rewrite (with effort) or keep on classic.
- L9: Drift check compares every bundle job in Git (resources/*.yml) with the deployed job (schedule, timeout, tags, parameters, a task moved to classic); top-level drift can be reverted to Git with the undo stored, and a rollback job undoes any logged change.
- L8: A/B diff of classic vs serverless output tables (schema, rows, values, keys, float tolerance) with pass or fail and reasons; a passing approved job moves to DONE, a failing one to ROLLED_BACK.
- L10: Clones firing on the same schedule as the original are detected; enforcement pauses the extra trigger and stores the undo settings.
- L11: Log delivery, SSH keys and mounts are inventoried and reported as lost on serverless.
- L12: DBUs and list cost per run before vs after, by job and team tag, from system.billing (sample billing by default).
- L13: Missing streaming triggers and ANSI-risky SQL are detected and rewritten (availableNow, try_cast), applied and proven together with L6.

Architecture (max 5 lines):
Serverless jobs (Asset Bundle) write Delta state tables in one Unity Catalog schema: inventory, findings, status, plans, actions, validation, cost.
Pure-Python logic in src/tracker is unit-tested; notebooks are thin wrappers.
A Databricks App (Streamlit) and an AI/BI dashboard read the tables through a SQL warehouse.
The approval gate writes only approvals and migration_status, using the signed-in admin's identity.
Anything that changes a workspace is opt-in, admin-only, limited to approved jobs, and logged.

Databricks components used: Asset Bundles, Lakeflow Jobs, Unity Catalog and Delta, Databricks SDK, system.billing, Databricks Apps, AI/BI dashboards, SQL warehouse.

How to deploy (commands or steps): See README. With the CLI profile set: databricks bundle deploy, then bundle run setup_state_tables, pipeline, cost, validate, migration_tracker_app, grant_app, apply_fixes, drift, rollback.

What is tested and how: 124 unit tests (pytest); the validator's SQL is also run on DuckDB for seven scenarios. Deployed and run end to end on a Free Edition workspace: all jobs, the app and the dashboard; the fix proof runs the original and rewritten notebook on serverless; drift was tested by editing a deployed job in the UI.

Known gaps and what you would do next: Free Edition has no classic compute, so the demo uses fixtures and the A/B demo runs both sides on serverless with seeded differences; LIVE discovery, enforcement, split apply and real classic-vs-serverless validation are implemented but untested on classic. Drift covers this bundle's jobs and reverts top-level settings only (task drift is fixed by redeploying). Rewrites are applied as copies; switching a job to the copy is manual. Next: point jobs at validated copies, write migrated config back to the bundle, run the A/B validator from a clone job, verify system.billing mode on a paid workspace.

Confirmation: no customer data, credentials or client workspace details are included (Yes / No): Yes
