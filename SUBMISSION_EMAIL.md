# Submission email (Option A: private GitHub repo)

**To:** nivethan.venkatachalam@diggibyte.com, anuj.sen@diggibyte.com, veerabadrachari.r@diggibyte.com, william.r@diggibyte.com
**Subject:** Serverless Challenge - Submission - Swati Konnuri

Fill the three <...> fields, check the name, then send **before Tuesday 27 Oct 2026, 12:00 PM IST**.

---

Serverless Migration Challenge - Submission

Name(s) and team: Swati Konnuri (individual)
Repo link and final commit SHA (Option A) or zip file name (Option B): <private repo URL>, commit <SHA>
Demo video link: <video link, access limited to the judges>
Cloud tested on (AWS / Azure / GCP): AWS (Databricks Free Edition)

Limitations addressed (IDs, e.g. L1, L3, L8): L1, L2, L3, L4, L5, L6, L7, L8, L10, L11, L12, L13 (L9 is not addressed)
For each one, 1-2 lines on how:
- L1: Inventory of jobs, notebooks and clusters; readiness score; four-wave plan; admin-only approval gate; status workflow; Databricks App and AI/BI dashboard on the same Delta tables.
- L2: Jobs over 10 tasks get a split plan (child jobs plus a paused orchestrator) that is verified against the original; applying it is opt-in, admin-only and for approved jobs.
- L3: Init scripts are detected and a script is translated into a serverless environment spec (shown on a sample script).
- L4: ML runtime, custom image, DBR below 13 and instance profiles are detected from cluster specs, each with a fix path.
- L5: Egg, JAR and Maven libraries are detected per job and task, with the wheel, volume or PyPI path.
- L6: Static scan for cache, checkpoint, global temp views and mounts; automated rewrites are produced and proven by rescanning (proposed, never applied to the original).
- L7: Scala and R are detected and classified as rewrite (with effort) or keep on classic.
- L8: A/B diff of classic vs serverless output tables (schema, rows, values, keys, float tolerance) with pass or fail and reasons; a passing approved job moves to DONE, a failing one to ROLLED_BACK.
- L10: Clones firing on the same schedule as the original are detected; enforcement pauses the extra trigger and stores the undo settings.
- L11: Log delivery, SSH keys and mounts are inventoried and reported as lost on serverless.
- L12: DBUs and list cost per run before vs after, by job and team tag, from system.billing (sample billing by default).
- L13: Missing streaming triggers and ANSI-risky SQL are detected, with try_cast and try_divide rewrites.

Architecture (max 5 lines):
Serverless jobs (Asset Bundle) write Delta state tables in one Unity Catalog schema: inventory, findings, status, plans, actions, validation, cost.
Pure-Python logic in src/tracker is unit-tested; notebooks are thin wrappers.
A Databricks App (Streamlit) and an AI/BI dashboard read the tables through a SQL warehouse.
The approval gate writes only approvals and migration_status, using the signed-in admin's identity.
Anything that changes a workspace is opt-in, admin-only, limited to approved jobs, and logged.

Databricks components used: Asset Bundles, Lakeflow Jobs, Unity Catalog and Delta, Databricks SDK, system.billing, Databricks Apps, AI/BI dashboards, SQL warehouse.

How to deploy (commands or steps): See README. With the CLI profile set: databricks bundle deploy, then bundle run setup_state_tables, pipeline, cost, validate, migration_tracker_app, grant_app.

What is tested and how: 108 unit tests (pytest); the validator's SQL is also run on DuckDB for seven scenarios. Deployed and run end to end on a Free Edition workspace: all jobs, the app (including an approval written by an admin) and the dashboard.

Known gaps and what you would do next: Free Edition has no classic compute, so the demo uses fixtures and the A/B demo runs both sides on serverless with seeded differences; LIVE discovery, enforcement, split apply and real classic-vs-serverless validation are implemented but untested on classic. L9 (Git/bundle drift) and a one-command rollback notebook are not built. Rewrites are proposals. Next: rollback over the stored undo settings, drift check, running the A/B validator from a clone job, verifying system.billing mode on a paid workspace.

Confirmation: no customer data, credentials or client workspace details are included (Yes / No): Yes
