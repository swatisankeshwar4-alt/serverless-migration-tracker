# Demo script (target 4:45, limit 5:00)

Record the screen with your voice. Keep the workspace URL, your email and the taskbar out of frame (crop or blur).

## Before recording
1. `databricks bundle deploy -p free`, then run: `pipeline`, `cost`, `validate`, `grant_app`, `apply_fixes`.
2. Drift demo: open the `[tracker] cost comparison` job in the UI, add a schedule (any time), save. Then run `drift`.
3. Restart the app: `databricks bundle run migration_tracker_app -p free`.
4. Reset the demo approval (SQL editor):
   `DELETE FROM workspace.serverless_migration.approvals WHERE workload_id='1009'` and
   `UPDATE workspace.serverless_migration.migration_status SET status='READY' WHERE workload_id='1009'`.

## Script

| Time | Show | Say |
|---|---|---|
| 0:00 | App, Overview | "Hi, I'm Swati from Diggibyte. This is my entry for the Classic to Serverless Migration challenge. Databricks' agent moves one job at a time. My tool tracks the whole migration, fixes as many limitations as it can, and checks the result. I built it with Asset Bundles, Jobs, Unity Catalog, the SDK, a Databricks App and an AI/BI dashboard, on Free Edition with sample jobs." |
| 0:30 | Overview | "2 of 9 jobs are on serverless, 1 is ready, 6 are blocked. Waves are the order to move them: wave 1 easy, wave 2 the tool fixes, wave 3 manual, wave 4 may stay on classic." |
| 0:50 | Workloads | "Every job, notebook and cluster, with owner and a readiness score from 0 to 100." |
| 1:05 | Blockers | "Each limitation, how many jobs it hits, and how many the tool can fix itself. Below, the exact problem and fix for each job." |
| 1:25 | Fixes, section 1 | "Here is a fix actually done. This notebook uses checkpoint, a global temp view and a CAST on bad data. Run on serverless as written, it fails. The tool rewrote it, and the rewrite runs successfully. Here is exactly what changed." |
| 2:00 | Fixes, sections 2 to 4 | "Every scanned notebook gets the same rewrite, written as a new copy, never touching the original. The init script becomes a serverless environment with its packages. The 12-task job is split into jobs of 10 and 2 with a paused main job." |
| 2:35 | Approvals | "Nothing moves without an admin. Only wave 1 and 2 can enter. I approve 1009 with a note; it's recorded with my name and time." Click Approve. |
| 3:00 | Safety log | "Every change is planned first, only for approved jobs, and saves how to undo it." |
| 3:15 | Drift & rollback | "Git is the source of truth. Someone added a schedule to this job by hand; the drift check catches it. It can put it back to Git, and any change can be rolled back from the log." |
| 3:45 | Validation | "This compares output before and after, row by row. One passes; one fails because I changed rows on purpose. Both sides ran on serverless here." |
| 4:05 | Cost | "DBUs per run fell 42 percent but cost rose, because serverless costs more per DBU. The tool shows both, by job and team. Sample billing here." |
| 4:25 | README, Known gaps | "It deploys as one Asset Bundle, with 124 tests. Gaps: live mode is untested on real classic compute, and task-level drift is fixed by redeploying. Thank you." |
