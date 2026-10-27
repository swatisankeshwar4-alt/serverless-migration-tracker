# Demo script (target 4:30, limit 5:00)

Record the screen with your voice. Keep the repo, workspace URL and email out of frame (hide the browser address bar
and the profile menu), or crop them when editing.

## Before recording (10 minutes)
1. Restart the app so it is fresh: `databricks bundle run migration_tracker_app -p free` (apps stop after 24 hours).
2. Make sure the demo data is current: run `pipeline`, `cost` and `validate` once.
3. Reset your demo approval if you want to show it live: in a SQL editor run
   `UPDATE workspace.serverless_migration.migration_status SET status='READY' WHERE workload_id='1009'`
   and `DELETE FROM workspace.serverless_migration.approvals WHERE workload_id='1009'`.
4. Open four tabs: the app (Overview), the dashboard, the Jobs page, the repo README. Zoom the browser to 110 percent.

## Script

| Time | Show | Say |
|---|---|---|
| 0:00 | Slide or README title | "The /compute agent migrates one job at a time. There is no fleet view, no proof the output is right, and many workloads are blocked. This tracker is the programme around the agent." |
| 0:25 | App, Overview | "Nine jobs, two already serverless, six blocked, average readiness 53. The funnel shows where jobs are, the waves show what to do first: wave 1 needs nothing, wave 2 has automated fixes, wave 3 needs manual work, wave 4 needs a decision." |
| 1:00 | Workloads tab, filter JOB | "Every job with its score, status and wave. 1002 scores 0: init script, old runtime. Notebooks and clusters are in the same inventory." |
| 1:25 | Blockers tab | "These are the limitations found in this workspace, L2 to L13, with how many are fixable automatically. For example L6 has 8 blockers and 6 have an automated rewrite. L11 lists what is lost on serverless, like log delivery and SSH keys." |
| 2:00 | Approvals tab | "Only a workspace admin can approve, and only wave 1 or 2 jobs can enter the gate. I approve 1009 with a note." Click Approve. "It is recorded with my identity, and the status becomes APPROVED." |
| 2:30 | Dashboard | "The same tables feed an AI/BI dashboard. The Approved counter just changed. Second page is cost." |
| 2:50 | App, Cost tab | "DBUs per run fell 41 percent for this job, but cost per run rose 36 percent because serverless costs more per DBU. The tracker shows both rather than hiding it. This uses sample billing; the SYSTEM option reads system.billing." |
| 3:20 | App, Safety log | "Safety first. Job 1004 has 12 tasks, over the limit of 10, so a split plan is generated and verified against the original. Job 1008 fires on the same schedule as 1001, so the guard plans to pause it. Nothing changes unless an admin turns enforcement on." |
| 3:50 | App, Validation tab | "Proof of correctness. On Free Edition there is no classic compute, so both sides ran on serverless and I seeded differences. The first table matches: float noise and the load time are tolerated. The second fails: a changed amount, a NULL, a missing and an extra row. Row counts are 200 and 200, so a count check would have passed it." |
| 4:25 | README, Known gaps | "Honest gaps: live mode and real classic runs are untested because the sandbox has none; no rollback notebook and no Git drift check yet. Code, 108 tests and a one-command bundle deploy are in the repo. Thank you." |

## If something fails while recording
Stop, press Refresh in the app, and retry. If the app shows a yellow "Could not read" warning, run `grant_app` again.
Do not improvise a fix on camera; cut and re-record that segment.
