import copy
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tracker import drift  # noqa: E402

MARK = "/.bundle/serverless-migration-tracker/"
VARS = {"catalog": "workspace", "schema": "serverless_migration", "discovery_mode": "FIXTURE",
        "cost_source": "SAMPLE"}


def bundle():
    docs = [yaml.safe_load(p.read_text()) for p in sorted((ROOT / "resources").glob("*.yml"))]
    return drift.bundle_jobs(docs, VARS)


def deployed(job_def, user="dev swati"):
    """What the workspace returns for a bundle job deployed in development mode."""
    s = {"name": f"[{user}] {job_def['name']}", "tags": {"dev": "swati"}, "timeout_seconds": 0,
         "max_concurrent_runs": 1, "email_notifications": {}, "tasks": []}
    for t in job_def["tasks"]:
        nb = copy.deepcopy(t["notebook_task"])
        nb["notebook_path"] = "/Workspace/Users/x" + MARK + "dev/files/src/" + nb["notebook_path"].split("/")[-1][:-3]
        task = {"task_key": t["task_key"], "notebook_task": nb}
        if t.get("depends_on"):
            task["depends_on"] = t["depends_on"]
        s["tasks"].append(task)
    return s


def test_all_bundle_jobs_parse_and_resolve():
    jobs = bundle()
    assert "pipeline" in jobs and "cost" in jobs
    params = jobs["pipeline"]["tasks"][0]["notebook_task"]["base_parameters"]
    assert params["catalog"] == "workspace" and "${" not in str(jobs)


def test_freshly_deployed_job_has_no_drift():
    for job in bundle().values():
        assert drift.compare(job, deployed(job)) == []


def test_schedule_added_in_ui_is_drift_and_revertable():
    job = bundle()["cost"]
    live = deployed(job)
    live["schedule"] = {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "UTC", "pause_status": "UNPAUSED"}
    rows = drift.compare(job, live)
    assert [r["field"] for r in rows] == ["schedule"] and rows[0]["auto_revert"]
    revert, undo = drift.revert_payload(job, live, rows)
    assert revert == {"new_settings": {}, "fields_to_remove": ["schedule"]}
    assert undo["new_settings"]["schedule"]["quartz_cron_expression"] == "0 0 6 * * ?"


def test_parameter_change_and_classic_cluster_are_reported_not_auto_reverted():
    job = bundle()["pipeline"]
    live = deployed(job)
    live["tasks"][0]["notebook_task"]["base_parameters"]["mode"] = "LIVE"
    live["tasks"][1]["existing_cluster_id"] = "0000-000000-abc"
    rows = {r["field"]: r for r in drift.compare(job, live)}
    assert set(rows) == {"task discover.parameters", "task code_scan.compute"}
    assert rows["task code_scan.compute"]["live"] == "CLASSIC"
    assert not any(r["auto_revert"] for r in rows.values())


def test_tag_and_timeout_revert_keep_dev_tag_and_undo_restores():
    job = bundle()["cost"]
    live = deployed(job)
    live["tags"]["owner"] = "someone"
    live["timeout_seconds"] = 60
    rows = drift.compare(job, live)
    revert, undo = drift.revert_payload(job, live, rows)
    assert revert["new_settings"]["tags"] == {"dev": "swati"}
    assert revert["new_settings"]["timeout_seconds"] == 0
    assert undo["new_settings"]["tags"] == {"dev": "swati", "owner": "someone"}
    assert undo["new_settings"]["timeout_seconds"] == 60


def test_match_live_job_only_in_this_bundle():
    job = bundle()["cost"]
    mine = {"job_id": 7, "settings": deployed(job)}
    other = {"job_id": 3, "settings": {**deployed(job), "tasks": [{"task_key": "x", "notebook_task": {"notebook_path": "/Users/y/cost"}}]}}
    assert drift.match_live_job(job["name"], MARK, [other, mine])["job_id"] == 7
    assert drift.match_live_job("[tracker] nope", MARK, [mine]) is None
