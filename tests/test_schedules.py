import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tracker import inventory, readiness, schedules  # noqa: E402

FIXTURES = str(Path(__file__).resolve().parents[1] / "fixtures")


@pytest.fixture(scope="module")
def jobs():
    j, _ = inventory.load_fixtures(FIXTURES)
    return j


def by_id(jobs, i):
    return next(j for j in jobs if str(j["job_id"]) == str(i))


def test_clone_with_same_schedule_is_found_and_clone_is_the_extra(jobs):
    d = schedules.find_duplicates(jobs)
    assert d == [{"keep": "1001", "extra": "1008", "schedule": "cron 0 0 2 * * ? UTC"}]


def test_paused_clone_is_not_a_duplicate(jobs):
    j2 = [dict(j) for j in jobs]
    clone = by_id(j2, 1008)
    clone["settings"] = {**clone["settings"], "schedule": {**clone["settings"]["schedule"], "pause_status": "PAUSED"}}
    assert schedules.find_duplicates(j2) == []


def test_different_schedule_or_different_workload_is_not_a_duplicate(jobs):
    j2 = [dict(j) for j in jobs]
    clone = by_id(j2, 1008)
    clone["settings"] = {**clone["settings"], "schedule": {**clone["settings"]["schedule"], "quartz_cron_expression": "0 0 5 * * ?"}}
    assert schedules.find_duplicates(j2) == []
    other = [dict(j) for j in jobs]
    c = by_id(other, 1008)
    c["settings"] = {**c["settings"], "tasks": [{"task_key": "x", "notebook_task": {"notebook_path": "/Shared/else"}}]}
    assert schedules.find_duplicates(other) == []


def test_periodic_and_file_arrival_signatures():
    a = {"job_id": 1, "settings": {"trigger": {"periodic": {"interval": 1, "unit": "HOURS"}}}}
    assert schedules.schedule_signature(a) == ("periodic", 1, "HOURS")
    b = {"job_id": 2, "settings": {"trigger": {"file_arrival": {"url": "s3://x/y"}}}}
    assert schedules.schedule_signature(b) == ("file_arrival", "s3://x/y")
    assert schedules.schedule_signature({"job_id": 3, "settings": {}}) is None


def test_clone_detection_by_name_then_age():
    a = {"job_id": 5, "settings": {"name": "etl"}}
    b = {"job_id": 6, "settings": {"name": "etl (copy)"}}
    assert schedules.is_clone_of(b, a) and not schedules.is_clone_of(a, b)
    c = {"job_id": 7, "created_time": 100, "settings": {"name": "same"}}
    d = {"job_id": 8, "created_time": 200, "settings": {"name": "same"}}
    assert schedules.is_clone_of(d, c)


def test_pause_settings_keep_the_schedule_and_only_set_pause(jobs):
    frag = schedules.pause_settings(by_id(jobs, 1008))
    assert frag == {"schedule": {"quartz_cron_expression": "0 0 2 * * ?", "timezone_id": "UTC", "pause_status": "PAUSED"}}
    assert schedules.pause_settings({"job_id": 1, "settings": {}}) is None
    assert by_id(jobs, 1008)["settings"]["schedule"]["pause_status"] == "UNPAUSED"   # input not mutated


def test_l10_finding_lands_on_the_clone_and_is_auto_fixable(jobs):
    clusters = inventory.load_fixtures(FIXTURES)[1]
    findings, _ = readiness.analyse_all(jobs, clusters)
    l10 = [f for f in findings if f["limitation_id"] == "L10"]
    assert len(l10) == 1 and l10[0]["workload_id"] == "1008" and l10[0]["auto_fixable"] is True
    assert "1001" in l10[0]["detail"]
