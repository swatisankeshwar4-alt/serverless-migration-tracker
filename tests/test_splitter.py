import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tracker import inventory, splitter  # noqa: E402

FIXTURES = str(Path(__file__).resolve().parents[1] / "fixtures")


@pytest.fixture(scope="module")
def jobs():
    j, _ = inventory.load_fixtures(FIXTURES)
    return {str(x["job_id"]): x for x in j}


def test_small_job_needs_no_split(jobs):
    assert splitter.plan_split(jobs["1001"]) is None


def test_big_job_is_split_into_children_of_at_most_ten(jobs):
    plan = splitter.plan_split(jobs["1004"])
    sizes = [len(c["tasks"]) for c in plan["children"]]
    assert sizes == [10, 2]
    assert splitter.verify_split(jobs["1004"], plan) == []


def test_orchestrator_is_paused_and_original_is_untouched(jobs):
    before = copy.deepcopy(jobs["1004"])
    plan = splitter.plan_split(jobs["1004"])
    assert jobs["1004"] == before
    orch = plan["orchestrator"]
    assert orch["name"].endswith("[split orchestrator]")
    assert [t["task_key"] for t in orch["tasks"]] == ["part_1", "part_2"]
    assert orch["tasks"][1]["depends_on"] == [{"task_key": "part_1"}]
    assert orch["tags"]["migration_origin"] == "1004"


def test_cross_chunk_dependencies_move_to_the_orchestrator(jobs):
    plan = splitter.plan_split(jobs["1004"])
    first_of_part2 = plan["children"][1]["tasks"][0]
    assert "depends_on" not in first_of_part2          # its dependency is now between jobs
    assert plan["children"][1]["tags"]["migration_parent"] == "1004"


def test_job_clusters_are_copied_into_each_child(jobs):
    plan = splitter.plan_split(jobs["1004"])
    assert all(c["job_clusters"][0]["job_cluster_key"] == "main" for c in plan["children"])


def test_orchestrator_schedule_is_paused():
    job = {"job_id": 7, "settings": {
        "name": "j", "schedule": {"quartz_cron_expression": "0 0 1 * * ?", "timezone_id": "UTC", "pause_status": "UNPAUSED"},
        "tasks": [{"task_key": f"t{i}", "notebook_task": {"notebook_path": "/x"}} for i in range(11)]}}
    plan = splitter.plan_split(job)
    assert plan["orchestrator"]["schedule"]["pause_status"] == "PAUSED"
    assert job["settings"]["schedule"]["pause_status"] == "UNPAUSED"


def test_cycle_and_unknown_dependency_cannot_be_split():
    tasks = [{"task_key": f"t{i}", "notebook_task": {"notebook_path": "/x"}} for i in range(11)]
    tasks[0]["depends_on"] = [{"task_key": "t1"}]
    tasks[1]["depends_on"] = [{"task_key": "t0"}]
    assert splitter.splittable({"job_id": 1, "settings": {"tasks": tasks}}) is False
    tasks2 = [{"task_key": f"t{i}", "notebook_task": {"notebook_path": "/x"}} for i in range(11)]
    tasks2[0]["depends_on"] = [{"task_key": "ghost"}]
    assert splitter.splittable({"job_id": 2, "settings": {"tasks": tasks2}}) is False


def test_condition_outcome_crossing_a_split_is_refused():
    tasks = [{"task_key": f"t{i:02d}", "notebook_task": {"notebook_path": "/x"}} for i in range(11)]
    tasks.append({"task_key": "after", "notebook_task": {"notebook_path": "/y"},
                  "depends_on": [{"task_key": "t00", "outcome": "true"}]})
    assert splitter.splittable({"job_id": 3, "settings": {"tasks": tasks}}) is False


def test_verify_split_catches_a_tampered_plan(jobs):
    plan = splitter.plan_split(jobs["1004"])
    plan["orchestrator"]["tasks"][1].pop("depends_on")
    assert any("lost between children" in p for p in splitter.verify_split(jobs["1004"], plan))
    plan2 = splitter.plan_split(jobs["1004"])
    plan2["children"][0]["tasks"].pop()
    assert splitter.verify_split(jobs["1004"], plan2)


def test_apply_split_dry_run_makes_no_calls(jobs):
    calls = []
    plan = splitter.plan_split(jobs["1004"])
    actions = splitter.apply_split(lambda *a: calls.append(a), plan, dry_run=True)
    assert calls == [] and len(actions) == 3 and all(a["dry_run"] for a in actions)


def test_apply_split_creates_children_then_orchestrator(jobs):
    created = []

    def fake_do(method, path, body):
        created.append(body)
        return {"job_id": 100 + len(created)}

    plan = splitter.plan_split(jobs["1004"])
    splitter.apply_split(fake_do, plan, dry_run=False)
    assert [b["name"] for b in created][:2] == [c["name"] for c in plan["children"]]
    orch = created[-1]
    assert [t["run_job_task"]["job_id"] for t in orch["tasks"]] == [101, 102]
