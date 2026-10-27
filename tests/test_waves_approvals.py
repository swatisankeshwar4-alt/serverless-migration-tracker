import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tracker import approvals, codescan, inventory, waves  # noqa: E402

FIXTURES = str(Path(__file__).resolve().parents[1] / "fixtures")


@pytest.fixture(scope="module")
def plan():
    jobs, clusters = inventory.load_fixtures(FIXTURES)
    rows, _ = inventory.build_inventory(jobs, clusters, "FIXTURE")
    paths = [r["workload_id"] for r in rows if r["workload_type"] == "NOTEBOOK"]
    found, _ = codescan.load_fixture_sources(FIXTURES, paths)
    nbf = {p: codescan.scan_with_fixability(p, s, l) for p, (s, l) in found.items()}
    return waves.plan_waves(jobs, clusters, nbf)


def test_waves_for_the_fixture_fleet(plan):
    assert plan["1009"]["wave"] == 1          # clean job
    assert plan["1001"]["wave"] == 2          # every blocker has an automated fix
    assert plan["1002"]["wave"] == 3          # DBR 12, instance profile, S3 init script
    assert plan["1003"]["wave"] == 3          # JAR / Maven / egg
    assert plan["1006"]["wave"] == 3          # small Scala notebook can be rewritten
    assert plan["1004"]["wave"] == 4          # one notebook is an R job that should stay on classic
    assert plan["1005"]["wave"] == 4          # ML runtime and custom image


def test_serverless_jobs_get_no_wave(plan):
    assert "1007" not in plan and "1008" not in plan


def test_blocker_counts_split_into_auto_and_manual(plan):
    assert plan["1001"]["manual_blockers"] == 0 and plan["1001"]["auto_fixable_blockers"] > 0
    assert plan["1002"]["auto_fixable_blockers"] == 0 and plan["1002"]["manual_blockers"] >= 3


# ---- approval gate ------------------------------------------------------------------

def test_non_admin_cannot_approve_or_reject():
    assert approvals.check_approval("READY", 1, False, "APPROVED")[0] is False
    assert approvals.check_approval("READY", 1, False, "REJECTED")[0] is False


def test_admin_can_approve_wave_1_and_2_only():
    assert approvals.check_approval("READY", 1, True, "APPROVED") == (True, "approved")
    assert approvals.check_approval("BLOCKED", 2, True, "APPROVED")[0] is True
    ok, why = approvals.check_approval("BLOCKED", 3, True, "APPROVED")
    assert not ok and "manual blockers" in why
    assert approvals.check_approval("BLOCKED", 4, True, "APPROVED")[0] is False


def test_gate_refuses_serverless_in_flight_and_unwaved_workloads():
    assert approvals.check_approval("DONE", None, True, "APPROVED")[0] is False
    assert approvals.check_approval("MIGRATING", 1, True, "APPROVED")[0] is False
    assert approvals.check_approval("READY", None, True, "APPROVED")[0] is False
    assert approvals.check_approval("READY", 1, True, "MAYBE")[0] is False


def test_admin_group_detection():
    assert approvals.is_admin_from_groups(["users", "admins"]) is True
    assert approvals.is_admin_from_groups(["users", "admins-clone"]) is False


def test_resolve_targets():
    rows = [{"workload_id": "1", "wave": 1}, {"workload_id": "2", "wave": 2}, {"workload_id": "3", "wave": None}]
    assert approvals.resolve_targets("wave:2", rows) == ["2"]
    assert approvals.resolve_targets("1, 3", rows) == ["1", "3"]
    assert approvals.resolve_targets("", rows) == []
