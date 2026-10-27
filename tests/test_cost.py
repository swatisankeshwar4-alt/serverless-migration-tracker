import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tracker import cost  # noqa: E402

SAMPLE = str(Path(__file__).resolve().parents[1] / "fixtures" / "sample_billing.json")


@pytest.fixture(scope="module")
def summary():
    periods = cost.compare(cost.load_sample(SAMPLE))
    return {r["job_id"]: r for r in cost.job_summary(periods)}, periods


def test_states(summary):
    s, _ = summary
    assert s["1009"]["state"] == "MIGRATED"
    assert s["1008"]["state"] == "SERVERLESS_ONLY" and s["1007"]["state"] == "SERVERLESS_ONLY"
    assert s["1001"]["state"] == "CLASSIC_ONLY" and s["1002"]["state"] == "CLASSIC_ONLY"


def test_migrated_job_before_and_after(summary):
    s, _ = summary
    j = s["1009"]
    assert j["before_runs"] == 10 and j["after_runs"] == 10
    assert j["before_dbu_per_run"] == 12.0 and j["after_dbu_per_run"] == 7.0
    assert j["dbu_per_run_change_pct"] == -41.7
    assert j["cost_per_run_change_pct"] == 36.1
    assert j["before_cost_usd"] == pytest.approx(18.0) and j["after_cost_usd"] == pytest.approx(24.5)


def test_period_rows_are_split_by_period(summary):
    _, periods = summary
    mine = [p for p in periods if p["job_id"] == "1009"]
    assert [p["period"] for p in mine] == ["AFTER", "BEFORE"]
    assert {p["active_days"] for p in mine} == {10}


def test_team_tag_is_carried(summary):
    s, _ = summary
    assert s["1009"]["team_tag"] == "analytics" and s["1002"]["team_tag"] == "finance"


def test_unknown_price_gives_unknown_cost_not_zero():
    rows = [{"job_id": "x", "usage_date": "2026-10-01", "is_serverless": False, "team": None,
             "dbus": 4.0, "list_cost": None, "runs": 2}]
    p = cost.compare(rows)[0]
    assert p["dbus"] == 4.0 and p["list_cost_usd"] is None and p["dbus_per_run"] == 2.0


def test_zero_runs_do_not_divide_by_zero():
    rows = [{"job_id": "x", "usage_date": "2026-10-01", "is_serverless": True, "team": None,
             "dbus": 1.0, "list_cost": 0.5, "runs": 0}]
    assert cost.compare(rows)[0]["dbus_per_run"] is None


def test_correction_rows_net_out():
    rows = [{"job_id": "x", "usage_date": "2026-10-01", "is_serverless": True, "team": None, "dbus": 10.0, "list_cost": 3.5, "runs": 1},
            {"job_id": "x", "usage_date": "2026-10-01", "is_serverless": True, "team": None, "dbus": -4.0, "list_cost": -1.4, "runs": 0}]
    p = cost.compare(rows)[0]
    assert p["dbus"] == 6.0 and p["list_cost_usd"] == pytest.approx(2.1)


def test_same_job_with_two_team_tags_merges_per_period():
    rows = [{"job_id": "x", "usage_date": "2026-10-01", "is_serverless": False, "team": "a", "dbus": 2.0, "list_cost": 1.0, "runs": 1},
            {"job_id": "x", "usage_date": "2026-10-02", "is_serverless": False, "team": "b", "dbus": 2.0, "list_cost": 1.0, "runs": 1}]
    s = cost.job_summary(cost.compare(rows))
    assert len(s) == 1 and s[0]["before_dbus"] == 4.0 and s[0]["before_runs"] == 2


def test_tag_key_validation():
    assert cost.valid_tag_key("team") and cost.valid_tag_key("cost-center.1")
    assert not cost.valid_tag_key("team'] OR 1=1 --") and not cost.valid_tag_key("")
