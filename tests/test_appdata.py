import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tracker import appdata  # noqa: E402

W = [
    {"workload_id": "1", "workload_type": "JOB", "compute_kind": "CLASSIC", "status": "READY", "wave": 1, "readiness_score": 100, "name": "a"},
    {"workload_id": "2", "workload_type": "JOB", "compute_kind": "CLASSIC", "status": "BLOCKED", "wave": 2, "readiness_score": 70, "name": "b"},
    {"workload_id": "3", "workload_type": "JOB", "compute_kind": "CLASSIC", "status": "BLOCKED", "wave": 4, "readiness_score": 10, "name": "c"},
    {"workload_id": "4", "workload_type": "JOB", "compute_kind": "SERVERLESS", "status": "DONE", "wave": None, "readiness_score": 100, "name": "d"},
    {"workload_id": "5", "workload_type": "JOB", "compute_kind": "CLASSIC", "status": "APPROVED", "wave": 1, "readiness_score": 95, "name": "e"},
    {"workload_id": "/nb", "workload_type": "NOTEBOOK", "compute_kind": "CLASSIC", "status": "READY", "wave": None, "readiness_score": 90, "name": "nb"},
]


def test_fq_rejects_injection():
    assert appdata.fq("workspace", "serverless_migration") == "`workspace`.`serverless_migration`"
    for bad in ("a`b", "a b", "a;drop", ""):
        with pytest.raises(ValueError):
            appdata.fq(bad, "s")


def test_all_queries_build():
    q = appdata.queries("c", "s")
    assert set(q) == {"workloads", "findings", "approvals", "actions", "split_plans", "validation", "cost"}
    assert all("`c`.`s`" in sql for sql in q.values())


def test_kpis_count_jobs_only():
    k = appdata.kpis(W)
    assert k["total_jobs"] == 5 and k["total_workloads"] == 6
    assert k["classic_jobs"] == 4 and k["on_serverless"] == 1 and k["pct_on_serverless"] == 20.0
    assert (k["ready"], k["blocked"], k["approved"]) == (1, 2, 1)
    assert k["avg_readiness"] == round((100 + 70 + 10 + 95) / 4, 1)
    assert k["by_wave"] == {1: 2, 2: 1, 4: 1}


def test_kpis_empty():
    k = appdata.kpis([])
    assert k["pct_on_serverless"] == 0.0 and k["avg_readiness"] is None


def test_funnel_order_and_zero_fill():
    f = dict(appdata.status_funnel(W))
    assert list(f)[0] == "DISCOVERED" and f["DONE"] == 1 and f["VALIDATING"] == 0


def test_limitation_coverage_sorted_numerically():
    fs = [{"workload_id": "1", "limitation_id": "L13", "severity": "WARNING", "auto_fixable": False},
          {"workload_id": "1", "limitation_id": "L3", "severity": "BLOCKER", "auto_fixable": True},
          {"workload_id": "2", "limitation_id": "L3", "severity": "BLOCKER", "auto_fixable": False}]
    out = appdata.limitation_coverage(fs)
    assert [r["limitation"] for r in out] == ["L3", "L13"]
    assert out[0]["workloads_affected"] == 2 and out[0]["blockers"] == 2 and out[0]["auto_fixable"] == 1


def test_pending_approvals_only_gate_eligible_classic_jobs():
    ids = [w["workload_id"] for w in appdata.pending_approvals(W)]
    assert ids == ["1", "2"]          # not 3 (wave 4), 4 (serverless), 5 (already approved), notebook


def test_cost_totals_only_migrated_jobs_with_known_cost():
    rows = [{"state": "MIGRATED", "before_dbus": 10, "after_dbus": 6, "before_cost_usd": 2.0, "after_cost_usd": 3.0},
            {"state": "CLASSIC_ONLY", "before_dbus": 99, "after_dbus": None, "before_cost_usd": 9, "after_cost_usd": None}]
    t = appdata.cost_totals(rows)
    assert t == {"jobs_compared": 1, "before_dbus": 10, "after_dbus": 6, "before_cost_usd": 2.0, "after_cost_usd": 3.0}
    rows[0]["after_cost_usd"] = None
    assert appdata.cost_totals(rows)["after_cost_usd"] is None


def test_decide_sql_is_parameterised():
    ins, st = appdata.decide_sql("c", "s", "APPROVED")
    assert ":wid" in ins and ":who" in ins and ":note" in ins and "READY" in st
    assert appdata.decide_sql("c", "s", "REJECTED")[1] == ""


def test_to_records_converts_by_type_and_keeps_nulls():
    cols = [("id", "STRING"), ("n", "INT"), ("x", "DOUBLE"), ("ok", "BOOLEAN"), ("d", "DECIMAL")]
    rows = appdata.to_records(cols, [["a", "3", "1.5", "true", "2.25"], ["b", None, None, "false", None]])
    assert rows[0] == {"id": "a", "n": 3, "x": 1.5, "ok": True, "d": 2.25}
    assert rows[1] == {"id": "b", "n": None, "x": None, "ok": False, "d": None}
    assert appdata.to_records(cols, None) == []
