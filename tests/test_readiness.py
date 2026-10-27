import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tracker import inventory, readiness  # noqa: E402

FIXTURES = str(Path(__file__).resolve().parents[1] / "fixtures")


@pytest.fixture(scope="module")
def result():
    jobs, clusters = inventory.load_fixtures(FIXTURES)
    findings, status = readiness.analyse_all(jobs, clusters)
    return findings, {s["workload_id"]: s for s in status}


def ids(findings, wid, lim=None):
    return [f for f in findings if f["workload_id"] == wid and (lim is None or f["limitation_id"] == lim)]


def test_clean_classic_job_is_ready(result):
    findings, status = result
    assert ids(findings, "1001") == []
    assert status["1001"] == {"workload_id": "1001", "readiness_score": 100, "status": "READY"}


def test_l3_l4_on_legacy_job(result):
    findings, status = result
    assert len(ids(findings, "1002", "L3")) == 1          # s3 init script
    l4 = [f["detail"] for f in ids(findings, "1002", "L4")]
    assert any("below 13" in d for d in l4)               # DBR 12.2
    assert any("instance profile" in d for d in l4)
    assert status["1002"]["status"] == "BLOCKED"
    assert status["1002"]["readiness_score"] == 40


def test_l5_libraries(result):
    findings, status = result
    d = " ".join(f["detail"] for f in ids(findings, "1003", "L5"))
    assert "egg" in d and "JAR" in d and "Maven" in d
    assert status["1003"]["status"] == "BLOCKED"


def test_l2_big_job(result):
    findings, status = result
    l2 = ids(findings, "1004", "L2")
    assert len(l2) == 1 and "12 migratable tasks" in l2[0]["detail"]
    assert l2[0]["auto_fixable"] is True                 # the splitter can generate a faithful plan
    assert status["1004"]["readiness_score"] == 95


def test_l4_ml_runtime_and_docker(result):
    findings, status = result
    d = " ".join(f["detail"] for f in ids(findings, "1005", "L4"))
    assert "ML runtime" in d and "Docker" in d
    assert status["1005"]["readiness_score"] == 60


def test_scala_job_has_no_config_findings_yet(result):
    findings, status = result
    assert ids(findings, "1006") == []   # Scala is found in step 4 (notebook source)


def test_serverless_jobs_are_done(result):
    _, status = result
    assert status["1007"]["status"] == "DONE" and status["1007"]["readiness_score"] == 100
    assert status["1008"]["status"] == "DONE"


def test_l11_on_clusters(result):
    findings, status = result
    d = [f["detail"] for f in ids(findings, "0000-000001-fixture1", "L11")]
    assert any("log delivery" in x for x in d) and any("SSH" in x for x in d)
    assert len(ids(findings, "0000-000001-fixture1", "L3")) == 1
    assert status["0000-000002-fixture2"]["status"] == "BLOCKED"   # ML runtime


def test_notebook_inherits_worst_status(result):
    _, status = result
    assert status["/Shared/fixtures/legacy_job"]["status"] == "BLOCKED"
    assert status["/Shared/fixtures/etl_extract"]["status"] == "READY"        # job 1001 READY, 1008 DONE
    assert status["/Shared/fixtures/serverless_job"]["status"] == "DONE"


def test_every_inventory_row_gets_a_status(result):
    _, status = result
    jobs, clusters = inventory.load_fixtures(FIXTURES)
    inv_rows, _ = inventory.build_inventory(jobs, clusters, "FIXTURE")
    assert {r["workload_id"] for r in inv_rows} == set(status)


def test_unresolved_existing_cluster_is_a_warning():
    job = {"job_id": 5, "settings": {"tasks": [{"task_key": "a", "existing_cluster_id": "nope",
                                                 "notebook_task": {"notebook_path": "/x"}}]}}
    f = readiness.scan_job(job, {})
    assert f and f[0]["severity"] == "WARNING"


def test_existing_cluster_is_resolved_when_known():
    job = {"job_id": 6, "settings": {"tasks": [{"task_key": "a", "existing_cluster_id": "c1",
                                                 "notebook_task": {"notebook_path": "/x"}}]}}
    f = readiness.scan_job(job, {"c1": {"cluster_id": "c1", "spark_version": "11.3.x-scala2.12"}})
    assert any("below 13" in x["detail"] for x in f)
