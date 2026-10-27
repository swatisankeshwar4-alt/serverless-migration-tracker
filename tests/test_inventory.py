import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tracker import inventory as inv  # noqa: E402

FIXTURES = str(Path(__file__).resolve().parents[1] / "fixtures")


@pytest.fixture(scope="module")
def built():
    jobs, clusters = inv.load_fixtures(FIXTURES)
    return inv.build_inventory(jobs, clusters, "FIXTURE")


def by_id(rows, wid):
    return next(r for r in rows if r["workload_id"] == wid)


def test_counts(built):
    rows, raw = built
    types = [r["workload_type"] for r in rows]
    assert types.count("JOB") == 9
    assert types.count("CLUSTER") == 2
    # one raw definition per job and cluster
    assert len(raw) == 11


def test_notebooks_are_deduplicated(built):
    rows, _ = built
    nb_ids = [r["workload_id"] for r in rows if r["workload_type"] == "NOTEBOOK"]
    assert len(nb_ids) == len(set(nb_ids))
    # etl_extract is used by jobs 1001 and 1008 but listed once
    assert nb_ids.count("/Shared/fixtures/etl_extract") == 1


def test_compute_classification(built):
    rows, _ = built
    assert by_id(rows, "1001")["compute_kind"] == "CLASSIC"
    assert by_id(rows, "1002")["compute_kind"] == "CLASSIC"
    assert by_id(rows, "1007")["compute_kind"] == "SERVERLESS"
    assert by_id(rows, "1008")["compute_kind"] == "SERVERLESS"


def test_task_counts(built):
    rows, _ = built
    assert by_id(rows, "1004")["task_count"] == 12
    assert by_id(rows, "1001")["task_count"] == 3


def test_runtime_version(built):
    rows, _ = built
    assert by_id(rows, "1002")["runtime_version"] == "12.2.x-scala2.12"
    assert by_id(rows, "1005")["runtime_version"].startswith("14.3.x-cpu-ml")
    assert by_id(rows, "1007")["runtime_version"] is None


def test_empty_or_pipeline_job_is_unknown():
    job = {"job_id": 9, "settings": {"name": "p", "tasks": [{"task_key": "x", "pipeline_task": {"pipeline_id": "abc"}}]}}
    assert inv.classify_compute(job) == "UNKNOWN"
    assert inv.classify_compute({"job_id": 10, "settings": {}}) == "UNKNOWN"


def test_job_cluster_key_resolves_to_classic():
    job = {"job_id": 11, "settings": {
        "job_clusters": [{"job_cluster_key": "k", "new_cluster": {"spark_version": "15.4.x-scala2.12"}}],
        "tasks": [{"task_key": "a", "job_cluster_key": "k", "notebook_task": {"notebook_path": "/x"}}]}}
    assert inv.classify_compute(job) == "CLASSIC"
    assert inv.runtime_version(job) == "15.4.x-scala2.12"


def test_redaction_removes_secret_values_but_keeps_names():
    job = {"job_id": 1, "settings": {
        "name": "j",
        "parameters": [{"name": "api_url", "default": "https://example.test/secretvalue"}],
        "tasks": [{"task_key": "a",
                   "notebook_task": {"notebook_path": "/x", "base_parameters": {"db_password": "hunter2"}},
                   "spark_python_task": {"python_file": "f.py", "parameters": ["--token", "abc123"]},
                   "new_cluster": {
                       "spark_version": "15.4.x-scala2.12",
                       "spark_env_vars": {"AWS_SECRET_ACCESS_KEY": "zzz"},
                       "spark_conf": {"spark.hadoop.fs.s3a.secret.key": "yyy"},
                       "ssh_public_keys": ["ssh-rsa AAAA"],
                       "docker_image": {"url": "img", "basic_auth": {"username": "u", "password": "p4ss"}}}}]}}
    blob = json.dumps(inv.redact_definition(job))
    for secret in ("hunter2", "abc123", "zzz", "yyy", "AAAA", "p4ss", "secretvalue"):
        assert secret not in blob
    red = inv.redact_definition(job)
    t = red["settings"]["tasks"][0]
    assert "AWS_SECRET_ACCESS_KEY" in t["new_cluster"]["spark_env_vars"]           # names survive
    assert "spark.hadoop.fs.s3a.secret.key" in t["new_cluster"]["spark_conf"]
    assert t["notebook_task"]["notebook_path"] == "/x" and t["new_cluster"]["spark_version"] == "15.4.x-scala2.12"
    assert job["settings"]["tasks"][0]["new_cluster"]["spark_env_vars"]["AWS_SECRET_ACCESS_KEY"] == "zzz"  # input intact


def test_stored_raw_definitions_are_redacted():
    job = {"job_id": 2, "settings": {"tasks": [{"task_key": "a", "notebook_task": {"notebook_path": "/x"},
                                                 "new_cluster": {"spark_version": "15.4.x-scala2.12",
                                                                 "spark_env_vars": {"K": "topsecret"}}}]}}
    _, raw = inv.build_inventory([job], [], "LIVE")
    assert "topsecret" not in raw[0]["definition_json"]
