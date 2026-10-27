import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tracker import codescan, fixes, initscripts, splitter  # noqa: E402


def test_runnable_demo_rewrite_removes_every_finding():
    src = (ROOT / "fixtures/notebooks/runnable_demo.py").read_text()
    assert len(codescan.scan_notebook("demo", src, "PYTHON")) >= 3
    new, changes = codescan.rewrite_notebook(src, "PYTHON")
    assert codescan.scan_notebook("demo", new, "PYTHON") == []
    d = fixes.unified_diff(src, new, "runnable_demo")
    assert "-orders = orders.checkpoint()" in d and '+orders = _materialise_to_delta(orders, "orders")' in d
    assert "+    SELECT id, try_cast(amount AS INT) AS amount_int" in d
    assert fixes.diff_stats(src, new)["lines_added"] > 0


def test_environment_spec_from_init_script():
    t = initscripts.translate_init_script((ROOT / "fixtures/init_scripts/install.sh").read_text())
    y = fixes.environment_spec_yaml(t)
    assert '- "pandas==2.1.4"' in y and "environment_key: default" in y
    assert "libpq-dev" in y and "APP_ENV" in y and "curl" in y


def test_split_rows_for_the_12_task_job():
    jobs = json.loads((ROOT / "fixtures/jobs.json").read_text())
    jobs = jobs["jobs"] if isinstance(jobs, dict) else jobs
    big = next(j for j in jobs if str(j["job_id"]) == "1004")
    rows = fixes.split_rows(splitter.plan_split(big))
    assert [r["task_count"] for r in rows[:-1]] == [10, 2]
    assert rows[-1]["role"].startswith("orchestrator") and "part_1" in rows[-1]["tasks"]


def test_demo_target_path_never_the_original():
    p = fixes.demo_target_path("/Workspace/Shared/demo", "/Shared/fixtures/etl transform", "_serverless")
    assert p == "/Workspace/Shared/demo/etl_transform_serverless"


def test_changes_list():
    assert fixes.changes_list('["a", "b"]') == ["a", "b"]
    assert fixes.changes_list('{"effort": "SMALL"}') == ["effort: SMALL"]
    assert fixes.changes_list(None) == []
