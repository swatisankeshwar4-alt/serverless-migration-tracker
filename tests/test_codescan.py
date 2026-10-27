import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tracker import codescan, inventory, readiness  # noqa: E402

FIXTURES = str(Path(__file__).resolve().parents[1] / "fixtures")
NB = "/Shared/fixtures/"


@pytest.fixture(scope="module")
def sources():
    jobs, clusters = inventory.load_fixtures(FIXTURES)
    rows, _ = inventory.build_inventory(jobs, clusters, "FIXTURE")
    paths = [r["workload_id"] for r in rows if r["workload_type"] == "NOTEBOOK"]
    found, missing = codescan.load_fixture_sources(FIXTURES, paths)
    return found, missing


def scan(sources, stem):
    src, lang = sources[0][NB + stem]
    return codescan.scan_notebook(NB + stem, src, lang)


def details(findings, lim=None):
    return " | ".join(f["detail"] for f in findings if lim is None or f["limitation_id"] == lim)


def test_loader_finds_fixtures_and_reports_missing(sources):
    found, missing = sources
    assert NB + "etl_transform" in found and NB + "scala_report" in found
    assert NB + "step_03" in missing           # no source file for it


def test_cache_is_supported_and_not_flagged(sources):
    assert scan(sources, "etl_extract") == []
    assert "cache" not in details(scan(sources, "etl_transform")).lower().replace("cache table", "")


def test_l6_and_l13_in_transform(sources):
    f = scan(sources, "etl_transform")
    d6 = details(f, "L6")
    assert "checkpoint" in d6 and "global temp view" in d6 and "global_temp" in d6 and "CACHE TABLE" in d6
    d13 = details(f, "L13")
    assert "CAST" in d13 and "division" in d13


def test_streaming_triggers(sources):
    f = [x for x in scan(sources, "etl_load") if x["limitation_id"] == "L13"]
    assert len(f) == 2
    assert any("without a trigger" in x["detail"] for x in f)
    assert any("unsupported streaming trigger" in x["detail"] for x in f)


def test_mount_rdd_and_mnt_path(sources):
    f = scan(sources, "legacy_job")
    assert "mount management" in details(f, "L6") and "RDD" in details(f, "L6")
    assert "/mnt" in details(f, "L11")


def test_scala_and_r_classification(sources):
    scala = scan(sources, "scala_report")
    assert len(scala) == 1 and scala[0]["limitation_id"] == "L7"
    assert "REWRITE_PYSPARK" in scala[0]["suggested_fix"]
    r = scan(sources, "step_01")
    assert "KEEP_ON_CLASSIC" in r[0]["suggested_fix"] and "CRAN" in r[0]["detail"]


def test_scala_cell_inside_python_notebook(sources):
    f = scan(sources, "train_model")
    assert any(x["limitation_id"] == "L7" and "%scala" in x["detail"] for x in f)


def test_sql_notebook(sources):
    f = scan(sources, "step_02")
    d = details(f)
    assert "CACHE TABLE" in d and "UNCACHE" in d or "cache/refresh" in d
    assert "CAST" in d and "::" in d and "division" in d


def test_findings_never_contain_source_text(sources):
    for stem in ("legacy_job", "etl_transform", "etl_load"):
        blob = str(scan(sources, stem))
        assert "example-bucket" not in blob and "silver.events" not in blob


# ---- rewrites ----------------------------------------------------------------

def rewrite(sources, stem):
    src, lang = sources[0][NB + stem]
    return codescan.rewrite_notebook(src, lang)


def test_python_rewrite_removes_blockers_and_stays_valid_python(sources):
    new, changes = rewrite(sources, "etl_transform")
    compile(new, "etl_transform", "exec")
    assert "_materialise_to_delta(clean," in new and "def _materialise_to_delta" in new
    assert "createOrReplaceTempView(" in new and "GlobalTempView" not in new
    assert "global_temp." not in new
    assert "try_cast(amount AS INT)" in new
    assert "CREATE OR REPLACE TEMPORARY VIEW hot_orders AS" in new
    assert "orders = orders.cache()" in new       # supported call untouched
    assert changes


def test_rewritten_source_has_no_remaining_automatable_blockers(sources):
    for stem in ("etl_transform", "etl_load"):
        _, lang = sources[0][NB + stem]
        new, _ = rewrite(sources, stem)
        left = codescan.scan_notebook(NB + stem, new, lang)
        text = details(left)
        assert "checkpoint" not in text and "global" not in text and "without a trigger" not in text
        assert "unsupported streaming trigger" not in text and "CAST" not in text


def test_streaming_rewrite(sources):
    new, _ = rewrite(sources, "etl_load")
    compile(new, "etl_load", "exec")
    assert new.count("availableNow=True") == 2


def test_sql_rewrite(sources):
    new, _ = rewrite(sources, "step_02")
    assert "CREATE OR REPLACE TEMPORARY VIEW sales_cache AS" in new
    assert "try_cast(amount AS DECIMAL(10,2))" in new
    assert "-- [serverless-migration] removed" in new and "UNCACHE TABLE sales_cache" in new


def test_rewrite_is_idempotent(sources):
    for stem in ("etl_transform", "etl_load", "step_02"):
        once, _ = rewrite(sources, stem)
        _, lang = sources[0][NB + stem]
        twice, _ = codescan.rewrite_notebook(once, lang)
        assert once == twice


def test_scala_r_and_clean_notebooks_are_not_rewritten(sources):
    for stem in ("scala_report", "step_01", "etl_extract"):
        src, lang = sources[0][NB + stem]
        new, changes = codescan.rewrite_notebook(src, lang)
        assert new == src and changes == []


def test_manual_only_rules_are_not_auto_rewritten(sources):
    new, _ = rewrite(sources, "legacy_job")
    assert "dbutils.fs.mount" in new and ".rdd" in new


# ---- integration with readiness ------------------------------------------------

def test_code_findings_lower_job_and_notebook_status(sources):
    jobs, clusters = inventory.load_fixtures(FIXTURES)
    nbf = {}
    for path, (src, lang) in sources[0].items():
        nbf[path] = codescan.scan_notebook(path, src, lang)
    _, status = readiness.analyse_all(jobs, clusters, nbf)
    st = {s["workload_id"]: s for s in status}
    assert st["1006"]["status"] == "BLOCKED"                    # Scala notebook
    assert st["1001"]["status"] == "BLOCKED"                    # transform/load notebooks have blockers
    assert st[NB + "etl_extract"]["status"] == "READY"          # clean notebook in a clean job
    assert st[NB + "etl_transform"]["status"] == "BLOCKED"
    assert st["1007"]["status"] == "DONE"


# ---- auto-fixable flag, scoring, helper cell -------------------------------------------

def fixable(sources, stem):
    src, lang = sources[0][NB + stem]
    return codescan.scan_with_fixability(NB + stem, src, lang)


def test_auto_fixable_matches_what_the_rewrite_really_removes(sources):
    f = fixable(sources, "etl_transform")
    auto = {x["detail"].split(" (line")[0]: x["auto_fixable"] for x in f}
    assert auto["df.checkpoint() is unsupported"] is True
    assert auto["CAST errors on invalid input under ANSI mode"] is True
    assert auto["CACHE TABLE is unsupported"] is True
    assert auto["division by a non-literal errors on divide-by-zero under ANSI mode"] is False


def test_manual_only_findings_are_not_auto_fixable(sources):
    assert not any(x["auto_fixable"] for x in fixable(sources, "legacy_job"))
    assert not any(x["auto_fixable"] for x in fixable(sources, "scala_report"))
    assert all(x["auto_fixable"] for x in fixable(sources, "etl_load"))


def test_same_rule_on_many_lines_counts_once():
    fs = [{"workload_id": "n", "limitation_id": "L13", "severity": "WARNING", "auto_fixable": False,
           "detail": f"CAST errors on invalid input under ANSI mode (line {i})"} for i in range(1, 6)]
    assert readiness.score(fs) == 95


def test_auto_fixable_blockers_cost_less():
    manual = [{"workload_id": "n", "limitation_id": "L6", "severity": "BLOCKER", "auto_fixable": False, "detail": "x"}]
    auto = [dict(manual[0], auto_fixable=True)]
    assert readiness.score(manual) == 80 and readiness.score(auto) == 95


def test_scores_stay_informative_with_code_findings(sources):
    jobs, clusters = inventory.load_fixtures(FIXTURES)
    nbf = {p: codescan.scan_with_fixability(p, s, l) for p, (s, l) in sources[0].items()}
    _, status = readiness.analyse_all(jobs, clusters, nbf)
    st = {s["workload_id"]: s for s in status}
    assert st[NB + "etl_transform"]["readiness_score"] == 73
    assert st[NB + "etl_load"]["readiness_score"] == 90
    assert st["1001"]["readiness_score"] == 63           # fixable by rewrites, so well above 0
    assert st["1001"]["status"] == "BLOCKED"
    assert st["1002"]["readiness_score"] < st["1001"]["readiness_score"]   # manual blockers rank worse


def test_helper_gets_its_own_cell(sources):
    new, _ = rewrite(sources, "etl_transform")
    head, rest = new.split("# COMMAND ----------", 1)
    assert "def _materialise_to_delta" in head and "# MAGIC %md" not in head
    assert rest.lstrip().startswith("# MAGIC %md")
