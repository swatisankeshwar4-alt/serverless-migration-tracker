import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tracker import validator as v  # noqa: E402

A = [("id", "bigint"), ("name", "string"), ("amt", "double"), ("loaded", "timestamp")]


def test_table_names_are_validated():
    assert v.quote_table("c.s.t") == "`c`.`s`.`t`"
    for bad in ("c.s.t.x", "a b", "t;drop", "", "c..t"):
        with pytest.raises(ValueError):
            v.quote_table(bad)


def test_type_families():
    assert v.type_family("decimal(10,2)") == "float" and v.type_family("BIGINT") == "int"
    assert v.type_family("array<int>") == "complex" and v.type_family("timestamp_ntz") == "timestamp"


def test_schema_ignore_and_problems():
    p, common = v.compare_schemas(A, A, ignore=["loaded"])
    assert p == [] and [c for c, _ in common] == ["id", "name", "amt"]
    p, _ = v.compare_schemas(A, [("id", "int"), ("name", "int"), ("extra", "string")])
    assert any("name type differs" in x for x in p) and any("amt exists on classic only" in x for x in p)
    assert any("extra exists on serverless only" in x for x in p)
    p, _ = v.compare_schemas([("ID", "bigint")], [("id", "int")])   # case-insensitive, int family equal
    assert p == []


def test_verdict():
    ok = {"classic_rows": 5, "serverless_rows": 5}
    assert v.verdict([], ok) == (True, "all 5 rows match")
    assert not v.verdict(["x"], ok)[0]
    assert not v.verdict([], {**ok, "changed_rows": 1})[0]
    assert not v.verdict([], {**ok, "serverless_rows": 4})[0]
    assert "not unique" in v.verdict([], {**ok, "dup_keys_classic": 2})[1]
    assert not v.verdict([], None)[0]


def test_next_status_only_moves_approved_work():
    assert v.next_status("APPROVED", True) == "DONE"
    assert v.next_status("VALIDATING", False) == "ROLLED_BACK"
    for s in ("READY", "BLOCKED", "DISCOVERED", "DONE", None):
        assert v.next_status(s, True) is None


def test_keys_must_exist():
    with pytest.raises(ValueError):
        v.build_diff_sql("a", "b", [("id", "int")], keys=["nope"])
    with pytest.raises(ValueError):
        v.build_diff_sql("a", "b", [])


# ---- the generated SQL, run on a real engine ----
duckdb = pytest.importorskip("duckdb")


@pytest.fixture()
def con():
    c = duckdb.connect()
    c.execute("CREATE TABLE classic AS SELECT i AS id, 'n' || i AS name, i * 10.25 AS amt, now() AS loaded FROM range(1, 21) t(i)")
    return c


def run(con, b_sql, keys, ignore=("loaded",), decimals=6):
    con.execute(f"CREATE OR REPLACE TABLE serverless AS {b_sql}")
    cols = [(r[0], r[1]) for r in con.execute("DESCRIBE classic").fetchall()]
    problems, common = v.compare_schemas(cols, [(r[0], r[1]) for r in con.execute("DESCRIBE serverless").fetchall()], ignore)
    sql = v.build_diff_sql("classic", "serverless", common, keys, decimals, q='"')
    res = dict(zip(["classic_rows", "serverless_rows", "only_in_classic", "only_in_serverless", "changed_rows",
                    "dup_keys_classic", "dup_keys_serverless"], con.execute(sql).fetchone()))
    return problems, res, common


@pytest.mark.parametrize("keys", [["id"], []])
def test_identical_with_float_noise_passes(con, keys):
    p, res, _ = run(con, "SELECT id, name, amt + 1e-10 AS amt, now() AS loaded FROM classic", keys)
    assert v.verdict(p, res)[0], res


@pytest.mark.parametrize("keys", [["id"], []])
def test_value_missing_and_extra_rows_fail(con, keys):
    p, res, _ = run(con, "SELECT id, name, CASE WHEN id = 5 THEN amt + 1.5 ELSE amt END AS amt, loaded FROM classic "
                         "WHERE id <> 9 UNION ALL SELECT 99, 'x', 1.0, now()", keys)
    assert not v.verdict(p, res)[0]
    assert res["only_in_classic"] >= 1 and res["only_in_serverless"] >= 1
    if keys:
        assert (res["only_in_classic"], res["only_in_serverless"], res["changed_rows"]) == (1, 1, 1)


def test_null_versus_value_is_a_difference(con):
    p, res, _ = run(con, "SELECT id, CASE WHEN id = 3 THEN NULL ELSE name END AS name, amt, loaded FROM classic", ["id"])
    assert res["changed_rows"] == 1 and not v.verdict(p, res)[0]


def test_duplicate_keys_are_reported(con):
    p, res, _ = run(con, "SELECT * FROM classic UNION ALL SELECT * FROM classic WHERE id = 1", ["id"])
    assert res["dup_keys_serverless"] == 1 and "not unique" in v.verdict(p, res)[1]


def test_unkeyed_counts_duplicates_as_a_multiset(con):
    p, res, _ = run(con, "SELECT * FROM classic UNION ALL SELECT * FROM classic WHERE id = 1", [])
    assert res["only_in_serverless"] == 1 and not v.verdict(p, res)[0]


def test_samples_name_the_keys_and_columns(con):
    _, _, common = run(con, "SELECT id, name, CASE WHEN id = 5 THEN amt + 1.5 ELSE amt END AS amt, loaded FROM classic WHERE id <> 9", ["id"])
    rows = con.execute(v.build_sample_sql("classic", "serverless", common, ["id"], q='"')).fetchall()
    assert sorted(rows) == [(5, "values_differ", "amt"), (9, "missing_in_serverless", "")]
    assert v.build_sample_sql("a", "b", common, []) is None
