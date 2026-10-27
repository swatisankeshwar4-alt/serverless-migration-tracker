"""A/B output validation (L8): diff the tables a workload wrote on classic and on serverless.

Pure functions that build SQL and judge the result, so they are unit-tested (on DuckDB in CI, Spark in the
workspace). The notebook (07_validate.py) runs the SQL and stores the verdict.

What is compared
* schema: same column names and same type family (int / float / string / date / timestamp / ...).
* rows: with key columns, a full outer join finds rows missing on one side, extra on the other, and rows
  whose values differ (and which columns). Without keys, a multiset diff (EXCEPT ALL) is used.
* floating point values are rounded to `decimals` before comparing, so harmless noise does not fail a run.
* columns listed in `ignore` (load timestamps, run ids) are left out.
A workload passes only when schema, row counts and every row match. A failed run is never promoted.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

_IDENT = re.compile(r"[A-Za-z0-9_\-]{1,128}")
_INT = {"tinyint", "smallint", "int", "integer", "bigint", "long", "short", "byte"}
_FLOAT = {"float", "double", "real"}


def valid_table_name(name: str) -> bool:
    parts = (name or "").split(".")
    return 1 <= len(parts) <= 3 and all(_IDENT.fullmatch(p) for p in parts)


def quote_table(name: str, q: str = "`") -> str:
    if not valid_table_name(name):
        raise ValueError(f"invalid table name: {name!r}")
    return ".".join(f"{q}{p}{q}" for p in name.split("."))


def type_family(spark_type: str) -> str:
    t = (spark_type or "").lower().strip()
    if t.startswith("decimal") or t in _FLOAT:
        return "float"
    if t in _INT:
        return "int"
    if t.startswith(("array", "struct", "map")):
        return "complex"
    if t.startswith("timestamp"):
        return "timestamp"
    return t.split("(")[0] or "other"


def compare_schemas(a: Sequence[Tuple[str, str]], b: Sequence[Tuple[str, str]],
                    ignore: Iterable[str] = ()) -> Tuple[List[str], List[Tuple[str, str]]]:
    """(problems, common columns as (name, family)). Names match case-insensitively; ignored columns are dropped."""
    skip = {i.strip().lower() for i in ignore if i.strip()}
    am = {n.lower(): (n, type_family(t)) for n, t in a if n.lower() not in skip}
    bm = {n.lower(): (n, type_family(t)) for n, t in b if n.lower() not in skip}
    problems = []
    for n in sorted(set(am) - set(bm)):
        problems.append(f"column {am[n][0]} exists on classic only")
    for n in sorted(set(bm) - set(am)):
        problems.append(f"column {bm[n][0]} exists on serverless only")
    common = []
    for n in am:
        if n in bm:
            if am[n][1] != bm[n][1]:
                problems.append(f"column {am[n][0]} type differs: {am[n][1]} vs {bm[n][1]}")
            else:
                common.append((am[n][0], am[n][1]))
    return problems, common


def _norm(col: str, family: str, decimals: int, q: str, alias: str = "") -> str:
    ref = f"{alias}{q}{col}{q}"
    if family == "float":
        return f"round({ref}, {int(decimals)})"
    if family == "complex":
        return f"to_json({ref})"
    return ref


def _side(table: str, cols: Sequence[Tuple[str, str]], decimals: int, q: str, keyed: bool) -> str:
    sel = ", ".join(f"{_norm(c, f, decimals, q)} AS {q}{c}{q}" for c, f in cols)
    return f"SELECT {sel}{', 1 AS _p' if keyed else ''} FROM {table}"


def _keyed_join(a_t: str, b_t: str, cols, keys: Sequence[str], decimals: int, q: str) -> str:
    on = " AND ".join(f"a.{q}{k}{q} IS NOT DISTINCT FROM b.{q}{k}{q}" for k in keys)
    diff = ", ".join(f"CASE WHEN NOT (a.{q}{c}{q} IS NOT DISTINCT FROM b.{q}{c}{q}) THEN '{c}' END"
                     for c, _ in cols if c.lower() not in {k.lower() for k in keys})
    key_sel = ", ".join(f"coalesce(a.{q}{k}{q}, b.{q}{k}{q}) AS {q}{k}{q}" for k in keys)
    diff_sel = (f"CASE WHEN a._p IS NOT NULL AND b._p IS NOT NULL THEN concat_ws(',', {diff}) ELSE '' END"
                if diff else "''")
    return (f"WITH a AS ({_side(a_t, cols, decimals, q, True)}), b AS ({_side(b_t, cols, decimals, q, True)}), "
            f"j AS (SELECT {key_sel}, a._p AS a_p, b._p AS b_p, {diff_sel} AS diff_cols "
            f"FROM a FULL OUTER JOIN b ON {on})")


def _check_keys(cols, keys: Sequence[str]) -> None:
    names = {c.lower() for c, _ in cols}
    missing = [k for k in keys if k.lower() not in names]
    if missing:
        raise ValueError(f"key column(s) not in both tables: {missing}")


def build_diff_sql(a_table: str, b_table: str, cols: Sequence[Tuple[str, str]], keys: Sequence[str] = (),
                   decimals: int = 6, q: str = "`") -> str:
    """One row: classic_rows, serverless_rows, only_in_classic, only_in_serverless, changed_rows, dup_keys_*."""
    a_t, b_t = quote_table(a_table, q), quote_table(b_table, q)
    if not cols:
        raise ValueError("no comparable columns")
    counts = f"(SELECT count(*) FROM {a_t}) AS classic_rows, (SELECT count(*) FROM {b_t}) AS serverless_rows"
    if keys:
        _check_keys(cols, keys)
        kl = ", ".join(f"{q}{k}{q}" for k in keys)
        dup = lambda t: f"(SELECT count(*) FROM (SELECT {kl} FROM {t} GROUP BY {kl} HAVING count(*) > 1) d)"  # noqa: E731
        return (_keyed_join(a_t, b_t, cols, keys, decimals, q) +
                f" SELECT {counts}, (SELECT count(*) FROM j WHERE b_p IS NULL) AS only_in_classic, "
                f"(SELECT count(*) FROM j WHERE a_p IS NULL) AS only_in_serverless, "
                f"(SELECT count(*) FROM j WHERE a_p IS NOT NULL AND b_p IS NOT NULL AND diff_cols <> '') AS changed_rows, "
                f"{dup(a_t)} AS dup_keys_classic, {dup(b_t)} AS dup_keys_serverless")
    return (f"WITH a AS ({_side(a_t, cols, decimals, q, False)}), b AS ({_side(b_t, cols, decimals, q, False)}) "
            f"SELECT {counts}, (SELECT count(*) FROM (SELECT * FROM a EXCEPT ALL SELECT * FROM b) x) AS only_in_classic, "
            f"(SELECT count(*) FROM (SELECT * FROM b EXCEPT ALL SELECT * FROM a) y) AS only_in_serverless, "
            f"0 AS changed_rows, 0 AS dup_keys_classic, 0 AS dup_keys_serverless")


def build_sample_sql(a_table: str, b_table: str, cols, keys: Sequence[str], decimals: int = 6,
                     limit: int = 5, q: str = "`") -> Optional[str]:
    """A few offending keys with the issue and the differing columns. Only possible with key columns."""
    if not keys:
        return None
    _check_keys(cols, keys)
    a_t, b_t = quote_table(a_table, q), quote_table(b_table, q)
    kl = ", ".join(f"{q}{k}{q}" for k in keys)
    return (_keyed_join(a_t, b_t, cols, keys, decimals, q) +
            f" SELECT {kl}, CASE WHEN b_p IS NULL THEN 'missing_in_serverless' WHEN a_p IS NULL THEN 'extra_in_serverless' "
            f"ELSE 'values_differ' END AS issue, diff_cols FROM j "
            f"WHERE b_p IS NULL OR a_p IS NULL OR diff_cols <> '' LIMIT {int(limit)}")


def verdict(schema_problems: Sequence[str], counts: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    """(passed, reason). Any schema problem, duplicate key or row difference fails the run."""
    if schema_problems:
        return False, "schema differs: " + "; ".join(schema_problems)
    if counts is None:
        return False, "no comparison result"
    c = {k: int(counts.get(k) or 0) for k in ("classic_rows", "serverless_rows", "only_in_classic",
                                               "only_in_serverless", "changed_rows",
                                               "dup_keys_classic", "dup_keys_serverless")}
    if c["dup_keys_classic"] or c["dup_keys_serverless"]:
        return False, (f"key columns are not unique (classic {c['dup_keys_classic']}, serverless "
                       f"{c['dup_keys_serverless']} duplicated keys): choose a unique key or leave keys empty")
    bad = c["only_in_classic"] + c["only_in_serverless"] + c["changed_rows"]
    if bad or c["classic_rows"] != c["serverless_rows"]:
        return False, (f"{c['only_in_classic']} rows only on classic, {c['only_in_serverless']} only on serverless, "
                       f"{c['changed_rows']} with different values (rows {c['classic_rows']} vs {c['serverless_rows']})")
    return True, f"all {c['classic_rows']} rows match"


# A workload can only be promoted through the workflow: validation never moves an unapproved job to DONE.
_VALIDATABLE = ("APPROVED", "MIGRATING", "VALIDATING")


def next_status(current: Optional[str], passed: bool) -> Optional[str]:
    """New migration status after validation, or None to leave it alone."""
    if current not in _VALIDATABLE:
        return None
    return "DONE" if passed else "ROLLED_BACK"
