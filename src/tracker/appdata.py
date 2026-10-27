"""Data layer for the tracking app (L1). No Streamlit and no Spark in here, so it is unit-tested.

* `QUERIES` holds the read-only SQL the app runs (catalog and schema are validated, then inserted).
* `kpis`, `status_funnel`, `limitation_coverage` turn query rows into the numbers on screen.
* `decide_sql` builds the parameterised writes for the approval gate.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import approvals

_IDENT = re.compile(r"[A-Za-z0-9_\-]{1,128}")

# Left to right: the order of the migration workflow, used for the progress funnel.
STATUS_ORDER = ["DISCOVERED", "BLOCKED", "READY", "APPROVED", "MIGRATING", "VALIDATING", "DONE", "ROLLED_BACK"]
LIMITATION_TITLES = {
    "L2": "Jobs with more than 10 tasks", "L3": "Init scripts", "L4": "Runtime / image / instance profile",
    "L5": "Eggs, JARs, Maven", "L6": "Cache, checkpoint, global temp views, mounts", "L7": "Scala and R",
    "L8": "Output validation", "L10": "Duplicate schedules", "L11": "Log delivery, SSH keys, mounts",
    "L13": "Silent behaviour changes",
}


def fq(catalog: str, schema: str) -> str:
    """Quote `catalog`.`schema` after checking both names: they come from app config, not from users."""
    for part in (catalog, schema):
        if not _IDENT.fullmatch(part or ""):
            raise ValueError(f"invalid catalog or schema name: {part!r}")
    return f"`{catalog}`.`{schema}`"


def queries(catalog: str, schema: str) -> Dict[str, str]:
    t = fq(catalog, schema)
    return {
        # One row per workload, with its blocker count. Notebooks and clusters roll up in the same view.
        "workloads": f"""
            SELECT i.workload_id, i.workload_type, i.name, i.owner, i.task_count, i.compute_kind,
                   i.runtime_version, s.readiness_score, s.status, s.wave, s.updated_at,
                   coalesce(f.blockers, 0) AS blockers, coalesce(f.warnings, 0) AS warnings
            FROM {t}.inventory_workloads i
            LEFT JOIN {t}.migration_status s ON s.workload_id = i.workload_id
            LEFT JOIN (SELECT workload_id,
                              count_if(severity = 'BLOCKER') AS blockers,
                              count_if(severity = 'WARNING') AS warnings
                       FROM {t}.readiness_findings GROUP BY workload_id) f
                   ON f.workload_id = i.workload_id
            ORDER BY s.readiness_score, i.name""",
        "findings": f"""
            SELECT f.workload_id, i.name, i.workload_type, f.limitation_id, f.severity, f.detail,
                   f.suggested_fix, f.auto_fixable
            FROM {t}.readiness_findings f
            LEFT JOIN {t}.inventory_workloads i ON i.workload_id = f.workload_id
            ORDER BY f.limitation_id, f.severity, i.name""",
        "approvals": f"SELECT workload_id, decision, decided_by, decided_at, note FROM {t}.approvals "
                     f"ORDER BY decided_at DESC LIMIT 200",
        "actions": f"SELECT performed_at, action_id, workload_id, action, detail, dry_run, performed_by "
                   f"FROM {t}.actions_log ORDER BY performed_at DESC LIMIT 200",
        "split_plans": f"SELECT workload_id, verified, applied, created_at FROM {t}.split_plans "
                       f"ORDER BY created_at DESC",
        "validation": f"SELECT * FROM {t}.validation_results ORDER BY validated_at DESC",
        "cost": f"SELECT * FROM {t}.cost_summary ORDER BY job_id",
        # Optional tables (created by later jobs): a missing one just hides its section.
        "rewrites": f"SELECT workload_id, language, decision, changes_json, original_source, proposed_source, applied "
                    f"FROM {t}.proposed_rewrites ORDER BY decision, workload_id",
        "proofs": f"SELECT notebook, version, source, result, error, proved_at FROM {t}.rewrite_proofs ORDER BY version",
        "init_translations": f"SELECT script_name, original_script, environment_yaml FROM {t}.init_script_translations",
        "split_plans_full": f"SELECT workload_id, plan_json, verified, applied FROM {t}.split_plans ORDER BY created_at DESC",
        "drift": f"SELECT job_key, job_id, job_name, field, in_git, in_workspace, auto_revert, checked_at "
                 f"FROM {t}.drift_findings ORDER BY job_name, field",
        "approvers": f"SELECT lower(email) AS email FROM {t}.approvers",
    }


OPTIONAL_QUERIES = {"rewrites", "proofs", "init_translations", "split_plans_full", "drift", "approvers"}


def is_approver(email: Optional[str], approver_rows: List[Dict[str, Any]]) -> bool:
    """True when the signed-in user's email (set by the Databricks Apps proxy) is on the approver list.

    The list is kept in a Unity Catalog table and only filled by a workspace admin (09_grant_app)."""
    e = (email or "").strip().lower()
    return bool(e) and any((r.get("email") or "").strip().lower() == e for r in approver_rows)


def _count(rows: Iterable[Dict[str, Any]], key: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        k = r.get(key)
        out[k if k is not None else "UNKNOWN"] = out.get(k if k is not None else "UNKNOWN", 0) + 1
    return out


def kpis(workloads: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Headline numbers. The programme scope is jobs: only they are approved, migrated and costed."""
    jobs = [w for w in workloads if w.get("workload_type") == "JOB"]
    classic_jobs = [w for w in jobs if w.get("compute_kind") != "SERVERLESS"]
    done = [w for w in jobs if w.get("status") == "DONE"]
    scores = [int(w["readiness_score"]) for w in classic_jobs if w.get("readiness_score") is not None]
    by_status = _count(jobs, "status")
    return {
        "total_workloads": len(workloads),
        "total_jobs": len(jobs),
        "classic_jobs": len(classic_jobs),
        "on_serverless": len(done),
        "pct_on_serverless": round(100 * len(done) / len(jobs), 1) if jobs else 0.0,
        "ready": by_status.get("READY", 0),
        "blocked": by_status.get("BLOCKED", 0),
        "approved": by_status.get("APPROVED", 0),
        "in_flight": by_status.get("MIGRATING", 0) + by_status.get("VALIDATING", 0),
        "avg_readiness": round(sum(scores) / len(scores), 1) if scores else None,
        "by_type": _count(workloads, "workload_type"),
        "by_wave": _count([w for w in classic_jobs], "wave"),
    }


def status_funnel(workloads: List[Dict[str, Any]]) -> List[Tuple[str, int]]:
    jobs = [w for w in workloads if w.get("workload_type") == "JOB"]
    counts = _count(jobs, "status")
    return [(s, counts.get(s, 0)) for s in STATUS_ORDER]


def limitation_coverage(findings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Per limitation: how many workloads are hit, and how many findings an automated fix removes."""
    acc: Dict[str, Dict[str, Any]] = {}
    for f in findings:
        a = acc.setdefault(f["limitation_id"], {"workloads": set(), "blockers": 0, "auto": 0, "total": 0})
        a["workloads"].add(f["workload_id"])
        a["total"] += 1
        a["blockers"] += f.get("severity") == "BLOCKER"
        a["auto"] += bool(f.get("auto_fixable"))
    return [{"limitation": lid, "title": LIMITATION_TITLES.get(lid, ""), "workloads_affected": len(a["workloads"]),
             "blockers": a["blockers"], "auto_fixable": a["auto"], "findings": a["total"]}
            for lid, a in sorted(acc.items(), key=lambda kv: int(kv[0][1:]) if kv[0][1:].isdigit() else 99)]


def cost_totals(cost_rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    """Sum of before/after over jobs that have BOTH periods (a fair like-for-like view)."""
    both = [r for r in cost_rows if r.get("state") == "MIGRATED"]

    def total(key: str) -> Optional[float]:
        vals = [r[key] for r in both]
        return None if not vals or any(v is None for v in vals) else round(sum(vals), 4)
    return {"jobs_compared": len(both), "before_dbus": total("before_dbus"), "after_dbus": total("after_dbus"),
            "before_cost_usd": total("before_cost_usd"), "after_cost_usd": total("after_cost_usd")}


def pending_approvals(workloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Classic jobs that can enter the gate right now (wave 1 or 2, READY or BLOCKED-with-auto-fix)."""
    out = []
    for w in workloads:
        if w.get("workload_type") != "JOB" or w.get("compute_kind") == "SERVERLESS":
            continue
        ok, _ = approvals.check_approval(w.get("status"), w.get("wave"), True, "APPROVED")
        if ok:
            out.append(w)
    return sorted(out, key=lambda w: (w.get("wave") or 9, w.get("name") or ""))


def decide_sql(catalog: str, schema: str, decision: str) -> Tuple[str, str]:
    """(insert_sql, status_sql) with named parameters :wid :decision :who :note. Values are never inlined."""
    t = fq(catalog, schema)
    insert = (f"INSERT INTO {t}.approvals (workload_id, decision, decided_by, decided_at, note) "
              f"VALUES (:wid, :decision, :who, current_timestamp(), :note)")
    status = (f"UPDATE {t}.migration_status SET status = 'APPROVED', updated_at = current_timestamp() "
              f"WHERE workload_id = :wid AND status IN ('READY', 'BLOCKED')") if decision == "APPROVED" else ""
    return insert, status


_NUMERIC_INT = {"BYTE", "SHORT", "INT", "LONG"}
_NUMERIC_FLOAT = {"FLOAT", "DOUBLE", "DECIMAL"}


def to_records(columns: List[Tuple[str, str]], data_array: Optional[List[List[Any]]]) -> List[Dict[str, Any]]:
    """Statement Execution API returns every value as a string: convert by the column's SQL type."""
    out = []
    for row in data_array or []:
        rec: Dict[str, Any] = {}
        for (name, typ), v in zip(columns, row):
            t = (typ or "").upper()
            if v is None:
                rec[name] = None
            elif t in _NUMERIC_INT:
                rec[name] = int(v)
            elif t in _NUMERIC_FLOAT:
                rec[name] = float(v)
            elif t == "BOOLEAN":
                rec[name] = str(v).lower() == "true"
            else:
                rec[name] = v
        out.append(rec)
    return out
