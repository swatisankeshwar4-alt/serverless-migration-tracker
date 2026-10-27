"""Cost before vs after migration (L12), by workload, tag and team.

Input is DAILY usage rows per job, already aggregated by the notebook (from system.billing.usage in a
real workspace, or from the sample file). A row is "AFTER" when its compute was serverless and "BEFORE"
otherwise, so a job that moved mid-window shows both periods, and the comparison needs no migration log.

DBUs and list cost are reported side by side on purpose: serverless usually consumes fewer DBUs for the
same run but at a higher price per DBU, so the cost per run can go up even when DBUs go down.
Net usage is summed including correction rows (negative quantities), which is what billing does.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional


def _period(is_serverless: Any) -> str:
    return "AFTER" if is_serverless else "BEFORE"


def compare(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One row per (job, team, period) with totals and per-run figures."""
    acc: Dict[tuple, Dict[str, Any]] = {}
    for r in rows:
        key = (str(r["job_id"]), r.get("team"), _period(r.get("is_serverless")))
        a = acc.setdefault(key, {"job_name": None, "dbus": 0.0, "cost": 0.0, "cost_known": True,
                                 "runs": 0, "days": set()})
        a["dbus"] += float(r.get("dbus") or 0)
        if r.get("list_cost") is None:
            a["cost_known"] = False
        else:
            a["cost"] += float(r["list_cost"])
        a["runs"] += int(r.get("runs") or 0)
        a["days"].add(str(r["usage_date"]))
        if r.get("job_name"):
            a["job_name"] = r["job_name"]

    out = []
    for (job_id, team, period), a in sorted(acc.items(), key=lambda kv: (kv[0][0], kv[0][2])):
        out.append({
            "job_id": job_id, "job_name": a["job_name"], "team_tag": team, "period": period,
            "dbus": round(a["dbus"], 6),
            "list_cost_usd": round(a["cost"], 6) if a["cost_known"] else None,
            "runs": a["runs"], "active_days": len(a["days"]),
            "dbus_per_run": round(a["dbus"] / a["runs"], 6) if a["runs"] else None,
        })
    return out


def _pct(before: Optional[float], after: Optional[float]) -> Optional[float]:
    if before is None or after is None or before == 0:
        return None
    return round((after - before) / before * 100, 1)


def job_summary(period_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One row per job: before and after side by side with the change per run."""
    by_job: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for r in period_rows:
        slot = by_job.setdefault(r["job_id"], {})
        # a job can carry two team tags over time; keep the figures of each period together
        cur = slot.get(r["period"])
        if cur is None:
            slot[r["period"]] = dict(r)
        else:  # same job and period under a different team tag: merge
            for k in ("dbus", "runs", "active_days"):
                cur[k] = (cur[k] or 0) + (r[k] or 0)
            cur["list_cost_usd"] = (None if cur["list_cost_usd"] is None or r["list_cost_usd"] is None
                                    else cur["list_cost_usd"] + r["list_cost_usd"])
            cur["dbus_per_run"] = round(cur["dbus"] / cur["runs"], 6) if cur["runs"] else None

    out = []
    for job_id, periods in by_job.items():
        b, a = periods.get("BEFORE"), periods.get("AFTER")
        state = "MIGRATED" if (b and a) else "SERVERLESS_ONLY" if a else "CLASSIC_ONLY"

        def per_run(p):
            return None if not p or p["list_cost_usd"] is None or not p["runs"] else p["list_cost_usd"] / p["runs"]

        out.append({
            "job_id": job_id,
            "job_name": (a or b).get("job_name") or (b or a).get("job_name"),
            "team_tag": (a or b).get("team_tag"),
            "state": state,
            "before_dbus": b["dbus"] if b else None, "after_dbus": a["dbus"] if a else None,
            "before_runs": b["runs"] if b else None, "after_runs": a["runs"] if a else None,
            "before_dbu_per_run": b["dbus_per_run"] if b else None,
            "after_dbu_per_run": a["dbus_per_run"] if a else None,
            "dbu_per_run_change_pct": _pct(b["dbus_per_run"] if b else None, a["dbus_per_run"] if a else None),
            "before_cost_usd": b["list_cost_usd"] if b else None,
            "after_cost_usd": a["list_cost_usd"] if a else None,
            "cost_per_run_change_pct": _pct(per_run(b), per_run(a)),
        })
    return sorted(out, key=lambda r: r["job_id"])


def load_sample(path: str) -> List[Dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)["rows"]


def valid_tag_key(key: str) -> bool:
    """The tag key goes into a SQL expression, so only plain characters are allowed."""
    import re
    return re.fullmatch(r"[A-Za-z0-9_.\-]{1,64}", key or "") is not None
