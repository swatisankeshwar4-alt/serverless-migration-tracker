"""Local preview data: runs the real discovery, scan, readiness and wave code on `fixtures/`.

Used only when TRACKER_LOCAL=1, so the app can be previewed and demoed without a workspace.
Nothing here is read in a deployed app.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from tracker import codescan, cost, inventory, readiness, waves

FIXTURES = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "fixtures")


def build() -> Dict[str, List[Dict[str, Any]]]:
    jobs, clusters = inventory.load_fixtures(FIXTURES)
    inv, _ = inventory.build_inventory(jobs, clusters, "FIXTURE")

    nb_paths = [r["workload_id"] for r in inv if r["workload_type"] == "NOTEBOOK"]
    sources, _missing = codescan.load_fixture_sources(FIXTURES, nb_paths)
    nbf: Dict[str, List[Dict[str, Any]]] = {}
    for path, (src, lang) in sources.items():
        nbf[path] = codescan.scan_with_fixability(path, src, lang)

    findings, status_rows = readiness.analyse_all(jobs, clusters, nbf)
    wave_plan = waves.plan_waves(jobs, clusters, nbf)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    status = {s["workload_id"]: {**s, "wave": wave_plan.get(s["workload_id"], {}).get("wave")} for s in status_rows}
    all_findings = findings + [f for fs in nbf.values() for f in fs]

    by_wid: Dict[str, Dict[str, int]] = {}
    for f in all_findings:
        c = by_wid.setdefault(f["workload_id"], {"blockers": 0, "warnings": 0})
        c["blockers"] += f["severity"] == "BLOCKER"
        c["warnings"] += f["severity"] == "WARNING"

    names = {r["workload_id"]: r["name"] for r in inv}
    types = {r["workload_id"]: r["workload_type"] for r in inv}
    workloads = [{**r, "readiness_score": status.get(r["workload_id"], {}).get("readiness_score"),
                  "status": status.get(r["workload_id"], {}).get("status"),
                  "wave": status.get(r["workload_id"], {}).get("wave"), "updated_at": now,
                  "blockers": by_wid.get(r["workload_id"], {}).get("blockers", 0),
                  "warnings": by_wid.get(r["workload_id"], {}).get("warnings", 0)} for r in inv]
    sample = cost.load_sample(os.path.join(FIXTURES, "sample_billing.json"))
    return {
        "workloads": workloads,
        "findings": [{**f, "name": names.get(f["workload_id"]), "workload_type": types.get(f["workload_id"]), "auto_fixable": bool(f.get("auto_fixable"))}
                     for f in all_findings],
        "approvals": [], "actions": [], "split_plans": [], "validation": [],
        "cost": cost.job_summary(cost.compare(sample)),
    }
