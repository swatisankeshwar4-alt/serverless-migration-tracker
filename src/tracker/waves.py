"""Wave planning (L1): which classic jobs to migrate first.

Wave 1  quick wins          no blockers, ready now
Wave 2  automated fixes     every blocker has an automated fix (code rewrites, job split)
Wave 3  manual remediation  at least one blocker needs manual work, but the workload can move
Wave 4  keep on classic     a blocker that normally means staying on classic (ML runtime, custom image,
                            Scala/R with JVM or CRAN dependencies): needs an owner decision

Only classic JOBS get a wave. Notebooks and clusters roll up under their jobs; serverless jobs need none.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from . import inventory, readiness

WAVE_NAMES = {
    1: "Wave 1: quick wins (ready now)",
    2: "Wave 2: automated fixes",
    3: "Wave 3: manual remediation",
    4: "Wave 4: keep on classic (decision needed)",
}


def is_hard_blocker(f: Dict[str, Any]) -> bool:
    detail = f.get("detail", "")
    if f.get("limitation_id") == "L4" and any(k in detail for k in ("ML runtime", "Docker", "custom runtime image")):
        return True
    return "KEEP_ON_CLASSIC" in (f.get("suggested_fix") or "")


def classify_wave(findings: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    blockers = [f for f in findings if f["severity"] == readiness.BLOCKER]
    auto = [f for f in blockers if f.get("auto_fixable")]
    if any(is_hard_blocker(f) for f in blockers):
        wave = 4
    elif not blockers:
        wave = 1
    elif len(auto) == len(blockers):
        wave = 2
    else:
        wave = 3
    return {"wave": wave, "wave_name": WAVE_NAMES[wave], "blockers": len(blockers),
            "auto_fixable_blockers": len(auto), "manual_blockers": len(blockers) - len(auto)}


def plan_waves(jobs: List[Dict[str, Any]], clusters: List[Dict[str, Any]],
               notebook_findings: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> Dict[str, Dict[str, Any]]:
    """job_id -> wave info, for classic jobs only."""
    nbf = notebook_findings or {}
    clusters_by_id = {c["cluster_id"]: c for c in clusters}
    out: Dict[str, Dict[str, Any]] = {}
    for job in jobs:
        if inventory.classify_compute(job) != inventory.CLASSIC:
            continue
        paths = [r["workload_id"] for r in inventory.notebook_rows(job, "x")]
        findings = readiness.scan_job(job, clusters_by_id) + [f for p in paths for f in nbf.get(p, [])]
        out[str(job["job_id"])] = classify_wave(findings)
    return out
