"""Detect jobs that fire on the same schedule for the same workload (L10).

The typical case: the migration agent clones a job to serverless, and until the clone is promoted both
the original and the clone run on the same schedule, so work is done twice. We report the extra job
(the clone) and can build the settings fragment that pauses its trigger. Nothing here changes a workspace;
the notebook applies the pause only when asked, and only for an admin.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

from . import inventory


def schedule_signature(job: Dict[str, Any]) -> Optional[Tuple]:
    """A comparable description of the job's ACTIVE trigger, or None if it has none or is paused."""
    s = job.get("settings") or {}
    sch = s.get("schedule")
    if sch and sch.get("pause_status") != "PAUSED":
        return ("cron", sch.get("quartz_cron_expression"), sch.get("timezone_id"))
    trig = s.get("trigger") or {}
    if trig and trig.get("pause_status") != "PAUSED":
        if trig.get("periodic"):
            p = trig["periodic"]
            return ("periodic", p.get("interval"), p.get("unit") or p.get("units"))
        if trig.get("file_arrival"):
            return ("file_arrival", trig["file_arrival"].get("url"))
        if trig.get("table_update"):
            return ("table_update", tuple(sorted(trig["table_update"].get("table_names") or [])))
    cont = s.get("continuous")
    if cont and cont.get("pause_status") != "PAUSED":
        return ("continuous",)
    return None


def run_targets(job: Dict[str, Any]) -> Set[str]:
    """What the job actually runs: notebook paths, python files, pipelines, jar classes, wheel entry points."""
    out: Set[str] = set()
    for t in inventory._tasks(job):
        if t.get("notebook_task"):
            out.add("nb:" + t["notebook_task"].get("notebook_path", ""))
        if t.get("spark_python_task"):
            out.add("py:" + t["spark_python_task"].get("python_file", ""))
        if t.get("pipeline_task"):
            out.add("dlt:" + t["pipeline_task"].get("pipeline_id", ""))
        if t.get("spark_jar_task"):
            out.add("jar:" + t["spark_jar_task"].get("main_class_name", ""))
        if t.get("python_wheel_task"):
            w = t["python_wheel_task"]
            out.add(f"whl:{w.get('package_name', '')}.{w.get('entry_point', '')}")
    out.discard("nb:")
    return out


def _name(job: Dict[str, Any]) -> str:
    return (job.get("settings") or {}).get("name") or ""


def _created(job: Dict[str, Any]) -> int:
    return int(job.get("created_time") or 0)


def is_clone_of(candidate: Dict[str, Any], other: Dict[str, Any]) -> bool:
    """True when `candidate` looks like the later copy of `other`."""
    cn, on = _name(candidate), _name(other)
    if cn != on and cn.startswith(on):
        return True
    if on != cn and on.startswith(cn):
        return False
    if _created(candidate) != _created(other):
        return _created(candidate) > _created(other)
    return int(candidate["job_id"]) > int(other["job_id"])


def find_duplicates(jobs: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """Pairs of jobs with the same active schedule and at least one shared run target.

    Returns [{"keep": job_id, "extra": job_id, "schedule": text}], where `extra` is the clone to pause."""
    sigs = {str(j["job_id"]): schedule_signature(j) for j in jobs}
    targets = {str(j["job_id"]): run_targets(j) for j in jobs}
    by_id = {str(j["job_id"]): j for j in jobs}
    ids = [i for i in by_id if sigs[i] is not None]
    pairs, seen = [], set()
    for ai, a in enumerate(ids):
        for b in ids[ai + 1:]:
            if sigs[a] != sigs[b] or not (targets[a] & targets[b]):
                continue
            extra, keep = (a, b) if is_clone_of(by_id[a], by_id[b]) else (b, a)
            if (keep, extra) in seen:
                continue
            seen.add((keep, extra))
            pairs.append({"keep": keep, "extra": extra, "schedule": " ".join(str(x) for x in sigs[a])})
    return pairs


def pause_settings(job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The jobs/update `new_settings` fragment that pauses the job's active trigger, or None."""
    s = job.get("settings") or {}
    if s.get("schedule") and s["schedule"].get("pause_status") != "PAUSED":
        return {"schedule": {**s["schedule"], "pause_status": "PAUSED"}}
    trig = s.get("trigger")
    if trig and trig.get("pause_status") != "PAUSED":
        return {"trigger": {**trig, "pause_status": "PAUSED"}}
    cont = s.get("continuous")
    if cont and cont.get("pause_status") != "PAUSED":
        return {"continuous": {**cont, "pause_status": "PAUSED"}}
    return None
