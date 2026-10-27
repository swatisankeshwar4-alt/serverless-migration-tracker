"""Split a job with more than 10 migratable tasks into child jobs plus an orchestrator (L2).

Design rules:
* Tasks are put in dependency order and cut into chunks of at most MAX migratable tasks.
* Each chunk becomes a child job. Dependencies inside a chunk are kept; dependencies that cross a
  chunk boundary are expressed on the orchestrator with run_job_task + depends_on.
* The orchestrator is a NEW job created with its schedule paused. The original job is never edited,
  so the original keeps running until someone cuts over (this also avoids the L10 double-firing problem).
* A dependency on a condition task's outcome that would cross a chunk boundary cannot be expressed this
  way: plan_split raises ValueError and the finding stays manual.
"""
from __future__ import annotations

import copy
from typing import Any, Callable, Dict, List, Optional

from . import inventory

MAX = inventory.MAX_MIGRATABLE_TASKS


def is_migratable(task: Dict[str, Any]) -> bool:
    return not any(k in task for k in inventory.NON_MIGRATABLE_TASK_KEYS)


def topo_order(tasks: List[Dict[str, Any]]) -> List[str]:
    """Task keys in dependency order, ties broken by original order. Raises ValueError on a cycle."""
    by = {t["task_key"]: t for t in tasks}
    indegree = {k: 0 for k in by}
    for t in tasks:
        for d in t.get("depends_on") or []:
            if d["task_key"] not in by:
                raise ValueError(f"task {t['task_key']} depends on unknown task {d['task_key']}")
            indegree[t["task_key"]] += 1
    ready = [k for k in by if indegree[k] == 0]
    order: List[str] = []
    while ready:
        k = ready.pop(0)
        order.append(k)
        for t in tasks:
            if any(d["task_key"] == k for d in t.get("depends_on") or []):
                indegree[t["task_key"]] -= 1
                if indegree[t["task_key"]] == 0:
                    ready.append(t["task_key"])
    if len(order) != len(by):
        raise ValueError("dependency cycle between tasks")
    return order


def plan_split(job: Dict[str, Any], max_tasks: int = MAX) -> Optional[Dict[str, Any]]:
    """Return a split plan, or None when the job already fits. Raises ValueError when it cannot be split."""
    s = job.get("settings") or {}
    tasks = s.get("tasks") or []
    if sum(1 for t in tasks if is_migratable(t)) <= max_tasks:
        return None

    by = {t["task_key"]: t for t in tasks}
    chunks: List[List[str]] = [[]]
    count = 0
    for key in topo_order(tasks):
        if is_migratable(by[key]):
            if count == max_tasks:
                chunks.append([])
                count = 0
            count += 1
        chunks[-1].append(key)

    chunk_of = {k: i for i, keys in enumerate(chunks, start=1) for k in keys}
    for t in tasks:
        for d in t.get("depends_on") or []:
            if chunk_of[d["task_key"]] != chunk_of[t["task_key"]] and "outcome" in d:
                raise ValueError(f"condition outcome dependency {d['task_key']} -> {t['task_key']} crosses a split")

    job_clusters = {jc["job_cluster_key"]: jc for jc in s.get("job_clusters") or []}
    environments = {e["environment_key"]: e for e in s.get("environments") or []}
    base_tags = dict(s.get("tags") or {})
    n = len(chunks)

    children = []
    for i, keys in enumerate(chunks, start=1):
        members = set(keys)
        child_tasks = []
        for k in keys:
            t = copy.deepcopy(by[k])
            deps = [d for d in t.get("depends_on") or [] if d["task_key"] in members]
            if deps:
                t["depends_on"] = deps
            else:
                t.pop("depends_on", None)
            child_tasks.append(t)
        child: Dict[str, Any] = {
            "name": f"{s.get('name')} [part {i}/{n}]",
            "tags": {**base_tags, "migration_parent": str(job["job_id"])},
            "tasks": child_tasks,
        }
        used_jc = {t.get("job_cluster_key") for t in child_tasks} - {None}
        used_env = {t.get("environment_key") for t in child_tasks} - {None}
        if used_jc:
            child["job_clusters"] = [copy.deepcopy(job_clusters[k]) for k in sorted(used_jc) if k in job_clusters]
        if used_env:
            child["environments"] = [copy.deepcopy(environments[k]) for k in sorted(used_env) if k in environments]
        children.append(child)

    orchestrator = {k: copy.deepcopy(v) for k, v in s.items()
                    if k not in ("tasks", "job_clusters", "environments", "name", "tags")}
    orchestrator["name"] = f"{s.get('name')} [split orchestrator]"
    orchestrator["tags"] = {**base_tags, "migration_origin": str(job["job_id"])}
    for key in ("schedule", "trigger", "continuous"):
        if isinstance(orchestrator.get(key), dict):
            orchestrator[key]["pause_status"] = "PAUSED"   # the original keeps the live schedule

    orch_tasks = []
    for i, keys in enumerate(chunks, start=1):
        deps = sorted({chunk_of[d["task_key"]] for k in keys for d in by[k].get("depends_on") or []} - {i})
        t = {"task_key": f"part_{i}", "run_job_task": {"job_id": f"@child_{i}"}}
        if deps:
            t["depends_on"] = [{"task_key": f"part_{j}"} for j in deps]
        orch_tasks.append(t)
    orchestrator["tasks"] = orch_tasks

    return {"source_job_id": str(job["job_id"]), "max_tasks": max_tasks,
            "children": children, "orchestrator": orchestrator}


def splittable(job: Dict[str, Any], max_tasks: int = MAX) -> bool:
    try:
        return plan_split(job, max_tasks) is not None
    except ValueError:
        return False


def verify_split(job: Dict[str, Any], plan: Dict[str, Any]) -> List[str]:
    """Return a list of problems (empty means the plan is faithful to the original job)."""
    problems: List[str] = []
    original = (job.get("settings") or {}).get("tasks") or []
    where: Dict[str, int] = {}
    for i, child in enumerate(plan["children"], start=1):
        migratable = sum(1 for t in child["tasks"] if is_migratable(t))
        if migratable > plan["max_tasks"]:
            problems.append(f"child {i} has {migratable} migratable tasks")
        for t in child["tasks"]:
            if t["task_key"] in where:
                problems.append(f"task {t['task_key']} appears twice")
            where[t["task_key"]] = i
    if set(where) != {t["task_key"] for t in original}:
        problems.append("task set differs from the original job")
        return problems

    orch = {t["task_key"]: t for t in plan["orchestrator"]["tasks"]}
    for t in original:
        for d in t.get("depends_on") or []:
            a, b = where[d["task_key"]], where[t["task_key"]]
            if a > b:
                problems.append(f"dependency {d['task_key']} -> {t['task_key']} runs backwards")
            elif a == b:
                child_task = next(x for x in plan["children"][a - 1]["tasks"] if x["task_key"] == t["task_key"])
                if not any(x["task_key"] == d["task_key"] for x in child_task.get("depends_on") or []):
                    problems.append(f"dependency {d['task_key']} -> {t['task_key']} lost inside child {a}")
            else:
                deps = {x["task_key"] for x in orch[f"part_{b}"].get("depends_on") or []}
                if f"part_{a}" not in deps:
                    problems.append(f"dependency {d['task_key']} -> {t['task_key']} lost between children")
    return problems


def apply_split(do: Callable[[str, str, Dict[str, Any]], Dict[str, Any]], plan: Dict[str, Any],
                dry_run: bool = True) -> List[Dict[str, Any]]:
    """Create the child jobs and the paused orchestrator. `do(method, path, body)` is an API caller.
    dry_run only reports what would be created. The original job is never touched."""
    actions: List[Dict[str, Any]] = []
    child_ids: Dict[int, int] = {}
    for i, child in enumerate(plan["children"], start=1):
        action = {"action": "CREATE_CHILD_JOB", "name": child["name"], "dry_run": dry_run}
        if not dry_run:
            child_ids[i] = do("POST", "/api/2.1/jobs/create", child)["job_id"]
            action["job_id"] = child_ids[i]
        actions.append(action)

    orch = copy.deepcopy(plan["orchestrator"])
    for t in orch["tasks"]:
        idx = int(t["run_job_task"]["job_id"].split("_")[1])
        t["run_job_task"]["job_id"] = child_ids.get(idx, 0)
    action = {"action": "CREATE_ORCHESTRATOR_PAUSED", "name": orch["name"], "dry_run": dry_run}
    if not dry_run:
        action["job_id"] = do("POST", "/api/2.1/jobs/create", orch)["job_id"]
    actions.append(action)
    return actions
