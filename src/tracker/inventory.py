"""Pure-Python discovery logic: turns Jobs/Clusters API dicts into inventory rows.

No Spark or SDK imports here, so it can be unit-tested anywhere.
Covers L1 (inventory) and the raw inputs that later steps scan (L2-L7, L11).
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Optional

CLASSIC = "CLASSIC"
SERVERLESS = "SERVERLESS"
UNKNOWN = "UNKNOWN"

# Limit from the migration agent documentation; tasks that only orchestrate are not "migratable".
MAX_MIGRATABLE_TASKS = 10
NON_MIGRATABLE_TASK_KEYS = ("condition_task", "run_job_task")

TASK_TYPE_KEYS = (
    "notebook_task", "spark_python_task", "spark_jar_task", "spark_submit_task",
    "python_wheel_task", "sql_task", "dbt_task", "pipeline_task", "run_job_task",
    "condition_task", "for_each_task",
)


def _tasks(job: Dict[str, Any]) -> List[Dict[str, Any]]:
    return (job.get("settings") or {}).get("tasks") or []


def _job_clusters(job: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out = {}
    for jc in (job.get("settings") or {}).get("job_clusters") or []:
        out[jc.get("job_cluster_key")] = jc.get("new_cluster") or {}
    return out


def task_cluster(task: Dict[str, Any], job_clusters: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return the classic cluster spec a task uses, or None if it has none."""
    if task.get("new_cluster"):
        return task["new_cluster"]
    key = task.get("job_cluster_key")
    if key and key in job_clusters:
        return job_clusters[key]
    if task.get("existing_cluster_id"):
        return {"existing_cluster_id": task["existing_cluster_id"]}
    return None


def classify_compute(job: Dict[str, Any]) -> str:
    """CLASSIC if any task needs a cluster, SERVERLESS if tasks run with no
    cluster reference, UNKNOWN when nothing tells us (e.g. pipeline-only jobs)."""
    clusters = _job_clusters(job)
    kinds = set()
    for t in _tasks(job):
        if task_cluster(t, clusters):
            kinds.add(CLASSIC)
        elif t.get("environment_key") or any(k in t for k in (
                "notebook_task", "spark_python_task", "python_wheel_task", "sql_task")):
            kinds.add(SERVERLESS)
    if CLASSIC in kinds:
        return CLASSIC
    if SERVERLESS in kinds:
        return SERVERLESS
    return UNKNOWN


def runtime_version(job: Dict[str, Any]) -> Optional[str]:
    clusters = _job_clusters(job)
    for t in _tasks(job):
        spec = task_cluster(t, clusters)
        if spec and spec.get("spark_version"):
            return spec["spark_version"]
    return None


def job_row(job: Dict[str, Any], source: str) -> Dict[str, Any]:
    s = job.get("settings") or {}
    return {
        "workload_id": str(job["job_id"]),
        "workload_type": "JOB",
        "name": s.get("name"),
        "owner": job.get("creator_user_name"),
        "task_count": len(_tasks(job)),
        "compute_kind": classify_compute(job),
        "runtime_version": runtime_version(job),
        "source": source,
    }


def notebook_rows(job: Dict[str, Any], source: str) -> List[Dict[str, Any]]:
    """One NOTEBOOK row per distinct notebook path referenced by the job."""
    seen, rows = set(), []
    for t in _tasks(job):
        path = (t.get("notebook_task") or {}).get("notebook_path")
        if path and path not in seen:
            seen.add(path)
            rows.append({
                "workload_id": path,
                "workload_type": "NOTEBOOK",
                "name": path.rsplit("/", 1)[-1],
                "owner": job.get("creator_user_name"),
                "task_count": None,
                "compute_kind": classify_compute(job),
                "runtime_version": runtime_version(job),
                "source": source,
            })
    return rows


def cluster_row(cluster: Dict[str, Any], source: str) -> Dict[str, Any]:
    return {
        "workload_id": cluster["cluster_id"],
        "workload_type": "CLUSTER",
        "name": cluster.get("cluster_name"),
        "owner": cluster.get("creator_user_name"),
        "task_count": None,
        "compute_kind": CLASSIC,
        "runtime_version": cluster.get("spark_version"),
        "source": source,
    }


REDACTED = "<redacted>"
# Dict-valued settings whose VALUES may hold secrets: the names are kept, the values removed.
_REDACT_DICT_VALUES = {"spark_env_vars", "spark_conf", "base_parameters", "notebook_params", "python_named_params",
                       "named_parameters", "job_parameters", "env_vars", "environment_variables", "sql_params"}
# List-of-string settings (command-line style arguments) that may hold secrets.
_REDACT_LIST_STRINGS = {"parameters", "python_params", "jar_params", "ssh_public_keys"}
_SECRET_KEY_RE = re.compile(r"(password|passwd|secret|token|api[_-]?key|private[_-]?key|credential)", re.I)


def redact_definition(obj: Any, key: Optional[str] = None) -> Any:
    """Copy of a job/cluster definition with secret-prone values removed.

    The scanner needs names, paths and presence of settings, never the values of environment
    variables, Spark configs or parameters, so those are replaced before anything is stored."""
    if isinstance(obj, dict):
        if key in _REDACT_DICT_VALUES:
            return {k: REDACTED for k in obj}
        out = {k: redact_definition(v, k) for k, v in obj.items()}
        if "name" in out and "default" in out and isinstance(out["default"], str):
            out["default"] = REDACTED      # job-level parameter definitions
        return out
    if isinstance(obj, list):
        if key in _REDACT_LIST_STRINGS:
            return [REDACTED if isinstance(x, str) else redact_definition(x) for x in obj]
        return [redact_definition(x, key) for x in obj]
    if isinstance(obj, str) and key and _SECRET_KEY_RE.search(key):
        return REDACTED
    return obj


def build_inventory(jobs: Iterable[Dict[str, Any]], clusters: Iterable[Dict[str, Any]], source: str):
    """Return (inventory_rows, raw_definition_rows).

    raw_definition_rows keep the full API JSON so later steps (readiness scan,
    L3/L4/L5/L11) can analyse init scripts, libraries, log delivery, SSH keys.
    Notebooks are de-duplicated across jobs.
    """
    inv: List[Dict[str, Any]] = []
    raw: List[Dict[str, Any]] = []
    nb_seen = set()

    for job in jobs:
        inv.append(job_row(job, source))
        raw.append({"workload_id": str(job["job_id"]), "workload_type": "JOB",
                    "definition_json": json.dumps(redact_definition(job), sort_keys=True)})
        for nb in notebook_rows(job, source):
            if nb["workload_id"] not in nb_seen:
                nb_seen.add(nb["workload_id"])
                inv.append(nb)

    for c in clusters:
        inv.append(cluster_row(c, source))
        raw.append({"workload_id": c["cluster_id"], "workload_type": "CLUSTER",
                    "definition_json": json.dumps(redact_definition(c), sort_keys=True)})
    return inv, raw


# ---- loaders -------------------------------------------------------------

def load_fixtures(fixtures_dir: str):
    with open(f"{fixtures_dir}/jobs.json", encoding="utf-8") as f:
        jobs = json.load(f)
    with open(f"{fixtures_dir}/clusters.json", encoding="utf-8") as f:
        clusters = json.load(f)
    return jobs, clusters


def load_live(workspace_client):
    """Read every job (with tasks) and cluster through the Databricks SDK."""
    jobs = [j.as_dict() for j in workspace_client.jobs.list(expand_tasks=True)]
    clusters = [c.as_dict() for c in workspace_client.clusters.list()]
    return jobs, clusters
