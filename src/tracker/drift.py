"""L9: drift between a job as defined in the Asset Bundle (Git) and the job as it runs in the workspace.

The bundle in Git is the source of truth. Someone editing a job in the UI (adding a schedule, changing a
parameter, putting a migrated task back on a classic cluster) makes the workspace drift from Git.
`compare` lists every difference. Top-level settings can be reverted automatically; task changes are
reported and fixed by redeploying the bundle. Every revert stores the payload that undoes it.

Pure Python, unit-tested. The notebook fetches live jobs with the SDK and the bundle YAML from the
deployed bundle files.
"""
from __future__ import annotations

import copy
import re
from typing import Any, Dict, List, Optional, Tuple

VAR_RE = re.compile(r"\$\{var\.([A-Za-z0-9_]+)\}")

# Top-level settings that are compared and can be reverted with jobs/update. Value = the API default.
TOP_LEVEL_DEFAULTS: Dict[str, Any] = {
    "schedule": None, "trigger": None, "continuous": None,
    "timeout_seconds": 0, "max_concurrent_runs": 1, "tags": {}, "email_notifications": {},
}
# Tags the CLI adds in development mode; not drift.
IGNORED_TAGS = {"dev"}
CLUSTER_KEYS = ("existing_cluster_id", "new_cluster", "job_cluster_key")


def resolve_vars(obj: Any, variables: Dict[str, str]) -> Any:
    if isinstance(obj, str):
        return VAR_RE.sub(lambda m: str(variables.get(m.group(1), m.group(0))), obj)
    if isinstance(obj, list):
        return [resolve_vars(x, variables) for x in obj]
    if isinstance(obj, dict):
        return {k: resolve_vars(v, variables) for k, v in obj.items()}
    return obj


def bundle_jobs(parsed_yaml_docs: List[Dict[str, Any]], variables: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    """resource key -> job definition (variables resolved), from every parsed resources/*.yml."""
    out: Dict[str, Dict[str, Any]] = {}
    for doc in parsed_yaml_docs:
        for key, job in (((doc or {}).get("resources") or {}).get("jobs") or {}).items():
            out[key] = resolve_vars(copy.deepcopy(job), variables)
    return out


def _leaf(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    leaf = path.rstrip("/").split("/")[-1]
    return re.sub(r"\.(py|sql|scala|r|ipynb)$", "", leaf, flags=re.I)


def _schedule(v: Any) -> Any:
    if not isinstance(v, dict):
        return None
    # pause_status is ignored: development-mode deploys pause schedules on purpose.
    return {"cron": v.get("quartz_cron_expression"), "timezone": v.get("timezone_id")}


def normalise(settings: Dict[str, Any]) -> Dict[str, Any]:
    """The comparable shape of a job, the same for a bundle definition and a live job."""
    s = settings or {}
    top: Dict[str, Any] = {}
    for f, default in TOP_LEVEL_DEFAULTS.items():
        v = s.get(f, default)
        if f == "schedule":
            v = _schedule(v)
        elif f in ("trigger", "continuous"):
            v = "configured" if v else None
        elif f == "tags":
            v = {k: str(x) for k, x in (v or {}).items() if k not in IGNORED_TAGS}
        elif f == "email_notifications":
            v = {k: sorted(x) if isinstance(x, list) else x for k, x in (v or {}).items()
                 if x not in (None, [], {}, False)}
        elif v is None:
            v = default
        top[f] = v
    tasks = {}
    for t in s.get("tasks") or []:
        nb = t.get("notebook_task") or {}
        tasks[t["task_key"]] = {
            "notebook": _leaf(nb.get("notebook_path")),
            "parameters": {k: str(v) for k, v in (nb.get("base_parameters") or {}).items()},
            "depends_on": sorted(d["task_key"] for d in t.get("depends_on") or []),
            "compute": "CLASSIC" if any(t.get(k) for k in CLUSTER_KEYS) else "SERVERLESS",
        }
    return {"top": top, "tasks": tasks}


def compare(bundle_settings: Dict[str, Any], live_settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Every difference, as rows: field, in Git (bundle), in the workspace (live), auto_revert."""
    b, l = normalise(bundle_settings), normalise(live_settings)
    rows: List[Dict[str, Any]] = []
    for f in TOP_LEVEL_DEFAULTS:
        if b["top"][f] != l["top"][f]:
            rows.append({"field": f, "bundle": b["top"][f], "live": l["top"][f], "auto_revert": True})
    for key in sorted(set(b["tasks"]) | set(l["tasks"])):
        bt, lt = b["tasks"].get(key), l["tasks"].get(key)
        if bt is None or lt is None:
            rows.append({"field": f"task {key}", "bundle": "present" if bt else "missing",
                         "live": "present" if lt else "missing", "auto_revert": False})
            continue
        for attr in ("notebook", "parameters", "depends_on", "compute"):
            if bt[attr] != lt[attr]:
                rows.append({"field": f"task {key}.{attr}", "bundle": bt[attr], "live": lt[attr],
                             "auto_revert": False})
    return rows


def revert_payload(bundle_settings: Dict[str, Any], live_settings: Dict[str, Any],
                   drift_rows: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """(revert, undo): two jobs/update payloads {new_settings, fields_to_remove}.

    `revert` puts the drifted top-level fields back to the bundle values; `undo` restores the live
    values exactly as they were, so the revert itself can be rolled back.
    """
    revert: Dict[str, Any] = {"new_settings": {}, "fields_to_remove": []}
    undo: Dict[str, Any] = {"new_settings": {}, "fields_to_remove": []}
    for row in drift_rows:
        f = row["field"]
        if not row["auto_revert"]:
            continue
        b_has, l_has = f in bundle_settings, f in live_settings
        if f == "tags":
            dev = {k: v for k, v in (live_settings.get("tags") or {}).items() if k in IGNORED_TAGS}
            revert["new_settings"]["tags"] = {**(bundle_settings.get("tags") or {}), **dev}
        elif b_has:
            revert["new_settings"][f] = copy.deepcopy(bundle_settings[f])
        elif TOP_LEVEL_DEFAULTS[f] is None:
            revert["fields_to_remove"].append(f)
        else:
            revert["new_settings"][f] = copy.deepcopy(TOP_LEVEL_DEFAULTS[f])
        if l_has:
            undo["new_settings"][f] = copy.deepcopy(live_settings[f])
        elif TOP_LEVEL_DEFAULTS[f] is None:
            undo["fields_to_remove"].append(f)
        else:
            undo["new_settings"][f] = copy.deepcopy(TOP_LEVEL_DEFAULTS[f])
    return revert, undo


def match_live_job(bundle_name: str, bundle_folder_marker: str,
                   live_jobs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Find the deployed job for a bundle job. Development mode prefixes the name with "[dev <user>] ".

    Only jobs whose notebooks live in this bundle's deployed folder are considered, so a job with the
    same name in someone else's bundle is never touched.
    """
    def ours(job: Dict[str, Any]) -> bool:
        for t in (job.get("settings") or {}).get("tasks") or []:
            if bundle_folder_marker in ((t.get("notebook_task") or {}).get("notebook_path") or ""):
                return True
        return False

    hits = []
    for j in live_jobs:
        name = (j.get("settings") or {}).get("name") or ""
        if (name == bundle_name or (name.startswith("[") and name.endswith("] " + bundle_name))) and ours(j):
            hits.append(j)
    return sorted(hits, key=lambda j: str(j.get("job_id")))[0] if len(hits) >= 1 else None
