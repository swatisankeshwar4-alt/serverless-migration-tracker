"""Undo any change the tracker made, from the `rollback_json` it stored in `actions_log` (L9, safety).

Three kinds of undo are stored:
* {"job_id", "new_settings", "fields_to_remove"?}: a jobs/update that restores the settings as they were
  (schedule pause, drift revert)
* {"delete_jobs": [ids]}: jobs the tracker created (L2 split children and orchestrator)
* {"delete_path": path}: a notebook copy the tracker wrote (applied rewrite)
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Set


def operations(rollback_json: Optional[str]) -> List[Dict[str, Any]]:
    """The API calls that undo one action, in order. Raises ValueError on an unknown shape."""
    if not rollback_json:
        return []
    r = json.loads(rollback_json)
    ops: List[Dict[str, Any]] = []
    if "new_settings" in r or "fields_to_remove" in r:
        if "job_id" not in r:
            raise ValueError("settings undo without job_id")
        body = {"job_id": int(r["job_id"]), "new_settings": r.get("new_settings") or {}}
        if r.get("fields_to_remove"):
            body["fields_to_remove"] = list(r["fields_to_remove"])
        ops.append({"call": "POST /api/2.1/jobs/update", "body": body})
    for jid in r.get("delete_jobs") or []:
        ops.append({"call": "POST /api/2.1/jobs/delete", "body": {"job_id": int(jid)}})
    if r.get("delete_path"):
        path = str(r["delete_path"])
        if not path.startswith(("/Shared/serverless_migration_demo/", "/Workspace/Shared/serverless_migration_demo/")):
            raise ValueError(f"refusing to delete outside the tracker's demo folder: {path}")
        ops.append({"call": "POST /api/2.0/workspace/delete", "body": {"path": path}})
    if not ops:
        raise ValueError(f"unknown rollback shape: {sorted(r)}")
    return ops


def candidates(actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Real (not dry-run) actions with an undo that has not been rolled back yet, newest first."""
    undone: Set[str] = {a["detail"].split("undo of ", 1)[1].split()[0]
                        for a in actions if a.get("action") == "ROLLED_BACK" and "undo of " in (a.get("detail") or "")}
    out = [a for a in actions
           if a.get("rollback_json") and not a.get("dry_run") and a.get("action") != "ROLLED_BACK"
           and a.get("action_id") not in undone]
    return sorted(out, key=lambda a: str(a.get("performed_at")), reverse=True)
