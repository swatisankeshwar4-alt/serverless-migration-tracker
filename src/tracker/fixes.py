"""Turn the tracker's automated fixes into before / after examples people can read (L2, L3, L6, L13).

Pure Python: used by the app's Fixes tab and by the apply-rewrites notebook, and unit-tested.
"""
from __future__ import annotations

import difflib
import json
import re
from typing import Any, Dict, List, Optional

_SAFE_NAME = re.compile(r"[^A-Za-z0-9_\-]+")


def unified_diff(before: str, after: str, name: str = "notebook") -> str:
    """A standard unified diff (what changed, line by line)."""
    return "".join(difflib.unified_diff(
        before.splitlines(keepends=True), after.splitlines(keepends=True),
        fromfile=f"{name} (original, classic)", tofile=f"{name} (rewritten, serverless)"))


def diff_stats(before: str, after: str) -> Dict[str, int]:
    added = removed = 0
    for line in difflib.ndiff(before.splitlines(), after.splitlines()):
        if line.startswith("+ "):
            added += 1
        elif line.startswith("- "):
            removed += 1
    return {"lines_added": added, "lines_removed": removed}


def environment_spec_yaml(translation: Dict[str, Any]) -> str:
    """Render an init-script translation as the serverless `environments` block of a job (YAML)."""
    deps = translation.get("dependencies") or []
    lines = ["environments:", "  - environment_key: default", "    spec:",
             f"      client: \"{(translation.get('environment_spec') or {}).get('client', '2')}\"",
             "      dependencies:"]
    lines += [f"        - \"{d}\"" for d in deps] or ["        []"]
    manual = []
    for pkg in translation.get("os_packages") or []:
        manual.append(f"OS package {pkg}: not installable on serverless; find a pip wheel or keep on classic")
    for name in translation.get("env_var_names") or []:
        manual.append(f"env var {name}: set it as a job parameter or read it from a secret scope")
    for cmd in translation.get("other_commands") or []:
        manual.append(f"command `{cmd}`: review by hand (not translated)")
    if manual:
        lines.append("# Manual follow-up:")
        lines += [f"#   - {m}" for m in manual]
    return "\n".join(lines) + "\n"


def split_rows(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    """One row per job the split creates: the child jobs and the paused orchestrator."""
    rows = []
    for i, child in enumerate(plan.get("children") or [], start=1):
        keys = [t["task_key"] for t in child.get("tasks") or []]
        rows.append({"new_job": child.get("name"), "role": f"child {i}", "task_count": len(keys),
                     "tasks": ", ".join(keys)})
    orch = plan.get("orchestrator") or {}
    steps = []
    for t in orch.get("tasks") or []:
        deps = [d["task_key"] for d in t.get("depends_on") or []]
        steps.append(t["task_key"] + (f" (after {', '.join(deps)})" if deps else ""))
    paused = any(isinstance(orch.get(k), dict) and orch[k].get("pause_status") == "PAUSED"
                 for k in ("schedule", "trigger", "continuous"))
    rows.append({"new_job": orch.get("name"), "role": "orchestrator" + (" (schedule PAUSED)" if paused else ""),
                 "task_count": len(orch.get("tasks") or []), "tasks": " -> ".join(steps)})
    return rows


def demo_target_path(base_folder: str, workload_id: str, suffix: str) -> str:
    """Where a rewritten copy goes. Never the original path."""
    leaf = _SAFE_NAME.sub("_", workload_id.rstrip("/").split("/")[-1]) or "notebook"
    return f"{base_folder.rstrip('/')}/{leaf}{suffix}"


def changes_list(changes_json: Optional[str]) -> List[str]:
    if not changes_json:
        return []
    try:
        v = json.loads(changes_json)
    except ValueError:
        return [changes_json]
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, dict):
        return [f"{k}: {val}" for k, val in v.items()]
    return [str(v)]
