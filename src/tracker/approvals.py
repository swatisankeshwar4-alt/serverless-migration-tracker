"""Approval gate rules (L1). Pure functions: the notebook and the app both call these.

* Only workspace admins can approve or reject.
* A workload can be approved only when it is a classic job in wave 1 or 2 (no manual blockers left).
  Waves 3 and 4 must be remediated, or decided on, before they enter the gate.
* The decider is always taken from the authenticated user, never typed in.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple


APPROVABLE_STATUSES = ("READY", "BLOCKED")
APPROVABLE_WAVES = (1, 2)


def is_admin_from_groups(group_names: Iterable[str]) -> bool:
    return "admins" in set(group_names)


def check_approval(status: str, wave: Any, is_admin: bool, decision: str) -> Tuple[bool, str]:
    """Return (allowed, reason)."""
    if decision not in ("APPROVED", "REJECTED"):
        return False, f"unknown decision {decision!r}"
    if not is_admin:
        return False, "only workspace admins can approve or reject"
    if decision == "REJECTED":
        return (True, "rejection recorded") if status not in ("DONE",) else (False, "already on serverless")
    if status == "DONE":
        return False, "already on serverless"
    if status not in APPROVABLE_STATUSES:
        return False, f"status {status} is past the approval gate"
    if wave is None:
        return False, "no wave assigned (only classic jobs are approved)"
    if int(wave) not in APPROVABLE_WAVES:
        return False, f"wave {wave} still has manual blockers: remediate first"
    return True, "approved"


def resolve_targets(spec: str, status_rows: List[Dict[str, Any]]) -> List[str]:
    """`spec` is a comma list of workload ids, or `wave:N` for every job in that wave."""
    spec = (spec or "").strip()
    if spec.lower().startswith("wave:"):
        n = int(spec.split(":", 1)[1])
        return [r["workload_id"] for r in status_rows if r.get("wave") is not None and int(r["wave"]) == n]
    return [s.strip() for s in spec.split(",") if s.strip()]


def workspace_admin(workspace_client) -> Tuple[bool, str]:
    """Return (is_admin, user_name) for the authenticated user. Fails closed: any error means not admin."""
    try:
        me = workspace_client.current_user.me()
        groups = [g.display for g in (me.groups or []) if g.display]
        return is_admin_from_groups(groups), me.user_name or ""
    except Exception:  # noqa: BLE001
        return False, ""
