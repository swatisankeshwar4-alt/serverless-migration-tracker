"""SQL access for the app. Reads run as the app's service principal; the admin check runs as the user."""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from tracker import appdata


def warehouse_id() -> str:
    wid = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
    if not wid:
        raise RuntimeError("DATABRICKS_WAREHOUSE_ID is not set: add the SQL warehouse as an app resource.")
    return wid


def run(client, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Run one statement on the warehouse and return rows as dicts. Parameters are bound, never inlined."""
    from databricks.sdk.service.sql import StatementParameterListItem, StatementState

    items = [StatementParameterListItem(name=k, value=None if v is None else str(v)) for k, v in (params or {}).items()]
    resp = client.statement_execution.execute_statement(
        statement=sql, warehouse_id=warehouse_id(), wait_timeout="50s", parameters=items or None)
    if resp.status is None or resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error.message if resp.status and resp.status.error else "statement did not finish"
        raise RuntimeError(f"SQL failed: {err}")
    cols = [(c.name, str(c.type_name.value if hasattr(c.type_name, "value") else c.type_name))
            for c in (resp.manifest.schema.columns if resp.manifest and resp.manifest.schema else [])]
    return appdata.to_records(cols, resp.result.data_array if resp.result else None)
