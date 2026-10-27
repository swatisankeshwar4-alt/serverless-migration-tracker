import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from tracker import rollback  # noqa: E402


def test_settings_undo():
    ops = rollback.operations(json.dumps({"job_id": "12", "new_settings": {"schedule": {"x": 1}},
                                          "fields_to_remove": ["trigger"]}))
    assert ops == [{"call": "POST /api/2.1/jobs/update",
                    "body": {"job_id": 12, "new_settings": {"schedule": {"x": 1}}, "fields_to_remove": ["trigger"]}}]


def test_delete_jobs_and_paths():
    assert [o["body"]["job_id"] for o in rollback.operations('{"delete_jobs": [5, 6]}')] == [5, 6]
    ops = rollback.operations('{"delete_path": "/Shared/serverless_migration_demo/a_serverless"}')
    assert ops[0]["call"].endswith("workspace/delete")


def test_refuses_paths_outside_demo_folder_and_unknown_shapes():
    with pytest.raises(ValueError):
        rollback.operations('{"delete_path": "/Users/someone/important"}')
    with pytest.raises(ValueError):
        rollback.operations('{"something": 1}')
    assert rollback.operations(None) == []


def test_candidates_skip_dry_runs_and_already_undone():
    acts = [
        {"action_id": "a1", "action": "DRIFT_REVERTED", "dry_run": False, "rollback_json": "{}", "performed_at": "1"},
        {"action_id": "a2", "action": "PLANNED_PAUSE", "dry_run": True, "rollback_json": "{}", "performed_at": "2"},
        {"action_id": "a3", "action": "REWRITE_APPLIED", "dry_run": False, "rollback_json": "{}", "performed_at": "3"},
        {"action_id": "r1", "action": "ROLLED_BACK", "dry_run": False, "detail": "undo of a1 (DRIFT_REVERTED)",
         "rollback_json": None, "performed_at": "4"},
    ]
    assert [a["action_id"] for a in rollback.candidates(acts)] == ["a3"]
