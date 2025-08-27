# super simple in-memory run registry
from typing import Dict, Any
from datetime import datetime

RUNS: Dict[str, Dict[str, Any]] = {}

def new_run(run_id: str, payload: dict):
    RUNS[run_id] = {
        "id": run_id,
        "status": "queued",  # queued | running | done | error
        "created_at": datetime.utcnow().isoformat(),
        "finished_at": None,
        "stats": {"total": 0, "ok": 0, "err": 0},
        "error": None,
        "output_path": None,
        "payload": payload,
    }

def update_run(run_id: str, **fields):
    RUNS[run_id].update(fields)

def get_run(run_id: str) -> dict | None:
    return RUNS.get(run_id)
