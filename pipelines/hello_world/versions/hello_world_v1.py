"""hello_world v1 candidate."""

from __future__ import annotations
import time

DAG_VERSION = 1
LOGICAL_DAG_ID = "hello_world"


def build_logic() -> dict:
    time.sleep(2)
    return {"version": DAG_VERSION, "status": "ok", "message": "hello from v1"}
