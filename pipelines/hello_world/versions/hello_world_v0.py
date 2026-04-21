"""hello_world v0 baseline."""

from __future__ import annotations
import time

DAG_VERSION = 0
LOGICAL_DAG_ID = "hello_world"


def build_logic() -> dict:
    time.sleep(8)
    return {"version": DAG_VERSION, "status": "ok", "message": "hello from v0"}
