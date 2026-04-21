"""Versioned DAG logic for customer_sales v0 (champion baseline)."""

DAG_VERSION = 0
LOGICAL_DAG_ID = "customer_sales"


def build_logic() -> dict:
    return {"version": DAG_VERSION, "steps": ["extract", "transform", "load"]}
