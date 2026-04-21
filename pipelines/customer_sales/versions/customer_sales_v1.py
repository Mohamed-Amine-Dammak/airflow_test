"""Versioned DAG logic for customer_sales v1 (candidate)."""

DAG_VERSION = 1
LOGICAL_DAG_ID = "customer_sales"


def build_logic() -> dict:
    return {
        "version": DAG_VERSION,
        "steps": ["extract", "validate", "transform", "load"],
    }
