# Scoring Layer

Scoring is decoupled from DAG runtime code.

- `engine/`: score computation and promotion decisions.
- `policies/`: change-type-specific promotion rules.
- `profiles/`: weighted models by pipeline archetype.
- `thresholds/`: global and safety guardrails.
