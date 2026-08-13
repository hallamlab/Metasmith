# tests/e2e

End-to-end harness tests organized by execution model:

- `virtual/` — `VirtualE2ERuntime` + `contract_runtime` (no Docker, no Nextflow JVM). Marker `e2e_virtual`.
- `docker/` — real Nextflow + Docker via `NxfTestRunner`. Marker `e2e_docker + slow`.
- `agentic/` — opencode/claude-driven scenarios. Marker `e2e_agentic + slow`.

Coverage rule: when an e2e test asserts only on plan shape, channel wiring, or DAG correctness (not on protocol output content), prefer `contract_runtime` over `virtual_runtime`, and prefer either over `docker`. Real execution paths exist to verify the engine, not the planner.
