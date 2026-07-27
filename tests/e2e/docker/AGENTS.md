# tests/e2e/docker

End-to-end against real Nextflow + real Docker. Slow; opt-in for full local runs.

| File | Surface |
|---|---|
| `test_e2e_full_pipeline.py` | Agent.Deploy → generate → stage → run → verify. |
| `test_e2e_workflow_execution.py` | Full pipeline + trace + telemetry. |
| `test_e2e_trace.py` | Trace.jsonl integrity under real Nextflow. |
| `test_e2e_transform_isolation.py` | Container isolation per transform. |
| `test_orchestrator_exec.py` | The Orchestrator combinator cases that genuinely need real channels (split from old `test_e2e_orchestrator.py`). |
| `test_publish_intermediates.py` | Intermediate artifact publishing. |
| `test_direct_run.py` | Direct workflow execution without agent. |

Default marker: `e2e_docker + slow + requires_docker`. Not in CI smoke.

Reuse: `NxfTestRunner` (in `tests/integration/conftest.py` until promoted to `src/metasmith/testing/`).
