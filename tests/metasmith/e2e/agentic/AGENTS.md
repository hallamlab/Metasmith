# tests/e2e/agentic

Agent-driven end-to-end (opencode / claude). Real model, real Nextflow, real containers.

| File | Surface |
|---|---|
| `test_agent_stories.py` | Parametrized over scenarios: my_first_agent, custom_transforms, story_author_type, story_browse_libraries, story_plan_and_inspect, story_run_direct. |
| `test_deploy.py` | Agent-driven deploy scenario. |
| `test_recover.py` | Folded `recover_lineage_mismatch` + `recover_unreachable_target` into one parametrized recovery test. |
| `test_stage_real_libraries.py` | Real-library staging scenario. |
| `_harness/test_harness_smoke.py` | No-model harness checks (ralph loop, CONTROL.json budget). Demoted to `fast`. |
| `_harness/test_drivers.py` | Event-stream parsing from canned logs. Demoted to `fast`. |

Default marker: `e2e_agentic + slow`. Excluded from default and CI smoke.

Harness tests under `_harness/` are demoted to `fast` since they don't spawn a real model.

Story tests that assert only on plan shape / DAG / channel wiring are routed through `contract_runtime` (S3) and re-tagged `e2e_virtual` — keeps the agentic suite small and high-signal.

As of S8, none of the current scenarios qualify for a `contract_runtime`
virtual variant: every scenario asserts on real agent output (an
ANSWER.txt written by the agent, or pipeline artifacts produced by a
real run), not on plan/DAG/channel wiring. A future scenario whose pass
criteria can be checked by just inspecting a compiled
`workflow.nf` + `WorkflowPlan.steps` is the candidate to seed
`tests/e2e/virtual/test_agent_scenarios_virtual.py`.
