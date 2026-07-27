# tests/deploy

Deploy correctness: `Agent.Deploy()` against local and SSH targets, deploy-skip markers, sandbox materialization, install verification.

| File | Surface |
|---|---|
| `test_deploy_sandbox_step.py` | Deploy step contract (relocated from `tests/`). |
| `test_deploy_skip_marker.py` | Deploy-skip marker idempotence (relocated from `tests/`). |
| `test_sandbox_unit.py` | Sandbox materialization (relocated from `tests/e2e/agentic/`). |
| `test_ssh_localhost.py` | NEW — ssh-to-self deploy with project's `dev.sh -bd` image; idempotence + assertive redeploy (Bug E.4 pin) + 2-step plan trace match. |

Default marker: `slow + requires_apptainer`. `test_ssh_localhost.py` additionally `requires_ssh_localhost + requires_docker_dev_image`.

Reuse: `src/metasmith/agents.py:Agent.Deploy`, `_run_setup`; `src/metasmith/coms/terminals.py:LiveShell`; `src/metasmith/testing/docker_builder.py:get_docker_tag`.
