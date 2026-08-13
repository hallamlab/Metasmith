# tests/deploy

Deploy correctness: `Agent.Deploy()` against local and SSH targets, deploy-skip markers, sandbox materialization, install verification.

| File | Surface |
|---|---|
| `test_deploy_sandbox_step.py` | Deploy step contract. |
| `test_deploy_skip_marker.py` | Deploy-skip marker idempotence. |
| `test_sandbox_unit.py` | Sandbox materialization. |
| `test_ssh_localhost.py` | ssh-to-self deploy with the project's `dev/metasmith.sh -bd` image; idempotence + assertive redeploy (Bug E.4 pin) + 2-step plan trace match. |

Default marker: `slow + requires_apptainer`. `test_ssh_localhost.py` additionally `requires_ssh_localhost + requires_docker_dev_image`.

Reuse: `src/metasmith/agents/agent.py:Agent.Deploy`, `_run_setup`; `src/metasmith/coms/terminals.py:LiveShell`; `src/metasmith/testing/docker_builder.py:get_docker_tag`.

The two source-pattern tests here read `Agent.Deploy`'s text rather than running it, so they must locate it by globbing `agents/` — `agents` is a package and its `__init__` is re-exports only. Both slice "up to the next method"; `Deploy` is the last method in its module, so that slice needs an end-of-file fallback or it comes out empty, and every assertion in `test_deploy_skip_marker.py` pins an *absence* and would pass on it.
