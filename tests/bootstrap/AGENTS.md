# tests/bootstrap

Bootstrap correctness: the code paths that prepare the workspace before any flow runs.

| File | Surface |
|---|---|
| `test_libraries.py` | TransformInstanceLibrary.Load, DataTypeLibrary.Load, `_metadata/index.yml` discovery. |
| `test_library_unpack.py` | DataInstanceLibrary unpack invariants. |
| `test_container_binds.py` | ExecWithContainer bind-mount construction (Docker + Apptainer dialects). |
| `test_container_sandbox.py` | SIF vs sandbox decision probe (Bug E.4), GetSandboxPath, MakeBuildSandboxCommand. |
| `test_container_extra_args.py` | `args=` pass-through to runtime. |

Default marker: `slow` — some tests pull real images and exercise the apptainer/docker binaries.

Reuse: `src/metasmith/coms/containers.py:Container.MakeSandboxDecisionProbe`.
