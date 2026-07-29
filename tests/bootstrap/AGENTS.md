# tests/bootstrap

Bootstrap correctness: the code paths that prepare the workspace before any flow runs.

| File | Surface |
|---|---|
| `test_libraries.py` | TransformInstanceLibrary.Load, DataTypeLibrary.Load, `_metadata/index.yml` discovery. |
| `test_library_unpack.py` | DataInstanceLibrary unpack invariants. |
| `test_container_binds.py` | ExecWithContainer bind-mount construction (Docker + Apptainer dialects). |
| `test_container_sandbox.py` | SIF vs sandbox decision probe (Bug E.4), GetSandboxPath, MakeBuildSandboxCommand. |
| `test_container_extra_args.py` | `args=` pass-through to runtime. |

Default marker: `fast`. It was `slow` while the 10k-scale `DataInstanceLibraryPerformance` class
lived in `test_libraries.py`; six tests of 20-50s kept ~100 sub-millisecond ones out of the daily
loop. The scale tests are `tests/perf/test_library_scale.py` now. Nothing here pulls a real image —
every container test asserts the *emitted command string*, which is the point of the axis.

Reuse: `src/metasmith/env/environment.py:Environment.MakeSandboxDecisionProbe`.
