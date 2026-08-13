# tests/e2e/virtual

End-to-end via the virtual runtime — no Docker, no Nextflow JVM.

| File | Surface |
|---|---|
| `test_virtual_pipeline.py` | Full binning pipeline against `VirtualE2ERuntime`. |
| `cache_baseline.py` | Deterministic-output pinning across linear_3step / parallel_then_group / mixed_cacheability. |
| `cache_e2e.py` | Cold→hot cache run via virtual runtime. |
| `test_contract_*.py` | Plan→compile-only assertions via `contract_runtime` (faster than virtual runtime; no protocol exec). |

Default marker: `e2e_virtual` — runs by default in CI smoke, but separable from `fast`.

Reuse: `src/metasmith/testing/{virtual_runtime.py, contract_runtime.py, plan_oracle.py, mock_transforms.py}`.
