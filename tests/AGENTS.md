# Test suite layout

The test tree is keyed by **axis of concern** — each top-level dir owns one kind of correctness, and the directory's `AGENTS.md` is the single source of truth for what belongs there.

| Dir | Owns | Marker (auto) |
|---|---|---|
| `unit/` | Contract pins, schema dataclasses, version chains, CLI surface, path/source parsing | `fast` |
| `flow/` | Branching, batching, group_by, lineage forks, mixed cacheability, telemetry, concurrency — given a plan, does the runtime emit/route data correctly? See `flow/AGENTS.md` for the case catalog. | `fast` |
| `cache/` | Lineage-addressed task cache: identity, hit/miss, promote, cross-task, cross-workflow, kill-switch | `fast` |
| `gui/` | The web GUI's own suite — API routes, the CLI entry, ssh config, the fork discriminator. The inner loop while working on the page (`dev.sh -tg`). | `fast + gui` |
| `bootstrap/` | TransformInstanceLibrary load, container image build, sandbox decision probe — code paths that prepare the workspace before flow | `fast` |
| `perf/` | Does it still hold up at scale — 10k-item libraries, 21k-instance plans. Minutes, not milliseconds. | `slow` |
| `deploy/` | `Agent.Deploy()` — local + ssh-to-self, deploy-skip markers, install verification | `slow` |
| `e2e/virtual/` | Plan→compile→virtual exec via `VirtualE2ERuntime` / `contract_runtime` (no Docker, no JVM) | `e2e_virtual` |
| `e2e/docker/` | Plan→stage→`nextflow run` against real Docker via `NxfTestRunner` | `e2e_docker + slow + requires_docker` |
| `e2e/agentic/` | opencode/claude-driven scenarios | `e2e_agentic + slow` |
| `e2e/agentic/_harness/` | No-model harness checks (ralph loop, CONTROL.json, drivers) | `fast` |
| `audit/` | Quadrant probe, lineage audit reports | `fast` |

Markers are applied by directory in `tests/conftest.py:pytest_collection_modifyitems`, and a file matching no row **fails collection** rather than quietly running in no gate — that is what makes directory-as-declaration a contract instead of a convention. Explicit `@pytest.mark.X` decorators are additive. Capability gates (`requires_ssh_localhost`, `requires_docker`, `requires_apptainer`, `requires_docker_dev_image`) skip cleanly when the capability is absent.

## How to add a test

1. **Pick the axis.** What is this test actually pinning?
   - Behavior of one function with no DAG → `unit/`
   - Data routing through a planned workflow → `flow/`
   - Cache key, store, or trace behavior → `cache/`
   - Library load / image conversion / sandbox probe → `bootstrap/`
   - A GUI route, or the page's inner loop → `gui/`
   - Wall-clock or scale ceiling at 10k+ items → `perf/`
   - `Agent.Deploy()` surface → `deploy/`
   - End-to-end plan → stage → exec → result → `e2e/<virtual|docker|agentic>/`

2. **Check the catalog.** For flow tests, find or add a row in `flow/AGENTS.md`. New flow bugs ship a catalog row *before* the fix lands.

3. **Reuse stimuli.** Mock transforms live in `src/metasmith/testing/mock_transforms.py`. Don't define ad-hoc transforms per test.

4. **Assert through telemetry.** Flow + cache tests assert via `DataInstanceLibrary.Load(attach_trace=True)` — `find_invocations`, `walk_ancestors`, `get_lineage_of`, `summary()`. Never grep `workflow.nf` text or work-dir filenames.

5. **No compat shims.** When moving or renaming, `git mv` + update consumers in the same commit. Don't leave `# moved to X` stub files.

## Dev loop

- `mamba run -n msm pytest -m fast` — default loop, finishes <60s
- `mamba run -n msm pytest -m "fast or e2e_virtual"` — CI smoke gate
- `mamba run -n msm pytest -m e2e_docker` — full local exec (needs Docker)
- `mamba run -n msm pytest -m e2e_agentic --agent <opencode|claude>` — opt-in, model-billable
- `./dev.sh -tg` — the GUI suite alone (`tests/gui/`), for the page's inner loop

Set `PYTHONPATH` to this worktree's `src/`; do not merely unset it. `metasmith` is not installed into `msm`, so the subprocess tests need it, and an ambient workspace value resolves the import to some other checkout.

## Tracking new flow correctness gaps

If you find a flow bug not in `flow/AGENTS.md`:
1. Add a row to the appropriate axis with case ID + invariant + DAG shape
2. Write the test under `tests/flow/test_<axis>.py` referencing the case ID in the docstring
3. If it pins a historical bug, add a `tests/flow/repro/repro_<ticket>.py` and link it from the trap-case table
