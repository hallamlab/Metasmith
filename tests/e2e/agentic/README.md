# e2e_agentic — LLM-driven verification

Pre-release check that the **currently-built** metasmith conda package and
docker/apptainer image can be driven end-to-end by an LLM coding agent
reading the public docs.

Pairs with `tests/test_tutorial_replay.py` (programmatic, mock containers,
runs in CI). This suite is the real-container, model-in-the-loop sibling.

## How the spoof works

Each test runs in an ephemeral sandbox dir; nothing persists across tests.
Two public-fetch points are intercepted so the agent's commands resolve
to locally-built artifacts:

1. **Conda channel `hallamlab`** — the sandbox's `~/.condarc`
   (`custom_channels.hallamlab: file://<sandbox>/local-channels`) redirects
   any `-c hallamlab` to a local file:// channel hardlinked from
   `conda_build/`. The verbatim docs command
   `mamba create -n msm_env -c hallamlab -c bioconda metasmith` runs
   offline. Transitive deps (nextflow, pyyaml, …) still come from real
   conda-forge/bioconda over the network.
2. **Container registry `quay.io/hallamlab/metasmith:<VER>`** — `dev.sh
   -bd` tags the host docker daemon, so `docker run` finds the image
   locally. For apptainer, the harness pre-places `metasmith.sif` at the
   exact path `Agent.Deploy` computes inside
   `<sandbox>/agent_home/container_images/`, so the `[ -e ... ] || pull`
   gate skips pulling.

The sandbox also redirects `envs_dirs` and `pkgs_dirs` into itself, so
`mamba` writes everything inside the sandbox tree. A single `rm -rf`
teardown is exhaustive.

Bootstrap tooling (mamba + apptainer) lives in a host-shared env at
`~/.cache/msm-e2e/bootstrap-env/`. First test creates it (~30 s);
subsequent tests reuse it.

## Layout

```
tests/e2e/agentic/
├── harness/
│   ├── sandbox.py           per-test sandbox builder + env_for_agent
│   ├── bootstrap_env.py     host-shared mamba+apptainer env
│   ├── container_spoof.py   sif-path computation + pre-placement
│   ├── loop.py              Ralph outer loop
│   ├── control.py           CONTROL.json contract
│   └── budget.py            token budget
├── drivers/                 opencode + claude argv/stream-json parsing
├── install_mock/
│   ├── build_local_artifacts.sh    wheel/conda-pkg/docker/sif (host)
│   ├── verify_local_artifacts.py   preflight (no shared env)
│   └── condarc_template.yaml       .condarc with <SANDBOX> placeholders
├── scenarios/
│   ├── base.py              Scenario protocol + standard_verify
│   ├── _fixture_utils.py    helpers to pre-stage type/transform/data libs
│   ├── harness_smoke.py     metasmith pre-installed; agent runs msm --help
│   ├── install.py           agent runs verbatim docs install command
│   ├── deploy.py            agent runs save + deploy against local container
│   ├── my_first_agent.py    pangenome heatmap tutorial
│   ├── custom_transforms.py fastani transform tutorial
│   ├── story_browse_libraries.py   user story: list/show type+transform libs
│   ├── story_author_type.py        user story: author a new data type
│   ├── story_plan_and_inspect.py   user story: plan, then inspect task subtree
│   ├── story_run_direct.py         user story: metasmith run (no Nextflow)
│   ├── recover_unreachable_target.py  plan failure → data add-value recovery
│   └── recover_lineage_mismatch.py    plan failure → data set-parents recovery
├── prompts/ralph_system.md  fixed template (Geoff Huntley, Feb 2024)
├── test_sandbox_unit.py        sandbox materialization tests (no marker)
├── test_harness_smoke.py       loop/control/budget unit tests (no marker)
├── test_drivers.py             driver parser unit tests (no marker)
├── test_harness_smoke_live.py  live: msm --help (marker)
├── test_install.py             live: install from docs (marker)
├── test_deploy.py              live: agent save + deploy (marker, DOCKER+APPTAINER)
├── test_my_first_agent.py      live tutorial (marker, DOCKER+APPTAINER)
├── test_custom_transforms.py   live tutorial (marker, DOCKER+APPTAINER)
├── test_story_*.py             live user-story smoke (marker, DOCKER)
└── test_recover_*.py           live plan-failure recovery (marker, DOCKER)
```

## Prerequisites

1. **Local build artifacts** — built once, refreshed when `version.txt`
   bumps:

   ```bash
   tests/e2e/agentic/install_mock/build_local_artifacts.sh             # docker only
   tests/e2e/agentic/install_mock/build_local_artifacts.sh --apptainer # adds metasmith.sif
   tests/e2e/agentic/install_mock/build_local_artifacts.sh --force     # rebuild
   ```

   Produces (idempotent):
   - `dist/metasmith-<VER>-py3-none-any.whl`
   - `conda_build/...metasmith-<VER>...tar.bz2` (a valid local conda channel)
   - docker image `quay.io/hallamlab/metasmith:<VER>` on the host daemon
   - `metasmith.sif` at the project root (only with `--apptainer`)

   There is no longer a shared `msm_env_test` conda env. Per-test sandboxes
   create their own envs from the local channel and tear them down with
   the sandbox.

2. **Agent CLI** — one of:

   ```bash
   # opencode (primary; works with OpenRouter or opencode Zen credentials)
   curl -fsSL https://opencode.ai/install | bash
   opencode auth login              # or export OPENCODE_API_KEY / OPENROUTER_API_KEY

   # claude (secondary; useful for cross-validation)
   #   already installed if you're reading this from a Claude Code session
   ```

## Running

```bash
# (1) unit tests — no API keys, no containers
pytest tests/e2e/agentic/ -m "not e2e_agentic"

# (2) harness smoke (~90 s, ~10K tokens) — confirms wiring
pytest tests/e2e/agentic/test_harness_smoke_live.py -m e2e_agentic

# (3) install test (~2 min)
pytest tests/e2e/agentic/test_install.py -m e2e_agentic

# (4) deploy test, both runtimes (~3 + 4 min)
pytest tests/e2e/agentic/test_deploy.py -m e2e_agentic

# (5) full tutorial runs
pytest tests/e2e/agentic/test_my_first_agent.py tests/e2e/agentic/test_custom_transforms.py -m e2e_agentic -s

# subset by runtime
pytest tests/e2e/agentic/ -m e2e_agentic -k DOCKER
pytest tests/e2e/agentic/ -m e2e_agentic -k APPTAINER

# render prompts only (no model call)
pytest tests/e2e/agentic/ -m e2e_agentic --dry-run
```

### Options (`pytest --help` → "e2e_agentic")

| Flag | Default | Notes |
|---|---|---|
| `--agent` | `opencode` | `opencode` or `claude` |
| `--agent-model` | per-driver | e.g. `openrouter/deepseek/deepseek-v4-flash`, `haiku` |
| `--agent-effort` | none | `claude` only |
| `--max-iters` | 20 | Ralph outer-loop hard stop |
| `--max-tokens` | 2_000_000 | cumulative across all iterations |
| `--max-tokens-per-iter` | 200_000 | passed to driver where supported |
| `--iter-timeout-s` | 900 | per-iteration wall-clock cap (opencode) |
| `--runs-dir` | `tests/e2e/agentic/.runs/<ts>` | transcript output root |
| `--dry-run` | off | render the prompt to disk and skip |

## The Ralph loop, briefly

Each iteration the harness re-invokes the agent with the **same** prompt
template; the agent reads `sandbox/PROGRESS.md` to remember where it left
off and writes its terminal verdict to `sandbox/CONTROL.json` via:

```bash
metasmith e2e checkpoint done       --key <task_key>
metasmith e2e checkpoint give_up    --reason "..."
metasmith e2e checkpoint continue   --notes  "..."  # implicit if file is absent
metasmith e2e report_issue          --reason "<one line describing what you saw>"
```

`report_issue` is the **fail-fast** action: scenario prompts list verbatim docs
commands and instruct the agent to call `report_issue` on the first sign of any
unexpected error or output. The loop terminates immediately with outcome
`REPORTED_ISSUE` and the pytest failure message reads
`agent reported issue: <reason>`. This is how the suite surfaces real bugs —
the agent does not debug; it runs the docs and tattles.

Loop stops on `done`/`give_up`/`report_issue`/`max_iters`/`max_tokens` (whichever
fires first). See `harness/loop.py`. A wedged iteration (e.g. opencode hung on
a provider-side stall) terminates at `--iter-timeout-s` so it becomes an
iteration boundary instead of a permanent hang.

## Test taxonomy

The suite is structured as a progression of increasingly demanding tests:

| Test                          | Pre-install? | What it proves                          |
|-------------------------------|---|----------------------------------------------|
| `test_harness_smoke_live`     | yes | sandbox + driver + checkpoint wiring works |
| `test_install`                | no  | conda-channel spoof works; agent installs from docs |
| `test_deploy[DOCKER]`         | yes | host docker tag is served; no quay.io pull |
| `test_deploy[APPTAINER]`      | yes | pre-placed sif is served; no quay.io pull  |
| `test_my_first_agent[*]`      | yes | full pangenome tutorial end-to-end         |
| `test_custom_transforms[*]`   | yes | fastani transform tutorial end-to-end      |
| `test_story_*[DOCKER]`        | yes | user-story smoke: browse / author type / plan+inspect / direct run |
| `test_recover_*[DOCKER]`      | yes | plan-failure recovery via hints (unreachable target, lineage mismatch) |

Pre-install means the harness creates `<sandbox>/envs/msm_env` before the
agent runs; the agent then activates it. For `test_install` the agent does
the `mamba create` itself, exercising the channel spoof.

Scenarios that pre-stage workspace fixtures (type / transform / data libs
built via metasmith's own CLI) implement an optional `setup_fixtures(layout,
ctx)` hook on the `Scenario` protocol — run by the harness after the
pre-install step, before the agent starts. See
`scenarios/_fixture_utils.py` for shared helpers and any
`scenarios/story_*.py` / `scenarios/recover_*.py` for examples.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `preflight failed: docker image ... not found` | `tests/e2e/agentic/install_mock/build_local_artifacts.sh` |
| `preflight failed: no metasmith-...tar.bz2 in conda channel` | same |
| `preflight failed: apptainer runtime requested but metasmith.sif is missing` | `... build_local_artifacts.sh --apptainer` |
| `preflight failed: opencode has no credentials configured` | `opencode auth login` (or set `OPENCODE_API_KEY` / `OPENROUTER_API_KEY`) |
| Live test exits with `over_budget` after few iters | raise `--max-tokens`, or inspect `.runs/.../iter-*/stream.jsonl` |
| Live test stuck — same `PROGRESS.md` every iter | agent is wedged; `--iter-timeout-s` will eventually unstick it |
| `external_directory` permission spam in opencode log | sandbox tree wasn't materialized correctly; check `test_sandbox_unit.py` first |

Transcripts land under `tests/e2e/agentic/.runs/<timestamp>/<scenario>/<runtime>/iter-NNN/stream.jsonl`.
