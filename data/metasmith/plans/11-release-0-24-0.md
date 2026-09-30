# metasmith 0.24.0 release

## Context

The GUI tutorials work (round 2) is done and pushed on `feat/gui-tutorials`. The user asked for a
full release through the proper channels: integrate into the dev line, gate, cut `release`, build
every artifact, publish to quay and anaconda, open the upstream PR, and prove the published
artifacts as a consumer. `RELEASE_PROTOCOL.md` in the repo root is the SOP and is followed as
written.

## What you said

> compact and do a full release through proper channels

## Issues

- I1. The tutorials, the DAG layout port and the review fixes exist only on a feature branch.
- I2. `feat/dev` carries three lineage-report commits that no remote has.
- I3. The last published version is 0.23.0, which has none of this work.

## High-level goals

- G1. Ship the current dev line, tutorials included, as a new metasmith version.
- G2. Publish it only through the sanctioned route: fork, quay, anaconda and an upstream PR.
- G3. Prove that what the registries serve actually works.

## Acceptance criteria

- `feat/dev` contains `feat/gui-tutorials` and every library fix the product branches carry that
  `release` lacks. It is pushed to origin.
- The dependency-free tier is 0 failed on the release tip. The docker/nextflow lane has run, and
  every failure is triaged.
- `release` has a commit bumping `src/metasmith/version.txt` to 0.24.0, and an annotated `v0.24.0`
  tag. Both are pushed to origin.
- `-ud` and `-uc` succeed: quay serves `0.24.0-<hash>`, `0.24.0` and `latest`, and anaconda.org
  serves `metasmith=0.24.0`.
- An open PR from the fork's `release` into `hallamlab:release` is titled for 0.24.0.
- A consumer gate from a fresh conda env and a freshly pulled image passes: `msm --help`,
  `Backend("solve") == "rust"`, `clone_stdlib`, every template solved, and the library stamp
  agreeing between the two.
- One real workflow has run on the published artifacts.

## Tasks

- T1. Compact (the user asked).
- T2. Merge the tutorials branch and the product-branch library fixes into `feat/dev` (G1).
- T3. Gate `feat/dev` with the dependency-free suite and the docker lane (G1, G3).
- T4. Compact.
- T5. Merge `feat/dev` into `release`, bump to 0.24.0, and tag (G1, G2).
- T6. Build every artifact in protocol order (G1).
- T7. Compact.
- T8. Publish to quay and anaconda, push to origin, and open or retitle the upstream PR (G2).
- T9. Gate the published artifacts as a consumer (G3).
- T10. Run a real workflow on the published artifacts, with a control (G3).
- T11. Compact.
- T12. Debrief.

## Approach by task

**T2.** Work in the `engine/dev` worktree
(`/home/tony/agentic_workspace/projects/metasmith/engine/dev`, branch `feat/dev`). Merge
`feat/gui-tutorials` there. Then run the protocol's library-collection check,
`git log --oneline release..<branch> -- src/metasmith_libraries/`, for `fabfos/dev` and
`libraries/mono`. Read each answer as content: diff the paths and carry only the fixes `feat/dev`
lacks. Push `feat/dev` to origin.
Gotchas: that worktree belongs to another scope, so check `git status` first and stage by path. A
cherry-picked commit still lists forever. `release` usually looks like a deletion in a plain diff.

**T3.** Run the dependency-free tier from the protocol, with `PYTHONPATH` pinned to the dev
worktree's `src`, wrapped in `timeout`. Run it alone, never beside a build, and budget 30 min. Then
run the docker/nextflow lane, because the lineage commits touch the run lifecycle and the DAG port
touches plan geometry.
Gotchas: the perf axis asserts wall clock and fails under concurrent load. Never pipe a long run
through `tail`; write it to a log. Don't wait on background bash; use a Monitor or a task
notification.

**T4.** Compact at the seam between gating and cutting. Carry the gate results and the `feat/dev`
tip SHA.

**T5.** Work in the `engine/release` worktree (branch `release`). Merge `feat/dev`. Bump
`src/metasmith/version.txt` to 0.24.0 and commit only that file. Add an annotated `v0.24.0` tag.
The dev line stays a version behind, per protocol.
Gotchas: check that the worktree is clean first. Do not push the tag until the builds succeed, so
a failed build can move it.

**T6.** Use `unset PYTHONPATH`. Build in order: `-brc`, `-br`, `-be`, `--build-gui` (in the
`msm_node` env), `--vendor-library`, then `-bp`, then `-bd`, `-bs` and `-bc`.
Gotchas:
- Stop if `-br` reports a compile error. The darwin stubs then pass until `-bs`/`-ud`.
- Do not rebuild the GUI between `-bp` and `-bd`, because the hash covers it.
- Always re-stage the vendored library.
- The engine directory is never version-controlled.

**T7.** Compact after the builds. Carry the build hash, the artifact paths and the image tag.

**T8.** Run `-ud`, then `-uc`. Check the token with the env's `bin/anaconda org whoami`. The token
is per-host; the known copy lives on capella. Push `release`, `feat/dev` and `v0.24.0` to origin.
Check `gh pr list --repo hallamlab/Metasmith --state open`. If a PR from `release` is open, retitle
it for 0.24.0 and say which earlier version it also carries. Otherwise open a new PR.
Gotchas:
- A closed PR is never reused.
- `mamba run` swallows stdin, so call the env's anaconda binary directly.
- `-ud` moves the `latest` tag.

**T9.** Work outside every worktree, with `env -u PYTHONPATH`.
- Conda lane: `mamba create -n msm_gate … metasmith=0.24.0`.
- Docker lane: `docker rmi` the local tags first, then pull.
- In each lane, run `msm --help`, the rust backend check, `clone_stdlib` into an empty dir, and a
  solve of every shipped template. Compare the library stamps.
Gotchas: without the `rmi`, the local build is what gets tested.

**T10.** Run the local pangenome template (`pangenome_heatmap_from_assembly`, three E. coli
accessions) on a local docker agent built from the published package. Pair it with a control:
re-run it unchanged, which should hit the cache. That is also the run chapter of the tutorial,
which has never been driven end to end.
Gotchas: Nextflow green can hide dead steps, so read `nxf_tasks.csv`. Budget hours.

**T11.** Compact before the debrief. Carry the published versions, the PR URL and the run
outcome.

**T12.** Debrief: journal to metasmith/engine/gui, and to engine/release if appropriate. Report the
release links.

## Callouts

- The version is 0.24.0 rather than 0.23.1, because tutorials and a new DAG layout are features.
- The three unpushed lineage commits on `feat/dev` are another scope's work. They ship because
  `feat/dev` is the dev line, and releasing it is the proper channel.

## Autopilot

**Guardrails:**
- Never push to main or master, never force-push, and never delete a published tag.
- Stage by path.
- Run git and dvc unsandboxed. mamba fails in the sandbox, so call the env's python directly or
  run unsandboxed.
- Never run the gate beside a build.
- The registries (quay, anaconda) and the upstream PR are outward-facing, and the user authorised
  them explicitly in "do a full release through proper channels".

**Resume handles:**
- GUI server: task bofimdgck was replaced by b411gurd2, on 127.0.0.1:8095.
- awm lease: task b8zq7jeso on `/msm-tutorial`.
- Tutorials worktree: `engine/gui/.claude/worktrees/tutorials` at 760f2351.

### Live state

T2 and T3 are done. `feat/dev` is e838b0ed on origin, the same commit as `feat/gui-tutorials`.

The gate results:
- The dependency-free gate on 760f2351 had 1 failure, a perf test measurement fixed by d8eecc1f.
- The docker lane on d8eecc1f had 68 failures: 66 unmigrated fixtures, fixed by e838b0ed, and 2
  load-timing cases.
- The failed files re-ran with 82 passed.
- Do not re-run either suite; see the run log.

T5, T6 and T8 are done. 0.24.0 is published: quay `0.24.0-95e78da`, `0.24.0` and `latest`
all at sha256:ae96c13f…d3aa, and anaconda `metasmith 0.24.0`. `release` 695e7938 and tag
`v0.24.0` are on origin. PR #68 (https://github.com/hallamlab/Metasmith/pull/68) is retitled
"Release 0.24.0".

T9 and T10 are done and pass (run log). Consumer gate: both lanes 12/12, stamp
`0.24.0+9182483`. Real run: pangenome ZL0nMwjG cold 838 s all COMPLETED, re-run all cached.
Open finding for the next release: the shipped `my_first_agent` notebook and rst are broken
three ways (run log, T10 finding); the GUI tutorial path is not.

T12 is done: journals posted to metasmith/engine/gui and metasmith/engine/release, and this
plan copied into the gui scope's data. The run is finished. Background still running on
purpose: GUI server b411gurd2 on :8095, awm lease b8zq7jeso at /msm-tutorial.

The release covers tutorials, the DAG layout port, the lineage report, the cami-run library
additions (CarveMe, DAS Tool, MEMOTE, metabolicModelling) and the test fixes.

### Run log

- 2026-09-29: plan written. `feat/dev` fbb95db0 is 3 ahead of `origin/feat/dev`. `release`
  9c25206d is contained in `feat/dev`. `feat/gui-tutorials` 760f2351 is 26 ahead of `feat/dev` and
  0 behind.
- 2026-09-29 T2: library collection. `libraries/mono` has nothing `release` lacks. The only
  `fabfos/dev` library commits `feat/dev` lacks are the ECSPr arm, 632878fc..038bb281
  (calibration, the conditions table, community). They also change `src/ecspr`, `src/fabfos` and
  `research/`, so they are that product's ongoing work, not library fixes, and they are not
  carried. Decision taken without the principal. The ProteinBERT and parquet fixes are already in
  `feat/dev`, with no diff.
- 2026-09-29 T2: the fast-forward also carries `feat/engine/cami-run`, because `feat/gui` merged
  it at af1fe168. The consequences:
  - The library gains CarveMe, DAS Tool, MEMOTE and the metabolicModelling transforms.
  - `spades.py`, `slurm.nf` and `plan.py` change.
  - The release is broader than tutorials plus the DAG port.
  Unwinding it is impractical, and the gate plus the template solves cover it. Noted here for the
  report.
- 2026-09-29: the user paused the release to give GUI feedback. Commit e776e058 on
  `feat/gui-tutorials`, pushed, covers three changes:
  - The step lists are plain lists with chapter headings, in the navigator and in the Tutorials
    tab.
  - The per-chapter restart buttons are removed.
  - The waiting callout avoids the navigator only when the two overlap on both axes.
  The change is Svelte only, so the gate running on 760f2351 still covers every Python line.
  Before T5, fast-forward `feat/dev` to e776e058 again, then rebuild the GUI in the release
  worktree at T6.
- 2026-09-29 T3: the dependency-free gate on 760f2351 gave 1 failed, 3598 passed, 42 skipped and
  5 xfailed, in 18 min. The failure was `perf/test_dag_layout_cost`, at 1.005 s against 1.0 s.
  - Cause: `process_time` counts numpy's BLAS workers spinning while idle. The layout itself takes
    0.84–0.87 s of calling-thread CPU and the same in wall time, while `process_time` read
    1.36–1.65 s.
  - Fix: d8eecc1f measures with `thread_time`. It passed 3 of 3 runs.
  - The gate is therefore 0 real failures.
  - Still to run: the docker lane. The release remains paused on the user's word.
- 2026-09-29 T3: the user said to proceed, so `feat/dev` was fast-forwarded to d8eecc1f and
  pushed.
  - The docker lane, 204 selected, gave 131 passed, 35 failed and 33 errors in 40 min.
  - 66 of the 68 are `GivenNotImportedError`. The docker fixtures were never migrated to the
    0.23.0 refusal; 0f0f32cf did only the virtual ones.
  - Fixed with the same shape, load back after Save: conftest `mock_samples`,
    multicontainer_groupby, e2e_trace ×2, workflow_execution, full_pipeline.
  - The other 2 are timing: group_buffering hit a `docker run` timeout, and C25 took 3168 ms
    against 2000 ms. Both are re-running alone.
  - Log: `tmp/docker_rerun.log`, waiter bmu0ayyvu.
- 2026-09-29: the user said "try to not run the same 30min test repeatidly".
  - The gate on 760f2351 and the lane on d8eecc1f stand for this release.
  - From here, re-run only failed ids.
  - After the bump at T5, run only the three version→tag unit tests.
- 2026-09-29 T5: `release` fast-forwarded 9c25206d..e838b0ed (as prior releases did). The bump
  is commit 695e7938, holding only version.txt.
  - The three version→tag files gave 11 passed.
  - The post-merge DVC hook failed on `data/fabfos/runs/aska/gpr`. That pin was already in
    0.23.0, and its cache objects are missing on altair. It is research data, not in any
    artifact, so it was left alone.
  - Tag deferred to after T6: v0.23.0's annotated message lists the image digest and
    package, so the tag follows the builds.
- 2026-09-29 T6: builds launched as `tmp/build.sh`, logging to `tmp/build.log`, under Monitor
  bexa658w3.
- 2026-09-29 T6: every build step exited 0, from 14:45 to 15:17.
  - All four `-br` targets compiled, with no stubs.
  - The GUI bundle `index-CKst4bNF.js` contains the new list.
  - Build hash 95e78da, vendored library 6b72441.
  - dist holds `metasmith-0.24.0+95e78da` as a wheel and an sdist.
  - The image is `quay.io/hallamlab/metasmith:0.24.0-95e78da` (id ae96c13f016e).
  - The sif is `release/metasmith.sif`.
  - The conda package is `conda_build/noarch/metasmith-0.24.0-py_0.tar.bz2`.
  - The worktree stayed clean.
- 2026-09-29: the user reports that `msm gui` prints many escape-character warnings.
  - Not reproduced. `compile()` of every .py under src/ finds 0 invalid escapes, and neither the
    8095 server log nor a fresh server with an empty pycache shows any.
  - Asked the user for a sample line.
- 2026-09-29 T8: `tmp/publish.sh` ran `-ud` then `-uc`, 15:19 to 15:27, EXIT 0.
  - `-ud` found all four relay binaries and the solver engine in the image. It pushed
    `0.24.0-95e78da`, `latest` and `0.24.0`, all sha256:ae96c13f016e0bd40095d800ef1201c1fad972cd34feff4534275c55f9d8d3aa.
  - `-uc` clean-room install reported backend=rust, then uploaded
    `hallamlab/metasmith/0.24.0/noarch/metasmith-0.24.0-py_0.tar.bz2`.
  - Annotated `v0.24.0` on 695e7938, message in `tmp/tag_msg.txt`.
  - Pushed `release` 9c25206d..695e7938 (fast-forward) and the tag. `feat/dev` was already
    at e838b0ed on origin.
  - PR #68 was open at "Release 0.23.0" and had moved onto 695e7938. Retitled "Release 0.24.0"
    with a body that says it still carries 0.22.1 and 0.23.0 (`tmp/pr_body_final.md`).
- 2026-09-29 T9: `tmp/gate.sh`, 15:51 to 15:54, EXIT 0. Conda lane: new env `msm_gate_0240`
  from anaconda.org (earlier gate envs left alone). Docker lane: `rmi` of all three tags deleted
  ae96c13f, and the pull came back with the same digest.
  - Both lanes: full version 0.24.0+95e78da, backend rust, 12/12 templates, nothing dropped.
  - Library stamp `0.24.0+9182483` in both; per-template step counts identical.
  - binning_and_gpr_from_assembly solves in 22 steps (0.23.0: 26), from the binning library
    changes this release carries.
- 2026-09-29 T10: driving the shipped tutorial notebook `my_first_agent.ipynb` verbatim on
  `msm_gate_0240` (`tmp/r10_pangenome.py`, log `tmp/r10.log`). The notebook loads
  `resources/containers`, which the standard library no longer has (only `env` and `lib`);
  the driver records that and falls back to `env`. Cold run, then an unchanged re-run as the
  control.
- 2026-09-29 T10 finding: the shipped tutorial notebook `example_resources/tutorials/my_first_agent.ipynb`
  (and `docs/.../tutorials/my_first_agent.rst`) cannot run on 0.24.0, nor on 0.23.0. Three independent
  breaks, each checked on `msm_gate_0240`:
  1. `resources/containers` does not exist in the standard library (only `env`, `lib`).
  2. In-process `AddValue` givens are refused by GivenNotImportedError (0.23.0's rule).
  3. Its shape (accession directly under pangenome) does not solve even when loaded from disk:
     heatmap dropped, 0 steps. The template's shape (pangenome -> genome_name -> accession)
     through `PoolGivens` solves: 3 samples, getNcbiAssembly -> ppanggolin -> heatmap
     (`tmp/r10_probe2.py`).
  The GUI path is not affected: it plans from a library path loaded from disk, and its tutorial
  sheet binds genome_name. Not a release blocker; a docs/notebook fix for the next release.
  R10 re-launched with the pool + template shape.
- 2026-09-29 T10 result: `tmp/r10.sh` 15:59 to 16:14, EXIT 0, on `msm_gate_0240` with a local
  DOCKER agent at `tmp/r10_ws/msm_home` running the pulled `0.24.0-95e78da` image. Task ZL0nMwjG.
  - Cold: 5 tasks (getNcbiAssembly x3, ppanggolin, heatmap), all COMPLETED exit 0, 838 s.
    Heatmap `results/pangenome-heatmap/*.svg` (18 KB) names DH10b, K12 and EPI300.
  - Control, unchanged re-run: all 5 as `*_cached`, ~0.2 s each, 30 s total.
  - lineage.csv written for both runs (15 rows each); no post-run step failed.
  - Cosmetic: the straddle-mounts warning says "metasmith refuses to compile" while it forces
    'copy' and proceeds. Message text is wrong, behaviour is fine.
- 2026-09-29 T12: debrief. `feat/gui-tutorials` is clean at e838b0ed (only the untracked
  frontend `node_modules`), so there is nothing to commit. Journals posted to engine/gui and
  engine/release. The escape-character warning report stays open: not reproduced, awaiting a
  sample line from the user.
