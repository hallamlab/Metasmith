# Release Protocol

How to cut a metasmith release: verify it works, build the artifacts, and
publish them. Keep this followable end-to-end without re-deriving the steps.

All commands run from the repo root in the `msm` conda env:

```
mamba run -n msm ./dev/metasmith.sh <flag>
```

---

## 1. Verify it works

A release ships only after the test suite is green and the end-to-end paths
have been exercised. Focus the verification on what a release actually changes.

### Run the suite

```
mamba run -n msm ./dev/metasmith.sh -tr
```

`-tr` is the release set and the gate: it must report **0 failed** before
anything is built. It builds the test image from the checkout, then runs perf
and the lifecycle suite alone, then the docker lane beside the rest of the
tree. `-ts` (smoke) and `-tm <group>` (merge check) are the development loops
over the same tree. The groups and the smoke set live in
`tests/metasmith/_groups.py`.

**Budget 30 minutes.** Measured 2026-11-22 on altair, 16 cores: 13:35 with the
image built. A cold image build adds about 17 minutes, mostly the conda solve.
The source hash is part of the image tag, so any change under `src/metasmith/`
forces that rebuild.

Run nothing heavy beside it. The docker lane runs 8 workers, each with one
long-lived container, and container creation on a busy daemon takes 11-17 s.

`-tr` also probes every URL the download contracts declare (`network` marker, about 30 s). A dead link fails
the gate; fix it in the transform's `URLS`, or re-run if the host is down for the day.

Three opt-in tiers stay outside `-tr` because they need infrastructure a
release host may lack. Run the ones a release touches:

- **`remote_host`** — remote-shell paths. Needs `LIVESHELL_REMOTE_HOST`.
- **`requires_apptainer`, `requires_ssh_localhost`, `requires_docker_dev_image`**
  — the host features their names state.
- **`e2e_agentic`** — live agent-driven scenarios. Needs an API key and a
  compiled standard library (`dev/libraries.sh -b`).

### Collect the shipped library before you gate it

The engine ships `src/metasmith_libraries/`, but the release branch merges only
the engine's own line. A library fix authored on a product branch reaches no
release until somebody carries it, and nothing warns.

Before gating, ask each product branch what it has that `release` does not:

```
git log --oneline release..<branch> -- src/metasmith_libraries/
```

Run it for `fabfos/dev` and `libraries/mono` at least, and read the answer as
content rather than as commits — cherry-picks land under new SHAs, so a picked
commit still lists here forever. Diff the paths to confirm.

**CAUTION** Check the direction before merging anything. `release` is usually
*ahead* on the same files, so a plain `git diff release <branch>` shows the
engine's own work as a deletion. The commit list above is the reliable question.

0.22.0 nearly shipped two library bugs this way: a regex anchored on a trailing
number that made every ProteinBERT run exit at the embedder, and an env pinned
to an image with no pyarrow without the change that stops parquet going through
it.

### Focus areas

- **Scope the depth to the change.** When `src/metasmith/` is unchanged from the
  last release, the version→tag chain tests (`test_container_tag`,
  `test_dev_sh_tag`, `test_docker_build_context`, all under `tests/metasmith/unit/`)
  are sufficient. When code changed, run the full suite.
- **The build hash covers the GUI bundle.** `src/metasmith/gui/static/` is
  inside the tree `_build_hash` walks, so the version depends on the frontend
  build as well as the commit. Build the bundle once, before `-bp`, and do not
  rebuild it between `-bp` and `-bd`. Output is byte-reproducible as long as
  `package-lock.json` is honoured.
- **Deploy across runtimes.** The container path (docker/apptainer) and the
  relay-free path (mamba/native) are distinct execution modes — confirm the ones
  the release affects actually run a real tool end-to-end.
- **Apptainer is host-specific.** SIF-vs-sandbox behavior, userns permissions,
  and module availability differ per HPC host; validate on a real target host
  (`research/metasmith/local_mock/smoke_hpc_deploy.py`) rather than assuming the dev box
  generalizes.

---

## 2. Bump the version

The version lives in one file: `src/metasmith/version.txt`, a bare PEP 440
release segment (e.g. `0.19.0`) — no `+` or `-`. Everything downstream
(`constants.VERSION`/`FULL_VERSION`/`CONTAINER_TAG`, `setup.py`, the `dev/metasmith.sh`
docker tag, the default agent container) derives from it. `build_hash.txt` is a
short hash over the source tree, stamped automatically at build time.

> **`v0.19.0` on `origin` is not a release.** The tag points at `110a5bf` on the
> abandoned first reentrancy line — a version that was tagged and then never
> shipped. It is left in place rather than deleted, because deleting a published
> tag is worse than an inaccurate one, but do not treat it as a predecessor:
> 0.18.8 is the last thing that actually went out before 0.20.0.

The standard flow merges `smoke → dev → release` and bumps on the **release**
branch only (dev stays a version behind). Stage just the version file so the
bump commit carries nothing else:

```
git add src/metasmith/version.txt
git commit -m "Bump version to 0.20.0"
```

---

## 3. Build the artifacts

Build in this order. Clear `PYTHONPATH` first so the wheel can't pick up a stale
metasmith from the shell environment.

```
unset PYTHONPATH

./dev/metasmith.sh -brc        # one-time: fetch the rust cross-compile container
./dev/metasmith.sh -br         # build the relay binaries (all four arch/os targets)
./dev/metasmith.sh -be         # build the solver engine (same four targets) + stage it
./dev/metasmith.sh --build-gui # build the frontend bundle (needs node; see below)
./dev/metasmith.sh --vendor-library # stage the standard library into the package
./dev/metasmith.sh -bp         # build the pip wheel + sdist  (stamps build_hash.txt)
./dev/metasmith.sh -bd         # build the docker image, tagged <version>-<hash>
./dev/metasmith.sh -bs         # build the apptainer .sif from the local docker image
./dev/metasmith.sh -bc         # build the conda package from the wheel
```

`-brc`/`-br` produce the relay binaries that get baked into the docker image;
build them before `-bd`. `-bp` stamps `build_hash.txt`, which fixes the build
hash that ties the wheel, image tag, and SIF to the exact source state.

The order is only load-bearing where one step consumes another's output. The
four producers above `-bp` — relays, solver engine, GUI bundle, vendored library
— are independent of each other and can run concurrently; so can `-bd` and `-bc`
below it, which read the same frozen `dist/` sdist and write to disjoint places.

`--vendor-library` re-stages `src/metasmith_libraries/` into
`src/metasmith/vendor/`, which is inside the tree `_build_hash` walks — so it
must land before `-bp` stamps the hash, or the wheel ships a library the version
does not describe. **Re-stage it every release rather than trusting the bundle a
previous build left behind**: `_assert_library_bundle` checks that the bundle
exists, not that it is current, so a library edit since the last build is
shipped stale and nothing says so.

The cross-compile container is an **upstream** image
(`joseluisq/rust-linux-darwin-builder`) and `-brc` now pulls it. It used to
`docker build` a local Dockerfile over that same tag, which replaced the
osxcross toolchain with a plain rust image; `-br` then failed both
`*-apple-darwin` targets and left 28-byte stubs in `target/` for `-bd` to bake.
`_assert_real_relays` catches that at `-bs`/`-ud`, not at `-bd` — so if `-br`
reports a compile error, stop and fix it rather than continuing to `-bd`.

`-be` cross-compiles `src/workflow_solver/` to the same four targets, using the
same upstream container (`-bec` pulls it, and is interchangeable with `-brc`),
and stages the binaries into `src/metasmith/engine/`. That directory is
generated, never committed, and shipped as package data — because unlike the
relay, the solver runs **locally at plan time** in whatever process is planning,
so the agent-deploy path never sees it. `-bp` and `-bc` refuse to run without
all four, and that refusal is now the only thing between a packaging slip and a
wheel that cannot plan at all. There used to be a Python solver behind it, so a
missing engine meant "correct and about 7x slower" and the failure was invisible
unless something checked. The Python solver is gone: a missing engine now raises.

`-bel` is the dev-loop build (host toolchain,
host target only); it writes a `BUILD_KIND` marker the guard reads, because
nothing about a Linux ELF says whether it was linked against musl or against the
build machine's glibc.

**Do not put that directory under version control of any kind, including DVC.**
It was DVC-tracked briefly so sibling worktrees could share one cross-build. DVC
materialises outputs as read-only hardlinks and does not carry the exec bit, so
the binaries checked out mode 444, the handshake failed with a permission error,
and every plan in every worktree fell back to the Python solver — which is one of
the reasons that fallback no longer exists. The same accident today stops the
planner instead of silently slowing it. The mode is not
repaired by packaging either: 444 survives an sdist unchanged and normalises to
644 in a wheel. `_assert_solver_engine` now checks the exec bit, and each scope
builds its own stage; `MSM_SOLVER_TARGET_DIR` (default
`~/.cache/metasmith/solver-target`) shares the cargo build directory across
scopes so only the first build is cold.

`--build-gui` compiles the web GUI into `src/metasmith/gui/static/`. That
directory is generated and never committed, so a fresh checkout has none, and
without it the package would ship an empty static directory — a failure nobody
notices until someone opens the page. `-bp` and `-bd` refuse to run when it is
missing. It needs node, which is a build dependency only and deliberately absent
from `envs/metasmith/base.yml`:

```
mamba create -n msm_node -c conda-forge nodejs
mamba run -n msm_node ./dev/metasmith.sh --build-gui
```

### Run the unpublished build on a cluster

An agent on an apptainer host pulls `docker://quay.io/hallamlab/metasmith:<version>-<hash>`,
which does not exist before §4. Stage the local SIF where that pull would land instead:

1. Copy `metasmith.sif` into the host's `$APPTAINER_CACHEDIR` under the name
   `Environment._cached_name()` gives the image URL. For 0.25.0 that was
   `docker..quay.io_hallamlab_metasmith..0.25.0-28ecc51.sif`.
2. Run `apptainer exec --no-home --cleanenv <sif> true` on the host.
3. Create an empty `<sif>.verified` beside it only when step 2 exits 0. The engine trusts the
   stamp and skips the pull.
4. Install the conda package from `file://conda_build` into a throwaway env for the driver.

A rebuild after a fix changes the hash. Stage each rebuild's SIF under its own name.

---

## 4. Publish

A release goes to the fork (`origin`) and to upstream (`hallamlab`) by direct
push. Claim the version number with the other release sessions before you push.

```
./dev/metasmith.sh -ud    # push the docker image to quay.io/hallamlab/metasmith
./dev/metasmith.sh -uc    # upload the conda package to anaconda.org/hallamlab
```

Both refuse to publish an artifact whose solver engine will not run there — `-ud`
inspects the image, `-uc` installs the built package into a throwaway env — and
that is the last gate, because packaging damage to the engine is invisible at
build, install and import time. `-ud` also moves the `latest` and bare-version
tags. Expect `-uc` to spend a minute on the clean-room install.

`-uc` authenticates with an anaconda.org API token, never with `anaconda org
login`. The login needs a real TTY and hangs in an agent shell. The client reads
the default token from `~/.config/binstar/https%3A%2F%2Fapi.anaconda.org.token`,
or from `ANACONDA_API_TOKEN`. Keep each token file at mode 600. Copy the file to a
host that has none. Check it with `anaconda org whoami`, run from the env's
`bin/anaconda` directly, because `mamba run` swallows stdin.

**WARNING** Never run `anaconda remove` without Tony's explicit approval. Users and
other sessions install from the published versions.

Then:

1. Push `release` and the annotated version tag to both remotes:
   `git push origin release vX.Y.Z && git push upstream release vX.Y.Z`.
2. Check `gh pr list --repo hallamlab/Metasmith --state open`. An open PR from the
   fork's `release` into `hallamlab:release` closes as merged once upstream
   `release` holds its head. Retitle it for the version first, so the closed PR
   names what shipped.
3. Drive both published artifacts as a **consumer**, not as the builder. Every
   guard above reads the local build — `-uc` installs from `file://conda_build`
   and `-ud` inspects the image docker already holds — so nothing so far has
   touched what anaconda.org and quay actually serve. Two lanes, both from
   outside every worktree and with `env -u PYTHONPATH`, since an ambient
   `PYTHONPATH` resolves `metasmith` to a checkout and the run proves nothing:

   ```
   mamba create -y -n msm_gate -c hallamlab -c bioconda -c conda-forge metasmith=X.Y.Z
   docker rmi quay.io/hallamlab/metasmith:X.Y.Z{,-<hash>} :latest && docker pull …:X.Y.Z-<hash>
   ```

   In each: `msm --help`, `Backend("solve") == "rust"`, `clone_stdlib` into an
   empty directory, and every shipped template solved. The `docker rmi` is the
   load-bearing half — without it the locally built image is what gets tested,
   which is the thing this step exists to avoid. The library stamp
   (`stdlib.discover(root)["commit"]`) must agree between the two artifacts;
   they are built from one vendored tree and a disagreement means one of them
   was staged from something else. Also run each shipped driver and the Python
   tutorial snippet in the conda lane. 0.26.1 solved every template while the
   documented Python path failed on a removed `Template` attribute.
4. Then **run something**. Everything above proves the release plans; none of it
   executes a workflow, and that is where 0.22.0's defect was — it shipped
   solving eleven templates cleanly while failing the case it was cut for. Drive
   whatever scenario the release claims to fix, on the published artifacts, and
   pair it with a control that should behave the *opposite* way. A single run
   that misses tells you nothing: it could be a broken cache or a correct miss.
   The 0.22.0 pass was three trio runs — cold, add-a-sample, re-run-unchanged —
   and only the third one turned the second from an observation into a
   diagnosis. Budget hours, not minutes.

## What goes in this file

The followable sequence for cutting a release, and the order constraints inside it — which
artifact must exist before which, and which guard refuses a build that skips one. What a flag
does is `dev/metasmith.sh --help`; what a step produces is the step. Neither is transcribed
here, and nothing is duplicated between sections.
