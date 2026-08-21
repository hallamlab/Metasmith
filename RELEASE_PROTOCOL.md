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
PYTHONPATH="$PWD/src" mamba run -n msm python -m pytest tests/metasmith \
    -m "not docker and not e2e_docker and not e2e_agentic and not nextflow \
        and not network and not requires_docker and not requires_apptainer \
        and not requires_ssh_localhost and not requires_docker_dev_image"
```

This is the dependency-free tier — the gate that must be **0 failed** before
anything is built. It covers the planner/solver, the path layer, library
load/serialization, the CLI surface, the container/runtime command generation,
the web GUI's API, and the virtual end-to-end pipeline
(`tests/e2e/virtual/`, which runs plan→stage→run→collect with no real
containers).

`PYTHONPATH` must be **set**, not merely unset: metasmith is not installed into
`msm`, so the subprocess tests spawn `python -m metasmith` and fail without it.

Note this does **not** exclude `slow` — the `perf` axis (10k-item libraries) is
a few minutes and belongs in a release gate. For the minute-by-minute dev loop
use `-m fast`, and `./dev/metasmith.sh -tg` for the GUI alone.

**Budget 25 minutes.** Measured 2026-08-20 on a 16-core box under other load:
1968 selected, 296 deselected, 20m38s wall. `perf` and `flow` are two thirds of
that (290s and 362s), so run them last if you need to cut the sweep short — the
other eight axes together are under seven minutes. `pytest-timeout` is not in
`msm`, so every `@pytest.mark.timeout` in the tree is inert and nothing bounds a
hung test; wrap the run in `timeout` if you are leaving it unattended.

### Exercise the gated tiers when relevant

The marker tiers above are skipped in the fast sweep because they need
infrastructure the dev box may lack. Run the ones a release touches:

- **`docker` / `nextflow`** — real container + Nextflow execution. Run on a host
  with a working Docker daemon when the change affects deploy, staging, or
  workflow generation.
- **`network`** — remote-shell paths; needs `LIVESHELL_REMOTE_HOST`.
- **`e2e_agentic`** — live agent-driven scenarios; opt-in, needs an API key and a
  compiled standard library (`dev/libraries.sh -b`).

### Focus areas

- **Scope the depth to the change.** When `src/metasmith/` is unchanged from the
  last release, the version→tag chain tests (`test_container_tag`,
  `test_dev_sh_tag`, `test_build_pip_version_split`, all under `tests/unit/`)
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
all four. The refusal matters more here than for the relay: a wheel with no
engine still plans, on the Python solver, just slower — so the failure is
invisible unless something checks. "Just slower" is now literal and large:
7.5s versus 1.1s on `metagenomics_from_paired_reads`, for the same plan. `-bel` is the dev-loop build (host toolchain,
host target only); it writes a `BUILD_KIND` marker the guard reads, because
nothing about a Linux ELF says whether it was linked against musl or against the
build machine's glibc.

**Do not put that directory under version control of any kind, including DVC.**
It was DVC-tracked briefly so sibling worktrees could share one cross-build. DVC
materialises outputs as read-only hardlinks and does not carry the exec bit, so
the binaries checked out mode 444, the handshake failed with a permission error,
and every plan in every worktree fell back to the Python solver. The mode is not
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

---

## 4. Publish

The account has no write access to the upstream (`hallamlab`) repo, so releases
go out through the fork and a pull request per release.

```
./dev/metasmith.sh -ud    # push the docker image to quay.io/hallamlab/metasmith
./dev/metasmith.sh -uc    # upload the conda package to anaconda.org/hallamlab
```

Both refuse to publish an artifact whose solver engine will not run there — `-ud`
inspects the image, `-uc` installs the built package into a throwaway env — and
that is the last gate, because packaging damage to the engine is invisible at
build, install and import time. `-ud` also moves the `latest` and bare-version
tags. Expect `-uc` to spend a minute on the clean-room install.

The anaconda-client token is a **per-host** file at `~/.config/binstar/*.token`
and lasts a year, so a host that has never published has none and the cheapest
fix is to copy the file from one that has. Check with `anaconda org whoami`, not
`anaconda whoami`: anaconda-client 1.14 split anaconda.com from anaconda.org, and
the bare subcommands prompt for a destination and die on a non-TTY with
`Inappropriate ioctl for device` — which reads as an auth failure and is not one.
A real `anaconda org login` needs a real TTY; `conda run`/`mamba run` swallow
stdin, so invoke the env's `bin/anaconda` directly.

Then:

1. Push `release` (and `dev`) and the annotated version tag to **origin** (the
   fork): `git push origin release dev && git push origin vX.Y.Z`.
2. Open a **new PR** from the fork's `release` into `hallamlab:release`, titled
   for the version. A maintainer with upstream write access merges it. There is
   no standing PR to reuse: each one closes on merge (#63 → 0.17.1, #64 →
   0.18.3, #65 → 0.18.8), and treating the last one as still open is how 0.20.0
   and 0.20.1 shipped to quay and anaconda without ever reaching upstream.

## What goes in this file

The followable sequence for cutting a release, and the order constraints inside it — which
artifact must exist before which, and which guard refuses a build that skips one. What a flag
does is `dev/metasmith.sh --help`; what a step produces is the step. Neither is transcribed
here, and nothing is duplicated between sections.
