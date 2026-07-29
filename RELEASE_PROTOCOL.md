# Release Protocol

How to cut a metasmith release: verify it works, build the artifacts, and
publish them. Keep this followable end-to-end without re-deriving the steps.

All commands run from the repo root in the `msm` conda env:

```
mamba run -n msm ./dev.sh <flag>
```

---

## 1. Verify it works

A release ships only after the test suite is green and the end-to-end paths
have been exercised. Focus the verification on what a release actually changes.

### Run the suite

```
PYTHONPATH="$PWD/src" mamba run -n msm python -m pytest tests/ \
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
use `-m fast`, and `./dev.sh -tg` for the GUI alone.

### Exercise the gated tiers when relevant

The marker tiers above are skipped in the fast sweep because they need
infrastructure the dev box may lack. Run the ones a release touches:

- **`docker` / `nextflow`** — real container + Nextflow execution. Run on a host
  with a working Docker daemon when the change affects deploy, staging, or
  workflow generation.
- **`network`** — remote-shell paths; needs `LIVESHELL_REMOTE_HOST`.
- **`e2e_agentic`** — live agent-driven scenarios; opt-in, needs an API key and a
  MetasmithLibraries checkout.

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
  (`main/local_mock/smoke_hpc_deploy.py`) rather than assuming the dev box
  generalizes.

---

## 2. Bump the version

The version lives in one file: `src/metasmith/version.txt`, a bare PEP 440
release segment (e.g. `0.19.0`) — no `+` or `-`. Everything downstream
(`constants.VERSION`/`FULL_VERSION`/`CONTAINER_TAG`, `setup.py`, the `dev.sh`
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

./dev.sh -brc        # one-time: fetch the rust cross-compile container
./dev.sh -br         # build the relay binaries (all four arch/os targets)
./dev.sh --build-gui # build the frontend bundle (needs node; see below)
./dev.sh -bp         # build the pip wheel + sdist  (stamps build_hash.txt)
./dev.sh -bd         # build the docker image, tagged <version>-<hash>
./dev.sh -bs         # build the apptainer .sif from the local docker image
./dev.sh -bc         # build the conda package from the wheel
```

`-brc`/`-br` produce the relay binaries that get baked into the docker image;
build them before `-bd`. `-bp` stamps `build_hash.txt`, which fixes the build
hash that ties the wheel, image tag, and SIF to the exact source state.

The cross-compile container is an **upstream** image
(`joseluisq/rust-linux-darwin-builder`) and `-brc` now pulls it. It used to
`docker build` a local Dockerfile over that same tag, which replaced the
osxcross toolchain with a plain rust image; `-br` then failed both
`*-apple-darwin` targets and left 28-byte stubs in `target/` for `-bd` to bake.
`_assert_real_relays` catches that at `-bs`/`-ud`, not at `-bd` — so if `-br`
reports a compile error, stop and fix it rather than continuing to `-bd`.

`--build-gui` compiles the web GUI into `src/metasmith/gui/static/`. That
directory is generated and never committed, so a fresh checkout has none, and
without it the package would ship an empty static directory — a failure nobody
notices until someone opens the page. `-bp` and `-bd` refuse to run when it is
missing. It needs node, which is a build dependency only and deliberately absent
from `envs/base.yml`:

```
mamba create -n msm_node -c conda-forge nodejs
mamba run -n msm_node ./dev.sh --build-gui
```

---

## 4. Publish

The account has no write access to the upstream (`hallamlab`) repo, so releases
go out through the fork and a standing pull request.

```
./dev.sh -ud    # push the docker image to quay.io/hallamlab/metasmith
./dev.sh -uc    # upload the conda package to anaconda.org/hallamlab
                #   (run `anaconda login` first)
```

Then:

1. Push `release` (and `dev`) and the annotated version tag to **origin** (the
   fork): `git push origin release dev && git push origin vX.Y.Z`.
2. Update the **standing release PR** to upstream — it auto-updates when the
   fork's `release` branch is pushed; retitle it to the new version. A
   maintainer with upstream write access merges it.
3. Update the quay **`latest`** tag to point at the new image (manual, via the
   quay web UI — there is no dev.sh step for it).

---

## Quick reference

| Step | Command | Produces |
|------|---------|----------|
| Test | `pytest -m "not docker and not e2e_docker and not e2e_agentic and not nextflow and not network and not requires_*"` | green gate |
| Bump | edit `src/metasmith/version.txt` + commit | new version |
| Relay | `./dev.sh -brc` (pull) then `./dev.sh -br` | relay binaries — 4 targets, none stubs |
| GUI | `./dev.sh --build-gui` (needs node) | `src/metasmith/gui/static/` |
| Wheel | `./dev.sh -bp` | pip wheel + sdist, build hash |
| Docker | `./dev.sh -bd` | local image `<version>-<hash>` |
| SIF | `./dev.sh -bs` | `metasmith.sif` |
| Conda | `./dev.sh -bc` | conda package |
| Publish image | `./dev.sh -ud` | image on quay.io |
| Publish conda | `./dev.sh -uc` | package on anaconda.org |
| Tags/branches | `git push origin …` + standing PR | release on the fork → upstream |

> The env is `msm`. Run `dev.sh` and tests through `mamba run -n msm`.
