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
    -m "not docker and not e2e_agentic and not nextflow and not slow and not network"
```

This is the fast, dependency-free tier — the gate that must be **0 failed**
before anything is built. It covers the planner/solver, the path layer, library
load/serialization, the CLI surface, the container/runtime command generation,
and the virtual end-to-end pipeline (`tests/e2e_virtual/`, which runs
plan→stage→run→collect with no real containers).

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
  `test_dev_sh_tag`, `test_build_pip_version_split`) are sufficient. When code
  changed, run the full suite.
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

The standard flow merges `smoke → dev → release` and bumps on the **release**
branch only (dev stays a version behind). Stage just the version file so the
bump commit carries nothing else:

```
git add src/metasmith/version.txt
git commit -m "Bump version to 0.19.0"
```

---

## 3. Build the artifacts

Build in this order. Clear `PYTHONPATH` first so the wheel can't pick up a stale
metasmith from the shell environment.

```
unset PYTHONPATH

./dev.sh -brc   # one-time: build the rust cross-compile container
./dev.sh -br    # build the relay binaries (all four arch/os targets)
./dev.sh -bp    # build the pip wheel + sdist  (stamps build_hash.txt)
./dev.sh -bd    # build the docker image, tagged <version>-<hash>
./dev.sh -bs    # build the apptainer .sif from the local docker image
./dev.sh -bc    # build the conda package from the wheel
```

`-brc`/`-br` produce the relay binaries that get baked into the docker image;
build them before `-bd`. `-bp` stamps `build_hash.txt`, which fixes the build
hash that ties the wheel, image tag, and SIF to the exact source state.

---

## 4. Publish

The account has no write access to the upstream (`hallamlab`) repo, so releases
go out through the fork and a standing pull request.

```
./dev.sh -ud    # push the docker image to quay.io/hallamlab/metasmith
./dev.sh -uc    # upload the conda package to anaconda.org/hallamlab
```

The anaconda-client token persists at `~/.config/binstar/*.token` and lasts a
year, so `anaconda login` is rarely needed — check with `anaconda whoami` /
`anaconda auth --list` before assuming you're logged out. If you do need to
re-auth, it must run on a real TTY: `conda run`/`mamba run` swallow stdin, so
the `Username:` prompt dies on `[ERROR] EOF when reading a line`. Pass
`--no-capture-output`, or invoke the env's `bin/anaconda` directly.

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
| Test | `pytest -m "not docker and not e2e_agentic and not nextflow and not slow and not network"` | green gate |
| Bump | edit `src/metasmith/version.txt` + commit | new version |
| Relay | `./dev.sh -brc` then `./dev.sh -br` | relay binaries |
| Wheel | `./dev.sh -bp` | pip wheel + sdist, build hash |
| Docker | `./dev.sh -bd` | local image `<version>-<hash>` |
| SIF | `./dev.sh -bs` | `metasmith.sif` |
| Conda | `./dev.sh -bc` | conda package |
| Publish image | `./dev.sh -ud` | image on quay.io |
| Publish conda | `./dev.sh -uc` | package on anaconda.org |
| Tags/branches | `git push origin …` + standing PR | release on the fork → upstream |

> The env is `msm`. Run `dev.sh` and tests through `mamba run -n msm`.
