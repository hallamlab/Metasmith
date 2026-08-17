# Agent notes — the metasmith monorepo

## What goes in this file

This is the **index**, not a manual. It holds three things: the repository's shared layout, the
rules for getting a working tree, and one entry per module pointing at that module's own
document. Everything else belongs somewhere more specific.

Two registers, two homes, and the distinction is deliberate:

- **Architecture** — what a module is, why it is shaped that way, which two files must agree —
  lives at `docs/<module>/architecture.md`.
- **An authoring brief** — how to add an env, write a transform, author a template — lives at
  `AGENTS.md` in the package root it applies to. Its *location is the mechanism*: `AGENTS.md`
  resolves by directory proximity, so somebody editing a transform picks up that library's rules
  without being told to look for them. `src/metasmith_libraries/AGENTS.md` is the case that
  matters; do not relocate it into `docs/`.

The test for any paragraph, here or there: **would reading the code have told me this?** If yes,
it does not belong. Signatures, flag lists, command trees and directory inventories are all
recoverable — name the source of truth (`--help`, the directory, `docs/`) instead of transcribing
it, since a transcript goes stale without the doc changing.

## Layout

One subdirectory per module, repeated across facets. A module is therefore a *name that recurs*
rather than a directory in one place:

    src/  tests/  docs/  envs/  docker/  conda_recipe/  research/

Not every facet holds every module, and that is the shape rather than a gap — the two Rust
products have no conda recipe or test directory of their own, and `research/aspire/` is a
pipeline lane rather than a `src/` module. `dev/` breaks the pattern deliberately: it holds one
script per buildable product (`metasmith.sh`, `libraries.sh`, `fabfos.sh`, `ecspr.sh`), each with
`--help`.

## Environment

Use the `msm` mamba environment: `mamba run -n msm <command>`.

**Pin `PYTHONPATH` to this worktree's `src/`; do not merely unset it.** `metasmith` is not
installed into `msm` at all, and `tests/metasmith/conftest.py` inserts `src/` for in-process
imports — so an unset `PYTHONPATH` looks fine until a subprocess test spawns `python -m
metasmith` and fails on its own. An ambient workspace `PYTHONPATH` is the opposite trap: it
resolves the import to some other checkout. `PYTHONPATH="$PWD/src" mamba run -n msm …` is the
form that is right under both.

## A fresh checkout is not runnable until the libraries are compiled

Every transform library carries a `_metadata/` directory compiled from its `data_types/*.yml` and
its transform Python. **It is a build product and is not tracked**, so a fresh clone has none —
and a library with no metadata does not degrade, it raises: `DataTypeLibrary` asserts the index
exists before planning begins. Three commands, because there are three libraries and only one of
them is reached by the vendoring step:

    dev/libraries.sh -bm                        # the standard library under src/metasmith_libraries
    dev/fabfos.sh -bm                           # fabfos's own algorithm library, inside the package
    src/fabfos/build_references/build.sh        # the build-side library (also vendors src/ecspr)

The second is easy to forget precisely because it sits inside `src/fabfos/` rather than under a
library root, which is also why `--vendor-library` never sees it. The third does two jobs in one
script and the order between them is load-bearing: it copies `src/ecspr` in as `buildlib::ecspr`
*before* compiling, so the index it writes describes the tree that was actually staged. A copy
made by any other invocation is a copy the index does not describe.

`dev/libraries.sh -b` is `-bm` plus a solve of every shipped template — an author's gate on
whether a changed transform still supports them, not a prerequisite for using the library. It is
also much slower, so the split matters.

The ordering that makes this work at all: compiling metadata needs a working engine, and the
engine needs the library — so the compile must run **from the source tree**, never from an
installed package. `dev/metasmith.sh --vendor-library` does exactly that before it copies, and
refuses to stamp a bundle whose metadata came out empty. Shipping one that did would be silent:
the GUI's type panel simply goes blank.

A fourth step is needed before anything *stages an agent*, and its absence looks nothing like
its cause: `bash envs/fabfos/setup_agent_env.sh` (idempotent; the script's own header explains
what it installs and why). Without it a reference build plans fine and then dies at staging on
`No module named metasmith`. **The `msm-fabfos` environment it builds is one host-level env
shared by every worktree, and the activation hook inside it points at exactly one checkout** —
so re-running the script silently repoints every other worktree's agent at yours, and two
worktrees building concurrently will fight over it. Re-run it after switching worktrees, and
do not assume a green build in one tree means another is still wired up.

## Four products, one repository, and what that costs the ref namespace

The engine, the standard transform library, fabfos and ASPIRE live here as directories rather
than as pinned submodules, so a change to a library and to the consumer that needs it is **one
commit** — which is the entire reason for the shape. Each product also gets a workspace scope
layer named for it, and a scope's branch is named for the scope with no `feat/` prefix:
`engine/dev`, `fabfos/dev`, `libraries/mono`, `aspire/release`.

**Git stores refs as paths, so a nested branch permanently forbids a bare branch of its first
segment.** With `fabfos/dev` in this repository, no ref may ever be named `fabfos` — and the
reverse holds too, which is why the old `feat/fabfos` had to be deleted before the nested set
could exist. `engine`, `fabfos`, `libraries` and `aspire` are therefore burnt names here,
permanently. A create that would collide is refused by name when it goes through `scope_create`;
a hand-rolled `git branch fabfos` just fails.

`feat/monorepo` is the exception and predates the convention: it is the migration that assembled
the tree, and it merges onto the engine's mainline rather than being a product line of its own.

## The modules

| module | what it is | detail |
|---|---|---|
| `metasmith` | The engine. A type system for bioinformatics data plus a planner that searches backwards from a target, compiles the chain to Nextflow, and runs it on a deployment target. Also the CLI, the web GUI and the notebook API, which are veneers over one `ops` layer. | [docs/metasmith](docs/metasmith/architecture.md) |
| `metasmith_libraries` | The standard transform library: the type graph, the transforms, the tool environments and the shipped GUI templates. Content, not engine code. | [docs/metasmith_libraries](docs/metasmith_libraries/architecture.md) · authoring: [`AGENTS.md`](src/metasmith_libraries/AGENTS.md) |
| `fabfos` | A thin metasmith front end for fosmid pool processing. Owns its pipelines, its `algorithm::` methods and a build-side reference library that does not ship. | [docs/fabfos](docs/fabfos/architecture.md) |
| `ecspr` | Atom-resolved conductance measurement over metabolic GPR tables (`ecspr.model`), plus the metabolism bake that builds the tables it reads (`ecspr.bake`). A package with a command line, because these are algorithms rather than dispatches into somebody else's tool. The two subpackages never import each other at module scope. | [docs/ecspr](docs/ecspr/architecture.md) |
| `bash_relay` | `msm_relay` — the Rust binary metasmith drives a remote host through. Cross-built to four targets and baked into the agent container image. | [docs/bash_relay](docs/bash_relay/architecture.md) |
| `workflow_solver` | `msm_solver` — the Rust plan-search engine. Runs locally, so it is staged into `src/metasmith/engine/` and ships as package data rather than in the image. | [docs/workflow_solver](docs/workflow_solver/architecture.md) |

Test directories carry their own `AGENTS.md` per axis under `tests/metasmith/`; test selection is
by directory rather than by hand-written marks.

## Releasing

`RELEASE_PROTOCOL.md` is the followable sequence, and the order in it is load-bearing rather than
stylistic: relays before the image that bakes them, GUI bundle before the wheel whose hash covers
it. A release ships both a quay image and a conda package. Version derivation and the guards that
refuse a build missing a generated artifact are in `docs/metasmith/architecture.md`.

Open bugs, accepted residual risks and rejected designs carried forward from retired scopes are
in `docs/metasmith/plans/consolidation-followups.md`. User-facing documentation is the sphinx
tree under `docs/metasmith/source/`.
