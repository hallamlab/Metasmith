# FabFos dev — agent brief

FabFos is a thin metasmith front end. The pipeline definition (transforms, data
types, resources) lives in the sibling **`src/metasmith_libraries`** module; the
wheel bundles it as `fabfos/_library`. `src/metasmith` is the engine. All three,
plus `src/ecspr`, are plain directories in the same repo — no submodules.

This file is the architectural map: where things live, and what fails *silently*
when two of them stop agreeing. It is not an inventory — the tree, `--help`,
`README.md` and `build_references/REFERENCES.md` are each their own source of
truth, and a transcript of any of them here would go stale without this file
changing.

## Syncing and building

`dev/fabfos.sh -b`/`-bm`/`-bp`/`-bc`/`-r` bundle, regenerate metadata, build, and
run; `dev/ecspr.sh --iecspr`/`-e`/`-te`/`-be` do the same for ecspr. `--help` on
either lists the flags.

**Bundling only copies; `_metadata/` is generated.** Edit a `data_types/*.yml` or
a transform and the planner keeps resolving against the stale index until `-bm`
runs — the symptom is `datatype [X] not found in [ns]`, naming a type you just
added. `build_references/build.sh` does the same for the build side.

## Where code goes

Two transform libraries, split by *when* a transform runs, not by what it does:
`src/metasmith_libraries/` is the **run** side and ships in the wheel;
`build_references/` is the **build** side and does not.

They share one type graph. `LoadTypeLibraries` raises on a duplicate namespace,
so a type a run consumes (`ref::`) must live only in `src/metasmith_libraries/`
and cannot be redeclared build-side; build-only namespaces (`raw::`, `interm::`,
`bench::`, `buildlib::`) live in `build_references/data_types/`.
`build_references/build.sh` compiles `_metadata/` for both, which is why it is
one script and not two.

Three resource namespaces, same split:

| namespace | where | ships |
|---|---|---|
| `lib::` | `src/metasmith_libraries/resources/lib/` | yes |
| `buildlib::` | `build_references/resources/buildlib/` | no |
| `algorithm::` | `src/fabfos/algorithm/` | yes, in the package |

`algorithm::` is the exception that resolves against the *installed package*
rather than a library resource dir — the namespace IS the directory name, and
renaming either breaks resolution. It holds the methods FabFos *is*, and they
live there so a cut or a threshold can be run directly against a directory of
assemblies with no planner, staging or container, which is how one gets inspected
before it is trusted.

A method too big for one staged file gets a **package with a command line**, not
more resource files: `src/ecspr` is the only one, because ECSPr is the only
transform whose protocol is an algorithm rather than a dispatch into somebody
else's tool. `env::ecspr.env` declares it like any other tool, so the transform
and a benchmark run the same executable and cannot drift into two call sequences.
`dev/ecspr.sh --iecspr` installs it editable into its own env; `-e` runs it,
`-te` tests it, `-be` builds the conda package and regenerates
`envs/metasmith_libraries/tools/ecspr.yml`.

## Entry points

`fabfos assemble|annotate|ecspr` dispatches to `src/fabfos/pipelines/{assembly,
annotation,ecspr}.py`; each module stays runnable alone (`python -m
fabfos.pipelines.<name>`) and its own parser is the only description of its
flags. `research/fabfos/examples/` holds worked drivers that call metasmith
directly.

`method_version.txt` versions the **composition**, not the package: canon, the
library commit, the engine's `FULL_VERSION`, container digests, the data index,
the type contract and the planner's domain list. `fabfos --describe-method`
prints the document the id hashes.

**Known-broken until the library-versioning fix lands:** the "transform library
commit" component (`method.py`'s `transform_library.source_commit`/`dirty`/
`bundled`) works by asking whether `resolve_library_root()`'s path IS its own git
repo root (`_repo_root_of`, which requires `git rev-parse --show-toplevel` at that
path to equal the path itself). Before the monorepo merge, the submodule boundary
made that true for a real checkout and false for a bundled copy, so the check
told the two apart. Post-migration `src/metasmith_libraries` is a plain
subdirectory of this repo, so `git rev-parse --show-toplevel` from inside it
returns the *monorepo's* root instead — the check now returns false unconditionally,
for BOTH cases. Confirmed live: `describe_method()` reports
`{"bundled": true, "source_commit": null, "dirty": null}` even when running
against the unbundled sibling module, not a real bundled copy. Tracked follow-up
(T6): replace the whole mechanism with a content hash over
`src/metasmith_libraries/` (the same tree-hashing approach `metasmith`'s own
`build_hash.txt` already uses) — content hashing doesn't depend on a repo
boundary that no longer exists.

## Things that fail silently

- **A stale `_metadata/` index** — see "Syncing and building" above.
- **A BLAST table read under the wrong `-outfmt`.** A column-count mismatch used
  to yield zero rows, not an error, so a similarity matrix came back empty and
  the clustering merged nothing. `fabfos_recovery` now keeps two formats — nine
  columns for the shipped `junctions.tsv`, wider for the all-vs-all — and its
  reader raises when a non-empty file parses to nothing.
- **A mapped-read fraction compared across vector policies.** Recovery excises
  the pCC1fos backbone from every insert, so mapping against `inserts.fna` alone
  loses the 13–18% of each pool that is vector. Only compare a
  `fraction_reads_mapped` to one taken against a reference with the same policy.
- **A pin nested one level too deep.** `data/.gitignore` re-includes exactly
  `!/*/*.dvc` and `!/*/*/*.dvc`; anything deeper is invisible to git *and* makes
  DVC scatter a second ignore file. `find data -name .gitignore` must return 1.
- **Writing into a materialized DVC chunk.** The cache is shared across
  worktrees and `dvc checkout` hardlinks, so an in-place edit corrupts that chunk
  for every worktree. There is no DVC remote: `dvc checkout` works, `dvc pull`
  does not.
- **A count transcribed out of a data chunk into prose.** Insert, ORF and row
  counts, per-channel totals and reference pin hashes belong to the chunk that
  holds them. Copied into a README they go stale on the next repin with no diff
  to the sentence, so the document keeps asserting a number the pin beside it
  contradicts. Name the chunk and the script that produces it; never the number.
  The chunks hold no prose of their own to point at: a `PROVENANCE.md` inside a
  chunk was a third copy of what the commit already states, and it went stale the
  same way. The commit pins the script and the `.dvc` together, so re-running the
  script *at that commit* reproduces the data — that is the provenance record.
- **Mixing bake versions.** `vocab`/`atom_pairs`/`direction` are ONE artifact
  sharing a bake-identity block; v1 carries MetaNetX's `EMPTY` sentinel, so a
  v1/v2 mix shifts every code by one and decodes each node to the wrong
  metabolite. `refs.assert_same_bake()` is the guard — never repin one of the
  three alone.
- **Two staged inputs with the same basename.** Nextflow stages by basename, so
  two reference directories both named `pool/` collide. That is why the ESM-C
  pool's leaf is `pool_esmc/`.
- **`mamba run -n msm-fabfos` overriding `PYTHONPATH`.** That env's activation
  hook prepends *another worktree's* `src/metasmith` ahead of whatever you set,
  so an ad-hoc one-liner imports the wrong engine and reports a build hash (and
  so an agent image tag) for a tree you are not running. The drivers are safe —
  each inserts its own engine at `sys.path[0]`. Check with `python -c "import
  metasmith; print(metasmith.__file__)"` before trusting any such number.
- **`Agent.container`'s default tag.** It derives from the engine hash and names
  an image nobody necessarily built; the failure surfaces as `manifest unknown`,
  then a missing `.sif`, then a missing relay binary, none of which says so. Name
  the site's own tag instead — `--agent-env` under mamba, where the same field
  means a conda env NAME and the derived default is a `docker://` URI that mamba
  rejects as a filename. The dev overlay reaches a remote run as **two**
  artifacts — `<agent_home>/dev/metasmith/` for the login node and
  `…/metasmith.tar` for every SLURM task — and shipping one without the other
  runs two different engines inside one job.

## Runtime is one global setting

`Agent.runtime` is global, and an env declares either a container image or a
`conda:` spec, never both. That is why the reference build is split in two
(annotation half under APPTAINER, metabolism half under MAMBA) and why the run
side is container-only. It is arithmetic, not preference — one graph spanning
both cannot execute.

## Where to read next

- `README.md` — repo layout, data dependencies, known gaps
- `build_references/REFERENCES.md` — the reference tier contract: every compiled
  artifact, what it requires, and why everything else under `data/` was deleted
- `tests/fabfos/README.md` — what each test covers, and what is deliberately not
  tested
