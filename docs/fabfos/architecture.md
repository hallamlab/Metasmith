# fabfos — architecture

FabFos is a thin metasmith front end. The pipeline definition — transforms, data types,
resources — lives in `src/metasmith_libraries`; the wheel bundles it as `fabfos/_library`.
`src/metasmith` is the engine.

This is the map: where things live, and what fails *silently* when two of them stop agreeing.
It is not an inventory — the tree, `--help`, `README.md` and `build_references/REFERENCES.md`
are each their own source of truth, and a transcript of any of them here would go stale without
this file changing.

## Where code goes

**Two transform libraries, split by *when* a transform runs, not by what it does.**
`src/metasmith_libraries/` is the **run** side and ships in the wheel; `build_references/` is
the **build** side and does not. They share one type graph, and `LoadTypeLibraries` raises on a
duplicate namespace — so a type a run consumes must live only on the run side and cannot be
redeclared build-side, while build-only namespaces live under `build_references/data_types/`.
`build_references/build.sh` compiles `_metadata/` for both, which is why it is one script and
not two.

Three resource namespaces follow the same split: `lib::` and `algorithm::` ship, `buildlib::`
does not. **`algorithm::` is the exception that resolves against the *installed package*** rather
than a library resource dir — the namespace IS the directory name, so renaming either breaks
resolution. It holds the methods FabFos *is*, and they live there so a cut or a threshold can be
run directly against a directory of assemblies with no planner, staging or container, which is
how one gets inspected before it is trusted.

**Custom algorithms live in a src module; work that is only a call to somebody else's tool
stops at a transform.** That is the whole rule, and the size of the method is what decides which
side of it you are on: one staged file is fine, but flat files that import each other by bare
name are a package only by accident of landing in the same staged directory — and the accident's
price is a requirement list that is a hand-maintained copy of an import graph, where a missing
entry is an ImportError six hours into a queued job.

So a method that imports itself gets a **package with a command line**, vendored into the
library as one staged directory and invoked with `-m`. `src/ecspr` holds both: `ecspr.model` is
the measurement ECSPr *is*, and `ecspr.bake` is the ~7,000-line method that builds the tables it
reads. The two never import each other at module scope — they are installed into images with
disjoint dependency stacks, so an import across that seam fails at load rather than degrading.
See `docs/ecspr/architecture.md`.

**Drivers stay where they are.** `research/fabfos/examples/` and `tests/fabfos/` hold scripts
that call metasmith directly to orchestrate a run; moving one into `src/fabfos/` is a form of
*publishing*, and most of these are genuine research runs that should not be published. The
question to ask before promoting one is not "is it useful?" but "does a study depend on it?" —
a driver a paper's numbers rest on belongs in the package, versioned with everything else that
produced them; a driver that answered a question once belongs where it is.

## Entry points

`fabfos assemble|annotate|ecspr` dispatches to modules under `src/fabfos/pipelines/`; each stays
runnable alone and its own parser is the only description of its flags.
`research/fabfos/examples/` holds worked drivers that call metasmith directly.

**`method_version.txt` versions the composition, not the package**: canon, the library commit,
the engine's full version, container digests, the data index, the type contract and the
planner's domain list. `fabfos --describe-method` prints the document the id hashes.

The transform-library component of that document hashes whatever `resolve_library_root()`
resolves to, the same tree-hashing approach the engine's own build hash uses. It previously
identified a bundled copy by asking whether that path was its own git repo root — a check the
submodule boundary made meaningful and the monorepo merge made vacuous, since both cases became
"a plain subdirectory of this repo". `bundled` stays in the document alongside the hash on
purpose: a bundled copy is a plain `cp -r` that never stamps anything, so nothing proves it
matches its source at the moment it was taken.

## Runtime is one global setting

`Agent.runtime` is global, and an env declares either a container image or a `conda:` spec,
never both. That is why the reference build is split in two — annotation half under APPTAINER,
metabolism half under MAMBA — and why the run side is container-only. It is arithmetic, not
preference: one graph spanning both cannot execute.

## Things that fail silently

- **A stale `_metadata/` index.** Bundling only copies; metadata is generated. Edit a data type
  or a transform and the planner keeps resolving against the stale index until `-bm` runs — the
  symptom is `datatype [X] not found in [ns]`, naming a type you just added.
- **A BLAST table read under the wrong `-outfmt`.** A column-count mismatch used to yield zero
  rows rather than an error, so a similarity matrix came back empty and the clustering merged
  nothing. `fabfos_recovery` now keeps two formats and its reader raises when a non-empty file
  parses to nothing.
- **A mapped-read fraction compared across vector policies.** Recovery excises the pCC1fos
  backbone from every insert, so mapping against inserts alone loses the 13–18% of each pool that
  is vector. Only compare fractions taken against references with the same policy.
- **A DVC pin nested one level too deep.** `data/.gitignore` re-includes exactly two depths;
  anything deeper is invisible to git *and* makes DVC scatter a second ignore file.
  `find data -name .gitignore` must return 1.
- **Writing into a materialized DVC chunk.** The cache is shared across worktrees and checkout
  hardlinks, so an in-place edit corrupts that chunk for every worktree. There is no DVC remote:
  `dvc checkout` works, `dvc pull` does not.
- **A count transcribed out of a data chunk into prose.** Counts, totals and pin hashes belong to
  the chunk that holds them; copied into a README they go stale on the next repin with no diff to
  the sentence. Name the chunk and the script that produces it, never the number. The chunks hold
  no prose of their own to point at, deliberately — the commit pins the script and the `.dvc`
  together, so re-running the script *at that commit* reproduces the data, and that is the
  provenance record.
- **Mixing bake versions.** `vocab`/`atom_pairs`/`direction` are ONE artifact sharing a
  bake-identity block; v1 carries MetaNetX's `EMPTY` sentinel, so a v1/v2 mix shifts every code by
  one and decodes each node to the wrong metabolite. `refs.assert_same_bake()` is the guard —
  never repin one of the three alone.
- **Reading `direction`'s ratio as a standard-state number.** From r10 it is a PHYSIOLOGICAL
  quantity: `exp(dG'/RT)` with a reaction quotient formed from measured E. coli
  concentrations, where every bake before it shipped `exp(dG'o/RT)`. Nothing in the type or
  the column name says which, so a consumer that assumes the old meaning gets a plausible
  number and no error. The annotation seam carries `dG_standard`, `dG_correction` and
  `sigma_conc` so any ratio comes apart again.
- **Two staged inputs with the same basename.** Nextflow stages by basename, so two reference
  directories both named `pool/` collide.
- **`mamba run -n msm-fabfos` overriding `PYTHONPATH`.** That env's activation hook prepends
  *another worktree's* engine ahead of whatever you set, so an ad-hoc one-liner imports the wrong
  engine and reports a build hash — and therefore an agent image tag — for a tree you are not
  running. The drivers are safe, each inserting its own engine first. Check with
  `python -c "import metasmith; print(metasmith.__file__)"` before trusting any such number.
- **`Agent.container`'s default tag.** It derives from the engine hash and names an image nobody
  necessarily built; the failure surfaces as `manifest unknown`, then a missing `.sif`, then a
  missing relay binary, none of which says so. Name the site's own tag instead. Under mamba the
  same field means a conda env NAME, and the derived default is a `docker://` URI that mamba
  rejects as a filename. The dev overlay reaches a remote run as **two** artifacts — one for the
  login node, one for every SLURM task — and shipping one without the other runs two different
  engines inside one job.

## Where to read next

- `README.md` — repo layout, data dependencies, known gaps
- `src/fabfos/build_references/REFERENCES.md` — the reference tier contract: every compiled
  artifact, what it requires, and why everything else under `data/` was deleted
- `tests/fabfos/README.md` — what each test covers, and what is deliberately not tested
