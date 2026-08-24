# The standard transform library — authoring notes

The type system, the solver, and the execution model are the engine's and are
documented in `docs/metasmith/architecture.md`; what this module *is* and how it
relates to its neighbours is `docs/metasmith_libraries/architecture.md`. This
file is only about authoring the
library that sits on top of them: what the build guarantees, how an environment is
declared, and the handful of rules whose failure mode is silence rather than an
error.

Paths here are repo-relative. The library is `src/metasmith_libraries/`; its tests
are `tests/metasmith_libraries/`, its conda recipes `envs/metasmith_libraries/`,
and its research drivers `research/metasmith_libraries/`.

## What goes in this file

Authoring rules for this library: what the build guarantees, how an environment is declared,
and the handful of conventions whose failure mode is silence rather than an error. Not what the
library contains — the tree is that — and not the engine's behaviour, which is
`docs/metasmith/architecture.md`.

## The build is not optional

    dev/libraries.sh -bm    # compile _metadata/ -- seconds
    dev/libraries.sh -b     # the same, then solve every template -- much slower

`-bm` rebuilds every `_metadata/` from the source YAML and transform Python. `-b`
adds the template gate on top; take that before you push a transform whose products
changed shape, not every time you need a working tree.

**`_metadata/` missing is fatal, not cosmetic.** With sources present and
`_metadata/index.yml` absent, a solve does not degrade to resolving fewer types — it
raises before planning begins. Nothing about a library that resolves nothing
announces itself in the GUI, so treat a fresh checkout as unusable until this has run.

Two invariants the build enforces, both by failing:

- every `resources/env/<tool>.env` has a matching type in `data_types/env.yml`
- every `lib.GetType("ns::type")` reached from a transform exists in `data_types/ns.yml`

and one it enforces by solving: a transform whose products change shape takes its
templates down here, by name, rather than in someone's GUI a week later.

## Environments

`resources/env/<tool>.env` declares a tool's environment generically: an optional
`container:` (a `docker://…` OCI URI, used by the DOCKER/APPTAINER runtimes) and an
optional `conda:` (an env name, used by MAMBA). One `.env` serves both worlds; the
engine picks by the global runtime. To add one:

1. add the type to `data_types/env.yml` with `extends: env` and a `provides` list
2. write `resources/env/<name>.env` with `container:` and/or `conda:`
3. for a conda arm, add `envs/metasmith_libraries/tools/<name>.yml` — or re-run
   `envs/metasmith_libraries/gen_tool_envs.py`, which derives biocontainers specs
   automatically and leaves its `HAND_WRITTEN` set alone
4. rebuild; `dev/libraries.sh --create-envs` materialises the conda envs to test against

**Pin by digest and say what the digest is.** A tag is not stable and a digest is not
readable, so a bare digest with no comment is a pin nobody can audit. This is not
hypothetical: `python_for_data_science` was once pinned to a digest that resolved to
an *older* image than its version number suggested and carried no polars at all —
80 tasks in one run died on `ModuleNotFoundError` after their expensive work had
already succeeded. Every digest here carries a comment naming the tag it is, the date,
and what was verified inside it.

**`python_for_data_science` carries no pyarrow.** Parquet under that env goes through
`lib::fabfos_evidence`'s `read_parquet` / `write_parquet`, which select polars or pyarrow
by what the interpreter has — the same module is imported outside any container, where
pyarrow is what exists. `pd.read_parquet` and `DataFrame.to_parquet` are pyarrow front
ends and fail there, at module import, after the expensive lanes have already succeeded.

`ExecWithContainer` is retired; the engine rejects it statically. Use
`context.ExecWithEnv().ifContainerDo(env=, cmd=)`, and add `.ifVirtualEnvDo(env=, cmd=)`
only for a conda arm you have actually run. See `docs/metasmith_libraries/ENV_PORT.md`.

## Two rules that fail quietly

**A collecting transform must not pair two grouped slots by position.**
`InputGroup(a)[i]` and `InputGroup(b)[i]` arrive in independent task-arrival order and
are deduped separately, so the only thing relating them is declared lineage:
`context.SourceOf(item, other_slot)` answers which item of `other_slot` a given item
descends from. `transforms/pangenome/ppanggolin.py` is the reference — it names each
genome from the `ncbi::genome_name` its gbk descends from. `aggregator.py` shows the
older in-band alternative (join on a key present in both files) for the cases where no
lineage relates them.

**Anything downloaded from an accession is named by whoever asked for it, not by its
header.** `ncbi::genome_name` sits above `ncbi::assembly_accession` in lineage, so every
product of `getNcbiAssembly` inherits it. That transform *requires* a name and never
reads one — the declaration exists solely to put it in the lineage — which means every
caller registering an accession must register a name above it. No GenBank field works
instead: `ORGANISM` is bare species for most isolates, `DEFINITION` carries the strain
only sometimes, and two assemblies of one species collide under either. PPanGGOLiN then
refuses the whole run over duplicate names.

## An annotator emits its hits and its descriptions

Two products, not one: a hit table keyed on an accession, and a table mapping that
accession to what it means. The description is written once per key rather than repeated
on every hit row, and both halves travel the chunk/merge pair — a `<type>_descriptions_chunk`
beside the `<type>_chunk`, merged with the keys deduplicated.
`transforms/functionalAnnotation/diamond_uniref50.py` and its merge are the reference.

**A merged description table is only delivered if it is a target.** A run publishes what was
asked for and nothing else, so a template naming `annotation::kofamscan_results` and not
`annotation::kofamscan_descriptions` gets one of the two files.

## Templates

A template is a starting point a user picks in the GUI: a `metasmith.Spec` whose input
paths are `DEFERRED`, saved as `templates/<name>/spec.yml` with the deferred input
library packed inline. There is no template format — it is the same object a stored
workflow is, so a template validates the way a workflow does, by solving. Shipping them
beside the transforms is what makes versioning free: a template arrives in the same
commit as the transforms it names and cannot be older than the library it was found in.

An author is a module here defining `NAME`, `DESCRIPTION` and `build_spec(rebuild=False)`,
listed in `build_templates.py`; `_authoring.py` owns the rest. Read
`pangenome_heatmap_from_assembly.py` first — it is the smallest complete example. One
that cannot ship stays listed in `BLOCKED` with the reason rather than being deleted, so
a build that omits it does not read as "these are all the templates there are".

Five rules, each quiet if you break it:

- **Pin every target that descends from an ambiguous type.** A target names a type, and
  the solver may satisfy it from any transform producing a subtype — so once a second
  producer joins a type (`spades_assembly` beside `megahit_assembly` under
  `sequences::assembly`), each unpinned target downstream is free to be answered from a
  different one. `metagenomics_from_paired_reads` then ran both assemblers and split its
  binning across them, and the search that found that took ~190s and 6 GB where the
  pinned form takes 3s. Neither symptom names its cause: the plan is valid, and the cost
  lands on whoever is waiting behind the GUI's plan lock. Write the shared ancestor as
  target 0 and hang the rest off it with `parents=[0]`.
- **No agent.** A template says what to build, never where. Whoever loads it supplies the host.
- **References stay inside the repo.** `Template.Save` refuses one that does not, because an
  absolute path is one machine's checkout and arrives at a colleague naming nothing.
- **Nothing rendered ships.** `--dag` writes an SVG under a git-ignored path so you can see
  whether the spec you wrote is the one you meant. The repository ships the spec; the GUI
  draws it in its own theme.
- **The input library is built once, then loaded back.** Deferred paths are minted on
  `AddItem` and identity follows the path, so re-minting on every build would change the
  template's task key each time and pile up duplicate rows. `--rebuild` is the deliberate
  way to start over after changing what the inputs *are*.

## A protocol body is not a place to keep a program

A transform that only calls somebody else's tool stops at a shell command. Anything with
an algorithm in it — a reshape worth testing, a scoring pass, a model wrapper — goes in a
file under `resources/lib/`, is declared as a `lib::` type, and is taken as a requirement
like any other input. Reached that way it is staged by the same mechanism as a reference
database: no container change, no bind, no copy.

The failure of the alternative is not aesthetic. A program inside a string literal has no
import graph, no syntax check, no test and no diff granularity, and interpolating paths
into it with `.format()` means every one of its callers can silently pass the wrong thing.
Take arguments on argv instead, and let the file be a file.

The rule for shape: one file when the method is one file, a directory when several modules
belong together. Build-side helpers go in `buildlib::` rather than `lib::`, because `lib::`
ships in the wheel and nothing in that namespace runs during a pipeline.

## Drivers

Template authors live here in the package. Everything under
`research/metasmith_libraries/` runs against a real cluster, and
`probe_planner.py` is the plan-only one — solve a target set and print which transforms
were picked, nothing opened. It is the tool for "why did the planner add that step".

- **Public-repo-safe config.** No hardcoded absolute paths, allocations, usernames or DB
  paths. Site-specific values come from env vars with `<placeholder>` defaults (`MSM_SRC`,
  `MSM_HPC_HOST`, `MSM_SLURM_ACCOUNT`, `MSM_REF_DB_DIR`, …); HPC drivers
  `require_configured()` and exit if a placeholder is unfilled.
- **A `sample_type` masks the library down to that row's lineage.** Anything with no lineage
  relation to it — a weight tarball, a reference DB — becomes invisible to the plan and comes
  back as a `download*` step. Either leave the sample type unset or list the entry in
  `shared_input_paths`.
- To force all sibling transforms (e.g. all three binners), target a downstream that requires
  them all (`binning_local::cluster_table`), or give each sibling's target a distinct parent.

## Layout

`transforms/*/_metadata/` and `resources/*/_metadata/` are build-generated — never
hand-edited, never committed. The hand-edited surface is `data_types/`, the
`resources/*/` instance files, and the transform `.py` files themselves.
`transforms/_template.py` is the minimal skeleton. Disabled transforms live in
`transforms/*/_disabled/` or are renamed `<name>.py.disabled` so the build skips them.
