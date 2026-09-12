# metasmith_libraries — architecture

The standard transform library: the type graph, the transforms, the tool environments and the
shipped templates that the engine plans over. It is a *content* module — it declares tools and
types, and holds no engine code.

**Authoring is not this file's subject.** How to add an environment, write a transform or author
a template lives at `src/metasmith_libraries/AGENTS.md`, beside the code, so that an agent
editing a transform picks it up by proximity rather than by being sent to find it. This file is
what the module *is* and how it relates to its neighbours; the engine's type system, solver and
execution model are in `docs/metasmith/architecture.md`.

## What goes in this file

What the module is and how it relates to its neighbours. Authoring — how to add an environment,
write a transform, author a template — is `src/metasmith_libraries/AGENTS.md`, beside the code.
The engine's type system, solver and execution model are `docs/metasmith/architecture.md`.
Nothing enumerable belongs here: not the transforms, not the types, not the environments.

## Its place among the modules

The library is consumed three ways and must not assume any of them. The engine plans against it;
fabfos bundles it into its wheel as `fabfos/_library`, a plain `cp -r` that stamps nothing; and
research drivers under `research/metasmith_libraries/` run it against real clusters.

Because a bundled copy is a copy, the run side and the build side must stay separable — fabfos's
build-only reference machinery lives in `src/fabfos/build_references/` and deliberately does not
ship. See `docs/fabfos/architecture.md` for that split; the constraint it puts here is that a
type a run consumes must be declared on the run side and cannot be redeclared build-side, since
`LoadTypeLibraries` raises on a duplicate namespace.

## Compiled metadata is a build product

Every library carries a `_metadata/` compiled from its source YAML and transform Python. It is
**not tracked**, and its absence is fatal rather than cosmetic: a solve does not degrade to
resolving fewer types, it raises before planning begins, and nothing about a library that
resolves nothing announces itself in the GUI. Treat a fresh checkout as unusable until
`dev/libraries.sh -bm` has run. The template gate (`-b`) is a separate, much slower step —
an author's check that a changed transform still supports the shipped templates, not a
prerequisite for using the library.

The build enforces two invariants by failing — every declared tool env has a matching type, and
every type a transform reaches for exists — and a third by solving: a transform whose products
change shape takes its templates down here, by name, rather than in someone's GUI a week later.

## Environments

One `.env` per tool declares a container image and a conda env, serving both worlds; the engine
picks by the global runtime, and a transform never learns which it got. Declaring only one of
the two is what makes a tool unrunnable on the other kind of agent, and the staging preflight
refuses it there by name — in both directions.

`ExecWithContainer` and the retired per-runtime arms are rejected **statically** by the engine's
`dispatch_scan._FORBIDDEN_CALLS`, so a transform still calling one fails at scan time rather
than at run time. `ENV_PORT.md` beside this file is the record of that collapse.

**Pin container images by digest and say what the digest is.** A tag is not stable and a digest
is not readable, so a bare digest with no comment is a pin nobody can audit. Not hypothetical:
one tool was pinned to a digest resolving to an *older* image than its version number suggested,
carrying none of the library it advertised, and 80 tasks in one run died after their expensive
work had already succeeded. Container tags come from `mamba search -c bioconda`; biocontainer
hash suffixes are not guessable.

## Templates

A template is a starting point a user picks in the GUI: a `Spec` whose input paths are
`DEFERRED`, with its input library packed inline. **There is no template format** — it is the
same object a stored workflow is, so a template validates the way a workflow does, by solving.
Shipping them beside the transforms is what makes versioning free: a template arrives in the same
commit as the transforms it names and cannot be older than the library it was found in.

One that cannot ship stays listed as blocked with a reason rather than being deleted, so a build
that omits it does not read as "these are all the templates there are".

## ASPIRE

`transforms/aspire/` is an upstream standalone Nextflow amplicon/ASV pipeline re-expressed as a
typed graph, so the planner selects stages by what is asked for rather than by which of ~35 YAML
toggles was flipped. It is generated: one table row per ported process writes both the data types
and the stubs, each row carrying the source process and line so the port stays auditable.
Collapsing two nodes is a table edit and a regenerate — which stops being safe once the bodies
become real protocols. `ASPIRE_PORT.md` beside this file is the live correspondence map to
`research/aspire/upstream/`, and is what anyone extending those transforms needs.

## A rule that fails quietly, and belongs to this library rather than the engine

**Anything downloaded from an accession is named by whoever asked for it, not by its header.**
The name type sits above the accession type in lineage, so every product inherits it. The
downloading transform *requires* a name and never reads one — the declaration exists solely to
put it in the lineage — which means every caller registering an accession must register a name
above it. No GenBank field works instead: `ORGANISM` is bare species for most isolates,
`DEFINITION` carries the strain only sometimes, and two assemblies of one species collide under
either, after which PPanGGOLiN refuses the whole run over duplicate names.
