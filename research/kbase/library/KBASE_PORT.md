# The KBase port

## Purpose & Contents

This file is the correspondence map between a generated transform and the KBase
app it came from, and the record of what the port does to the source that a
reader could not infer from either side. It is the file to read before changing
`_generate.py`.

What the library *is* and how the tree is organised is `research/kbase/README.md`.
Where the material came from is `research/kbase/upstream/PROVENANCE.md`. Counts
are `research/kbase/catalog/census.md` and `ledger.md`.

**CAUTION** Every transform body here is a stub that touches its outputs. The
library compiles and plans. It does not run a KBase app.

## One generator, one table, one row per app

`_generate.py` reads `research/kbase/catalog/apps.jsonl` and writes
`data_types/kbase.yml`, `data_types/env.yml`, every `resources/env/*.env` and
every stub under `transforms/kbase/`. Nothing here is edited by hand.

Each stub's docstring carries its app id, spec version, module, git URL, git
commit and service method. That is the correspondence map: any node traces back
to the spec that produced it without consulting this file.

**CAUTION** The generator is authoritative only while the bodies are stubs.
Regenerating after someone writes a real protocol destroys it. Delete
`_generate.py` at that point, or keep it solely as the record of provenance.

## Five things the port does to the source

KBase and metasmith do not describe a workflow the same way. Each difference
below is a decision, not a translation.

1. **A report is a product.** 207 of the 235 active apps declare a `KBaseReport`
   in their service output mapping rather than by a parameter flag, and 97 of
   them declare nothing else. They produce `kbase::report`. Without this the port
   covers 138 active apps instead of 216.

2. **An optional parameter is not a requirement.** A metasmith requirement is
   mandatory, so modelling KBase's optional inputs as requirements makes
   `run_flux_balance_analysis` demand an expression matrix nobody has. 138 of the
   599 typed input parameters are optional. Each stub lists the ones it dropped.

3. **A multi-typed parameter becomes a union.** 25% of typed parameters accept
   more than one workspace type. A union is one bare property token that every
   member type also carries, so membership is structural subtyping.
   **`_` is a matched property, not a comment** — a union carries its token and
   nothing else, or it is satisfied by nothing and every solve using it silently
   returns no plan. The lint asserts each union is satisfied by each member.

4. **A multi-typed output becomes a product group**, one branch per type, capped
   at eight. Past that the app is a generic object shuffler rather than a
   workflow step and is dropped. One app hits the cap: `KButil_copy_object`,
   which accepts 58.

   The cap was 4 first, and 4 was wrong. `kb_cutadapt/remove_adapters` declares 6
   output types because an adapter trimmer preserves whatever read type it was
   handed, so it looked polymorphic while being an ordinary pipeline step. It
   alone blocked 18 of the 623 curated workflows, and the failures surfaced not
   as a low solve rate but as a compile error: a masked library holding no
   transform at all has no `_metadata/` to load.

5. **Lineage is invented.** KBase has none. Every transform requires
   `kbase::study`, then `kbase::sample` with the study as parent, and every typed
   input descends from the sample. Without ancestry the planner cannot tell two
   Genomes apart, and 130 apps produce a Genome.

## A mask is not optional

46 of the 370 transforms are pure sources: uploaders and importers that require
nothing. They are therefore the cheapest producer of anything, and an unmasked
solve answers every target with "import it from staging" — a complete, correct,
useless plan. `mask.py` builds the view a solve is allowed to use.

`transforms/w_*` are the committed masks, one per shipped template. A template
records its transform library by bare name, so a mask cannot be a temporary
directory — each shipped workflow owns one.

## The templates

`templates_src/*.py` are the author modules and `build_templates.py` runs them
all, exactly as the standard library does. Each is one workflow reconstructed
from the public narratives that ran it, named `<product>_from_<input>`, with the
copy count in its docstring. `templates/*/spec.yml` is what falls out; it is
never written by hand.

## Environments

One env per app module, container arm only. KBase ships images and no conda
recipes, and an arm that has never been run answers "can this tool run without a
container" wrongly.

KBase tags embed the module's git commit, so a tag is already immutable by
construction. The `.env` files pin the digest anyway, resolved from
`dockerhub-prod.kbase.us` on 2026-09-04, because a digest is what this library
pins by. That registry sits behind Cloudflare and answers urllib's default
User-Agent with 403.

## What the catalog itself gets wrong

15 of the 121 referenced workspace types do not resolve: 7 retired types, 6 whose
type module is gone, and 2 that are not type names (`*`, and a bare
`KBaseMatrices`). 48 apps reference at least one. The port declares them anyway,
from the app spec's own naming, and each affected stub says so.
