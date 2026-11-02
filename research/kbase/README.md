# The KBase port

KBase publishes a catalog of typed bioinformatics apps and a public archive of Narratives, the
notebooks in which its users record a real analysis app by app. This tree turns the first into a
metasmith transform library and the second into metasmith templates.

## Purpose & Contents

This file states what each subfolder owns and which one is authoritative for what. It is the entry
point for anyone resuming the port.

The correspondence from a generated transform back to the KBase app that produced it is
`library/KBASE_PORT.md`. The endpoints and dates every artifact came from are `upstream/PROVENANCE.md`.
What the port achieved is `reports/`.

**CAUTION** This tree is a research port. `library/` is not shippable library content and its
transform bodies are stubs that touch their outputs. The library compiles and plans. It does not run
a KBase app.

## Layout

| directory | owns |
|---|---|
| `scrape/` | the fetchers, and only the fetchers |
| `upstream/` | `PROVENANCE.md` — service, endpoint and date per artifact |
| `catalog/` | the distilled per-app, per-parameter and per-type tables, the census and the conversion ledger |
| `library/` | a self-contained metasmith library root, and the generator that writes it |
| `narratives/` | extracted app cells and the reconstructed dependency DAGs |
| `curation/` | one numbered round per curation pass, `r1/` … `r5/` — a round never edits an earlier one |
| `templates/` | template author modules |
| `reports/` | findings |

The bulk corpus lives at `data/kbase/` under DVC. Raw app specs and narrative objects never enter
git.

## `library/` is a library root, not a subdirectory of one

`library/` has the same shape as `src/metasmith_libraries/` — `data_types/`, `transforms/`,
`resources/`, `templates/`, and its own `_authoring.py`. That is what keeps the port separate from
the standard library while staying loadable.

The shape works because nothing resolves against a hard-coded standard library root.
`Template.Load` derives its root from the template's own path, `library_index` reads `data_types/`,
`transforms/` and `resources/` beneath it, and `_authoring.py` anchors on its own directory. A
template saved into this root records its libraries by bare name and resolves them back here. That
was measured, not assumed: a two-type probe library round-tripped a template through save and load
with an empty `unresolved` list and re-solved after loading.

Build it by naming its directories explicitly, since `dev/libraries.sh` is hardcoded to the standard
library:

    PYTHONPATH="$PWD/src" mamba run -n msm python -m metasmith build all \
        --types research/kbase/library/data_types \
        --uniques research/kbase/library/resources/env \
        --transforms research/kbase/library/transforms/kbase

**CAUTION** `_metadata/` is a build product and is not tracked. A library without it raises before
planning begins rather than resolving fewer types.

## Counting is part of the deliverable

`catalog/census.*` fixes how many apps, types and narratives KBase has, before any conversion
decision can move the denominator. `catalog/ledger.*` carries one row per census app with its
disposition — converted, or dropped with a named reason. The generator asserts the two reconcile and
fails when they do not.

A silent drop is the failure this exists to catch. A generator that skips forty apps still emits a
library that compiles clean, and nothing about it announces the gap.
