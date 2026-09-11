# Curation round 2 -- normalising the universe

## Purpose & Contents

This file is the in-repo record of what round 2 produced and which table owns what. The argument
lives in the published report, not here, so that it lives in one place and cannot drift:

**<https://claude.ai/code/artifact/5c91173d-f2e3-4789-ad53-82587f03a357>**

Round 1 reduced 3,965 public narratives to 1,633 workflow shapes and judged none of them. Round 2
proposes the vocabulary a curated library would be built from: what an app *does*, what a workspace
type *is*, and which of KBase's 2,758 declared parameters are knobs. **It proposes. It does not
build.** No transform, type or template is written here.

## The four axes

| axis | before | after | |
|---|---|---|---|
| apps | 493 in the catalog | 54 core task verbs | 47 scientific, 7 plumbing |
| workspace types | 121 referenced | 35 entities | 93 fully specified meanings on an 11-axis grid |
| parameters | 2,758 declared | 262 config fields | 239 distinct names in 39 config types |
| workflow shapes | 1,633 wired | 631 canonical workflows | 83 distinct product-from-source names |

Of the 67 parameter unions, 40 collapse to one entity and 24 to one declared family. The three
survivors all belong to apps that accept every object type by design.

## Rebuild it

    curate.py            # the four stages, then the gate
    report_data.py       # every number the report shows, read from the tables
    build_report.py      # inject that into report.tmpl.html -> report.html

`curate.py` ends by running `check.py`, which exits non-zero rather than leaving the tables in a
state that does not add up. Each of its 22 assertions joins back to `catalog/`, `curation/r1/` or
the census, never to the file it is checking.

## What each file owns

| file | owns |
|---|---|
| `tasks.yml` | the verb vocabulary, each verb's proposed signature, and the rules that assign apps to it |
| `app_tasks.jsonl` | one row per catalog app: verb, the rule that assigned it, narrative copies, canonical flag |
| `types.yml` | the entities, the property axes and the families |
| `type_map.yml` | every referenced workspace type placed on that grid, authored |
| `type_map.jsonl` | the same, resolved, plus one row per parameter union |
| `task_io.jsonl` | each verb's proposal beside what its apps actually declare |
| `config.yml` | the dispositions, the normalisation rules and the shared knob vocabulary |
| `param_map.jsonl` | one row per declared parameter: normalised name, verb, disposition |
| `workflows.jsonl` | one row per canonical workflow, with its member shapes and copies |
| `report.tmpl.html` | the report's argument and layout, with a `/*__DATA__*/` placeholder |

`report.html` and `report_data.json` are build products of the two above them.

**CAUTION** The parameter tables read `catalog/params.jsonl`, which this round added to
`scrape/distill.py`. `apps.jsonl` keeps only the parameters that carry a workspace type, because
that is all the generator can model. Re-running `distill.py` rewrites `apps.jsonl`, `types.jsonl`
and `census.json` as well -- diff them and prove nothing but the parameter table moved, because the
generator asserts that census and ledger reconcile.

## What is left open

The proposal rests on one untested bet, stated in the report and repeated here because a later
round has to test it: **making the 46 pure sources deferred template inputs rather than transforms
is what would remove the need for a per-workflow mask.** Nothing here proves it. Neither does the
claim that a `qc` axis stops an importer answering a target downstream of trimming.

Thirteen verbs' most-used implementation declares no typed output, and no workspace type anywhere
in KBase carries a taxonomic assignment. An output override table for those thirteen buys more
coverage than any change to the planner.
