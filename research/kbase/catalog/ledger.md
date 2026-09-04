# Conversion ledger

One row per app in the census. `ledger.jsonl` is the machine-readable form.
The generator asserts this file's totals against `census.json` and fails when
they disagree, so a silently skipped app cannot happen.

## Headline

**Of the 235 active KBase apps, 215 became metasmith transforms.**
Across all 493 apps in the catalog, 370 converted.

## Active apps by disposition

| reason | apps |
|---|---|
| converted | 215 |
| no_product | 18 |
| polymorphic_output | 2 |

## All 493 apps by disposition

| reason | apps |
|---|---|
| converted | 370 |
| no_product | 121 |
| polymorphic_output | 2 |

## What the reasons mean

- **converted** — a stub transform exists under `library/transforms/kbase/`.
- **no_product** — the app names nothing it creates: no typed output parameter and
  no KBaseReport in its service output mapping. These are in-browser visualizations,
  a staging exporter and an ACL update. A planner has nothing to target.
- **polymorphic_output** — an output parameter accepting more than 4 workspace
  types. These are generic object shufflers, not workflow steps.

## Library

189 data types, 106 tool environments, 370 transforms.
