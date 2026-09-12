# Regenerated 4-lane GPR tables — 2026-08-18

The mapper outputs from the tuned lanes. **These are PUBLISHED**: each one was passed
through its publisher and now backs the DVC-pinned table for its host.

| file | accession | published to | rows | pbert | clean |
|---|---|---|---|---|---|
| `8vBqTmYA__NC_000913.3` | NC_000913.3 | `e_coli_k12` | 31,338 | 6,898 | 20,032 |
| `AcPrhjVJ__NC_017638.1` | NC_017638.1 | `e_coli_dh1`, and `e_coli_ag1` by derivation | 30,722 | 6,751 | 19,566 |
| `O1pJ5sNh__CP165600.1` | CP165600.1 | `e_coli_w3110` | 31,657 | 6,984 | 20,240 |
| `ZNhV7mvU__NC_010473.1` | NC_010473.1 | `e_coli_dh10b` | 31,246 | 6,788 | 20,033 |
| `l0Ka6ikn__CP189566.1` | CP189566.1 | `e_coli_epi300` | 31,353 | 6,826 | 20,088 |
| `eUzXfepL__CP193896.1` | CP193896.1 | `e_coli_bw25113`, and `e_coli_lw06` by derivation | 30,838 | 6,761 | 19,679 |
| `YJhzIWGG__eydallin_clones` | — | `eydallin_clones` | 642 | 152 | 376 |

`ag1` withholds DH1's four loss alleles (11 rows) and `lw06` withholds bw25113's seven
(240 rows); both derive from their parent's table, so publishing the parent is what
publishes them. All nine published tables pass `validate_gpr` and carry
`clean_confidence`.

Produced on Sockeye (SLURM 12644126 / 12644139, plus one direct run for CP193896.1) from
the staged lane outputs under
`/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/runs/<run>/results`. The annotators
were NOT re-run — nothing in this work changes them. What changed is the landmark set
(`ref::label_transfer_landmarks`, rebuilt) and two lane rules (the pbert quota, CLEAN's
direction and abstain).

## What still carries the old lanes

Every de-novo E. coli host and the eydallin clones are now on the new lanes. These are
not, and they will read as a different measurement until they are re-run:

- `scadc_metagenome/gpr_4lane` — 1.4M ORFs, real queue work.
- `scadc_fosmids/gpr_4lane` and `gpr_7lane` — lanes not located on this cluster.
- `aska/gpr_denovo` and `scales/gpr_denovo` — cohort tables, no staged run here.
- the 3 nostoc annotation tables and the 8 nostoc ecspr networks — derived from those.

`validate_gpr` names them itself: they carry the retired `clean_maxsep_inv` score_kind
and print a warning on every read, so a mixed comparison announces itself rather than
passing silently.

## Unrelated, pre-existing

`e_coli_epi300/gpr/gpr_epi300_clone2.parquet` and `gpr_union_epi300_183.parquet` carry a
half-present `attribution` block (`build_id` and `host` without `unit_id`), so
`extensions_of` refuses them. Neither was touched here and neither is a de-novo host
table; noted because a reader validating the tree will hit them.
