# Regenerated 4-lane GPR tables — 2026-08-18

Mapper outputs from the tuned lanes, **staged, not published**. They are what
`gpr_4lane` writes; the published `data/fabfos/runs/<host>/gpr/gpr_denovo.parquet`
is that plus the attribution / feature / universe blocks added by
`build_references/host_denovo_from_mapper.py` (hosts) or
`research/fabfos/benchmarks/eydallin/build_clone_gpr_denovo.py` (clones).

Republishing over the DVC-pinned host tables is a scope decision, not a mechanical
step: other scopes and already-published figures read them, bench-eydallin's glycogen
numbers among them. These sit here until that call is made.

Produced on Sockeye (SLURM 12644126 / 12644139) from the staged lane outputs under
`/scratch/st-shallam-1/txyliu/fabfos_b2/agent_home/runs/<run>/results`. The annotators
were NOT re-run — nothing in this work changes them. What changed is the landmark set
(`ref::label_transfer_landmarks`, rebuilt) and two lane rules (the pbert quota, CLEAN's
direction and abstain).

| file | accession | host | rows | pbert | clean |
|---|---|---|---|---|---|
| `8vBqTmYA__NC_000913.3` | NC_000913.3 | e_coli_k12 | 31,338 | 6,898 | 20,032 |
| `AcPrhjVJ__NC_017638.1` | NC_017638.1 | e_coli_ag1, e_coli_dh1 | 30,722 | 6,751 | 19,566 |
| `O1pJ5sNh__CP165600.1` | CP165600.1 | e_coli_w3110 | 31,657 | 6,984 | 20,240 |
| `ZNhV7mvU__NC_010473.1` | NC_010473.1 | e_coli_dh10b | 31,246 | 6,788 | 20,033 |
| `l0Ka6ikn__CP189566.1` | CP189566.1 | e_coli_epi300 | 31,353 | 6,826 | 20,088 |
| `YJhzIWGG__eydallin_clones` | — | eydallin_clones | 642 | 152 | 376 |

Two accessions serve two hosts each, so the mapping is not 1:1 — resolve before
publishing.

All six pass `fabfos_evidence.validate_gpr`. The `.log` beside each is the mapper's
own stdout, which reports how many ORFs the pbert lane refused and how many CLEAN
calls fell below the abstain.

**Not regenerated here** (25 of the 31 affected tables): `CP193896.1` (bw25113, lw06)
— its run has more than one candidate file for a lane, so the batch skipped it rather
than guess; `aska` and `scales` — no staged run on this cluster; the 3 nostoc
annotation tables and 8 nostoc ecspr networks — derived from those; `scadc_fosmids`
4lane and 7lane — lanes not located; `scadc_metagenome` — 1.4M ORFs, real queue work.
