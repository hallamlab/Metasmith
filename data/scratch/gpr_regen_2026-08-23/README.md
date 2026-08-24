# Re-annotated from source — 2026-08-23

The 2026-08-18 regen re-ran the mapper over lane outputs that lived only on Sockeye. This
one re-ran **the lanes themselves**, so every host and the clone cohort now has an
`annotations/lanes/` in the tree that its de-novo table is actually built from. That was
the point: eleven of thirteen runs had a de-novo GPR table with nothing behind it here.

| run | site that won | rows | pbert | clean | matches 08-18 |
|---|---|---|---|---|---|
| `e_coli_k12` | sockeye | 31,338 | 6,898 | 20,032 | yes |
| `e_coli_dh10b` | sockeye | 31,246 | 6,788 | 20,033 | yes |
| `e_coli_epi300` | sockeye | 31,353 | 6,826 | 20,088 | yes |
| `e_coli_dh1` | **fir** | 30,722 | 6,751 | 19,566 | yes |
| `e_coli_bw25113` | sockeye | 30,838 | 6,761 | 19,679 | yes |
| `e_coli_w3110` | sockeye | 31,657 | 6,984 | 20,240 | yes |
| `eydallin_clones` | sockeye | 642 | 152 | 376 | yes |

Row-for-row agreement with the previous campaign is the evidence that re-running the
annotators measured the same thing rather than a new one. `e_coli_ag1` (30,711, DH1 minus
four loss alleles) and `e_coli_lw06` (30,598, BW25113 minus seven) derive from their
parents; `aska` and `scales` read AG1 and BW25113 in turn.

k12, dh10b and epi300 also lost the 2024 lane trees they had been carrying beside the new
ones — those keyed on `NP_414542.1` / `ECDH10B_0001` and overlapped their own tables by
zero ORFs.

## Four defects had to be fixed before any run could finish

Each was found by a run that reported `completed`. Nextflow ignores a process that
exhausted its retries, so a workflow with a dead step is green — the driver's `verify()`
is the only thing that says otherwise, and it was itself wrong (below).

- **`proteinbert` chunk order.** The guard against stacking chunk 10 before chunk 2
  matched a *trailing* integer; the image writes `<stem>.<k>.embedding.npy` and announces
  that pattern itself. No four-lane run had passed this step since the guard landed.
- **`python_for_data_science` at tag 1.4.0.** No pyarrow, and pandas 3.0.5 has no parquet
  engine without it. `gpr_4lane` imports pyarrow at module level, so it died after all
  four lanes had already succeeded. Pinned back to 1.2.5.
- **Unstamped images on a read-only store.** The engine gates on `<sif>` AND
  `<sif>.verified`; an unstamped image sends the task to re-stamp it, which flocks the
  store. Sockeye's is `/arc`, read-only from a compute node, so it fell through to
  `apptainer pull` and `no route to host`. Preflight checked existence only, and now
  checks both.
- **`landed_products` read only `_manifests/`,** which these result trees do not have, so
  it called all seven targets absent from a run that had produced six.

## Running it again

`clone_gpr_on_hpc.py --site {fir,sockeye} --run` per ORF set, then
`rebuild_gpr_cascade.py` for everything downstream. Two things about scale, both measured
here rather than guessed:

- **A full nine-step run is ~15 minutes**, not the four hours the declared KOfam walltime
  implies. Those are ceilings.
- **fir's login node caps around six concurrent Nextflow drivers.** The seventh died with
  `unable to create native thread` before submitting anything — zero tasks recorded, and
  the other six unaffected.

`race_sites.py` runs both clusters per set and cancels the loser on the first exit 0.
`sbatch --test-only` predicted a two-day GPU wait on *both* clusters; the real wait was
twenty minutes, which is why racing beats picking.

## What still carries the old lanes

- `scadc_metagenome/gpr_4lane` — 1.4M ORFs, real queue work.
- `scadc_fosmids/gpr_4lane` and `gpr_7lane`.
- the 3 nostoc annotation tables and the 8 nostoc ecspr networks, derived from those.

These four keep their existing annotations and stay on the old mapper contract.
`validate_gpr` names them itself — they carry the retired `clean_maxsep_inv` score_kind
and warn on every read, so a mixed comparison announces itself.
