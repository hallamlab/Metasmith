# Benchmark campaign findings

## Purpose & Contents

This directory holds what research support established for the five benchmark experiments (E1 nf-core/mag parity, E2 metasmith parity, E3 Pratama, E4 metaGEM, E5 optimized). It holds findings only. The experiment design lives in the "Five Benchmark Experiments" artifact.

- `PROVEN.md` is the capability inventory. Each transform, nf-core process and install carries an OBSERVED, SOURCE or UNPROVEN status with its evidence.
- `BLOCKERS.md` lists the active blockers (B1–B12), the relaunch preconditions and the known latent defects.
- This README records design findings that belong in neither ledger.

CAUTION: research support edits the ledgers in `~/scratch/cami_campaign/` from a different worktree. The copies here are a snapshot. Copy them again before each commit.

## Pratama Nanopore data is a per-run hybrid, not a pool

`research/pratama2026/runs.tsv` shows one MinION run per well, all from the 0.2 µm fraction. Pratama assembled each with every 0.2 µm 2022 Illumina replicate from the same well in a hybrid metaSPAdes `--nanopore` assembly. H14, H32, H41, H52 and H53 have 3 replicates each and H51 has 2, which gives the paper's 17 hybrid assemblies. The 0.1 µm fraction has no Nanopore partner.

CAUTION: `spades.py` reads short reads only and Flye reads long reads only, so parity needs a new transform. The viral merge needs every run under one `viromics::contig_study` root. Separate `read_metadata` per sample, the CAMI fix for read-type crossing, is therefore unavailable. Parent read-type-sensitive requirements to a specific read or assembly ancestor instead.

## CAMI III

- The toy human gut long reads are simulated ONT R10 (mean 3,998 bp). E1 and E2 use them as the long-read set.
- The CAMI III challenge dataset released on 2026-07-14. Its gold-standard assemblies release on 2026-10-15 and binning closes on 2027-01-31. Nothing can score it before then, so the campaign uses the toy set plus the CAMI II challenge sets.

## metaGEM published products

All five studies publish MAGs, proteins, assemblies and models on Zenodo. `research/metagem/README.md` lists the records and the fir location. E4 starts from these MAGs. E5 still needs the reads.

## nf-core/mag DAG

`nextflow run nf-core/mag -preview -with-dag dag.dot` is the only export that carries channel names on edges. The `.mmd` export has none, and `dag.verbose` does not add them. `nfcore/dot_to_msm.py` converts the `.dot` graph into metasmith's `DagRenderer` with no hand mapping.
