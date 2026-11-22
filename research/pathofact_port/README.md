# PathoFact port: comparison against upstream PathoFact 2.0 (planned, not run)

This file plans the one-sample check that the port reports what PathoFact 2.0 reports.
The library runs PathoFact's report rules over its own lanes rather than running PathoFact's
Snakemake pipeline. The upstream source is
https://gitlab.com/uniluxembourg/lcsb/systems-ecology/pathofact2 at commit `bd5d527`
(branch `submission`). The old GitLab address in the preprint no longer serves it.

- PathoFact's own parts are lanes of the annotation table. `pathofact_hmm` searches its toxin
  and VF CDD HMMs at bitscore 40. `pathofact_ml` runs its own `predict.py` and
  `vf_prediction2.py` from `ref::pathofact_db/src`. `deeparg` runs the LS model at
  `--min-prob 0.7`.
- RGI, AMRFinderPlus, geNomad and antiSMASH are the library's existing lanes.
  `lib::fabfos_gpr/pathofact_report.py` applies PathoFact's cut-offs to them.
- `probe_pathofact_solve.py` is the solve.

## Run

One CPE3 sample on fir, both arms on the same assembly:

1. **Ours.** Target the six `annotation::pathofact_*` reports. Add
   `annotation::pathofact_report_signalp` once the `signalp6` lane from
   `feat/engine/licensed-build` is merged.
2. **Upstream.** Run PathoFact 2.0 at `bd5d527` on the assembly as a `contigs` sample.
   Use `Config.yaml` defaults, with SignalP on to match step 1's SignalP report.
   `quay.io/hallamlab/external_pathofact:2.0` holds the pipeline and its conda envs. It is
   not anonymously pullable.

## Compare

Join the rows by contig, strand and gene overlap. Upstream genes are geNomad's
(pyrodigal-gv). Ours are Prodigal's.

- Per protein: membership in each report, `Query_*`, `Rfprob_*`, the three ARO columns,
  `Plasmid_marker`, `Virus_marker` and `BGC`. Report each disagreement by cause.
- Expected causes:
  - The gene caller.
  - The cluster representative. Ours is the first identical copy in FASTA order.
  - RGI run with BLAST, not DIAMOND (`-a DIAMOND` upstream).
  - AMRFinderPlus 4.2.7 against 3.12.8, with a newer database.
  - geNomad 1.11 against 1.8. The conservative preset is cut from a default run, so a
    contig can win where upstream kept its provirus.
  - antiSMASH's own gene finding (`prodigal-m`) against pyrodigal-gv genes.
- Expected and not a finding:
  - The `Rfprediction_*` and `Rfprob_*` columns are always present. Upstream drops them
    when no protein is called positive.
  - Proteins carrying an `X` get a VF probability. Upstream's VF encoder fails on them.
