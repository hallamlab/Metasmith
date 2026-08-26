# Fuhrer 2017 — the Keio / metabolome lane

Fuhrer et al., *Genomewide landscape of gene–metabolome associations in Escherichia coli*,
Mol Syst Biol 13:907 (2017), doi:10.15252/msb.20167150. 3,806 Keio single-gene deletions
profiled by flow-injection mass spectrometry against 7,534 metabolite ions in M9 glucose.

## Purpose & contents

This file says how the arm is wired and where each script's output goes. What the arm
*found* is in `REPORT.md`; what each script does and why is in that script's own module
docstring, which its `--help` prints. Nothing is transcribed here that `--help` already says.

## Why this paper was added to the campaign

Three arms preceded it — Eydallin (glycogen), Fang (free fatty acids), Woodruff (solvent
tolerance) — and all three returned the same verdict against the same three weaknesses: a
handful of positives, a one-sided label set, and no external baseline. This screen removes
all three at once, and that is the whole reason for the port:

- **Measured negatives.** Every gene in the population was profiled. A negative here is a
  deletion the instrument read and found not to move the metabolite, not an unlabelled gene
  assumed innocent.
- **A signed label.** The z-score's sign says whether the metabolite rose or fell, so the
  probe can be asked *which* and not only *whether*.
- **Hundreds of candidate sinks** instead of one, which is what makes a genome-wide arm
  possible at all.
- **A published competing number.** Table EV4 carries a per-ion ROC AUC for the same
  gene–metabolite association.

It is also the first **deletion** arm run on the ratio protocol. Fang and the 2010 Eydallin
arm overexpress at fold 2; this one deletes at fold 0, and the difference is not cosmetic —
see *Things that would go wrong silently*.

## The chain

Every script takes `--publish`; without it nothing is written and the counts are printed.
Environments differ by stage and are in each script's usage line.

### Acquire and decode

    parse/acquire.py                 -> data/fabfos/originals/benchmarks/fuhrer/
    parse/decode_screen.py           -> runs/fuhrer_clones/parse/screen/

`acquire.py` copies the article and its four supplements from `~/downloads` and fetches
seven files from EBI BioStudies S-BSST5. `decode_screen.py` turns the two headerless
matrices and their four BIFF8 key workbooks into a gene roster, an ion key and two parquet
matrices, behind four independent guards on the positional joins.

### Resolve and declare

    parse/resolve_readouts.py        -> runs/fuhrer_clones/parse/readout_resolution.tsv
    sinks/declare_axes.py            -> benchmarks/fuhrer/{axes.tsv,axis_candidates.tsv}
    sinks/declare_wide_axes.py       -> benchmarks/fuhrer/axes_wide.tsv

Every KEGG compound the screen puts on an ion, carried to a MetaNetX node or to the reason
it is not one. Then two panels: eight declared axes chosen by the paper's own AUC, and 105
chosen by reachability and power alone. **The two are a pair and neither stands alone** —
see the caution in `declare_axes.py`.

### Cohort

    parse/build_lof_table.py         -> runs/fuhrer_clones/parse/lof/{lof.csv,lof_resolution.tsv}
    parse/build_lof_reactions.py     -> rewrites lof.csv, writes lof_reaction_edges.csv
    parse/write_y_sidecars.py        -> benchmarks/fuhrer/Y/  (and Y_wide/ with --axes)
    parse/build_extraction.py        -> benchmarks/_extractions/fuhrer/extraction.tsv
    gpr_build/build_fuhrer_gpr.py    -> runs/fuhrer_clones/gpr/

### Measure

    sinks/probe_axes.py --channel {gem,denovo} [--axes axes_wide.tsv]
    sweeps/sweep_fuhrer_ratio.py --channel gem --roles target control --workers 10
    sweeps/sweep_fuhrer_ratio.py --channel denovo --roles target control --min-lanes 2 --workers 10
    sweeps/sweep_fuhrer_ratio.py --channel gem --axes ...axes_wide.tsv --y-dir ...Y_wide

`probe_axes.py` must run first for each (channel, panel). The sweep refuses to start without
its output, because a sweep over an unreachable sink returns a flat column that is
indistinguishable from a real null.

### Score

    sweeps/analyse_fuhrer_ratio_sweep.py     the eight declared axes
    sweeps/analyse_fuhrer_wide.py            the 105-metabolite distribution
    sweeps/reproduce_published_auc.py        can Table EV4 be rebuilt from this tree?
    ../ratio_cross_arm.py                    all four arms in one cut

## Things that would go wrong silently

- **The positional joins.** The z-score matrices are headerless numeric grids and their row
  and column keys are four separate workbooks. An off-by-one produces a complete, plausible,
  entirely wrong benchmark. `decode_screen.py` proves the correspondence four ways and exits
  rather than writing; the guards are named G1–G4 in its docstring.
- **`wt` is a matrix column.** The deposit's 3,807 columns are 3,806 deletions plus a
  wild-type control, and nothing flags it. Every consumer excludes it by name.
- **Two z thresholds, used for different things.** `2.765` is the reproducibility threshold
  that defines a positive on one ion. The `0.1` percentile is the tail cutoff that defines
  how busy a mutant is across all ions. Swapping them changes what every count means.
- **The gene name is not a join key.** 646 of this screen's 3,806 names have been retired
  since 2017. The curated channel joins on the b-number, the de-novo channel on the BW25113
  protein accession, and both are resolved once in `lof_resolution.tsv`.
- **A deletion can remove a terminal from the graph.** At fold 0 the sink itself can stop
  being a node. The fang arm's probe treats that as fatal; here it is a named state, because
  `SystemExit` inside a pool worker is not caught and the gene vanishes from the output
  instead of failing the run.
- **The denominator can reach zero.** `finite`, `sink_severed`, `isolated`,
  `sink_not_node` and `source_not_node` mean different things and are recorded as a column
  rather than inferred downstream from `isinf`.
- **Resolving a metabolite by name.** The eydallin arm records that a name lookup returns the
  wrong identifier for ADP-glucose and glucose-1-phosphate, and the failure presents as
  "absent from the graph". Everything here goes through `chem_xref.tsv`.
- **`in_atom_universe` is null on the de-novo channel** and is recomputed with transport
  excluded. Defaulting it is wrong in a different direction on each channel.
- **DVC hands checkouts out as read-only hardlinks.** Every writer unlinks first, and the
  sweep opens its partial file only when there is something to append.

## What the deposit supplies and what is deliberately not fetched

S-BSST5 carries ten files. Seven are acquired. `rawdata_neg_all.tsv` and
`rawdata_pos_all.tsv` (543 MB, four columns per gene) and `sample_id_all.xls` (their gene
key) are **not**: the paper's own unit of analysis is the modified z-score, this arm
re-derives nothing upstream of it, and the gap is recorded rather than silent.

The paper does not supply the adjacency set behind Table EV4's AUC. That is why
`reproduce_published_auc.py` exists and why its answer changes how the column may be used.

## Where the numbers are not comparable

- **Across channels.** The de-novo background carries six times the reactions and every
  conductance is larger there. Only ranks within one channel mean anything.
- **Across arms, for anything but a rank.** This is the only fold-0 arm in the campaign.
  Severing an edge and doubling one are different sizes of perturbation, so the `movers`
  column in the cross-arm cut does not cross that boundary. AUC, the size control and sign
  agreement against a base rate all do.
- **Against Table EV4, as though it were the same task.** It is the transpose: Fuhrer ranks
  by the phenotype and labels by adjacency; this arm ranks by topology and labels by the
  phenotype.
