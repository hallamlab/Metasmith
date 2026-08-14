# Keio: can ECSPr predict the auxotrophy?

Three Keio single-gene knockouts, plus two controls, measured against the *E. coli*
K-12 background under the universal-leakage ground. `run_ko_panel.py` does the solves
and the ranking, `run_null_pool.py` builds the null that makes the ranking mean
something; the numbers are in `out/*.tsv`.

This file holds the reasoning that is not recoverable by reading the scripts. Per-step
behaviour is in each module's docstring.

## Why loss-of-function is the axis that works

The LASER harness next door had to abandon the obvious metric — rank the measured
target among a metabolite panel — because a gain-of-function study **has no negatives
on the metabolite axis**: a paper records what its authors assayed, never that nothing
else moved, and Rayleigh monotonicity makes conductance rise on any addition, so "the
target went up" is guaranteed rather than informative.

A Keio deletion inverts both problems. The phenotype is a *requirement* — this strain
now needs L-arginine in the medium and nothing else — and "nothing else" is an
experimental claim, not an absence of data. So every other metabolite in the same
solve is a genuine negative, and the other conditions' targets are negatives too,
since ΔargA is not a histidine auxotroph. The metabolite panel that LASER could not
construct is simply *there*.

## The draws are shares, and that decides how to read them

`measure_leak` injects one unit at the source and every metabolite drains to ground, so
`sum(draw) == 1` exactly, in every solve (checked). A deletion therefore cannot lower
everything: it pushes a small set down and lifts the whole rest by the redistributed
remainder. Two consequences that a naive reading gets backwards:

- **"The target fell" is the wrong question.** Under ΔhisG the field rises by
  +4.43e-5 and L-histidine rises by +4.36e-5 — it went *up*, and it is still the 17th
  most depleted metabolite of 991. The measurement is always relative to the field.
- **Magnitude and rank are different findings.** Across these four arms the required
  metabolite's move spans six orders of magnitude (−9.0e-2 for tryptophan, −7.6e-8 for
  arginine under ΔargA) while its rank stays inside the top 2%. Reporting either alone
  misrepresents the result.

`total` is a conductance, not a current, and is not comparable across arms the way the
draws are. Compare draws.

## Why the answer needs a null, and what bounds it

Three of the four arms move the target by 1e-5 to 1e-7 of a unit share. That is small
enough that "12th of 991" has to be earned. It is not a numerical artifact — `Keio:argD`
deletes no reaction (isozyme redundancy, `n_dead=0`), and its solve reproduces the
background draw *bit for bit*, 991 exact zeros, which is what establishes that the
solver is deterministic and that any nonzero move is a real response to the edit.

What determinism does not establish is whether the rank is special. So each target is
scored against 100 size-matched single-reaction deletions from two pools:

- **`keio`** — the reactions the *other* Keio genes delete. Matched in character: each
  is a real biosynthetic step some auxotroph lost, so a result here cannot be waved
  away as "random reactions sit in dead corners". This is the primary, and it is the
  harder one — its null median `frac_beaten` is 0.69, i.e. an arbitrary Keio deletion
  already leaves these amino acids above the field median.
- **`graph`** — uniform over every reaction in both the host GEM and the element's
  atom-pair table. Broader, says something about the graph rather than about
  biosynthesis.

`K=100` is a deliberate choice, not a default: the empirical p cannot resolve below
`1/(K+1)`, and at `K=30` that floor (0.032) sat above the Benjamini-Hochberg threshold
for the primary three, so the test could not have rejected at *any* effect size. The
floor is printed beside every p for exactly this reason.

## What it found

| condition | role | required | rel. change | rank of 991 | fallers | p (keio) | p (graph) |
|---|---|---|---|---|---|---|---|
| `Keio:argA` | first committed step | L-arginine | −7.6e-08 | 12 | 18 | 0.030 | 0.020 |
| `Keio:hisG` | first committed step | L-histidine | **+4.4e-05** | 17 | 4 | 0.079 | 0.040 |
| `Keio:trpE` | first committed step | L-tryptophan | −9.0e-02 | 4 | 8 | 0.030 | 0.0099 |
| `Keio:argH` | terminal step (positive ctrl) | L-arginine | −6.9e-07 | 9 | 26 | 0.020 | 0.020 |
| `Keio:argD` | deletes nothing (null ctrl) | — | 0.0 | — | 0 | 1.0 | 1.0 |

Under BH at α=0.05 over the primary three: against `graph` all three hold; against the
matched `keio` pool `argA` and `trpE` hold and `hisG` does not. The null control is
correctly not significant.

The 3x3 condition-by-target matrix (`out/panel_matrix.tsv`) puts the required
metabolite as the most-depleted of the three in **all three** distal conditions —
ΔargA ranks arginine at `frac_beaten` 0.988 against histidine 0.163 and tryptophan
0.080, and the other two rows separate as cleanly. Specificity is not the weak part.

**The pathway is a far stronger readout than the end product.** A deletion drives only
4 to 26 of 991 metabolites down, and that small set is the disrupted pathway, in order.
ΔtrpE's eight fallers are anthranilate, indole, N-acetylanthranilate, tryptophan,
indole-glycerol-phosphate, PRA and CdRP — the tryptophan branch and nothing else.
ΔargA's top eleven are the five acetylated arginine intermediates, ornithine,
citrulline and the polyamines, with arginine twelfth. If the question is "which
pathway did this knockout break", the answer is legible without any statistics; if it
is "which single compound will the strain need", the end product is the *weakest* place
in the pathway to read it.

Why the terminal compound attenuates is not settled here, and the obvious explanation
is wrong: it is not the target's connectivity. Tryptophan (degree 6) shows −9.0e-2
while histidine (degree 4) shows none of it, and L-histidine has no alternative
producer in this graph — the chain from PRFAR is the only route, yet a −1.0e-2 lesion
four steps up arrives as −7e-7 relative to the field. That attenuation is the live
question this pilot leaves open.

## Landmines

- **The cohort's mechanical answer key is orientation-naive.** `Y/expectations.tsv`
  expects every product of a deleted reaction to fall, taking "product" from the
  MetaNetX *as-written* orientation. MetaNetX writes `MNXR95843` backwards relative to
  biosynthesis (`anthranilate + pyruvate + L-glutamate = chorismate + L-glutamine`), so
  two of ΔtrpE's three key rows name compounds that are physiologically *substrates* of
  TrpE and should accumulate. ECSPr disagrees with the key on exactly those two rows,
  and that disagreement is not evidence against ECSPr. Do not read
  `out/mechanical_expectations.tsv`'s 3-of-6 as an accuracy.
- **A cofactor row is not a scoreable expectation either.** The same key expects CoA to
  fall under ΔargA. CoA's share is set by the whole network; one reaction does not move
  it.
- **Chorismate's `+8.8e-04` under ΔtrpE is the field, not a signal.** Every metabolite
  rose by that amount. Accumulation upstream of a lesion is not something this run
  detected, and claiming it would be reading the redistribution offset as biology.
- **The counterfactual pool is seeded once** (`run_null_pool.SEED`) and shared by both
  pools and every arm. Re-drawing per arm voids every head-to-head comparison. Note
  that changing `--k` re-draws the sample rather than extending it — the `K=30` and
  `K=100` runs are different pools, and only the `K=100` numbers are reported.
- **Carbon only.** The Keio conditions carry N/P/S rows and none were run. Cysteine and
  methionine auxotrophs in particular are the wrong thing to read off a C solve, and
  sulfate has no atom-mapped route to any S precursor in this reference tier at all.
- **tier4 atom pairs, not the canonical MNXref release** — the same caveat
  `examples/scadc_ecspr.py` carries. A pilot, not a number to publish as-is.
- **This worktree ships no submodules.** `docker/fabfos/bin/ecspr_cli.py` imports the
  ECSPr library from `src/metasmith_libraries/resources/lib`, so the run dies with
  `ModuleNotFoundError: ecspr_build` until the submodules are checked out. Do it from
  the sibling local bares rather than GitHub, which needs the file-transport override:
  `git -c protocol.file.allow=always -c submodule."src/metasmith".url=<…>/projects/metasmith/.bare … submodule update --init`.

## Order

    run_ko_panel.py → run_null_pool.py --k 100

Both are resumable from `cache/` and both need the `fabfos:local` image. `cache/` is
gitignored; `out/` is committed.
