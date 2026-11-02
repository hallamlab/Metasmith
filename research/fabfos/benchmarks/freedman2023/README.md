# `freedman2023` — CANDIDATE, not adopted

Freedman, B.G.; Lee, P.W.; Senger, R.S. *Engineering the Metabolic Profile of Clostridium
cellulolyticum with Genomic DNA Libraries.* Fermentation 2023, 9, 605.
<https://doi.org/10.3390/fermentation9070605>

Cohort `freedman2023`, arm `gof`, host *Ruminiclostridium cellulolyticum* H10 ATCC 35319
(CP001348.1). Random-fragment overexpression library (DOP-PCR, `thl` constitutive promoter,
multi-copy pSOSGate), serially enriched on cellobiose.

This directory lives under `main/` and NOT under `data/benchmarks/` deliberately. It is a
read of the paper made to decide whether the study is worth adopting, and the answer is no.
See `ASSESSMENT.md`. Nothing here is wired to a reader; `in_base` is `UNKNOWN` throughout
because no host background exists for this organism.

| | |
|---|---|
| inserts | 4 |
| inserts with a measured phenotype | 1 |
| inserts with a mappable edge | 3 |
| inserts with **both** | **0** |
| conditions | 1 (GS-2 + 6 g/L cellobiose, 34 °C, anaerobic, batch, 118 h) |
| elements | C only |
| host background | none — every `gpr_gem.parquet` under `data/fabfos/runs/` is an E. coli one |

## The library

40,000 *E. coli* colonies in pCR8, 100,000–120,000 clones in pSOSGate — but
electroporation into the Clostridium host is the bottleneck and delivered **fewer than
2,000 mutants**. The genomic library covers 76% of the reference genome at 1× and 26% at
5×. After ten serial re-inoculations only two unique fragments remained in the genomic arm
(n = 10 colonies) and two in the metagenomic arm (n = 18). The four survivors are the whole
dataset.

Fragments are explicitly "not full-length genes". Every insert in this study is a partial
ORF under a strong constitutive promoter, which is the confound that runs through
everything below.

## Metabolic input is invariant, and that is the one good property

Cellobiose consumption rates were "nearly identical" and complete by 96 h in both arms, and
acetate was likewise unchanged. Ethanol went 2.4 → 8.8 mM at 118 h, lactate fell ~50% to
≤11 mM, pyruvate rose 250%, OD600 fell ~20%.

Same carbon in, different carbon out. That is a compositional redistribution across
competing sinks — exactly the shape `delta_clr` exists to score, and the reason this paper
looked promising. The carbon budget does not visibly close from the text alone (ethanol
gains ~12.8 mM C, lactate frees ~33 mM C, pyruvate has no absolute number); closing it
needs Figure 2 digitised.

## Why the axis has to be a share, not a total

For a pure gain-of-function addition the atom graph only ever gains edges. In the symmetric
limit (`gm == gp`, see `src/ecspr/graph.py:_undirected_phi`) the solve is a plain resistive
Laplacian, and Rayleigh monotonicity makes effective conductance between any two nodes
non-decreasing when an edge is added. A two-terminal `delta I_eff` from cellobiose to
ethanol is therefore `>= 0` by construction for every insert, and its sign carries no
information.

The informative measurement is ethanol's **share** of an aggregate ground merging
ethanol + lactate + acetate + pyruvate + biomass, scored as a CLR coordinate. This is the
same argument that retired the two-terminal probe in favour of the media → ground probe.

(In the asymmetric directed mode monotonicity is not guaranteed by Rayleigh. Whether it can
actually be violated there is unverified and would need checking before anyone leans on a
negative `delta_total` from an addition.)

## Terminal coverage — the good news

All five terminals are present and atom-mapped in `data/processed/metabolism_bake/`:

| metabolite | MNXM | atom-pair rows |
|---|---|---|
| cellobiose | MNXM726891 | 92 |
| ethanol | MNXM1108092 | 228 |
| (S)-lactate | MNXM1371363 | 380 |
| pyruvate | MNXM23 | 2392 |
| acetate | MNXM26 | 1281 |
| acetyl-CoA | MNXM1104266 | 40263 |

The probe geometry is buildable. It is the perturbation side that is empty.

Caveat: `MNXM726891` carries no formula in MetaNetX 4.5 `chem_prop`, so confirm it is the
specific disaccharide and not a generic entry before using it as an injection terminal.

## Reaction attribution

`per curated_set`, never per gene. Each insert's EC set spans many MNXR (164 / 13 / 18) and
the `mnxr` column holds a REPRESENTATIVE, not a curator's pick. Choosing one would
manufacture an attribution nobody made — the same rule `forsberg/README.md` states.
