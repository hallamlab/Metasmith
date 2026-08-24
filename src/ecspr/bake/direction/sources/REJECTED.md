# Sources surveyed for physiological metabolite concentrations, and why twelve were refused

## Purpose & contents

One entry per candidate source, with the measurement that decided it. Two were pinned;
twelve were not. A rejected source stated with its number is worth as much as a kept one,
and this is the file that stops the same twelve being re-surveyed.

Per-BNID refusals inside BioNumbers live in `bionumbers_rejected.tsv`, not here.

## The policy every entry was judged against

Ground on **E. coli intracellular pools**. Take **measured values only**. Reject fitted
concentrations, because feeding a thermodynamically-derived number back into a thermodynamic
calculation is circular. Reject stated conventions, because a convention is not evidence.
Reject relative and fold-change data, which carries no absolute scale. Reject blood, serum,
urine and CSF concentrations, which are a different physical quantity from a cytoplasmic
pool.

## Kept

| source | contributes | why |
|---|---|---|
| ECMDB 2.0 | 243 MNXM | 1,186 growth-condition measurements over 891 metabolites, already at its coverage ceiling |
| BioNumbers | 1 MNXM | dissolved O2, which no metabolomics database measures and which is the third most common participant in the universe |

## Refused

| source | measurement that decided it |
|---|---|
| **Park 2016** | 100 MNXM, overlapping ECMDB on 102 of 110. Adds 8 accessions worth 258 in-graph incidences — **+0.15 points**. Its phosphate is a thermodynamic fit, so the measured-only rule refuses the one number that would have mattered. |
| **Gerosa 2015** | 42 metabolites over 8 carbon sources, entirely inside ECMDB's central-carbon coverage. Gives a second growth condition to **zero** of ECMDB's single-condition metabolites. |
| **Metabolomics Workbench** | 58 E. coli studies, of which three carry an absolute intracellular unit. ~40 metabolites, names only, ~100% ECMDB overlap. Two further studies report mM *in the NMR tube* with no dilution factor. |
| **MetaboLights** | The best identifiers in the survey attached to no molar numbers. Its MAF format has no units column at all. |
| **Radoš 2022** | Fold change relative to glucose. No absolute scale. |
| **iML1515** | A genome-scale model. Contains no concentrations. |
| **SABIO-RK** | Enzyme kinetics. Km and kcat are not pools. |
| **YMDB** | Yeast, and it publishes no concentration index. |
| **HMDB** | Biofluid concentrations — serum, urine, CSF. A different physical quantity. |
| **Bennett 2009** | Paywalled to automation, and already inside ECMDB by PMID 19561621. |
| **Ishii 2007** | Paywalled to automation, and already inside ECMDB by PMID 17379776. |
| **eQuilibrator MDF defaults** | The 1 µM–10 mM window is a stated convention, not a measurement. It is used here only as the WIDTH of an unmeasured participant, never as its value. |

## What no available source fixes

**36% of the table rests on a single growth condition**, so `sigma_conc` on those metabolites
is a floored guess rather than a measured spread. Phosphate is the heaviest such case: 5 mM
at n=1, and the single most load-bearing number in the correction. Park 2016 would give a
second condition to sixteen of them and is refused as a fit; Gerosa 2015 to none. Naming this
is the deliverable, because no available data closes it.

**Dissolved CO2 stays at the default**, 3,349 in-graph incidences. See
`bionumbers_rejected.tsv`.

**No in-vivo poise exists for the flavins, ferredoxins and cytochromes.** Those carriers
block a further large block of incidences and stay at the default. The NAD, NADP and
glutathione ratios that *are* in vivo come from ECMDB, which measures both members of each
couple.
