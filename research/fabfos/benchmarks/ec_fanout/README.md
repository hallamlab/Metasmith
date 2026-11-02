# The EC bridge, audited in both directions

What an EC number buys the GPR, and what it smuggles in with it. Measured against
MetaNetX 4.5 `reac_prop` and the two published GPR tables.

Run order: `ec_fanout_audit.py <currency_fraction> <out.jsonl>` writes the per-EC
component report; `ec_split_usage.py <dir>` joins it to the runs; `ec_split_cause.py
<dir>` classifies why each split happened; `ec_promiscuity.py` is the reverse
direction and stands alone.

## The gate holds

Every path that emits an EC-keyed GPR row filters to a four-level EC first
(`gpr_4lane.py:172`, `gpr_7lane.py:120,145,158`, `fabfos_evidence.py:399`,
`bench_edges.py:89`). Confirmed on the data rather than only in the source: **0
non-level-4 rows** in either published table.

The bridge itself keeps partials, because `reac_prop` writes them — 1,274 of the
8,663 ECs in `classifs`. They are unreachable through an inner join on a level-4
key, so they are dead weight rather than a contamination path. The cost is silent
and one-directional: **8,089 reactions whose only classifs are partial can be
reached by no EC lane at all.** `route_ec`'s docstring justifies keeping them with
"the CLEAN lane can emit them too", which the level-4 filter has since made false.

## Forward: one EC, many reactions

Collapse each level-4 EC to its MNXR set, pool both sides of each equation into a
participant set, and join two reactions when they share a metabolite. An EC whose
reactions fall into two or more components is one where the annotation credits an
ORF with chemistry that shares nothing.

| currency metabolites removed | split ECs, of 5,383 with ≥2 MNXR |
|---|---|
| 61 (≥0.5% of reactions) | 2,814 |
| 28 (≥1%) | 2,494 |
| 18 (≥2%) | 2,140 |
| 11 (≥5%) | 1,772 |
| none — must share literally nothing | **375** |

The 375 are the defensible floor: two reactions under one four-level EC with not
one metabolite in common, not even water or a proton. Cause, by whether the
minority group's metabolites have any structure in `chem_prop`:

- 168 (44.8%) — all real small molecules. Genuine disagreement about what the EC means.
- 137 (36.5%) — mixed.
- 70 (18.7%) — pseudo-species only: proteins, complexes, polymers.

Worked cases. EC 2.5.1.18 (glutathione transferase) carries 86 reactions of one
chemistry plus a lone androstenedione isomerisation. EC 3.2.1.129 has exactly two
reactions, and they share nothing because one operates on
`fragments-of-polysialic-acid` and the other on `polysialic-acid` — the polymer
budget, arriving through the EC bridge instead of the balance check.

## One reaction, many contradictory ECs — a marker, not a second direction

**Nothing traverses MNXR → EC.** `classifs` is read in exactly two places: exploded
into the forward bridge by `load_ec_to_mnxr`, and carried as an inert string column
in `aam_worklist.parquet` and `mnx_lookups`. No lane, gate or score branches on a
reaction's EC list. So the multiplicity below is only ever exercised through the
same forward `ec == ec` join, and it identifies a promiscuous *node* rather than a
wrong *edge* — 107 other ECs on a reaction do not make this ORF's credit wrong.

Of 34,587 reactions carrying a level-4 EC, 633 span two or more EC subclasses and
**375 span two or more top-level classes**. One reaction carries 166.

- `MNXR104634` — succinate + L-selenocystathionine ⇌ L-selenocysteine +
  O-succinyl-L-homoserine. Fourteen ECs across five classes, including chitinase
  (3.2.1.14) and N-acetylneuraminate lyase (4.1.3.3). Its fourteen xrefs are all
  the *same* reaction (BiGG `SUCHMSSELCYSL`, KEGG `R04946`, SEED `rxn31266`), so
  the merge is correct and the disagreement is upstream: **KEGG's own `R04946`
  entry carries exactly one EC, 2.5.1.48.** The other thirteen entered from the
  other institutions' copies.
- `MNXR153054` — ADP + phosphate ⇌ ATP. 108 ECs: every ABC transporter, every
  helicase, ATP synthase. Any ORF called as a 3.6.4.x helicase is credited with
  ATP hydrolysis, which is true and useless — it is the same failure that made the
  atom cap refuse nitrogenase for hydrolysing ATP rather than for its chemistry.
- `MNXR183533` / `MNXR198756` — generic peptide-bond formation over `[*]`
  R-groups, from ModelSEED and MetaCyc respectively. 165 and 166 ECs, essentially
  the whole of 3.4.

These are not filtered downstream. All four are `mappable` in the worklist and all
four carry atom pairs in the deployed bake, so they become conductance edges.

## What it costs the metagenome

`scadc_metagenome`, 17,493,361 EC-routed rows over 4,944 distinct ECs — 88.7% of
the whole 4-lane table, because CLEAN dominates it.

| | rows | ORFs |
|---|---|---|
| through a currency-tolerant split EC | 10,295,356 (58.9%) | — |
| through a share-nothing split EC | 2,836,599 (16.2%) | 175,827 |
| landing on a cross-class reaction (exposure, not error) | 567,358 (3.2%) | 236,196 |
| ...where that reaction reaches `atom_pairs` | 495,920 (2.8%) | 206,601 (14.5%) |

`scadc_fosmids` tracks it closely at the smaller scale: 29.0% of emitted ECs split
under the tolerant rule, 7.0% under the strict one.

**Read the last two rows as exposure to a promiscuous node, not as error.** The
wrong-by-construction number is the share-nothing row: those are the ECs where at
least one of the two credits must be false.

Two things shrink the practical harm further. Belief is conserved per ORF and
divided by fan-out — `contrib = w_n / F_n`, asserted to sum to 1.0 within 1e-9 per
ORF (`ecspr/model/evidence.py:37,49`) — so an EC reaching 100 reactions already
contributes a hundredth to each. Dilution does not remove the mass, though; it
spreads it onto the wrong reactions rather than dropping it.

The two 375s are a coincidence, not a double-count: the forward and reverse sets
share only 28 ECs, and the reverse figure was recounted independently.

## Lanes on disk

`scadc_metagenome` publishes 4 lanes (`clean`, `pbert`, `kofam`, `uniref50`).
`deepec` and `esmc` inputs are staged in `annotation_alts/` and unbuilt; `ezpred`
has no input here, so the 7-lane table cannot be built for this run without one.
`scadc_fosmids` is the only run with all seven.

The independence argument is what makes the missing lanes worth building.
`route_ko`'s docstring already records that routing KO through EC would make 91%
of the kofam lane redundant with the EC lane. Today that leaves `clean` as the
only large EC-routed lane in the metagenome, so every defect above is un-crossed
by any second EC opinion. `deepec` and `ezpred` are the two that would cross it.
