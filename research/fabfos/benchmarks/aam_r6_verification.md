# The r6 bake, verified

The three checks the campaign set for itself, measured against the finished reference at
`data/fabfos/temp/metabolism/` and against the deployed `metabolism_bake` it replaces.
Every number here is read off an artifact the run produced; nothing is restated from a
plan or from the seed design document.

The reference is one bake — `assert_same_bake` passes over the trio: 2,472,761 pair rows,
35,349 metabolites, 83,795 reactions, elements C/N/P/S, `orientation as_written`.

## V1 — coverage

|  | deployed | r6 |
|---|---|---|
| reactions carrying at least one pair row | 66,051 (78.8%) | **67,008 (80.0%)** |
| `(mnxr, element)` keys | 167,216 (80.9% of achievable) | **171,123 (82.8%)** |

**The ledger closes.** Thirteen outcomes over exactly 83,795 reactions — the deployed bake
has no ledger, no worklist summary and no evidence directory for any AAM lane, so this is
the first generation where a reaction that did not bank says why. `mapped_nothing` is 571,
which is why the top-up pass the plan held in reserve was never needed.

### The net hides a regression, and the regression has one cause

+957 net is **+2,204 new and −1,247 lost**. All 1,247 are `rescue_declined`, and **1,088 of
them (87%) are blocked by a single thing**: the NADPH–hemoprotein reductase couple —
`MNXM1090405`, `MNXM1090406`, `MNXM728239`, `MNXM729103` — four ids with no formula and no
SMILES. That is cytochrome P450 reductase, a protein electron donor MetaNetX models as an
explicit participant.

The deployed run banked those reactions by mapping them with the protein **absent from the
reaction SMILES**, and 92.5% of their 32,192 rows are indigo+rxnmapper agreement — so both
mappers concurred on a mutilated equation. This generation refuses that: a reaction whose
participants are not all structurally known is not mapped, because a partial SMILES maps
the atoms it does have onto the wrong destinations. The loss is a principled refusal, not a
capability regression — but it is a loss, and reporting only the net would hide it.

**It is also the cheapest lever left.** Those four ids are element-neutral in exactly the
sense that made `Acceptor` → `MNXM35` safe: a protein redox partner carrying no tracked
C/N/P/S can neither absorb nor emit a mapped atom, so a curated `*` body is admissible
under the same argument. The twin search cannot reach them because MNXref holds no
structured twin to copy. Across the whole worklist this would unblock **1,179 reactions**.

### Where the table comes from

| layer | pair rows | reactions | keys |
|---|---:|---:|---:|
| curated (MetaCyc L1) | 379,199 | 13,620 | 30,499 |
| whole | 1,696,006 | 46,699 | 117,670 |
| rescued | 379,559 | 9,465 | 22,855 |
| forced (conservation) | 17,997 | 69 | 99 |

**The rescued layer is the one that changed character.** 9,580 reactions completed, 9,484
banked, **7,969 of them with consensus**. The deployed table's 9,089 rescue-derived
reactions are all `mcs_only` at half weight, because the crosswalk did not exist when the
neural members ran — the completion happened after them. Preparing before mapping is what
turns one member's assertion into corroboration, and it is the single largest quality
change in this generation.

## V2 — the redox repair

10,649 reactions in scope, 5,800 carrying a refusal, 58,781 rows refused under one
predicate (`cofactor_skeleton_crossing`), and **46,515 arms rescaled rather than deleted**.

| | before | after |
|---|---:|---:|
| `(mnxr, element)` keys | 171,152 | 171,123 |
| reactions | 67,008 | 67,008 |
| S rows | 25,877 | 25,877 |

GAPDH `MNXR144947`, the worked example, is exact: `NADH → glyceraldehyde 3-phosphate`
(4 rows) and `BPG → NAD(+)` (3 rows) are **gone**; `NADH → NAD(+)` (33) and
`BPG → glyceraldehyde 3-phosphate` (4) **remain**, as does `BPG → phosphate`. Every
multi-armed source atom sums to 1.0. Single-armed `<member>_only` rows stay at 0.5
deliberately — rescaling those would promote one member's uncorroborated word to full
weight on the strength of a refusal elsewhere in the reaction.

At population scale: zero refusals with the same family on both sides; **1,904 refusals
between two different families**, which is the transhydrogenase case a one-sided reading of
the rule would miss; refused elements are C/N/P only, **S untouched** — asserted rather than
assumed, since the verb exits non-zero if the sulfur count moves.

240 cofactor↔substrate C/N/P rows survive inside repaired reactions, and **none is a leak**:
every one is FAD or FMN present in only a single redox state in its reaction — riboflavin →
FMN → FAD biosynthesis, where the carbon skeleton genuinely does carry over. That is the
both-sides scope predicate working. Of 10,560 reactions carrying an accepted cofactor, the
repair touched 5,800 and left 4,760 alone.

**The one criterion missed literally.** Keys fell by 29 where the plan asked that they not
fall. 42 keys had nothing survive the refusal; 13 were re-derived from the forced cofactor
bijection; the residue is 27 where conservation does not settle the remainder and 2 where
nothing is left once the couple is removed. The plan predicted a residue of 26. **No
reaction was emptied**, so `redox_emptied` is 0 and nothing traded its outcome for the
correction.

## V3 — nitrogenase, and Nostoc

Of 21 nitrogenase reactions (EC 1.18.6.1 / 1.19.6.1), **13 bank pair rows and all 13 are
injective**; the other 8 are `non_molecule`, a named refusal. **10 carry an N₂↔NH₄⁺ pair.**
The deployed table's nitrogenases hold none.

`MNXR109381` carries it as `NH4(+) → N2`, because MetaNetX files that reaction as N₂
evolution and the bake stores `orientation: as_written`. Of its six nitrogen rows, one is
the N₂↔ammonium correspondence and five are ADP→ATP adenine — so **failure mode #7 is
avoided**: the `(MNXR109381, N)` key is not being filled by adenine while the N₂ node
stays bare.

In the Nostoc network (`NOS` GPR, 13,127 reactions, element N):

| | deployed | r6 |
|---|---|---|
| N₂ a node at all | **no** | **yes** |
| N₂↔NH₄⁺ edges | 0 | 4 (`MNXR109381`, `MNXR163643`, `MNXR166146`, `MNXR175605`) |
| two-point probe from N₂ | `_missing_source=1`, **abstained** | converged, `_conservation_error=0` |
| effective conductance | — | **3.2157** |
| biomass endpoints reached | — | **31 of 31** |
| reactions used / AAM gap | 7,820 / 5,307 | 7,978 / 5,149 |

The deployed bake does not return a small number; it **abstains**, because the source
metabolite is not in the graph. And the largest draws from N₂ are **L-glutamine (0.216)
and L-glutamate (0.187)** — the GS/GOGAT route fixed nitrogen actually takes into
metabolism. That the two biggest sinks are the biologically correct ones is evidence the
network is right rather than merely non-empty.

## The seed design document's claims, restated as our outcomes

The three CSVs (`prep_lane_results`, `prep_lane_union`, `prep_lane_union_reactions`) are not
on this machine, so the 934-reaction join could not be run. Each lane's own evidence
answers the same question.

| item | claimed | here |
|---|---|---|
| A1 curated blockers, element-neutral | 607 rxns | 30 metabolites accepted, unblocking 1,856 reactions — but **1,795 were already reachable by `lane_acceptor`**, so **61 reactions are reached by this lane alone** |
| A2 ACP family | 881 rxns | **held**, refused by the element-neutral gate (2,608 refusals) |
| A3 name-twin dedup | 42 rxns | **0** — no accepts survived; 13,143 candidates had no same-name twin, 1,320 failed the evidence gate |
| A4 formula/SMILES recount | 3,878 + 97 | counts recovered for 201,722 metabolites per element; 5,736,648 of 5,982,672 slots now carry one |
| B1 conservation algebra | 5,492 rxns | **0 forced pair rows** — see below |
| B2 generic carrier class | 713 rxns | 360 carrier-class conjugate claims, 720 species-grain claims total, **none banked**, as designed |
| C1 redox gate | 5,457 rxns, 51,685 rows | **5,800 reactions, 58,781 rows** — and repaired rather than filtered |

**A1's overlap is the finding, not a disappointment.** Its yield was measured before this
arbiter existed; `lane_acceptor`'s regex already reached 1,795 of the 1,856. What the twin
search buys is generality — MNXref's own curated record for any structureless role, rather
than six hand-written spellings — and the 61 reactions only it reaches are the honest
marginal number.

**B1 produced nothing, and the reason is structural.** Of 16,633 targets, 55 have a
cancelling conjugate, 13,556 are refused per element because their residue slots do not
cancel, 1,585 carry more than one unknown, and 1,364 would empty a side. That is the
expected shape — the targets are precisely the reactions holding an unresolved generic, so
a one-sided `*` is the normal case — but the gap against 5,492 is wide enough that it
should be read as a lane that ran and refused, not a lane that failed to run. The
difference is almost certainly the arbiter: this one requires the unspecified slots to
cancel before it will call a count a conservation claim.

**A3 at zero is unexplained and is the open question here.** The gate is the same one the
seed document describes (shared accession *or* post-substitution balance), so a yield of 0
against 42 more likely reflects a narrower name normalisation in the twin search than a
stricter gate. It is worth one measurement, not a rewrite.

## What this does not verify

- The forecast's recall against the prior run's recorded silences is reported by the lane
  (`aam_forecast/summary.tsv`) but not re-measured here; 22,592 of the buildable universe
  carry no prior record at all, which is the denominator any recall claim needs.
- Two LocalMapper submissions (`MNXR187267#N`, `MNXR206123#N`) were removed from the member
  table, cache and sidecars: their maps came from library calls interrupted by hand while
  diagnosing a hang, so their internal state is not attestable. 5 pair rows of 2.47 M.
- The community measurements (`NOS-ERY-RHI` and the six ordered pairs) still reference the
  deployed bake. Only the `NOS` singleton was re-measured against r6.
