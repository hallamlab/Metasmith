# The bake, verified

The three checks the campaign set for itself, measured against the deployed
`metabolism_bake` this generation replaces. Every number here is read off an artifact a run
produced; nothing is restated from a plan or from the seed design document.

**r7 has since been promoted: it IS `metabolism_bake` now.** The "deployed" column below is
the generation it replaced, reachable from commit `9354584`, and r6 from `2f4f70a` —
neither is on disk, which is what makes this the reading of record for both.

Two generations are reported because the second is a gapfill of the first rather than a
rebuild: **r6** is the bake the three checks below were run against, and **r7** is r6 with
the body ledger closed, re-mapping only the submissions that changed. V1 and V3 cover both.
V2 was measured on r6 and is not re-measured here — see *What this does not verify*.

The reference is one bake — `check_references.py` passes over the r7 trio, bake identity
`0ffd4c8c6231696e`, equivalence included: 2,530,589 pair rows, 35,860 metabolites, 83,795
reactions, elements C/N/P/S, `orientation as_written`.

## V1 — coverage

|  | deployed | r6 | r7 |
|---|---|---|---|
| reactions carrying at least one pair row | 66,051 (78.8%) | 67,008 (80.0%) | **69,292 (82.7%)** |
| `(mnxr, element)` keys | 167,216 (80.9% of achievable) | 171,123 (82.8%) | **175,005 (84.7%)** |

r7 against r6 is **+2,284 reactions and +3,882 keys, with none of either lost**. Every one
of the 2,284 was `rescue_declined` in r6.

**The ledger closes.** Thirteen outcomes over exactly 83,795 reactions — the deployed bake
has no ledger, no worklist summary and no evidence directory for any AAM lane, so this is
the first generation where a reaction that did not bank says why. `mapped_nothing` is 571,
which is why the top-up pass the plan held in reserve was never needed.

### The net hid a regression, and the regression was a defect — since fixed

+957 net is **+2,204 new and −1,247 lost**. All 1,247 are `rescue_declined`, and **1,090 of
them (87.4%) turn on one thing**: the NADPH–hemoprotein reductase couple — `MNXM1090405`,
`MNXM1090406`, `MNXM728239`, `MNXM729103`. That is cytochrome P450 reductase, a protein
electron donor MetaNetX models as an explicit participant.

**This was first read as a principled refusal, and it was not.** The four are not
structureless at the gate: `curation.PLACEHOLDERS` has carried an atom-matched
`[Fe+3]`/`[Fe+2]` pair for them since the library was written, and 1,185 of the 1,189
reactions containing them pass triage as `completable`. They died at `concrete_balance`,
because `lane_fragment` had already claimed the two *oxidized* ids and handed them
C33/N11/P3 — it resolved the name token `nadph` to real NADPH and `oxidized` to
`MNXM588580`, a ModelSEED fragment stub whose name is the bare word `Oxidized-`, its
trailing hyphen erased by `norm`. The reduced twins have no such stub to collide with, so
they fell through to `[Fe+2]`, and the couple stopped balancing on carbon.

The lane now defers to the placeholder library where it already holds a pair. Measured
over all 24,098 targets: **+1,122 reactions rescued, 0 lost; +1,467 balanced
`(mnxr, element)` keys, 0 lost** — C 1,179, N 271, P 17. The guard is scoped to
`lane_fragment`: the same rule applied at merge scope costs 30 banked reactions, because
`lane_conserved`'s cytochrome and ferredoxin rows are better than a placeholder.

### The `*` body was tallied in two places, each blind to the other's half

The same shape twice more, and both were arithmetic rather than chemistry.

`gate_bodies_cancel` counted `*` only in the curated crosswalk; `concrete_balance` counted
residue slots only for metabolites the recount had reached. The two saw **disjoint halves
of the same participant set**, so a curated body on one side of an equation and a MetaNetX
R-group on the other failed both checks — each for the half it could not see — when between
them the two bodies cancel exactly. `residue_slots` is now the one answer to how many
unspecified slots a participant carries, and both gates read it.

Separately, a lane that *builds* a vehicle now draws the cap count its name declares.
`lane_conserved` drew `4-methyl-trans-hex-2-enoyl-ACP` with one `*` while `lane_transform`
drew its substrate twin with none, and the dehydratase between them was refused for an
imbalance neither lane's chemistry claims: the carrier cannot leave. ACP is the top unpaired
body by a wide margin, which is what pointed at a convention mismatch rather than a
shortage of carriers.

Neither change relaxes the arbiter. The rule that an unpaired `*` refuses a completion is
exactly as it was; the rule can now see the whole equation.

Measured together over all 24,098 targets, against what r6 baked: **rescued 9,580 →
12,472 (+2,892, 0 lost); balanced keys 23,138 → 28,027 (+4,889, 0 lost)**. Two candidate
changes were measured and **refused for costing coverage**: resolving a transform's base
against the carrier-stripped core (−338 reactions) and declining a borrowed twin whose name
declares a body its donor structure lacks (−52). Two motif classes are excluded from the cap
rule for the same reason — `holo`/`apo`/`trna` are state prefixes rather than body markers
(−49), and `[protein]` costs MNXR171321, which genuinely joins two protein bodies into one.

### Where the table comes from

Grouped from `aam_stack` by source, method and the universe's submission class, both
generations the same way:

| layer | pair rows | reactions | keys | r7 Δ reactions |
|---|---:|---:|---:|---:|
| curated (MetaCyc L1) | 379,217 | 13,620 | 30,499 | — |
| whole | 1,748,741 | 46,699 | 117,684 | — |
| rescued | 443,501 | 11,752 | 26,752 | **+2,287** |
| forced (conservation) | 17,997 | 69 | 99 | — |

**The whole gain is in the rescued layer, and it kept its character.** 9,874 of the 11,752
carry consensus, against r6's 7,970 of 9,465 — the same 84% share, so the added reactions
are corroborated at the rate the existing ones were rather than being one member's
assertion. The deployed table's 9,089 rescue-derived reactions are all `mcs_only` at half
weight, because the crosswalk did not exist when the neural members ran. Preparing before
mapping is what turns one member's assertion into corroboration, and it remains the single
largest quality change in this generation.

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

**Re-run on r7 the repair holds its shape**, which is the check worth making on a bake whose
rescued layer grew by a quarter: scope rises with the table (10,785 reactions, 5,828 with a
refusal, 58,979 rows refused, 46,617 arms rescaled), and every invariant is unmoved — S rows
26,352 → 26,352 untouched, refusals C/N/P only, the same 27 / 2 / 13 rederivation split, 29
keys emptied after rederivation, and **0 reactions emptied**.

## V3 — nitrogenase, and Nostoc

Of 21 nitrogenase reactions (EC 1.18.6.1 / 1.19.6.1), **13 bank pair rows and all 13 are
injective**; the other 8 are `non_molecule`, a named refusal. **10 carry an N₂↔NH₄⁺ pair.**
The deployed table's nitrogenases hold none. Counted the other way round — every reaction
in the bake carrying an N₂↔NH₄⁺ pair, whatever its EC — r6 and r7 hold the same **17**
against the deployed bake's 5.

`MNXR109381` carries it as `NH4(+) → N2`, because MetaNetX files that reaction as N₂
evolution and the bake stores `orientation: as_written`. Of its six nitrogen rows, one is
the N₂↔ammonium correspondence and five are ADP→ATP adenine — so **failure mode #7 is
avoided**: the `(MNXR109381, N)` key is not being filled by adenine while the N₂ node
stays bare.

In the Nostoc network (`NOS` GPR, 13,127 reactions, element N):

| | deployed | r6 | r7 |
|---|---|---|---|
| N₂ a node at all | **no** | **yes** | **yes** |
| N₂↔NH₄⁺ edges | 0 | 4 (`MNXR109381`, `MNXR163643`, `MNXR166146`, `MNXR175605`) | the same 4 |
| two-point probe from N₂ | `_missing_source=1`, **abstained** | converged | converged |
| effective conductance | — | **3.2209** | **3.2210** |
| biomass endpoints reached | — | **31 of 31** | **31 of 31** |
| reactions used / AAM gap | 7,820 / 5,307 | 7,978 / 5,149 | **8,091 / 5,036** |

The deployed bake does not return a small number; it **abstains**, because the source
metabolite is not in the graph. And the largest draws from N₂ are **L-glutamine (0.219)
and L-glutamate (0.187)** — the GS/GOGAT route fixed nitrogen actually takes into
metabolism. That the two biggest sinks are the biologically correct ones is evidence the
network is right rather than merely non-empty.

**r7 holds it and adds to it.** The two bakes carry the same four N₂↔NH₄⁺ reactions and
the same conductance to three decimals; what moved is the network beneath it, 113
reactions out of the AAM gap and into use. The gapfill did not touch nitrogen fixation
and the measurement says so rather than assuming it.

The driver is `aam_v3_nostoc.py`, beside this file. The r6 column above was re-measured
through it rather than restated: the original reading was taken by hand and the loader's
GPR schema has changed since, so the two are separated by a shim whose docstring says
what it maps. Every structural number reproduces exactly — the deployed abstention, both
reaction/gap splits, the endpoint count and the four reaction ids — while the recorded
r6 conductance was **3.2157** against the 3.2209 the driver now returns. That 0.16% is
unexplained; it is a scalar under a re-derived harness and it moves no verdict here.

## The seed design document's claims, restated as our outcomes

### The 934-reaction union banks at 1.6%, and one gate explains it

The union of the three preparation lanes — the reactions the seed document expected to
unblock, ~913 of 934 after its 97.7% mapper-survival discount — resolves in our ledger as:

| outcome | n |
|---|---:|
| `rescue_declined` | 918 |
| `banked` | **15** |
| `rescued_nothing` | 1 |

All 934 are present in the ledger, so this is a refusal, not a miss. The funnel locates it
exactly:

| stage | n |
|---|---:|
| in the union | 934 |
| every blocker has a proposed structure | 903 |
| **passes `gate_bodies_cancel`** | **61** |
| reaches the per-element balance test | 24 |
| banked | 15 |

**842 of 903 fail the body-cancel gate**, and the mechanism is not a defect. Take
`MNXR115025`: `MNXM1364002 + MNXM36 = MNXM1364163 + Acceptor`. The generic acceptor stands
on **one side only** — MNXref writes no conjugate partner for it. Substituting a curated
`H4*` body there makes its unknown residue count as zero atoms for every element, so the
balance test that follows would be about a molecule that does not exist. The gate refuses
rather than let that through, which is what its docstring says it is for.

So the disagreement with the seed document is real and principled rather than a bug. Their
lanes treat an element-neutral twin as bankable on the strength of its explicit atoms; this
arbiter additionally requires the *unspecified* residue to cancel across the equation before
any count is allowed to mean conservation. One hypothesis was tested and rejected on the way:
the gate compares star COUNTS per side, not species ids, so the oxidised and reduced twins
being different MNXM ids is not what refuses them.

**The same gate explains B1's zero.** 13,556 of the algebra lane's targets are refused per
element on residue slots not cancelling. One conservative rule accounts for the union's 1.6%,
for B1's 0 against 5,492, and for a large share of the 13,910 `rescue_declined` overall.
Whether to relax it is a judgement with a real correctness cost on the other side, and it is
the decision this verification most wants a human to make.

### Lane by lane

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

**A3 at zero is unexplained and is the second open question.** The gate is the same one the
seed document describes (shared accession *or* post-substitution balance), so a yield of 0
against 42 more likely reflects a narrower name normalisation in the twin search than a
stricter gate. It is worth one measurement, not a rewrite.

## The decision this hands back, and why it got smaller

**The body-cancel gate** refuses 2,468 of the 13,910 `rescue_declined`, and that number
was read as its price. It is not: **the gates are sequential**, so a reaction the gate
stops refusing goes on to meet `concrete_balance`, and most fail there instead. Measured
over all 24,098 targets by disabling the gates outright:

| | rescued | Δ | projected reaction coverage |
|---|---:|---:|---|
| the lane as it now stands | 10,702 | — | 68,130 (81.3%) |
| body-cancel gate relaxed | 11,229 | **+527** | 68,657 (81.9%) |
| …and every generic handed a `*` | 14,575 | +3,873 | 72,003 (85.9%) |

`no element balances` absorbs what the earlier gates release: 1,174 → 3,718 → 9,485 down
that column. So relaxing the body-cancel gate buys **527 reactions, 0.6 points**, against
weakening every balance verdict in the table — a worse trade than "on the order of a
thousand" made it look, and the third row is the stop-line's own definition of wrong.

**The rung that is left is not a gate.** In the permissive run the 9,523 still refused
split into **4,808 disproved** — balance ran and every element came back False — and
**4,715 abstained**, where no element could be tested because a concrete participant's
count is untrustworthy. Only the second half is addressable, and it is the recount lane's
territory rather than the arbiter's: A4 already put 201,722 metabolites on structure-derived
counts and filled 5,736,648 of 5,982,672 element slots. Closing the remainder converts
abstentions into verdicts at no cost in warrant.

For calibration, the hard ceiling is **97.5%**: 1,355 `no_transfer` (both sides the same
multiset), 603 `non_molecule` and 128 `unparseable_equation` are structurally out of scope,
and no relaxation reaches them.

The reductase couple was listed here as a second decision. It was not one — it was the
fragment-lane collision above, and it is fixed rather than traded.

**And the gate's price was smaller again than this section makes it look**, because 1,447
of the 3,072 it refused were never its to refuse: it was reading half the equation. Closing
the ledger moved them without touching the rule, so the decision that remains is narrower
than the 527 measured above — it is about genuinely unpaired bodies only. It is still open,
and it is still not one to settle unilaterally.

### The 621 keys r7 still owes the deployed bake

r6 was 2,063 `(mnxr, element)` keys short of the deployed table; r7 is **621**, over 462
reactions, of which 311 are banked here and short one element rather than absent. The
refusals are overwhelmingly carbon (428 of 621) and they are refusals rather than gaps: the
rescue tested carbon and it did not balance.

The family is glycosyltransfer onto a polymer, and the mechanism is one lane's budget. In
MNXR100000 the fragment lane gives the chondroitin polymer **C=14 on both sides** of a
reaction that transfers a GalNAc onto it — 31 carbons in, 23 out — because both names
resolve to the same residue tokens and the lane cannot see that the chain grew. MNXR100004
loses a hexose the same way. The deployed bake carries `consensus` and `disagree_diluted`
pairs for these: both mappers agreed on a completed string whose carbon does not add up.

So the remaining shortfall against the deployed table is not a gate to relax but a budget to
fix, and closing it by admitting the pairs would bank carbon for reactions where carbon is
not conserved.

#### Why the two names resolve the same, and why the lane built for it does not take them

`_LOCANT` discards a bare integer, and on a polymer ladder that integer **is the chain
length**. `Keratan sulfate I, degradation product 2` and `… product 19` therefore reduce to
one token key, and `lane_fragment` hands both `C=28 N=2 S=1` and the *same* vehicle SMILES.
It is not confined to the shortfall: **1,468 of the fragment lane's 3,890 metabolites share
a token key with a sibling**, across 367 collisions — the glycosaminoglycan ladders (keratan
112, chondroitin 60, heparan 53), the O-antigen series, and the phosphatidylinositols, whose
`(16:0/18:2(9Z,12Z))` acyl shorthand is stripped the same way.

`lane_polymer` is built for exactly this ladder and would draw each rung as its levelled
monomer cargo — but it never gets the chance, and the priority order is not the reason. Of
the glycosaminoglycan chains it reaches, **183 of 188 are dropped as inconsistent families**;
`fragment` outranks it at merge only for what is left. The inconsistency has at least one
identified cause: **544 of the lane's 1,021 ladder edges carry an empty delta**, and an empty
delta between two DISTINCT chains is read as evidence they are the same length. MNXR100165 is
the whole of it — `Heparan sulfate, precursor 10 = Heparan sulfate, precursor 11`, one
substrate, one product, no donor recorded — so the residual is zero because MetaNetX wrote no
monomer, not because none moved. One such edge poisons a family through the BFS. Absence of
evidence is being read as evidence.

Dropping those edges alone was measured and **does not** rescue the families (183 → 182), so
there is a second inconsistency source and it has not been isolated. That is the state of the
lever, not a plan.

### Where the rest of `rescue_declined` sits

The rescue funnel over the 24,098 reactions adjudicated `blocked_no_structure`, r6 and then
r7. `rescue_declined` falls 13,910 → 11,622.

| triage bucket | r6 | r7 |
|---|---:|---:|
| no admissible placeholder | 9,146 | 9,146 |
| curated bodies do not cancel | 3,072 | 1,625 |
| completable, then no element balances | 2,300 | 853 |
| completable and rescued | 9,580 | 12,472 |

**What is left in `no element balances` is the balance check working.** Of the 853, 109 can
read no count at all and 725 read carbon fine and refuse it — a third of those by twenty
atoms or more. That is a lane-inferred budget being wrong, not a gate being wrong, and
making the lane decline them would move the bucket and recover nothing. It is the same
finding as the 621 keys below, arriving from the other direction.

`no admissible placeholder` is the largest and the least tractable, and its shape says why:
by family of the generic that refuses, **other_structureless 7,335**, acyl_carrier 798,
generic_rgroup 651, polymer 587, trna_holo 376, electron_carrier 21. The long tail is a
long tail — the biggest single blockers are `Enzyme-ligand complex` (183 reactions), `UDP`
`MNXM1102130` (144) and `L-lysyl-[protein]` (80), so there is no second lever the size of
the one just pulled. The 21 electron-carrier reactions are the only ones where the
machinery exists and a name spelling is all that stands in the way.

The fragment lane supplied a body in **1,915 of the 2,300** that die at the balance gate,
so after the reductase fix it remains the largest single source of what that gate refuses.
One hypothesis was tested and rejected on the way: dropping the other 28 fragment sums that
absorb an `oxidized` token gains **0** reactions and costs 11, and dropping the 704 that
absorb a `protein` token costs **515 banked** and gains 0 — that token is load-bearing,
because `[protein]` stands on both sides and its budget cancels.

## What this does not verify

- The forecast's recall against the prior run's recorded silences is reported by the lane
  (`aam_forecast/summary.tsv`) but not re-measured here; 22,592 of the buildable universe
  carry no prior record at all, which is the denominator any recall claim needs.
- Two LocalMapper submissions (`MNXR187267#N`, `MNXR206123#N`) were removed from the member
  table, cache and sidecars: their maps came from library calls interrupted by hand while
  diagnosing a hang, so their internal state is not attestable. 5 pair rows of 2.47 M.
- The community measurements (`NOS-ERY-RHI` and the six ordered pairs) still reference the
  deployed bake. Only the `NOS` singleton was re-measured against r6.
