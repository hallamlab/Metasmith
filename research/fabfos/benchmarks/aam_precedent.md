# Atom-atom mapping: what has been tried, how complete it is, what keeps going wrong

A synthesis of the AAM precedent across generations, written because the two benchmark
directories beside it (`aam_collapse/`, `aam_cap/`) each argue one point and neither says
where that point sits in the history. Every percentage is measured against the deployed
trio at `data/fabfos/processed/metabolism_bake` — bake `675826f1bdae1f20`, over an
**83,795-reaction universe**, which is the denominator throughout. 100% is the objective,
so completeness is the unit and raw pair counts are not reported.

## Three generations

**Gen 0 — the single-source universe** (the scadc tier-4 table). RXNMapper swept the
reaction universe; LocalMapper ran over a 487-reaction *gap* — reactions the pipeline
could reach and had no mapping for — and every one of its 401 contributions came from
that set. There was no provenance column, so a pair carried no record of who made it.
MetaCyc curation was appended as a small increment, and the generation **explicitly
declined** to let curation replace prediction on the 15,539 reactions where both had an
answer. Rescue-derived reactions were completed *after* the members ran and mapped by
Indigo alone: all 9,089 of them are `mcs_only`, one member at half weight.

**Gen 1 — R6, the ensemble** (deployed). Six model lanes across two passes plus a curated
member; three layers laid down in order of what they are worth, with gates that refuse
rather than warn; a ledger that closes every MNXR onto an outcome from a closed set. The
substantive change over Gen 0 is that curation moved *before* the mappers, so all three
members see a completed reaction and agreement can reach consensus at full weight instead
of one vote at half.

**Gen 2 — in the working tree, not yet baked.** Stoichiometric collapse; the atom cap
re-read as a member boundary that routes rather than a refusal; the partial
(element-reduced) lane with its own ledger outcome.

## Completeness today: 78.8% of reactions, 80.9% of element slots

**Reaction completeness — 78.8%.** 66,051 of 83,795 reactions carry at least one atom
correspondence.

**Slot completeness — 80.9%.** The atom graph needs pairs per element, so the sharper
metric is the (reaction, element) slot. Counted against slots that are *achievable* —
only reactions that actually carry the element, since a sulfur-free reaction can never
have an S pair — the table fills 150,703 of 186,192.

| element | complete | of achievable |
|---|---:|---:|
| C | 60,099 / 73,949 | 81.3% |
| N | 44,494 / 54,561 | 81.5% |
| P | 32,940 / 41,128 | 80.1% |
| S | 13,170 / 16,554 | 79.6% |

The flatness is the finding. Against the whole universe the same four numbers read C
76.6%, N 59.5%, P 43.5%, S 20.0%, and sulfur looks like a failure; against the reactions
that contain sulfur it is the same ~80% as everything else. **There is no weak element —
there is one gap, and it is shared.**

### What each method contributes toward 100%

Methods overlap heavily — 34.4 pp of coverage is reached by two or more — so coverage
alone overstates every member. The honest unit is the **marginal**: completeness lost if
that method were removed.

| method | covers | marginal |
|---|---:|---:|
| `consensus` (RXNMapper + Indigo agreeing) | 60.3% | **17.2 pp** |
| `curated` (MetaCyc) | 16.3% | **12.9 pp** |
| `rxnmapper_only` | 36.6% | 1.8 pp |
| `indigo_only` | 2.3% | 1.2 pp |
| `localmapper_only` | 0.6% | 0.6 pp |
| `disagree_diluted` | 32.4% | 0.5 pp |

Two members carry the table. MetaCyc reaches only 16.3% but nearly all of it is
irreplaceable — 12.9 pp, second only to the neural/Indigo consensus, and the strongest
argument in the build for keeping a curated database in an ensemble of models. RXNMapper
alone adds 1.8 pp beyond what it already contributes to consensus. LocalMapper adds 0.6 pp
and has cost a 24-hour lane and two OOM kills every time it was promoted past that role.

**The rescue lane is worth 11.6 pp on its own.** 9,690 of the covered reactions had no
buildable SMILES of their own — placeholders and curated structures are what put them in
the table. That is the largest single contribution after the consensus, and it is the
direct measure of the structureless machinery working.

### The 21.2 pp gap to 100%, decomposed

| | reactions | of universe |
|---|---:|---:|
| structureless, never rescued | 16,400 | **19.6 pp** |
| buildable, admitted, mappers produced nothing | 1,344 | 1.6 pp |

At slot level the same split is 17.7 pp on reactions that banked nothing at all and 1.3 pp
on reactions that banked some other element. **Structurelessness is ~93% of everything
still missing.** Nothing about counting, capping, or routing touches it.

For scale, the two Gen-2 strategies move the *admitted* population from 68.1% to 68.6% —
half a percentage point — because only reactions over the size cut can move at all, and
that population is ~530. The collapse takes 85% of its own headroom. It matters because of
*which* reactions it contains, not how many.

## The structureless answer

One structureless participant sinks a whole reaction before any mapper runs: 26,090
reactions, 31.1% of the universe, have no buildable reaction SMILES. The blockers have no
formula, no InChI, no InChIKey and no SMILES. What they have, 100% of the time, is a name.

Two mechanisms answer it, and the distinction between them is the hinge the whole design
turns on:

- **Placeholder — the cancelling-out.** A deterministic stand-in so the mapper sees a
  chemically sane reaction. The ox/red pairs are atom-matched on purpose — identical
  heavy-atom skeletons differing only in oxidation state — so a mapper pairs them
  trivially and cannot confuse them with a concrete metabolite: `[Fe+3]`/`[Fe+2]` for
  ferredoxin, flavodoxin, cytochrome, rubredoxin, adrenodoxin and ETF; `SCCCS`/`C1CCSS1`
  for the thioredoxin/glutaredoxin dithiol-disulfide couple; a quinone/quinol ring pair.
  Their atoms are **suppressed downstream** — they are not that molecule's atoms, so they
  must never become graph nodes.
- **Resolved — the opposite.** A real structure supplied where MetaNetX has none. Its
  atoms are **kept**, because the entire payoff is that the carrier's sulfur reaches the
  sink; suppressing it would kill the edge the row exists to license.

Three classes are refused on purpose, because a stand-in for them would be an invention
rather than a substitution: non-molecules (`Unknown`, an electron, a photon — there is
nothing to stand in *for*); acyl carriers, where the thioester genuinely does carry the
cargo through, so a stand-in would route real atoms through fabricated bonds *and still
balance*; and generic donor/acceptor templates, where `A + 2[H] <-> AH2` is a template,
not an instance.

Around the placeholder library sit ten proposer lanes — `twin`, `transform`, `fragment`,
`carrier`, `supplier`, `lipid`, `conserved`, `polymer`, `acceptor`, `override`, in that
priority order — and exactly **one arbiter**: curated `*` bodies must pair across the
equation, then Indigo must map the completed reaction, then per-element mass balance
decides, and a reaction banks only for the elements that balanced. That last clause is
what makes it safe rather than merely plausible, and the example is in the table: in
**MNXR166356** (IspH, EC 1.17.7.4) both participants blocking the reaction are
`oxidized`/`reduced [2Fe-2S]-[ferredoxin]`; the placeholder admits them, the balance gate
confirms ferredoxin is inert to carbon there, and the reaction banks C and P — while the
same stand-in is refused for sulfur in biotin synthase, with no special case anywhere.

## The recurring problems, each with a reaction

**1. A refusal rate read as a finding.** The failure mode of the lane, five times over.
`stripped` at 34.6% of the universe was two regexes disagreeing about water, not
unbalanced chemistry. `ambiguous_duplicate` was 98.7% self-inflicted — the same metabolite
twice is not an ambiguity; only 43 of 3,255 were real. "37.8% coverage" was reported as a
property of the atom mapping and nearly retired a set of biologically real axes. The
600-atom cap's yield curve was generated *by the cap*. A "non-converging LocalMapper
search" was in-process memory accumulation.
> **MNXR100117** — `1 MNXM1102376 + 4 MNXM741173 = 1 MNXM3428 + 4 H2O`. Both defects at
> once: the extractor counted the 4 waters the builder never wrote, *and* the four copies
> of one metabolite read as an ambiguity. Neither is chemistry. It banks 42 pairs across C
> and N today.

**2. A constant calibrated against the wrong measure.** `ATOM_LIMIT` counted a
stoichiometric expansion, so it measured how many times a molecule appears rather than how
much distinct chemistry there is to attend to. The threshold was defensible; what it was
applied to was not. The same shape appears in the direction lane's calibration prior,
which discriminated on ΔG where it had to discriminate on sigma.
> **MNXR163643** (nitrogenase, EC 1.18.6.2) — **2,892 atoms and 9,117 characters expanded,
> 98 atoms and 337 characters collapsed.** Refused by both size bounds; the same chemistry
> written once per distinct molecule clears both by an order of magnitude. 0 pairs today.

**3. One rule living in two files.** `EQ_TERM` exists in three copies that must agree
character for character. `SMILES_LEN_LIMIT` is stated twice and reconciled at runtime.
`NEURAL_ADMITS`/`INDIGO_ADMITS` were extracted into named constants precisely because two
readers in different modules drifting apart means a member silently seeing a different
universe from its siblings.
> **MNXR120073** — `2 H2 + O2 = 2 H2O`. Because `WATER` is not an `MNXM` token, the builder
> emits `[H][H].[H][H].O=O>>` — a reaction SMILES with **an entirely empty product side**.
> Nothing is lost here in pairs (water carries no C/N/S/P), and that is the point: the
> string was malformed for years without costing a number anyone was watching.

**4. Silent degradation where a loud failure was needed.** torch 2.2.1 against numpy 2.x
emits a *warning* and carries on with its tensor bridge broken, so both neural mappers
produce garbage hours into a run — this reproduced live while writing this document.
setuptools ≥81 drops `pkg_resources`, both neural mappers fail to import, and the lane
still writes a plausible empty table. An absent member used to look exactly like a present
one from outside, which is why every member is now a required graph node.
> **MNXR198828** — blamed for an 81-minute LocalMapper stall and diagnosed as a
> non-converging search. It maps in 4.3 s; the real cause was in-process accumulation
> across the whole shard. It banks 59 pairs today as `localmapper_only` — one of the 534
> reactions that member alone reaches.

**5. A hang is not a chemistry verdict.** Indigo's search hangs below the Python layer,
where neither `signal.alarm` nor its own `aam-timeout` reaches, so a plain loop does not
slow down — it stops, having written nothing. The answer is a sidecar written *before* the
attempt, plus a watchdog. Indigo's retry pass has never been measured and the evidence
points against it: a retry ran 22 minutes over ~33 reactions per shard with CPU equal to
wall time.
> **MNXR144749** — 1,211,600 expanded participant terms, an **80.7 MB** reaction SMILES.
> RDKit does not return from parsing it. This is why the character cap is checked before
> the atom count and why the collapse splits components as text: a run once walked 20,000
> reactions in three seconds and then spent an hour inside one call.

**6. The name is the only handle, and it is not evidence.** Using a metabolite's name as
both the claim and its only support is the curation sweep's trap #1 and has already
produced false verdicts. Hence the shape: propose from the name, let the chemistry judge.
> **MNXR166356** (IspH) — blocked by two participants whose entire identity is the string
> `oxidized [2Fe-2S]-[ferredoxin]`. The name picks the stand-in; the balance gate, not the
> name, decides it may be trusted for carbon.

**7. Admitted is not banked, and `(reaction, element)` is not the edge.** The newest
instance, found while writing this. After the collapse, nitrogenase is admitted and Indigo
maps it in 0.53 s — but Indigo pairs only the ADP/ATP/Pi machinery and the Fe-S cluster,
leaving `NH4+` and `N#N` bare. The reaction would still bank an `(MNXR109381, N)` row,
from the five adenine nitrogens of ADP→ATP, so a completeness check keyed on
(reaction, element) reads as filled while the N2 node stays disconnected.
> **MNXR109381** (nitrogenase, EC 1.18.6.1) — 1,236 atoms expanded, 98 collapsed, 0 pairs
> today. RXNMapper *does* produce the `NH4+ → N2` pair on the collapsed string, at
> confidence 0.061 and half weight. So the collapse only pays off if the reaction reaches
> the **neural** members, not merely a mapper.

## Where it stands

- **Collapse** is done and close to exhausted: ~85% of its own headroom, worth 0.5 pp of
  admitted completeness. Its value is which reactions it contains, not how many.
- **Cap-as-router** is implemented and *not safe to ship* until the Indigo hang is
  contained behind a killable subprocess — routing 67 reactions into the production lane
  as it stands trades them for a stalled member.
- **The partial lane** is implemented, tested, never baked. Its reaction-level target is
  1.6 pp (1,344 reactions admitted that produced nothing) plus 1.3 pp of element slots on
  reactions that banked something else. It returns nothing for nitrogenase, the reaction it
  was built for.
- **The R-group rule is the biggest identified lever, and it sits upstream of both gaps.**
  `count_formula` returns `None` for any formula carrying `*`, `(` or `)`. That `None`
  refuses the partial lane's reduction *and* makes `concrete_balance` abstain, which is the
  curation arbiter's own gate — so the same rule bounds the 1.6 pp mapper-silent block and
  part of the 19.6 pp structureless block. Nitrogenase is the poster child: ferredoxin's
  formula is `Fe2S2*8`, so all four elements are refused even though N balances 82/82
  across the specified participants.
- **19.6 pp is structureless and unrescued**, and no counting change reaches it. If the
  objective is 100%, that is where 93% of the remaining work is.
