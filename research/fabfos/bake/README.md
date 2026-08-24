# The direction lane has no concentration term, and that is where its wrong calls come from

Every ratio in the deployed bake is `exp(dG'o/RT)` — standard state, every participant at
1 M. `thermo_eq.py` calls eQuilibrator's `standard_dg_prime`, `thermo_dgbyg.py` calls
dGbyG's `standard_dGr_prime`, and `combine.py` fuses them and shrinks toward reversible
without ever forming a reaction quotient. There is no Q term anywhere in the lane, so no
row of the bake carries any information about intracellular concentration.

That is not a missing call. It is a missing quantity, and it is measurable.

## The four lanes, and which of them could carry the term

| lane | what it votes | r9 |
|---|---|---:|
| `thermo_eq.py` | eQuilibrator `standard_dg_prime`; reactant-contribution arm is tier 1, group-contribution tier 2 | 2,171 tier 1 |
| `thermo_dgbyg.py` | dGbyG's 100-head GNN ensemble, `standard_dGr_prime` | 19,314 `dgbyg` |
| `curated.py` + `calibrate.py` | MetaCyc/BioCyc direction category, calibrated to empirical dG' per category | 6,072 tier 3 |
| `substitute.py` | restages generic carriers and polymers onto real structures so the two members can speak at all | 32 models |

**eQuilibrator carries the hook; dGbyG does not.** `equilibrator_api` 0.7.0 — already in
`build-refs-equilibrator` — exposes `physiological_dg_prime`, and `PhasedReaction`
`set_abundance` + `ComponentContribution.dg_prime` for arbitrary concentrations. dGbyG at
the pinned commit `2202606` stops at `standard_dGr_prime` and
`transformed_standard_dGr_prime` (pH, ionic strength, pMg, e-potential); its `Compound`
carries `l_concentration`/`u_concentration` fields that no ΔGr method reads — they are MDF
bounds for `GEM_utils`.

**The term must not live in either member.** `RT·Σ ν_i ln c_i` is a function of the
stoichiometry and the concentration table alone, identical for both. Applying it inside
eQuilibrator's call and not dGbyG's would make the two incommensurable, and
`thermo_vote`'s disagreement floor would read a systematic offset as instrument
disagreement and inflate σ. It belongs once, at the combiner, at the seam `sigma_sub`
already uses.

## Uniform 1 mM is not the fix

The correction splits into two parts with completely different data requirements:

- **molecularity** — `RT·Δn·ln(1 mM)`, needs no data, is exactly
  `physiological_dg_prime`, worth 17.1 kJ/mol per net solute
- **skew** — `RT·Σ ν_i ln(c_i/1 mM)`, needs a measured concentration per participant, and
  is zero by construction under a uniform concentration

Median |molecularity| over the reactions the ensemble spoke on is **0.00 kJ/mol**: most
reactions are solute-count balanced, so `physiological_dg_prime` does nothing for the
median reaction. Median |skew| is 1.72 kJ/mol, and skew exceeds molecularity on 40.4% of
them. The cheap fix reaches almost none of the problem.

## What the term moves

`poc_blast_radius.py`, over the 46,622 in-graph reactions carrying a vote:

| | |
|---|---:|
| correction over one decade of conductance | 23,590 (50.6%) |
| over three decades (past `DIR_DG_CLAMP`) | 7,286 (15.6%) |
| direction call changes | 2,092 (4.5%) |
| outright reversal | 564 (1.2%) |
| **skew alone over one decade** — invisible to uniform 1 mM | **14,304 (30.7%)** |

## It is more accurate, on labels the number never saw

`poc_validate_curated.py` scores the 10,144 reactions carrying both a thermodynamic vote
and a directional MetaCyc/BioCyc category. The category is held out with respect to
`dG_raw`: it enters the bake only as a separate vote in the combiner, never into the
thermodynamic number. `REVERSIBLE` is excluded from the score — a curator calling a
reaction reversible is not a direction to get right, and counting it would let a method
score by being uninformative.

| population | n | standard state | concentration-corrected |
|---|---:|---:|---:|
| all curated-directional | 10,144 | 89.9% | **91.8%** |
| `PHYSIOL-*` only | 7,042 | 91.7% | **93.5%** |
| non-`PHYSIOL` only | 3,102 | 85.4% | 87.5% |
| tier 1 (measured dG) | 438 | 90.5% | **94.7%** |
| tier 2 (predicted dG) | 9,706 | 89.9% | 91.6% |
| every participant measured | 178 | 96.9% | **98.8%** |

603 newly right against 208 newly wrong — net **+395** on 1,081 changed calls, where a
coin flip gives zero. Of the 141 outright reversals, 118 (83.7%) land on the curated side.
Wrong calls fall 894 → 749 *and* hedges fall 1,293 → 1,043, so it is not buying accuracy by
abstaining: it makes more decided calls and gets more of them right.

The gradient is the argument. The gain rises with how measured the correction is (89.9% →
91.8% overall, 96.9% → 98.8% where nothing was defaulted) and is largest on `PHYSIOL-*`,
the categories where a curator explicitly asserted a *physiological* direction. It is not
concentrated in one cofactor family either — 19% of the newly-right involve a nicotinamide
pair, 22% an adenylate or phosphate. That is what real signal looks like rather than a knob.

## MNXR145036, the reaction the concern was raised about

MetaNetX writes glycogen phosphorylase `G1P = Glycogen + Pi`; r9's polymer substitution
restages it as `alpha-maltotriose + G1P = alpha-maltotetraose + Pi`, and the bake ships
ratio 0.204 — synthesis favoured about 5:1, backwards from what glgP and malP do.

The two glucans are the same polymer at n and n+1, so their activity ratio is 1 whatever
the polymer concentration is, and the whole correction is the measured pair:

    [Pi] 5.00 mM, [G1P] 0.107 mM      ->  +9.54 kJ/mol   (+1.67 decades)
    dG'  standard state   -4.175 kJ/mol   ratio 0.186
    dG'  with the pair    +5.362 kJ/mol   ratio 8.700

**A 46.9-fold swing, landing on phosphorolysis, with no curated override list.** A
phosphorylase's equilibrium constant sits near unity, so a two-order-of-magnitude pool skew
is not a correction to the direction call — it is the direction call. `dir_confidence` 0.94
was never wrong about the standard-state number; it was confident about a quantity nobody
asked for.

This only works on the restaged equation. MetaNetX's one-sided form leaves a glucosyl unit
unaccounted for, so a correction applied to it prices a molecule that is not there — the
polymer-budget fix is a prerequisite for the concentration fix on this family, not an
alternative to it.

## The data

ECMDB 2.0 is pinned at `data/fabfos/originals/ecmdb/2.0`: **1,186 growth-condition
measurements over 891 metabolites**, each with strain, media, growth phase, system,
temperature and the primary study's PMID (19561621 = Bennett 2009, 17379776 = Ishii 2007,
and others). The bulk `ecmdb.json` download carries the identifier crosswalk and **no
concentrations** — those exist only in the paginated HTML browse, so
`fetch_ecmdb_concentrations.py` is the acquisition step.

232 accessions join to MNXM, covering **33.0% of in-graph solute participant incidences**.

**BioNumbers is needed, for about forty numbers.** The coverage curve on in-graph solute
incidences:

    ECMDB alone                33.0%
    + top   5 curated numbers  40.9%
    + top  40                  47.1%
    + top 160                  52.2%

Five numbers buy eight points. The head of the unmeasured list is dissolved O2 (7,028
incidences) and CO2 (2,265) — activities set by a partial pressure rather than an
intracellular pool, which is why ECMDB structurally cannot carry them; then PMF (1,607), a
pseudo-metabolite at unit activity; then the generic redox carriers — flavins,
ferredoxins, `Acceptor`, hemoprotein reductase, cytochromes b, some 6,000 incidences.

**That last group is the same list the AAM carrier curation already enumerates.** For a
carrier appearing as oxidised and reduced on opposite sides the concentration term is
exactly `RT·ln([ox]/[red])`, the physiological redox poise, so one curated number per
couple reaches hundreds of reactions. The two curation efforts have one target list.

## Two traps, both paid for here

**A one-sided Q term is worse than none.** The first pass on MNXR145036 returned
−17.3 kJ/mol rather than +5.4, because [Pi] was measured and [G1P] was not and the missing
participant was skipped rather than defaulted — leaving it at 1 M and inventing a 50-fold
pool skew that is not there. A participant with no measurement must default to 1 mM, so the
correction degrades to `physiological_dg_prime` rather than to nonsense.

**The InChIKey connectivity block is not a compound.** `reac_prop` writes glgP with
`MNXM1364212` (D-glucopyranose 1-phosphate) while KEGG's C00103 resolves to `MNXM1364214`
(the alpha anomer), so the accession join misses the measurement on the very reaction that
motivated the work. Joining on the InChIKey first block reaches it — and also reaches
sixteen other accessions, because `HXXFSFRBOHSIMQ` holds glucose, galactose, mannose,
allose and gulose 1-phosphate, every anomer and both enantiomeric series. It is a
hexose-phosphate bucket. The join that works is MetaNetX's own alias lists in `chem_xref`
column 3, guarded on formula and charge: 180 → 232 accessions, `MNXM1364212` among them.

## What a re-bake must re-derive

- **σ₀.** It is the robust spread of the quantity being shrunk, and that quantity changes.
  On eQuilibrator's measured arm the spread roughly doubles between the standard and the
  physiological number, and the committed value 23.489 already sits close to the top of
  `DIR_SIGMA_0_BAND = (5, 40)`. `combine` refuses a value outside the band rather than
  warning, so the band itself may be the thing to re-derive — which is the canon rule, not
  a workaround.
- **`DIR_DG_CLAMP`.** 15.6% of in-graph reactions carry a correction past three decades on
  its own. The clamp applies after shrinkage, so its real pressure can only be measured
  once the correction and the re-derived σ₀ are both in.
- **The category calibration.** `calibrate.py` fits category → empirical dG' on the
  measured arm. If the target becomes physiological dG', the fit moves, and the
  `PHYSIOL-*` categories should separate more cleanly — which is a check, not just a
  chore. It must not be scored on the rows the validation above uses.

## The scripts

| script | what it answers |
|---|---|
| `fetch_ecmdb_concentrations.py` | the acquisition: ECMDB's concentration browse to one table |
| `build_concentration_table.py` | one concentration and one width per MNXM, and how the join was made |
| `poc_physiological.py` | three conventions against reactions whose direction is not in doubt |
| `poc_blast_radius.py` | how much of the bake a concentration term moves, split into molecularity and skew |
| `poc_validate_curated.py` | whether it agrees with curated physiology better, on held-out labels |
| `poc_glgp.py` | MNXR145036 priced three ways |

`work/` holds their outputs and is not pinned — every one is reproducible from the pinned
ECMDB chunk, MNXref 4.5 and the deployed bake's own seams.
