# Does ECSPr predict the ASKA/FFA phenotype?

**No.** Over the 60 ORFs Fang et al. rebuilt and assayed individually, the
atom-resolved conductance from glycerol to free fatty acid carries no information
about which clone raised the titer. It carries a great deal of information about
how many reactions the clone contributes. Both statements survive the controls
that were run to try to break them.

This is a negative result about the method, not about the study: the benchmark is
sound, the gate passes cleanly, and the failure is legible rather than lost in
noise.

## The four numbers

Numbers below are the two-point probe at the merged fatty-acid readout, scored
against the size-matched null. Regenerate with `analyse.py`; the tables beside
this file are the source and this prose is not.

**1. The gate passes, and decisively.** All 33 control conditions — masks that
reach no reaction the basis can carry an edge for — return the baseline
*bit-for-bit*. `control_sd` and `control_max_abs` are exactly `0`. The solver's
numerical floor is therefore zero, and any non-zero null spread clears it, so
`null_over_control` is undefined by division rather than by weakness. Every z
below is measured against a real floor.

**2. Reach is good, and the 2×2 is not anti-correlated.** Of 87 measured
conditions, ECSPr can move 58; 29 are invisible to it. Among single-clone
conditions the fraction carrying an atom-mapped edge is 36.0% for the strains that
moved the phenotype and 36.8% for those that did not — the method's reach is
independent of the phenotype, which is the precondition for any score afterwards
to mean anything. Only 3 of the 13 increases the paper scores are invisible. The
study is not the degenerate case where everything interesting is off-model.

**3. The score does not track the titer.** Over the 58 conditions ECSPr can move:

| | ρ vs fold change | AUC, increases vs rest | ρ vs reaction count |
|---|---|---|---|
| global `z` | +0.009 | 0.424 | +0.693 |
| size-matched `z_stratum` | +0.152 | 0.529 | +0.190 |

Neither separates the strains the paper scores as an increase from the rest.
Size matching does what it is for — the reaction-count correlation drops from
+0.69 to +0.19 — and what it leaves behind is an AUC of 0.529, which is chance.
The ρ of +0.152 is not nothing, but with 10 positives in 58 it is comfortably
inside what noise produces.

**A note on which of these the null earns.** The global z is an *affine* transform
of the delta: every condition at a given readout is divided by the same null mean
and sd. So any rank statistic computed on global z is identical to the same
statistic on the raw delta, and drawing more of the null cannot change it — which
is exactly what was observed, ρ and AUC unmoved to three decimals from 664 draws
to 3,000. The null earns its cost on `z_stratum`, where each condition is
standardised against draws of its own size, and on the empirical p. It is not
doing work in the global ranking, and reporting it as though it were would
overstate what the pool bought.

**4. The score tracks reaction count instead.** ρ between global z and the number
of reactions the clone contributes is **+0.69**. That is what the method is
measuring. Under a two-point probe Rayleigh monotonicity guarantees any addition
raises the effective conductance, so this is not a bug — it is the probe behaving
exactly as specified. The question was whether it carries anything *else*, and
after size matching removes most of it, what remains does not separate the hits.

**5. The `ground` probe agrees, and does not provide an independent check.** It
was run because it answers a different question in principle: under universal
leakage the draw sums to one exactly, so a perturbation redistributes rather than
lifts, which should remove the size effect that dominates the two-point ranking.
It does not, here. Every statistic lands within 0.005 of its two-point twin
(ρ +0.006, AUC 0.422, size ρ +0.695), and the two probes' deltas rank the 91
conditions at **Spearman +0.9999**, with the ground delta a constant 0.1317× the
two-point delta across the interquartile range.

The reason is a choice in this study's conditions table rather than a property of
the probe: the fatty-acid species are named as `sink_hub`, which under `ground`
gives them a PORT to ground rather than a leak, so the readout is close to a
rescaled two-point conductance. A genuinely independent ground reading would name
biomass precursors as the ports and the fatty acids as readouts only — the package
keeps `sink_hub` and `readout_hub` in separate columns precisely so that is
expressible. That variant was not run and is the obvious next thing to try if
anyone wants to revisit this.

## The one table that shows why

Hold reaction count fixed at one, so the added conductance is identical by
construction and only *which* reaction differs. Among those 33 clones, ρ between
delta and fold change is **+0.060** and AUC is **0.533**.

Worse, **23 of them return the identical delta, 2.809 × 10⁻⁵**, while their
measured titers span **303 to 5,736 mg/L** — a forty-fold range:

| strain | measured | paper's call |
|---|---|---|
| `rfaY⁺-yafL⁺-fadR⁺` | 5736.1 mg/L | increase |
| `rfaY⁺` | 2461.3 mg/L | increase (+207.8%) |
| `RF` (rfaY⁺, round-2 parent) | 2240.3 mg/L | reference |
| `rfaY⁺-ytfK⁺` | 1900.6 mg/L | no change |
| `rfaY⁺-norR⁺` | 303.0 mg/L | decrease |

Every one of these strains overexpresses rfaY, and `waaY`/`b3625` contributes the
only reaction among them that the basis can carry. The partner ORFs — `yafL`,
`rimM`, `norR`, `ygdD`, `yggR`, `ybeF`, `hydN`, `yjdF`, `opgD`, `yafZ`, `wcaA`,
`tas`, `rsxG`, `ytfK`, `lacI` — have no atom-mapped reaction at all. So in the
model these are not merely similar conditions: **they are the same condition**,
and no scoring choice downstream can separate strains whose networks are
identical.

## What this is and is not evidence for

It is not evidence that ECSPr is broken. The controls are exact, the two arms
measure the same network, the null is size-matched and drawn once, and the probe
does what its specification says. The benchmark was built to be able to return a
positive answer and did not.

It is evidence that **conductance to a product is the wrong observable for this
phenotype**, and the paper says so independently: its own conclusion is that the
winners act through *membrane homeostasis* — LPS core phosphorylation, outer
membrane integrity, permeability — not through carbon supply to fatty acid. Six of
the paper's own top hits are transporters or membrane proteins. A method that
reads a metabolic network cannot see a mechanism that is not in it, and here the
mechanism is not in it. That the strongest single-gene effect in the study (`rfaY`,
3.1×) resolves to a single LPS heptose kinase reaction whose removal from the
carbon graph changes almost nothing is the same fact stated in the model's terms.

## Caveats that bound the result

- **The scale of the effects is small in absolute terms.** `rfaY` moves the
  glycerol→FFA conductance by 2.8 × 10⁻⁵ against a baseline of 9.48, about 3 parts
  per million. It is far above the floor (which is exactly zero) but it is not a
  large perturbation of the network, and a study whose real effects are all this
  size gives the ranking little to work with.
- **TesA′ enters as duplicated host edges, not as a new reaction.** The plasmid's
  acyl-ACP thioesterase is represented by duplicating the four acyl-ACP ↔ free
  fatty acid reactions iML1515 already carries. It sits in the background of every
  condition including the baseline and every null draw, so it cancels in every
  delta — but the strain's actual flux route is more specific than that.
- **C16 has no thioesterase route in this basis.** The atom-pair table carries no
  palmitoyl-ACP thioesterase, so the paper's most abundant products reach the
  readout only through lysophospholipase. Per-species readouts are in the results
  table.
- **The tier and the basis disagree slightly.** The study tier's atom universe
  comes from bake v2 with transport excluded; ECSPr solves on the tier-4 decoded
  pairs. They disagree on a handful of reactions (`waaF` among them). Reach is
  reported both ways in `analyse.py` and neither reading changes the conclusion.
- **BH-q cannot reach 0.05 here** at any effect size: an empirical p over m draws
  floors at 1/(m+1), and the threshold across the tests is below that. `p_floor`
  is carried in the output rather than worked around.

## Provenance

The run is `main/benchmarks/aska/out/n1000/`, scored against the **complete
3,000-draw null** — 1,000 per size stratum, from one seed (20260812), drawn once
and shared between the arms by sharing a file. `out/interim/` holds the
stratum-1-only scoring kept as the stability check described above.

Both probes are complete. `README.md` beside this file lists the chain that
produced every input and the five things that would have gone wrong silently.

To regenerate either probe's numbers — the driver shards per condition and
resumes, so re-running it costs nothing now that both are done:

    python main/benchmarks/aska/analyse.py --run main/benchmarks/aska/out/n1000
    python main/benchmarks/aska/analyse.py --run main/benchmarks/aska/out/n1000 --probe ground

What is still genuinely open is the ground variant described in point 5: ports on
the biomass precursors, fatty acids as readouts only. That is the reading that
would actually exercise redistribution, and it is a change to
`build_ecspr_tables.py`'s `SINKS`/`readout_hub` split rather than to any solver.
