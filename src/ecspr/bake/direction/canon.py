from __future__ import annotations

import math as _math

DIR_R = 8.314e-3
DIR_T = 298.15
DIR_RT = DIR_R * DIR_T
DIR_DECADE = DIR_RT * _math.log(10.0)

DIR_TAU_SHARED = DIR_DECADE
# A GUARD, AND UNDER r10's PRIOR IT NEVER BINDS. It caps how confident one curated category
# may be by flooring its fitted width at one decade. Swept 0.25 to 3 decades on the held-out
# folds, the accuracy, the decided count and every ratio are identical, because the narrowest
# directional bin fitted on the corrected number is 19.06 kJ/mol -- above the widest floor
# swept. It stays at one decade as a bound on a future fit, not as a fitted value.
DIR_TAU_CUR_FLOOR = DIR_DECADE
DIR_S_MEAS_FLOOR = 0.1                  # kJ/mol; numerical only -- a real measurement
                                        # is trusted at its own sigma
DIR_SIGMA_CEILING = 100.0               # kJ/mol; a wider eQ uncertainty is no
                                        # information -> the reaction is eQ-silent
DIR_SIGMA_FLOOR = 1e-4                  # kJ/mol; a NARROWER one is no information
                                        # either. eQuilibrator returns dG'=0 at this
                                        # floor when a reaction's groups cancel
                                        # identically -- a statement about the equation,
                                        # not a measurement of it. Both the calibration
                                        # arm and the combiner's vote must reject these.

# The reversible-default prior width = robust marginal spread of measured dG' on the
# eQuilibrator reactant-contribution arm (1.4826*MAD), over the rows that clear
# DIR_SIGMA_FLOOR. ESTIMATOR committed here; the VALUE is frozen from the calibration
# run that produced it (532 measured reactions, marginal median -1.54). It must fall in
# the plausibility band or it is a finding, not a constant. The robust spread runs BELOW
# the outlier-inflated std, i.e. toward more shrinkage / more reversible -- the safe side.
#
# The previous value, 9.505, was fitted without the floor: 116 of its 471 anchors were
# group-cancelling zeros. A quarter of the mass sitting at dG'=0 exactly is what pinned
# that fit's median to 0.000 and halved its MAD, so it shrank every row of every bake
# too hard. Fit this over the floored arm or it will drift back.
# r10 RE-FITS IT ON THE PHYSIOLOGICAL QUANTITY, which is the one now being shrunk. Same
# estimator, same arm, same floor -- 565 measured anchors (up from r9's 532, because the
# balance gate moved below the member and stopped discarding restaged equations). The
# marginal median moves toward zero, -1.165 -> -0.683, so the corrected quantity is the
# more symmetric of the two, and the robust spread widens 24.075 -> 28.017 because a
# concentration correction is information the standard-state number did not carry.
#
# It lands inside DIR_SIGMA_0_BAND, so the band stands as committed rather than needing a
# re-derivation of its own.
#
# REFITTING THE CURATED PRIOR DOES NOT MOVE THIS. sigma_0 is the MARGINAL spread of the
# anchors, so it is a property of the points rather than of the fit -- the prior enters
# neither the population nor the estimator. `poc_constants.py` re-derives it.
DIR_SIGMA_0 = 28.017                    # kJ/mol
DIR_SIGMA_0_BAND = (5.0, 40.0)          # outside => stop, it is a finding

# The ratio must stay a FINITE conductance ratio, never a one-way gate: a handful of
# polymer reactions carry a genuine |dG'| in the thousands of kJ/mol, whose exp()
# underflows to 0.0. Clamped rows are flagged, not hidden.
#
# THE BOUND IS STATED IN DECADES OF CONDUCTANCE, not kJ/mol, because that is the unit
# the ratio is consumed in -- one decade per DIR_DECADE. Unbounded pass-through hands
# the graph asymmetries of 1e17, far past where the reverse branch is numerically dead.
# RE-EARNED ON r10's OWN TABLE, because the concentration term and the refitted prior both
# move the distribution, and a cap whose warrant describes a different distribution is not a
# warrant. Sweeping the mean forward share of the two-way conductance, 2r/(1+r), over the
# final posterior: 1.050603 at one decade, 1.060666 at two, 1.061456 at three, 1.061514 at
# four, and 1.061518 at six and at nine. Three decades sits 0.006% from where the level stops
# moving, and unbounded overflows exp() -- which is the other half of why this bound exists.
# `poc_constants.py` re-runs the sweep against a run's own annotation.
DIR_DG_CLAMP = 3.0 * DIR_DECADE          # three decades == 1000:1

# =====================================================================
# the reaction quotient
# =====================================================================
# THE RATIO IS A PHYSIOLOGICAL QUANTITY, so the number shrunk toward reversible is
# dG'o + RT*ln(Q), not dG'o. The correction decomposes into two parts with completely
# different data requirements, and conflating them is why the cheap fix looks sufficient:
#
#   molecularity  RT * dn * ln(1 mM)             needs NO data. This is exactly what
#                                                eQuilibrator's `physiological_dg_prime`
#                                                returns, and over the voted set its
#                                                MEDIAN |value| is 0.00 kJ/mol -- most
#                                                biochemistry conserves solute count.
#   skew          RT * sum(nu_i * ln(c_i/1 mM))  needs a measured concentration per
#                                                participant, and is identically zero
#                                                under a uniform concentration. 30.7% of
#                                                in-graph reactions carry one past a
#                                                decade, which no uniform prior can see.
#
# A PARTICIPANT WITH NO MEASUREMENT IS PRICED AT THE DEFAULT, NEVER SKIPPED. Skipping
# leaves it at the 1 M standard state while its partners move to millimolar, which makes
# the correction one-sided: on MNXR145036 that returns -17.3 kJ/mol where the balanced
# answer is +5.4. A one-sided quotient is worse than none.
DIR_CONC_DEFAULT_mM = 1.0

# Widths, in DECADES of concentration, folded into sigma at the seam sigma_sub uses.
# The floor exists because 41% of the measured metabolites rest on a single growth
# condition, and a single measurement has no spread of its own -- a confident zero width
# on one number is the failure this prevents.
DIR_CONC_SPREAD_FLOOR = 0.30
# sigma of a log-uniform over the 1 uM .. 10 mM window MDF analyses use: 4/sqrt(12).
DIR_CONC_SPREAD_DEFAULT = 4.0 / _math.sqrt(12.0)

# Already inside eQuilibrator's prime potentials. Adding them again double-counts.
DIR_CONC_IMPLICIT = frozenset({"WATER", "MNXM1"})

# A DISSOLVED GAS PARTICIPATES LIKE ANY OTHER SOLUTE. Its activity is set by a partial
# pressure rather than by a pool the cell titrates, so its measurement comes from a
# solubility at a stated pO2 rather than from metabolomics -- but that is a statement about
# PROVENANCE, not a reason to leave it out.
#
# EXCLUDING A GAS IS THE WORST OF THE THREE OPTIONS, which is why this set is a label and
# not a skip list. Dropping a participant from the quotient is arithmetically identical to
# pricing it at the 1 M standard state: for dissolved O2 that is 1000 mM against a measured
# 0.264, an overstatement of 3.58 decades, where even the flat 1 mM default is only 0.58
# out. At 7,028 in-graph incidences O2 is the single largest participant in the universe,
# so the difference is not academic.
#
# Counted separately so a reader can see which of a reaction's concentrations came from a
# gas-phase source.
DIR_CONC_GAS_PHASE = frozenset({"MNXM735438", "MNXM13", "MNXM1098", "MNXM1101872",
                                "MNXM10917", "MNXM732448"})

# A polymer or an unspecified acceptor has no free-solute concentration. This is the same
# assertion `substitute.py` makes about its standard term, and it is why the phosphorylase
# family needs its polymer budget balanced BEFORE a concentration term means anything.
DIR_CONC_UNIT_ACTIVITY = frozenset({"MNXM738130", "MNXM8348", "MNXM727735", "MNXM725902",
                                    "BIOMASS", "MNXM01", "MNXM8975"})

# How wide a name expansion may go before it is refused.
#
# SET BY WHAT IT HAS TO REACH, because the held-out curated benchmark cannot choose it.
# Across caps 1 to unbounded that benchmark moves between 91.95% and 92.03% -- 7 reactions
# out of 9,060 decided -- so it does not discriminate, and reading its 0.08-point preference
# for the narrowest table as a result would be reading noise.
#
# What DOES discriminate is whether the join reaches `MNXM1364212`. KEGG's C00103 resolves
# to the alpha anomer `MNXM1364214`, while the reaction universe writes glycogen
# phosphorylase with the other id; the expansion has to be at least 3 wide to carry the
# measurement across. Below that, MNXR145036's correction is +3.99 kJ/mol against a
# break-even of +4.175 -- the reaction that motivated this whole lane stays backwards, and
# the benchmark cannot see it, because MNXR145036 carries no curated directional category
# and is therefore absent from the validation population entirely.
#
# 3 is the SMALLEST value that reaches it. Going wider buys nothing measurable and starts
# linking through generic entries: unbounded, `kegg.compound:C08353` reaches seven
# accessions including `lyxose` and `aldehydo-L-ribose`, a C2 epimer and an enantiomer that
# formula and charge cannot separate.
DIR_CONC_MAX_EXPANSION = 3

DIR_CONC_COLUMNS = ("mnxr", "member", "molecularity", "skew", "dG_correction",
                    "sigma_conc", "n_conc_measured", "n_conc_defaulted",
                    "n_conc_excluded", "n_conc_gas_phase", "delta_n",
                    "restaged_balanced")

# WHICH STORED SPREAD ESTIMATOR THE CURATED PRIOR USES.
#
# `calibrate.fit` computes a robust centre (`median`) and BOTH widths from the same points:
# `mad_spread` (1.4826*MAD, robust) and `tau` (a plain variance minus the mean member
# variance, not robust). The combiner paired the robust centre with the non-robust width,
# so a handful of outliers set the width of a bin whose centre had already been protected
# from them, and shrinkage then annihilated the vote -- PHYSIOL-RIGHT-TO-LEFT fits to
# +24.76 kJ/mol, a 21,734:1 ratio, and shipped 1.37.
#
# A ROBUST CENTRE AND A NON-ROBUST SCALE ARE NOT A PAIR.
# WHERE THE BALANCE TEST SITS RELATIVE TO THE MEMBER.
#
# `is_balanced` comes off raw `reac_prop`, but the member does not score the raw equation:
# the substitution lane restages polymer and carrier chemistry first. 546 curated reactions
# that reac_prop calls unbalanced come back from the member balanced and answered, and
# refusing them on the raw verdict discards a real measurement AND changes the population
# the category prior is fitted on -- the directional bins roughly double.
#
# `before_member` is r9's behaviour, kept so the re-bake can price this change as its own
# arm rather than confound it with the estimator change.
DIR_BALANCE_GATES = ("after_member", "before_member")
DIR_BALANCE_GATE = "after_member"

DIR_PRIOR_WIDTH_KINDS = ("robust", "tau")
DIR_PRIOR_WIDTH_KIND = "robust"

# WHICH QUANTITY THE CURATED PRIOR IS FITTED AGAINST.
#
# The calibration fits a category's centre from the eQuilibrator member's dG, and the
# combiner averages that centre with the thermo vote. Once the vote carries a reaction
# quotient the vote is a PHYSIOLOGICAL dG' and the centre is still a STANDARD-STATE one, so
# the two numbers being averaged are numbers about different things.
#
# WHAT THE COMBINER NEEDS IS THE DISTRIBUTION OF dG' WITHIN A BIN, so every category is
# fitted on the corrected column. The label's semantics explain why the bins differ from
# each other; they do not choose the column. Measured on the calibration population, every
# bin's centre moves the way its own label asserts once the correction lands --
# PHYSIOL-RIGHT-TO-LEFT 25.04 -> 36.09, PHYSIOL-LEFT-TO-RIGHT -20.05 -> -24.68,
# RIGHT-TO-LEFT 14.01 -> 21.14, LEFT-TO-RIGHT -27.11 -> -30.21, and REVERSIBLE toward zero,
# -1.45 -> -0.90. The correction never saw the labels, so that is corroboration rather than
# a fit.
#
# A PER-REACTION TRANSPORT OF THE IRREVERSIBLE BINS WAS TRIED AND REJECTED. Splitting the
# taxonomy by the `PHYSIOL-` prefix -- fitting the irreversible bins on dG'o and adding each
# reaction's own correction to that centre -- moves 16 held-out reactions and every one of
# them off the curated side, all in the two transported bins. An irreversibility claim is
# about the reaction, and pricing it per reaction injects concentration noise into a
# statement that was never about concentrations. `poc_prior_holdout.py` still scores that
# arm, so the rejection stays reproducible.
#
# `standard` is r9's behaviour -- fitted on dG'o and used as if it were dG'. It is kept so
# the re-bake prices this change as its own arm, and it writes `unstated` into the
# calibration table so the annotation names the scale it was built on.
DIR_PRIOR_QUANTITIES = ("physiological", "standard")
DIR_PRIOR_QUANTITY = "physiological"

# THE MAGNITUDE CAP'S SECOND JOB, MADE EXPLICIT.
#
# `DIR_DG_CLAMP` is a numerical bound and its warrant is a saturation sweep. It was also
# silently absorbing a population whose posterior is not thermodynamics -- the unbalanced
# fraction rises monotonically with |dG'|, 2.7% under one decade to 66.4% past a hundred.
# A direction label cannot find that bound, because clamping is monotone and
# sign-preserving, so decided accuracy is identical at every value.
#
# A suspect row is WIDENED by this much rather than dropped or squashed. Widening reuses
# the seam `sigma_sub` and `sigma_conc` already use, needs no new branch, preserves
# coverage, and keeps reversible-by-default a limit of one rule instead of an if-branch.
# Three decades: enough that a broken equation cannot carry a confident call, not so much
# that it is silenced.
DIR_SUSPECT_SIGMA = 3.0 * DIR_DECADE

# When two members disagree by more than this, the equation is the suspect rather than
# either estimate. MEASURED: across bands of |eq - dgbyg| the unbalanced fraction runs
# 2.5%, 8.7%, 11.1%, 18.4%, 21.9% -- monotone, and further-reaching than the magnitude
# test. This is the band where it passes 10%.
DIR_MEMBER_GAP = 15.0

DIR_CATEGORIES = ("PHYSIOL-LEFT-TO-RIGHT", "LEFT-TO-RIGHT", "REVERSIBLE",
                  "PHYSIOL-RIGHT-TO-LEFT", "RIGHT-TO-LEFT")

DIR_COLUMNS = ("mnxr", "dG_prime", "sigma", "ratio",
               "dir_tier", "dir_method", "dir_confidence")
