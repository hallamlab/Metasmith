from __future__ import annotations

import math as _math

DIR_R = 8.314e-3
DIR_T = 298.15
DIR_RT = DIR_R * DIR_T
DIR_DECADE = DIR_RT * _math.log(10.0)

DIR_TAU_SHARED = DIR_DECADE
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
DIR_SIGMA_0 = 23.489                    # kJ/mol
DIR_SIGMA_0_BAND = (5.0, 40.0)          # outside => stop, it is a finding

# The ratio must stay a FINITE conductance ratio, never a one-way gate: a handful of
# polymer reactions carry a genuine |dG'| in the thousands of kJ/mol, whose exp()
# underflows to 0.0. Clamped rows are flagged, not hidden.
#
# THE BOUND IS STATED IN DECADES OF CONDUCTANCE, not kJ/mol, because that is the unit
# the ratio is consumed in -- one decade per DIR_DECADE. Unbounded pass-through hands
# the graph asymmetries of 1e17, far past where the reverse branch is numerically dead.
DIR_DG_CLAMP = 3.0 * DIR_DECADE          # three decades == 1000:1

DIR_CATEGORIES = ("PHYSIOL-LEFT-TO-RIGHT", "LEFT-TO-RIGHT", "REVERSIBLE",
                  "PHYSIOL-RIGHT-TO-LEFT", "RIGHT-TO-LEFT")

DIR_COLUMNS = ("mnxr", "dG_prime", "sigma", "ratio",
               "dir_tier", "dir_method", "dir_confidence")
