from __future__ import annotations

import math as _math

DIR_R = 8.314e-3
DIR_T = 298.15
DIR_RT = DIR_R * DIR_T
DIR_DECADE = DIR_RT * _math.log(10.0)

DIR_TAU_SHARED = DIR_DECADE
DIR_TAU_CUR_FLOOR = DIR_DECADE
DIR_S_MEAS_FLOOR = 0.1
DIR_SIGMA_CEILING = 100.0

DIR_SIGMA_0 = 9.505
DIR_SIGMA_0_BAND = (5.0, 40.0)

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
