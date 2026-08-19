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

DIR_DG_CLAMP = 100.0

DIR_CATEGORIES = ("PHYSIOL-LEFT-TO-RIGHT", "LEFT-TO-RIGHT", "REVERSIBLE",
                  "PHYSIOL-RIGHT-TO-LEFT", "RIGHT-TO-LEFT")

DIR_COLUMNS = ("mnxr", "dG_prime", "sigma", "ratio",
               "dir_tier", "dir_method", "dir_confidence")
