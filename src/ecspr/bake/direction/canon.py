"""The direction ensemble's frozen constants.

The build-side mirror of ``src/fabfos/canon.py``'s ``DIR_*`` block, carried here because
a transform's driver runs in a conda env holding rdkit and equilibrator, not the fabfos
package -- there is no import path from inside the run to the CLI's canon module.

The two copies are checked against each other by ``build_references/check_references.py``
rather than trusted to stay in step by convention: they are the same numbers used at two
different times (this one to COMPUTE a ratio, canon.py's to VALIDATE a table that carries
one), and a silent divergence would produce a table that passes its own validator while
meaning something else.

Every value below is committed BEFORE a run, which is what makes them constants rather
than fitted parameters. ``DIR_SIGMA_0`` is the one exception in kind and says so.
"""
from __future__ import annotations

import math as _math

DIR_R = 8.314e-3                        # kJ/mol/K
DIR_T = 298.15                          # K
DIR_RT = DIR_R * DIR_T                  # the ratio's natural scale, ~2.48 kJ/mol
DIR_DECADE = DIR_RT * _math.log(10.0)   # one decade of conductance, ~5.71 kJ/mol

# Floors, committed BEFORE the run. TAU_SHARED is the TECRDB common-mode error the
# correlated eQ/dGbyG pair's spread cannot see; it floors a PREDICTED thermo vote
# (group-contribution arm / dGbyG) and the fused pair, so two correlated predictors
# never vote as two independent. TAU_CUR_FLOOR: a curated category alone resolves no
# better than one decade. Both are physical, not tuned.
DIR_TAU_SHARED = DIR_DECADE
DIR_TAU_CUR_FLOOR = DIR_DECADE
DIR_S_MEAS_FLOOR = 0.1                  # kJ/mol; numerical only -- a real measurement
                                        # is trusted at its own sigma
DIR_SIGMA_CEILING = 100.0               # kJ/mol; a wider eQ uncertainty is no
                                        # information -> the reaction is eQ-silent

# The reversible-default prior width = robust marginal spread of measured dG' on the
# eQuilibrator reactant-contribution arm (1.4826*MAD). ESTIMATOR committed here; the
# VALUE is frozen from the calibration run that produced it (464 measured reactions,
# marginal median 0.000 -> no orientation offset). It must fall in the plausibility
# band or it is a finding, not a constant. The robust spread runs BELOW the
# outlier-inflated std, i.e. toward more shrinkage / more reversible -- the safe side.
DIR_SIGMA_0 = 9.505                     # kJ/mol
DIR_SIGMA_0_BAND = (5.0, 40.0)          # outside => stop, it is a finding

# The ratio must stay a FINITE two-way conductance ratio -- never a hard one-way
# gate (the standing ruling). A handful of macromolecular/polymer reactions carry a
# genuine |dG'| of thousands of kJ/mol, whose exp() underflows to 0.0 (an infinite
# gate), so |dG'| is clamped before the exponential; clamped rows are flagged, not
# hidden.
#
# THE BOUND IS STATED IN DECADES OF CONDUCTANCE, not in kJ/mol, because that is the
# unit the ratio is consumed in. Three decades = the ensemble's confident level,
# 1000:1. A thermodynamic dG' is a real energy and exponentiating it converts one
# decade per DIR_DECADE (5.71 kJ/mol), so an unbounded pass-through hands the graph
# asymmetries of 1e17 -- far past the point where the reverse branch is already
# numerically dead, and past anything the evidence resolves.
DIR_DG_CLAMP = 3.0 * DIR_DECADE          # kJ/mol; three decades == 1000:1

# The five curated REACTION-DIRECTION values, in MNXR orientation. A sixth token
# would be a KeyError at the aligner, not a silent default (which is how the ~7%
# right-to-left corpus would otherwise invert).
DIR_CATEGORIES = ("PHYSIOL-LEFT-TO-RIGHT", "LEFT-TO-RIGHT", "REVERSIBLE",
                  "PHYSIOL-RIGHT-TO-LEFT", "RIGHT-TO-LEFT")

# Emitted schema. dir_tier > 0 means "carries directional information", NOT "usable":
# every row is usable and ratio==1.0 (reversible) is a real physical statement, so a
# consumer must read ALL rows. A `df[df.dir_tier>0]` filter would silently drop every
# reversible reaction -- turning "default reversible" into "default absent".
DIR_COLUMNS = ("mnxr", "dG_prime", "sigma", "ratio",
               "dir_tier", "dir_method", "dir_confidence")
