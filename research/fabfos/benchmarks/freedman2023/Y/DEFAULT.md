# The sparse default

Same contract as the other studies: any `(condition_id, element, mnxm)` triple ABSENT
from `expectations.tsv` is `expected_dir=0`, and a condition whose measured direction is
UNKNOWN contributes no rows at all.

Three of this study's four conditions have NO rows here, and that is not an omission.
`H10_BK`, `ENV_F1` and `ENV_M1` were each isolated, re-transformed and fermented in
triplicate, and the authors report no significant difference from the plasmid control on
any product. They are MEASURED nulls, and `conditions.tsv:measured_dir = 0` is where that
is recorded -- the sparse default alone cannot tell a measured zero from an unmeasured one.

Only element `C` appears. The study varies nothing about N, P or S: one medium, one
nitrogen source, one sulfur source, no perturbation. Emitting N/P/S rows would score
directions nobody measured.

The three rows that exist are all one condition. Read that as the study's shape, not as a
sparse encoding win.
