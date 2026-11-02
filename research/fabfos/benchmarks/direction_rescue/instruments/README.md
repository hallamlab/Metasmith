# Instruments

Four checks the re-bake protocol needs and the pipeline does not run for you. Each was
written against a real failure and demonstrated by negative control before being trusted —
an instrument that cannot fail on the case it exists for is not evidence.

All of them expect `PYTHONPATH=src` and an env with pandas + rdkit (`rdkit-scratch`).

## `sigma0.py` — re-derive `DIR_SIGMA_0`

`canon.DIR_SIGMA_0` is a committed constant whose estimator lives in **prose** and is
computed by no code in the pipeline. This is that estimator: the robust marginal spread
(1.4826 × MAD) of measured ΔG′ on the eQuilibrator **reactant**-contribution arm, over rows
that clear `DIR_SIGMA_FLOOR`.

Fit it from the calibration run's **own points table**, never from the annotation. It prints
the all-rows fit and the **unsubstituted-subset** fit, and the unsubstituted one is what gets
committed — asserted chemistry must not set the prior width that shrinks every row in the
table. Check the result against `DIR_SIGMA_0_BAND` by eye; the guard tests the argument, not
the fit.

Validated by reproducing r8 exactly from r8's own points: n=532, median −1.5397, σ₀ 23.4892.

## `prove_subs.py` — did the substitution actually reach the members?

Four checks per member, in increasing order of how hard they are to pass by accident. The
first is not a formality: `drive.cmd_eval` catches an ImportError, writes an **empty** member
table and returns **0**, so a member missing from an image produces a green run, a clean exit
and no votes.

The fourth is the one nothing else checks — that reactions the tables do **not** cover are
unchanged row for row. The whole mechanism rests on "no MetaNetX props key is ever
overwritten", and a diff there is worse than a missing substitution: it means chemistry moved
where no table touches it. Unit tests assert this locally; this asserts it at the far end of
the cluster.

## `splice.py` — price per-member admission without a cluster run

Where a member's substitution is refused, that member sees the **unsubstituted** equation —
which is exactly what the deployed bake's member table already holds. Splicing those rows in
simulates the refusal precisely, so a coverage cost can be priced against the real assembly
instead of estimated. Feed the result to `reassemble.py --dgbyg`.

## `check_stage.py` — the gate before `dvc add`

Run as `check_stage.py <deployed> <staged>`, required to exit 0 **before** the staged chunk is
pinned. Five checks, and the first one's failure is the only destructive one here: staged
files must share **zero inodes** with the deployed tree, because those are read-only hardlinks
into a DVC cache pinned by every sibling worktree and every historical commit that references
it. Editing one in place corrupts the object for all of them.

It discovers the tree's internal hardlink pairs **by inode** rather than hardcoding them, so a
new pair is covered without editing the script — that is the whole reason the stage uses
`cp -a`.
