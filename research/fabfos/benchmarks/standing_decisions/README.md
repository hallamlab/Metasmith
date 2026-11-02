# The four decisions the bake has carried open, each closed with a number

## Purpose & Contents

Four questions have been carried across generations r7 to r10 without a measurement:
the body-cancel relaxation, the `MNXM8348` polymer acceptor, the flavin `member_drift`,
and the polymer ladder's second inconsistency source. Two benchmark scopes have since
filed against two of them. This directory records what each one actually costs and what
would change it, so the next generation argues with a number rather than with a memory.

The flavin decision has its own directory -- see `../flavin_couple/` -- because it turned
out to be a defect rather than a judgement call.

## 1. The body-cancel relaxation -- NOT the lever it is remembered as

`gate_bodies_cancel` refuses a balance when the unpaired `*` residues do not cancel across
the equation. Relaxing it was measured before this session: **+527 reactions, +0.6 points**,
and the permissive variant that hands every generic a `*` is the stop line's own
definition of wrong -- an unpaired `*` counted as zero atoms makes the balance test
describe a molecule that does not exist.

**Disposition: the decision is narrower than it looks, and the measurement that would move
it is not a re-run of the relaxation sweep.** The loss underneath the gate is an
ACCOUNTING loss, not a gate loss: `count_element` returned `None` for any formula holding
`*`, `(` or `)`, so a formula stating its carbon exactly was read as stating nothing.
`atom_pairs.element_budget` now splits that single `None` into the two propositions it was
standing for -- an explicit count that may be summed, and a residue that may only be
required to cancel. Measured on the partial lane's own refusals:

| | |
|---|---|
| `no balanced reduction` slots the old rule refused | 11,731 |
| recovered by the recount, with structure for every kept participant | **7,919** over 2,611 reactions |
| of those reactions, already banked | 2,540 |
| genuinely new | **66 reactions / 306 keys** |

So the gate was never the thing to relax. The number it was hiding is real but small,
and it is now recoverable without asserting anything about a residue.

**CAUTION** -- a lower bound is not a count. `element_budget` returns the pair, and the
reduction may drop a participant only when BOTH halves are zero. Collapsing them again
anywhere would repeat the r7 defect in a new place.

## 2. `MNXM8348` -- a schema question, and the answer is that the schema cannot express it

MetaNetX supplies branching glycogen's model (MNXR145022 identifies MNXM8348 with
MNXM738131) but pairs it with no trisaccharide in any balanced reaction, so its acceptor
would have to be a BRANCHED trisaccharide chosen among several the anchor cannot separate.
The table refuses inventions, so the row has stayed out and MNXR145038 / MNXR145039 have
stayed tier 0.

The strategy piloted here was to change what the row supplies: a polymer row's job is the
acceptor's ATOM BUDGET, not its identity, and MNXR145021 -- glycogen to branching
glycogen, identical formula, identical charge -- is MetaNetX stating that the two carry
the same composition.

**Disposition: the row schema cannot express a budget, and that is the honest answer.**
`substitutions.tsv` names an acceptor through `terms`, which `_parse_terms` resolves to
MODEL keys, and `models.tsv` resolves a MODEL key to a STRUCTURE with a `source_mnxm` that
`_admit_models` checks the formula against. Every step of that chain is keyed on a
compound. A budget-only row would need a second row kind that carries a formula and no
structure, and a polymer scope that accepts a deficit closed by a formula rather than by
an inserted acceptor. That is a table-format change, not a curation call, and it is worth
doing only if something other than these two reactions wants it.

The cost stays exactly what the table already records: MNXR145038 and MNXR145039 tier 0,
and MNXR157776 with them. It is not "open for the principal" any more -- it is closed
against the schema, which is a different and more actionable statement.

## 3. The flavin `member_drift` -- see `../flavin_couple/`

Short version: the refusal is right, the recorded reason is wrong. The four rows that fail
are exactly the four whose model couple is internally inconsistent by one proton, and
every other couple in the table is clean. Four of four, no false positives.

## 4. The polymer ladder's second inconsistency source -- the hypothesis is refuted

The standing hypothesis was that `lane_fragment`'s token key discards a bare integer, and
that on a polymer ladder that integer IS the chain length -- so a polymer and its extended
product would receive the same carbon count and a glycosyltransfer would be refused for an
imbalance the chemistry does not have.

Measured over all 61,550 structureless metabolites:

| | colliding keys | metabolites covered |
|---|---:|---:|
| `_LOCANT` drops a bare integer (today) | 4,934 | 14,759 |
| a trailing integer retained | 4,238 | 9,725 |
| collisions the integer would SPLIT | **1,342** | |

**Disposition: refuted, and for a reason that also rules the fix out.** The collisions the
integer resolves are POSITIONAL ISOMERS -- `2-`, `3-` and `4-hydroxypentanoyl-CoA` under
one key, `3-hydroxydecanoyl-acp` with `(3R)-hydroxydecanoyl-[ACP]` under another. A
positional isomer has the same formula as its sibling, so separating them cannot change an
atom budget. And the mechanism forbids it structurally: `lane_fragment` sums the budgets
of the tokens that RESOLVE to a metabolite, and a bare integer resolves to nothing, so
retaining it changes the grouping and cannot change one sum.

No chain-length ladder appeared in the split set at all. The second source of the 183
inconsistent families is still unfound, and the 621-key debt the current bake owes the
retired one is still owed. **Carried, not fixed** -- and the next session should look
somewhere other than the token key, because this was the obvious place and it is empty.
