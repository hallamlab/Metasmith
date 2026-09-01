# Can the direction lane be gap-filled? Measured, and mostly no

## Purpose & Contents

36,151 of 83,795 reactions carry no direction evidence at all. This directory measures
whether the existing substitution machinery can close that, so nobody re-derives the
answer by authoring curation first and counting afterwards.

    PYTHONPATH=src mamba run -n rdkit-scratch python admission_report.py   # per-row verdicts
    PYTHONPATH=src mamba run -n rdkit-scratch python coverage_curve.py     # greedy set cover

## Where the tier-0 reactions are

| `dir_method` | reactions |
|---|---|
| `no_evidence` | 24,418 |
| `refused` (dGbyG declined a `*` participant) | 11,733 |

`no_evidence` has nothing to compute on. `refused` is the half `substitute.py` was built
for, so it is the one worth pricing.

## The substitution lane works. It converts 73 reactions.

`substitutions.tsv` holds 45 rows. For dGbyG 37 are admitted and 8 refused; for
eQuilibrator 28 are admitted and 17 refused. Every refusal is `member_drift` -- the row is
kept for the other member. The table is not broken and no accession is mismatched.

The admitted set touches 1,723 reactions, 665 of which are still refused. So an admitted
substitution is present on 665 refused reactions and converts almost none of them.

**The reason is that coverage is conjunctive.** dGbyG walks the participants and returns at
the first one it cannot use, so a reaction needs EVERY wildcard covered, not one. All 665
have at least one wildcard left after substituting what the table covers:

| wildcards left | reactions |
|---|---|
| 1 | 402 |
| 2 | 217 |
| 3 | 43 |
| 5 or 6 | 3 |

## The curve is flat, so authoring rows is not the lever

Greedy set cover over the 11,733, adding the metabolite that completes the most reactions
at each step:

    + 1  a reduced two electron carrier      unlocks 190   cumulative   190
    + 5  phosphatidate(2-)                   unlocks  20   cumulative   289
    +10  Cholesterol ester (from FULLR2)     unlocks  48   cumulative   428
    +20  Phosphatidylcholine                 unlocks  40   cumulative   627

Twenty curated rows buy **627 of 11,733 reactions, 5.3%**, and each row needs an anchor
reaction and a defensible model compound. The head is one pick deep and then flat.

**CAUTION** — that 627 is an upper bound, not a target. Most of the top picks are GENERIC
species: `a reduced two electron carrier`, `a quinol`, `a dicarboxylate`, `Peptides-holder`,
`R total 2 position`. `substitutions.tsv` already declines `A`, `AH2` and `Acceptor` on the
potential gate for the reason that applies to all of them -- a generic acceptor has no
tabulated potential to be right about, and a model compound chosen for it would be an
invention. Rows of that kind are refusals waiting to happen, not coverage.

## What is worth doing instead

The `member_drift` decline on the two flavin couples and the two NADPH-hemoprotein
reductase couples costs 377 refused reactions, and those are real redox couples with real
tabulated potentials -- unlike the generic head above. Whether dGbyG's drift there is the
gate working or the gate being too tight for one member is a bounded question with a real
answer. It is the only substitution lever this measurement supports.
