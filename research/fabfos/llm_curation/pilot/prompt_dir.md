# Task: call the physiological direction of a biochemical reaction

You are acting as a curator in an automated metabolic-model pipeline. For each reaction
below, decide which way it runs **in living cells**, as the equation is written.

The equation is written `LEFT = RIGHT`. Compound names are MetaNetX names and some are
deliberately generic (`an acceptor`, `a reduced two electron carrier`,
`[Oxidized NADPH---hemoprotein reductase]`, `a quinone`) because the database has no
structure for them. A generic name is still informative: you usually know what class of
carrier it is and which way that class runs in the reaction shown.

## What to output, per reaction

One JSON object per line, no other text:

    {"mnxr": "...", "call": "...", "confidence": "...", "reason": "..."}

`call` is exactly one of:

- `LEFT_TO_RIGHT` — in vivo this proceeds as written, left to right
- `RIGHT_TO_LEFT` — in vivo this proceeds right to left, i.e. the equation is written backwards
- `REVERSIBLE` — it genuinely operates in both directions depending on conditions (near-equilibrium
  reactions: most isomerases, mutases, many transaminases and aldolases)
- `UNKNOWN` — you cannot tell from what is shown

`confidence` is `high`, `medium` or `low`.

`reason` is ONE short clause naming the actual warrant — the enzyme or the thermodynamic
driver. "ATP hydrolysis drives it", "decarboxylation is irreversible", "NADPH-dependent
reduction", "monooxygenase, O2 consumed". Not "it looks like it goes this way".

## How to decide

Prefer these warrants, strongest first:

1. **An irreversible chemical step**: ATP/GTP hydrolysis to ADP/AMP + Pi/PPi, decarboxylation
   (CO2 released), O2 consumed by an oxygenase or oxidase, PPi released and hydrolysed.
2. **A named enzyme you recognise** and the direction it is known to run physiologically.
3. **Cofactor logic**: NAD(P)H + O2 consumed together means oxidation of the other substrate;
   a reduced carrier plus O2 on the same side means that side is the substrate side.
4. **Pathway context**: biosynthesis vs catabolism, if the metabolites place it.

## Rules that matter more than coverage

- **Abstaining is a valid, useful answer.** `UNKNOWN` costs the pipeline nothing; a confident
  wrong call corrupts a metabolic network. If your warrant is only "this feels biosynthetic",
  answer `UNKNOWN` or `low` confidence.
- **`REVERSIBLE` is a real answer, not a hedge.** Use it when the chemistry is near-equilibrium.
- Judge the equation **as written**. Do not normalise it to the direction you would write it in.
- Ignore whether the equation is balanced; that is checked elsewhere.

## Reactions
