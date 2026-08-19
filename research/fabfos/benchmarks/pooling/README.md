# pooling — what a reaction's evidence weight should be

Measurements behind `ecspr.model.evidence`'s log-odds pooling stage. All four scripts run
against `data/fabfos/runs/e_coli_k12/gpr` and score against iML1515's own reactome; run
them from anywhere, they resolve the repo root themselves.

**Read the truth set with suspicion.** iML1515 is E. coli K-12's curated reactome, so a
reaction is in it partly because it is *well studied* — which is the same reason it carries
many repeated annotations. Any change that discounts repetition is therefore scored by a
metric that rewards repetition. `n_ref` alone reaches P@100 0.70, better than the belief
weights ever did, and that number is the size of the problem rather than a result. AUROC
here is reported, never gated.

## What the four scripts settled

`divide_test.py` and `subunit_belief.py` asked whether the inflation is a *complex-size*
artifact — a four-subunit enzyme collecting four ORFs' worth of belief — and whether
dividing by the ORF count fixes it. It is not, and it does not: every family-size divisor
tested ranked worse (`E/n_orf` 0.762 → 0.756, `E/(1+n_ref)` collapsed to 0.499). The belief
quota is per-ORF, so a complex does collect more, but that is not what puts the outliers at
the top.

`expected_orfs.py` found what does. `MNXR172198` sat at E = 12.3, rank 6, on 65 ORFs all
asserting one EC number through one channel with no corroboration — while 848 of 2,137 ORFs
carry all three channels and got no credit for the agreement. The defect is that repetition
and independence were the same arithmetic, not that the numbers were too large.

`rank_eval.py` is the live one: it prints the panel used to choose `POOL_LAM0 / POOL_LAM1 /
POOL_TAU`, and `--sweep` walks the grid. Two things it makes obvious and neither of which is
guessable from the formula:

* **Only `tau` moves the ranking.** `lam0` shifts and `lam1` scales the pooled log-odds
  uniformly, so both are monotone in it and every ranking metric is invariant to them. They
  are a choice of dynamic range, not a fit.
* **The unconfounded number is `1-assert@100`** — the share of the top 100 resting on a
  single `(unit, channel, evidence)`. It went 25% → 0%, and `>1-chan@100` 63% → 93%, while
  AUROC moved 0.762 → 0.771 and P@100 0.49 → 0.66.

`uniprot_per_mnxr.parquet` is the family-size prior `expected_orfs.py` reads: distinct
UniProt proteins per MNXR, derived once and cached here because regenerating it needs the
full UniRef join.
