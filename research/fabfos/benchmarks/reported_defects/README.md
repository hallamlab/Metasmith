# The reactions two scopes reported, pinned before and after a re-bake

## Purpose & Contents

`fabfos/bench-fba` and `fabfos/bench-eydallin` each filed a defect report naming specific
accessions. Their own drivers rest on run data this worktree does not hold, so this
directory pins the part that is reproducible here and is the only part a re-bake moves:
what the reference itself says about those reactions.

    PYTHONPATH=src mamba run -n ecspr python baseline.py \
      --chunk data/fabfos/processed/metabolism_bake --out baseline_deployed.tsv

Run it against a new chunk and diff. `baseline_deployed.tsv` is the BEFORE, taken on the
deployed bake (vocabulary digest `0ffd4c8c6231696e`, direction stamp `554b4d8f9293e782`).

**CAUTION** — bench-eydallin's own driver is `research/fabfos/benchmarks/eydallin/parse/gof_audit.py`
on branch `fabfos/bench-eydallin` at `f203772`. It needs
`data/fabfos/runs/eydallin_clones/parse/gof/gof.csv`, a product of their pipeline and not a
DVC artifact, so it does not run from this worktree. Ask them to re-run it after a promote.

## Both reports reproduce exactly

Every number either scope quoted comes back from the deployed bake unchanged, so neither
report rests on a stale decode.

### Direction lane

| reaction | dG′ | tier | reported as |
|---|---|---|---|
| MNXR132476 | −0.198 | 2 | −0.20, anchor |
| MNXR145632 | −4.442 | 2 | −4.44, anchor |
| MNXR145633 | −1.795 | 2 | −1.80 |
| MNXR145634 | −8.987 | 2 | −8.99 |
| MNXR145635 | −3.783 | 2 | −3.78 |
| MNXR145046 | −0.198 | 2 | glgA |
| MNXR145036 | +4.442 | 2 | glgP/malP |
| MNXR145021 | −6.105 | 2 | −6.10, glgB |
| MNXR146501 | +17.123 | 2 | talA, at the rail |
| MNXR101036 | +17.123 | 2 | transketolase, same rail |

The propagation claim holds exactly. `MNXR145046` is byte-identical to anchor `MNXR132476`,
and `MNXR145036` is anchor `MNXR145632` negated to three decimals. The unbalanced
accessions inherit the anchors rather than deriving anything of their own.

`MNXR145050` is tier 1 as they said, so it is untouched by anything here.
`MNXR157776`, `MNXR145038` and `MNXR145039` are tier 0 / `no_evidence` — the recorded cost
of excluding `MNXM8348` as a polymer, a decision `substitutions.tsv` leaves open.

### Atom lane

`MNXR166070` carbon: rank 0 of MNXM1104186 reaches L-rhamnulose at 1.0, and ranks 8, 10,
11, 12 and 13 each split 0.5 between L-rhamnulose and ATP — the five carbons reported.
MNXM40333 ranks 10, 20-23 carry the within-ATP dilution they called benign. `MNXR206779`
carries the same defect.

`MNXR116249` is the curated twin, and it is CLEAN: all six MNXM1104186 carbons reach
MNXM38657 at 1.0 and all ten MNXM40333 carbons reach ATP at 1.0. The same chemistry is
already correct in the bake under the accession MetaNetX's crosswalk filed MetaCyc under.

## After the fix, in the source tree

Not yet in any chunk -- these come from running the new code over the deployed caches.
`verify_atom_fix.py` re-extracts the two reported reactions from `aam_cache` and fuses them.

    PYTHONPATH=src mamba run -n rdkit-scratch python verify_atom_fix.py

Indigo returns `injectivity_trimmed` on both, with 9 reused map numbers each; RXNMapper
returns `ok`. Fused, both reactions give:

| sugar carbon rank | destination | pair_w | method |
|---|---|---|---|
| 0 | L-rhamnulose | 1.000 | consensus |
| 8, 10, 11, 12, 13 | L-rhamnulose | 0.500 | rxnmapper_only |

Carbons reaching ATP: **0**, against five at 0.5 before. ADP's ten within-molecule carbon
rows survive with the same five diluted, so the benign ambiguity the report set aside is
untouched.

**CAUTION** — the five corrected carbons carry 0.5, not 1.0. Indigo abstains on the atoms
it contradicted itself about, so RXNMapper carries them alone and a lone member earns half
credit. That is a weaker edge than a consensus, and it is the right answer: the alternative
was half the weight landing on ATP.

## What the coefficient rule restores

Measured by fusing the three deployed member seams with and without `--reac-prop`:

| | value |
|---|---|
| reactions writing a substrate more than once | 10,451 |
| pair keys before / after | 2,698,051 / 2,724,994 |
| edges restored (absent before) | **26,943 over 2,569 reactions** |
| edges lost | **0** |
| total weight before / after | 2,117,573.5 / 2,117,573.5 |

Total weight is unchanged to the decimal because the weight is margin-conserving: two
copies of a participant share one atom node, so the margin divides across the destinations
instead of doubling. Every restored edge falls inside a doubled-substrate reaction.

Fusing with no coefficients reproduces the previous fusion exactly -- 2,698,051 pair keys,
zero differing by more than 1e-9, identical method mix -- so the change is inert wherever a
participant is written once.

## Validated on a real Sockeye run

`aam_stack` ran the full assemble step on Sockeye (~9 min, node se019/se036) with the fixed
fusion, against the deployed member tables. Its output compared to the deployed
`seams/aam_stack.parquet`:

| | deployed | new |
|---|---|---|
| rows | 2,589,456 | 2,609,669 |
| reactions | 69,292 | 69,292 |
| `(mnxr, element)` keys | 175,034 | 175,034 |

    edges only in NEW (restored) :  97,890
    edges only in OLD (trimmed)  :  77,677
    keys lost : 0        reactions lost : 0

**The stop-line holds.** The scope's goal says coverage may only go up, and the injectivity
trim REMOVES correspondences -- 1,622 unexplained rows were `indigo_only`, so a key Indigo
alone covered could have been emptied. None was. Every one of the 77,677 removed edges is a
transfer a reused map number invented, and the reaction and key sets are unchanged.

**CAUTION** -- the run does NOT complete. `aam_redox`, the step after the stack, fails with
`MissingInstanceError: required instance_id=... missing for dep='RWH8b6OZ'` (the
`interm::aam_stack` product). The required id changes every run. This is not caused by the
AAM change: it reproduces after reverting the transform's requirement set to its original
form, and its first occurrence predates rebuilding `src/metasmith_libraries/*/_metadata`.
So there is no r11 chunk yet, and `atom_pairs.parquet` still holds the numbers both defect
reports measured.
