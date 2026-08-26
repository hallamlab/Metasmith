#!/usr/bin/env python3
"""Which gain-of-function cohorts a curated carve cannot tell apart, and by how much.

    PYTHONPATH=$PWD/src mamba run -n ecspr python \
        research/fabfos/benchmarks/fba_vacuity.py

WHY THIS EXISTS INSTEAD OF THREE MORE ARMS. A flux balance arm over an overexpression
library reports a column of zeros, and a column of zeros beside a conductance column reads
as a negative result unless something says it could never have been anything else. For
these cohorts it could not, and the reason is one set difference: every reaction the clones
assert, the host already carries, so the carve keeps exactly the same reactions with the
clone as without it. `ecspr fba` implements `add` and only `add` -- the one convention that
reads a GPR as evidence about PRESENCE, which is what a conditions table can say -- and
`add` has nothing to add here.

VACUOUS IS NOT NULL, and the distinction is the whole point of this file. A null result
says the method was asked and had no answer. This says the method was never askable on this
cohort, which is a fact about the construction and not about flux balance.

**CAUTION** WOODRUFF'S NON-ZERO COUNT IS THE HOST'S OWN DELETIONS. Against `e_coli_lw06`
the clones assert 3 curated reactions and 10 de-novo ones the host lacks, and every one of
them is a reaction lw06 was engineered to remove -- `MNXR101037`, `MNXR198970` and
`MNXR99639`, on `frdA`, `frdB`, `frdD` and `ldhA`. Adding them back does not model a clone,
it un-deletes the strain, and `sweeps/sweep_woodruff_ratio.py` refuses them for that reason.
The same shape as the fuhrer gapfill: the only thing available to add is the host's genotype.
Against `e_coli_bw25113`, the strain lw06 was built from, the count is zero.

THE COUNT IS NOT ALWAYS ZERO AND THAT MATTERS. Where a handful of reactions ARE absent, the
arm is vacuous for every clone except those, and this file names how many rather than
rounding to "vacuous". Run the arm on a cohort whose count is non-zero; skip the ones at
zero and cite this table.

**CAUTION** THIS IS THE CURATED CHANNEL'S STATEMENT. A de-novo table's `intermediate_id` is
a KO or an EC number and reaches no frame reaction at all, so a de-novo clone can assert
MNXR the curated GEM never carried -- which is what `--mnx` and the insertion rules are for.
The de-novo columns are reported beside the curated ones for exactly that contrast.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import fba_arm as D                                                   # noqa: E402

RUNS = D.ROOT / "data/fabfos/runs"

#: `(cohort, host run, clone run, channel)`. The host is the background the arm unions the
#: clone onto, so it is the set the clone has to add to in order to change anything.
COHORTS = (
    ("eydallin2010", "e_coli_ag1", "eydallin_clones", "gem"),
    ("eydallin2010", "e_coli_ag1", "eydallin_clones", "denovo"),
    ("fang", "e_coli_k12", "fang", "gem"),
    ("fang", "e_coli_k12", "fang", "denovo"),
    # BOTH HOSTS, because this cohort has been measured against both and the answer is a
    # property of the pair. `sweeps/sweep_woodruff_ratio.py` runs on lw06 and the retired
    # `sweeps/sweep_scales.py` ran on bw25113, so a vacuity verdict quoted for one of them
    # says nothing about the other. Reading the wrong host has produced a wrong count in
    # this campaign before.
    ("woodruff", "e_coli_lw06", "woodruff_clones", "gem"),
    ("woodruff", "e_coli_lw06", "woodruff_clones", "denovo"),
    ("woodruff", "e_coli_bw25113", "woodruff_clones", "gem"),
    ("woodruff", "e_coli_bw25113", "woodruff_clones", "denovo"),
)


def _read(run: str, channel: str):
    p = RUNS / run / "gpr" / f"gpr_{channel}.parquet"
    return pd.read_parquet(p) if p.exists() else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", type=Path,
                    default=D.ROOT / "research/fabfos/benchmarks/fba_vacuity.tsv")
    a = ap.parse_args()

    rows = []
    for cohort, host_run, clone_run, channel in COHORTS:
        host, clone = _read(host_run, channel), _read(clone_run, channel)
        if host is None or clone is None:
            rows.append(dict(cohort=cohort, channel=channel, host=host_run,
                             clones=clone_run, note="a table is missing"))
            continue
        hm = set(host.mnxr.dropna().astype(str))
        hi = set(host.intermediate_id.dropna().astype(str))
        cm = set(clone.mnxr.dropna().astype(str))
        ci = set(clone.intermediate_id.dropna().astype(str))
        n_cond = (clone.condition_id.nunique()
                  if "condition_id" in clone.columns else None)
        rows.append(dict(
            cohort=cohort, channel=channel, host=host_run, clones=clone_run,
            conditions=n_cond, clone_mnxr=len(cm), mnxr_host_lacks=len(cm - hm),
            clone_ids=len(ci), ids_host_lacks=len(ci - hi),
            # BOTH SETS DECIDE IT. `carve` keeps a reaction whose own id is asserted OR
            # whose crosswalk maps it to an asserted MNXR, so a clone with no new MNXR can
            # still change the carve through an id -- which is exactly the Eydallin de-novo
            # case, 11 ids and not one reaction.
            verdict=("vacuous by construction" if not (cm - hm) and not (ci - hi)
                     else f"{len(cm - hm)} reaction(s) and {len(ci - hi)} id(s) "
                          f"can bite")))
    df = pd.DataFrame(rows)
    df.to_csv(a.out, sep="\t", index=False)
    print(df.to_string(index=False))
    print(f"\n-> {a.out}")
    vac = df[df.verdict == "vacuous by construction"]
    print(f"[vacuity] {len(vac)} of {len(df)} (cohort, channel) pairs cannot be moved by "
          f"an `add` convention at all. Report those as VACUOUS BY CONSTRUCTION, never "
          f"as a null: the method was not asked and failed, it was never askable.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
