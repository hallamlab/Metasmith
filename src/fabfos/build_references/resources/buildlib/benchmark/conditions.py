import argparse as _argparse
import ast as _ast

_p = _argparse.ArgumentParser()
_p.add_argument("--columns", required=True)
_p.add_argument("--cond-gpr", required=True)
_p.add_argument("--elements", required=True)
_p.add_argument("--extracts", required=True)
_p.add_argument("--het", required=True)
_p.add_argument("--lib", required=True)
_p.add_argument("--out", required=True)
A = _p.parse_args()
_LIT_columns = _ast.literal_eval(A.columns)
_LIT_elements = _ast.literal_eval(A.elements)

import os, sys
import pandas as pd

sys.path.insert(0, os.path.dirname(A.lib))
from bench_cohorts import load_all_cohorts

COLUMNS = _LIT_columns
ELEMENTS = _LIT_elements

cond_gpr = pd.read_parquet(A.cond_gpr)
genes = load_all_cohorts(laser=str(A.extracts) + "/laser", keio=str(A.extracts) + "/keio",
                         eydallin=str(A.extracts) + "/eydallin", het=A.het)

# The join that makes X derivable rather than stored. A condition may only be emitted if
# the condition GPR knows the edge set it names -- building the two independently and
# joining later produces a conditions table that looks complete and silently drops rows at
# scoring time.
#
# KEYED ON (cohort, condition_id), WHICH IS HOW THE COHORT FRAME IS KEYED. On the
# condition_id alone the guard was blind to exactly the case it exists to catch: 221 LASER
# observations appear under BOTH gof_native and gof_het, so a heterologous entry with no
# edge row still saw its own condition_id present via the native half and the guard printed
# `0 dropped` while 11 gof_het entries were absent from the edge table entirely.
known = set(zip(cond_gpr["cohort"], cond_gpr["condition_id"]))
named = set(zip(genes["cohort"], genes["condition_id"]))
missing = {k for k in named - known if k[1]}
if missing:
    shown = sorted(missing)[:10]
    more = f" and {len(missing) - len(shown):,} more" if len(missing) > len(shown) else ""
    print(f"[conditions] {len(missing):,} entries name no edge in the condition GPR and "
          f"are dropped: {shown}{more}", flush=True)
genes = genes[[(c, i) in known for c, i in zip(genes["cohort"], genes["condition_id"])]]

# ONE ROW PER OBSERVATION AND ELEMENT, NOT PER COHORT. A cohort is a property of a GENE
# -- whether it is the host's own or came from somewhere else -- and an observation that
# perturbs both is still one perturbation. Grouping by (observation, cohort) emitted two
# rows carrying the SAME condition_id with different gene lists for the 221 LASER records
# that do both, so 1,680 of 3,336 rows were duplicate ids and the run-time constructor,
# which keys on condition_id, would apply half of each such observation.
ARM = {"gof_native": "gof", "gof_het": "gof", "lof": "lof", "eydallin": "lof"}
genes["_arm"] = genes["cohort"].map(ARM).fillna(genes["cohort"])
mixed = genes.groupby("condition_id")["_arm"].nunique()
if (mixed > 1).any():
    raise SystemExit(
        f"[conditions] {int((mixed > 1).sum()):,} observations span more than one ARM "
        f"(e.g. {sorted(mixed[mixed > 1].index[:3])}). Collapsing them to one condition "
        f"would have to pick an arm, which is a decision this step must not make silently.")

per_obs = (genes.groupby("condition_id", sort=False)
           .agg(gene=("gene_label", lambda s: "|".join(sorted(set(s)))),
                n_units=("gene_label", "nunique"),
                arm=("_arm", "first"),
                cohort=("cohort", lambda s: "+".join(sorted(set(s)))),
                host=("host_hint", lambda s: next((x for x in s if x), "")),
                actions=("action", lambda s: sorted({a.strip() for v in s
                                                     for a in str(v).split(",")
                                                     if a.strip()})))
           .reset_index())

rows = []
for r in per_obs.itertuples(index=False):
    for el in ELEMENTS:
        rows.append({
            "condition_id": f"{r.condition_id}__{el}",
            "arm": r.arm,
            "tier": "scored",
            # `ptype` is the shape of the perturbation, read off the actions the cohort
            # recorded rather than assumed from the arm: a LASER record can add and delete
            # in the same observation. Taken from the cohort frame's own action tokens --
            # the old form stringified a Python list and split it on commas, so a
            # phenotype containing one fragmented into pieces.
            "ptype": "+".join(r.actions) or "unknown",
            "host": r.host,
            "gem": "",
            "n_units": int(r.n_units),
            "is_control": "",
            "citation": "",
            "note": "",
            "gene": r.gene,
            "element": el,
            # The target metabolites are a curated resolution (curated/benchmark_decisions/
            # target_resolution.tsv in the incumbent tree) and are NOT invented here. Left
            # empty rather than guessed: a wrong target silently scores the wrong axis.
            "target_mnxm": "",
            "target_name": "",
            "target_basis": "",
            "expected_dir": "",
            "essential_on_glucose_minimal": "",
            "is_neg": "",
            "obs_id": r.condition_id,
            "host_gem_is_proxy": 0,
        })

df = pd.DataFrame(rows, columns=list(COLUMNS))
df = df.sort_values(["condition_id"]).reset_index(drop=True)
# The id the run-time constructor keys on must identify one row. Asserted, because the
# way this broke was invisible: two rows, same id, different gene set, table the right
# shape and the right length.
if df["condition_id"].duplicated().any():
    d = df[df["condition_id"].duplicated(keep=False)]
    raise SystemExit(f"[conditions] {len(d):,} rows share a condition_id, which the "
                     f"run-time network construction keys on:\\n{d.head(6).to_string()}")
df.to_csv(A.out, sep="\t", index=False)
print(f"[conditions] {len(df):,} rows over {df['obs_id'].nunique():,} observations "
      f"x {len(ELEMENTS)} elements; arms {df['arm'].value_counts().to_dict()}",
      flush=True)
print("[conditions] NOTE target_mnxm/target_name/target_basis/expected_dir are EMPTY: "
      "they come from curated/benchmark_decisions/target_resolution.tsv, which is a "
      "hand-authored input this transform does not declare. See REFERENCES.md C2.",
      flush=True)
