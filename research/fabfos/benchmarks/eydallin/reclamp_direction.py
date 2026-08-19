# Re-derive the pinned direction annotation under a changed DIR_DG_CLAMP, without a re-bake.
#
# SCAFFOLDING, not a lane. It exists so a clamp change can be MEASURED against the pinned
# reference in a minute instead of a multi-hour ensemble rebuild; the authoritative table is
# still whatever `direction_ensemble` produces. Delete it once a real re-bake has landed.
#
# WHY A REPLAY IS SOUND HERE. DIR_DG_CLAMP enters `combine_row` in exactly one place
# (combine.py:105-108), AFTER mu_post, s_post, lambda and mu_eff = lambda*mu_post are all
# fixed. A clamp change therefore moves exactly three columns -- dG_prime, ratio, clamped --
# and the annotation records dG_raw and lambda_shrink, so mu_eff is recoverable. The script
# proves that isolation before it trusts it: it first re-applies the OLD bound and requires
# the shipped table back, column for column.
#
# WHY IT DOES NOT SIMPLY CALL combine_row. `src/ecspr`'s combiner is no longer the code that
# baked this table -- replaying through it moves 235 rows upstream of the clamp, because it
# now takes the measurement-precedence branch on eQuilibrator estimates whose sigma sits at
# the reporting floor, which the baking version declined as no-information. That divergence
# is real and wants fixing on its own terms; it is not this script's subject, and folding it
# in would make the clamp's effect unmeasurable.
#
#     mamba run -n msm python research/fabfos/benchmarks/eydallin/reclamp_direction.py OUT.parquet
#
# Then encode it against the bake's own vocabulary, which is what makes it loadable:
#
#     python -m ecspr.bake.metabolism direction --direction OUT.parquet --vocab <bake>/vocab.parquet --out <bake>/direction.parquet
import sys, math
from pathlib import Path
import numpy as np, pandas as pd

sys.path.insert(0, "src")
from ecspr.bake.direction import canon

OLD_CLAMP = 100.0
ANN = Path("data/fabfos/processed/metabolism_bake/seams/direction_annotation.parquet")
OUT = Path(sys.argv[1])

df = pd.read_parquet(ANN)
RT, DEC, NEW = canon.DIR_RT, canon.DIR_DECADE, canon.DIR_DG_CLAMP
print(f"[regen] clamp {OLD_CLAMP:g} -> {NEW:.4f} kJ/mol "
      f"({OLD_CLAMP/DEC:.2f} -> {NEW/DEC:.2f} decades; "
      f"{math.exp(OLD_CLAMP/RT):.3g}:1 -> {math.exp(NEW/RT):.0f}:1)")

mu = (df.lambda_shrink * df.dG_raw).to_numpy(float)


def apply_clamp(mu, bound):
    clamped = np.abs(mu) > bound
    return np.where(clamped, np.copysign(bound, mu), mu), clamped


mu_old, cl_old = apply_clamp(mu, OLD_CLAMP)
for col, got in (("dG_prime", mu_old), ("ratio", np.exp(mu_old / RT)), ("clamped", cl_old)):
    ref = df[col].to_numpy(float)
    bad = int((~np.isclose(ref, got.astype(float), rtol=1e-9, atol=1e-12)).sum())
    print(f"[regen] replay at old clamp: {col} matches on {len(ref)-bad:,}/{len(ref):,}")
    assert bad == 0, f"{col} did not replay -- the clamp is not isolated"

mu_new, cl_new = apply_clamp(mu, NEW)
out = df.copy()
out["dG_prime"], out["clamped"] = mu_new, cl_new
out["ratio"] = np.exp(mu_new / RT)

moved = ~np.isclose(df.ratio.to_numpy(float), out.ratio.to_numpy(float), rtol=1e-12)
lg = np.abs(np.log10(out.ratio.to_numpy(float)))
print(f"[regen] clamped rows: {int(cl_old.sum()):,} ({cl_old.mean():.2%}) -> "
      f"{int(cl_new.sum()):,} ({cl_new.mean():.2%})")
print(f"[regen] ratios changed: {int(moved.sum()):,} ({moved.mean():.2%})")
print(f"[regen] |log10 ratio| max = {lg.max():.6f} decades (cap 3.0)")
print(f"[regen] ratio range: {out.ratio.min():.6g} .. {out.ratio.max():.6g}")
assert lg.max() <= 3.0 + 1e-9
print("\n[regen] per-tier median ratio asymmetry (|log10|):")
for t, g in out.groupby("dir_tier"):
    o = df.loc[g.index]
    f = lambda s: np.abs(np.log10(s.ratio.to_numpy(float)))
    print(f"  tier {t}: n={len(g):>6}  {np.median(f(o)):.3f} -> {np.median(f(g)):.3f} decades")

OUT.parent.mkdir(parents=True, exist_ok=True)
out.to_parquet(OUT, index=False)
print(f"\n[regen] wrote {OUT}")
