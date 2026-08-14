"""``ecspr score`` -- delta, z, percentile rank, and the null-vs-control gate."""
from __future__ import annotations

from pathlib import Path

from .. import conditions as cond_mod
from .. import probes, scoring


def score(args, log):
    obs = probes.read_results(args.results)
    nul = probes.read_results(args.null)
    conds = cond_mod.read(args.conditions) if args.conditions else None
    if args.null_conditions:
        conds = (conds or []) + cond_mod.read(args.null_conditions)
    scored = scoring.score(obs, nul, baseline=args.baseline, conditions=conds)
    out = probes.write_results(scored, args.out)
    log(f"[score] {len(scored):,} scored rows -> {out}")

    g = scoring.gate(scored)
    if g.empty:
        log("[score] no readouts to gate")
        return 0
    gate_path = Path(args.gate_out) if args.gate_out else \
        Path(str(args.out)).with_suffix(".gate.tsv")
    g.to_csv(gate_path, sep="\t", index=False)
    log(f"[score] the gate -- null spread beside the control spread -> {gate_path}")
    log(g.to_string(index=False))
    if g.n_controls.max() == 0:
        log("[score] NOTE no conditions are marked is_control, so the noise floor "
            "was not measured; null_sd alone does not say a z is meaningful.")
    elif g.n_controls.max() < 2:
        log("[score] NOTE fewer than two controls, so control_sd (and with it "
            "null_over_control) is undefined. `control_max_abs` is what the floor "
            "rests on here -- a single no-op control can say the floor is zero, but "
            "not how wide it is.")
    return 0
