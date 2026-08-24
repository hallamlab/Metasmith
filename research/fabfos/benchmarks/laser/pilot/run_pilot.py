#!/usr/bin/env python3
"""Generic LASER pilot: <host GEM> background vs <host GEM> + <condition edits>, measured
at a named target under the universal-leakage ("universal ground") method.

    ./dev/ecspr.sh -e ...            # the measurement itself, whatever this script wraps
    mamba run -n ecspr python research/fabfos/benchmarks/laser/pilot/run_pilot.py \
        --condition-id "LASER:Record1419612292.57:M1" --host e_coli_k12 --target Isoprene

Writes research/fabfos/benchmarks/laser/pilot/cache/pilot_<slug>.json, and the
conditions table it measured against beside it.

WHAT THIS SCRIPT IS FOR, NOW THAT ECSPr HAS A CLI
--------------------------------------------------
Everything here is the EXPERIMENT DESIGNER'S half, and it is here rather than in the
`ecspr` package on purpose:

  * joining the bake's direction ensemble through the vocabulary onto MNXR;
  * resolving a metabolite NAME ("Isoprene") to an MNXM id, restricted to the
    atom-pairs universe and broken toward the most-connected candidate;
  * turning a LASER record's add/del rows into a MASK -- the added rows are selected
    by condition_id, and a deleted reaction is withheld by its MNXR, which takes the
    host's rows for it out too;
  * the AAM coverage diagnostic, which distinguishes "zero flux" from "never became
    a node".

None of those is a measurement, and each of them is a choice this study made. The
measurement is `ecspr ground`, run below with no arguments the deployed transform
does not also pass.

NOT the deployed reference basis -- tier4 atom pairs, not the canonical MNXref release
(same caveat examples/scadc_ecspr.py's own docstring states). Fine for a pilot, not a
number to publish as-is.

Supersedes run_pilot_lycopene.py, whose one invocation was:
    --condition-id "LASER:Record1423246634.26:M1" --host e_coli_k12 --target Lycopene
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd

from ecspr.model import conditions as cond_mod
from ecspr.model import probes

ROOT = Path(__file__).resolve().parents[5]
DEFAULT_OUT_DIR = Path(__file__).resolve().parent / "cache"

ATOM_PAIRS = ROOT / "data" / "fabfos" / "benchmark" / "reference_tier4" / "atom_pairs_tier4.parquet"
CHEM_PROP = ROOT / "data" / "fabfos" / "originals" / "metanetx" / "4.5" / "chem_prop.tsv"
RUNS = ROOT / "data" / "fabfos" / "runs"
EDITS = ROOT / "data" / "fabfos" / "benchmarks" / "laser" / "gpr_manual.parquet"

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import bake_identity                                                          # noqa: E402

_BAKE = bake_identity.DEPLOYED

BASELINE_ID = "baseline"


def build_direction_ratios(out_path: Path) -> Path:
    return bake_identity.build_direction_ratios(out_path, _BAKE)


def resolve_metabolite(name, exact_names, element) -> dict:
    pairs = pd.read_parquet(ATOM_PAIRS)
    pairs = pairs[pairs.element == element]
    universe = set(pairs["substrate"].unique()) | set(pairs["product"].unique())
    chem = pd.read_csv(CHEM_PROP, sep="\t", comment="#",
                       names=["id", "name", "reference", "formula", "charge", "mass",
                              "inchi", "inchikey", "smiles"])
    names = exact_names or [name]
    hits = chem[chem.name.str.lower().isin([n.lower() for n in names])
                & chem.id.isin(universe)]
    if hits.empty:
        raise SystemExit(f"no candidate for {name!r} among {names} in the {element} "
                         f"atom-pairs universe")
    chosen = hits.id.value_counts().idxmax()
    row = hits[hits.id == chosen].iloc[0]
    print(f"[resolve] {name!r} -> {row.id} ({row['name']!r}, formula={row.formula}) "
          f"among {sorted(hits.id.unique())}", file=sys.stderr)
    return dict(mnxm=str(row.id), name=str(row["name"]), formula=str(row.formula),
                element=element, n_candidates=int(hits.id.nunique()))


def write_conditions(path, condition_id, host_unit, source, target, element):
    edits = pd.read_parquet(EDITS)
    edits = edits[edits.condition_id == condition_id]
    if edits.empty:
        raise SystemExit(f"no rows for condition {condition_id!r} in {EDITS}")
    dropped = tuple(sorted(set(edits[edits.action == "del"].mnxr.astype(str))))
    common = dict(element=element, source_hub=source, sinks=(), readouts=(target,),
                  background_column="unit_id", background_values=(host_unit,))
    rows = [
        cond_mod.Condition(condition_id=BASELINE_ID, meta=dict(
            arm="baseline", n_units=1, is_control=False), **common),
        cond_mod.Condition(condition_id=condition_id, mask_column="condition_id",
                           mask_values=(condition_id,), drop_column="mnxr",
                           drop_values=dropped,
                           meta=dict(arm="observed", n_units=1, is_control=False),
                           **common),
    ]
    cond_mod.write(rows, path)
    print(f"[conditions] {condition_id}: mask on condition_id, "
          f"{len(dropped)} reaction(s) withheld -> {path}", file=sys.stderr)
    return path, dropped


def check_coverage(target_mnxm, condition_id, element):
    pairs = pd.read_parquet(ATOM_PAIRS)
    pairs = pairs[pairs.element == element]
    edits = pd.read_parquet(EDITS)
    edits = edits[edits.condition_id == condition_id]
    add_rxns = sorted(set(edits[edits.action == "add"].mnxr.astype(str)))

    rxn_rows = {r: int((pairs.mnxr == r).sum()) for r in add_rxns}
    touches = pairs[(pairs.substrate == target_mnxm) | (pairs.product == target_mnxm)]
    target_rxns = sorted(touches.mnxr.unique())
    return dict(
        perturbation_reactions_aam_rows=rxn_rows,
        perturbation_reactions_with_zero_aam_coverage=[r for r, n in rxn_rows.items()
                                                       if n == 0],
        reactions_touching_target_in_atom_pairs=target_rxns,
        target_reachable_via_any_perturbation_reaction=bool(set(target_rxns)
                                                            & set(add_rxns)),
    )


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--condition-id", required=True,
                   help='e.g. "LASER:Record1419612292.57:M1"')
    p.add_argument("--host", required=True, help="host dir name under data/fabfos/runs/")
    p.add_argument("--target", required=True, help="metabolite name to resolve and measure")
    p.add_argument("--target-names", nargs="*", default=None,
                   help="alternate exact names for --target (default: [--target])")
    p.add_argument("--source-name", default="D-glucose")
    p.add_argument("--source-names", nargs="*", default=["D-glucose", "glucose"])
    p.add_argument("--element", default="C")
    p.add_argument("--leak", type=float, default=1e-6)
    p.add_argument("--orientation", choices=("as-written", "reversed"),
                   default="as-written")
    # The default cache holds files written from inside the fabfos image, i.e. owned
    # by root and not rewritable from here. Re-running into a fresh directory is the
    # supported way to reproduce a cached record rather than overwrite it.
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = p.parse_args()

    global OUT_DIR
    OUT_DIR = args.out_dir
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    host_gem = RUNS / args.host / "gpr" / "gpr_gem.parquet"
    if not host_gem.exists():
        raise SystemExit(f"no such host GEM: {host_gem}")
    host_unit = str(pd.read_parquet(host_gem, columns=["unit_id"]).unit_id.iloc[0])

    slug = re.sub(r"[^A-Za-z0-9]+", "_", args.condition_id).strip("_")
    direction_path = build_direction_ratios(OUT_DIR / "direction_ratios.parquet")
    source = resolve_metabolite(args.source_name, args.source_names, args.element)
    target = resolve_metabolite(args.target, args.target_names or [args.target],
                                args.element)

    cond_path, dropped = write_conditions(
        OUT_DIR / f"conditions_{slug}.tsv", args.condition_id, host_unit,
        source["mnxm"], target["mnxm"], args.element)

    results_path = OUT_DIR / f"results_{slug}.tsv"
    # `--weighting uniform`: a curated GEM asserts a reaction is PRESENT, not how much
    # evidence there is for it, so each unit contributes 1.0 and an overexpression is
    # the host's 1.0 plus the clone's. The fosmid arm uses `belief` instead.
    cmd = [
        "ecspr", "ground",
        "--gpr", str(host_gem), str(EDITS),
        "--conditions", str(cond_path),
        "--atom-pairs", str(ATOM_PAIRS), "--direction", str(direction_path),
        "--element", args.element, "--weighting", "uniform",
        "--leak", str(args.leak), "--orientation", args.orientation,
        "--out", str(results_path), "--no-resume",
    ]
    print(f"$ {' '.join(cmd)}", file=sys.stderr)
    subprocess.run(cmd, check=True)

    res = probes.read_results(results_path)

    def val(cid, readout):
        r = res[(res.condition_id == cid) & (res.readout == readout)]
        return float(r.value.iloc[0]) if len(r) else None

    d_base, d_pert = val(BASELINE_ID, target["mnxm"]), val(args.condition_id,
                                                           target["mnxm"])
    if d_base is None or d_pert is None:
        print(f"\n[pilot] {target['mnxm']} ({args.target}) is ABSENT from at least one "
              f"arm's readout -- not a zero flux, a coverage gap. See 'coverage'.",
              file=sys.stderr)

    result = dict(
        condition_id=args.condition_id, host=args.host,
        target=args.target, target_mnxm=target["mnxm"],
        source=args.source_name, source_mnxm=source["mnxm"],
        element=args.element, ground="universal", leak=args.leak,
        orientation=args.orientation,
        reference_basis="tier4 (not the canonical deployed MNXref release)",
        withheld_reactions=list(dropped),
        base=dict(draw_target=d_base, total=val(BASELINE_ID, "total"),
                  converged=val(BASELINE_ID, "_converged"),
                  cholmod_available=val(BASELINE_ID, "_cholmod_available"),
                  n_aam_gap=val(BASELINE_ID, "_n_aam_gap")),
        pert=dict(draw_target=d_pert, total=val(args.condition_id, "total"),
                  converged=val(args.condition_id, "_converged"),
                  cholmod_available=val(args.condition_id, "_cholmod_available"),
                  n_aam_gap=val(args.condition_id, "_n_aam_gap")),
        delta_target_draw=(None if (d_base is None or d_pert is None)
                           else d_pert - d_base),
        coverage=check_coverage(target["mnxm"], args.condition_id, args.element),
    )
    out_path = OUT_DIR / f"pilot_{slug}.json"
    out_path.write_text(json.dumps(result, indent=1, sort_keys=True))
    print(json.dumps(result, indent=1, sort_keys=True))
    print(f"\nwrote {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
