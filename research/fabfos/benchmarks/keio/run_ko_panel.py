#!/usr/bin/env python3
"""Keio single-gene knockouts: does ECSPr predict the auxotrophy?

A Keio condition is a loss-of-function -- one gene deleted from the *E. coli* K-12
background, and the paper's phenotype is a *requirement*: "this strain now needs
L-arginine in the medium". So the prediction under test is

    delete the gene  ->  the required metabolite's draw collapses,
                         and the metabolites the strain does NOT require do not.

Both halves matter. Under a universal-ground leak solve the injected current is
fixed, so a deletion redistributes rather than uniformly shrinks -- the required
metabolite falling is only informative if it falls *further* than the field. Two
negative classes give that its teeth, and neither has to be invented:

  * every other metabolite in the same solve (the Keio strain grows on glucose
    minimal medium supplemented with exactly one compound, so "everything else is
    fine" is an experimental claim, not an absence of data); and
  * the other conditions' targets -- an N x N matrix whose off-diagonal is a real
    negative, since dArgA is not a histidine auxotroph.

This is the axis LASER could not offer: an addition has no negatives on the
metabolite panel, a deletion does.

Conditions default to first-committed-step knockouts, several reactions upstream of
the compound the cell ends up needing. Deleting the reaction that literally makes X
and finding X gone would be arithmetic; the claim worth testing is that the loss
*propagates* down the pathway. `Keio:argH` (terminal step, arginine) is carried as
the tautological positive control -- if the distal three are silent and argH is not,
that separates "ECSPr cannot see this" from "propagation is the part that fails".
`Keio:argD` deletes no reaction at all (isozyme redundancy, n_dead=0) and must
return an exactly-zero field; it is the harness's null.

Drives docker/fabfos/bin/ecspr_cli.py directly (no metasmith), same three
subcommands as main/benchmarks/laser/pilot/run_pilot.py. Run inside the image, from
the repo root:

    docker run --rm -v "$PWD":/ws -w /ws fabfos:local \
        python main/benchmarks/keio/run_ko_panel.py

NOT the deployed reference basis -- tier4 atom pairs, not the canonical MNXref
release (the same caveat examples/scadc_ecspr.py carries). A pilot, not a number to
publish as-is.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
CLI = ROOT / "docker" / "fabfos" / "bin" / "ecspr_cli.py"
HERE = Path(__file__).resolve().parent
CACHE = HERE / "cache"
OUT = HERE / "out"

ATOM_PAIRS = ROOT / "data" / "fabfos" / "benchmark" / "reference_tier4" / "atom_pairs_tier4.parquet"
CHEM_PROP = ROOT / "data" / "fabfos" / "originals" / "metanetx" / "4.5" / "chem_prop.tsv"
HOST_GEM = ROOT / "data" / "fabfos" / "runs" / "e_coli_k12" / "gpr" / "gpr_gem.parquet"
KEIO = ROOT / "data" / "fabfos" / "benchmarks" / "keio"
EDITS = KEIO / "gpr_manual.parquet"
EXTRACTION = KEIO / "extraction.tsv"
EXPECTATIONS = KEIO / "Y" / "expectations.tsv"

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bake_identity                                                          # noqa: E402

_BAKE = bake_identity.DEPLOYED

SOURCE_NAME = "D-glucose"
SOURCE_ALIASES = ["D-glucose", "glucose"]

PANEL = {
    "Keio:argA": ("L-arginine", ["L-arginine"], "distal"),
    "Keio:hisG": ("L-histidine", ["L-histidine"], "distal"),
    "Keio:trpE": ("L-tryptophan", ["L-tryptophan"], "distal"),
    "Keio:argH": ("L-arginine", ["L-arginine"], "terminal_control"),
    "Keio:argD": ("L-arginine", ["L-arginine"], "null_control"),
}
DEFAULT_CONDITIONS = list(PANEL)


def build_direction_ratios(out_path: Path) -> Path:
    # metabolism_bake's direction.parquet joined through vocab.parquet onto mnxr.
    #
    # Stamped with the bake it came from: the previous `if out_path.exists()` served r7's
    # ratios across the r8 repin without saying so.
    return bake_identity.build_direction_ratios(out_path, _BAKE)


def chem_names() -> dict:
    chem = pd.read_csv(CHEM_PROP, sep="\t", comment="#",
                       names=["id", "name", "reference", "formula", "charge", "mass",
                              "inchi", "inchikey", "smiles"])
    return dict(zip(chem.id.astype(str), chem.name.astype(str)))


def run(*args, want_json=True):
    cmd = [sys.executable, str(CLI), *[str(a) for a in args]]
    print(f"$ {' '.join(cmd)}", file=sys.stderr)
    r = subprocess.run(cmd, capture_output=True, text=True)
    sys.stderr.write(r.stderr)
    if r.returncode != 0:
        raise SystemExit(f"ecspr_cli failed ({r.returncode}): {' '.join(cmd)}")
    return json.loads(r.stdout) if want_json else None


def slug(condition_id: str) -> str:
    return condition_id.replace(":", "_")


def solve_condition(condition_id, direction_path, source_mnxm, element, leak):
    out = CACHE / f"solve_{slug(condition_id)}_{element}.json"
    if out.exists():
        print(f"[panel] reusing {out.name}", file=sys.stderr)
        return json.loads(out.read_text())

    w = CACHE / f"weights_{slug(condition_id)}.json"
    if condition_id == "__base__":
        run("build-weights", "--host-gem", HOST_GEM, "--out", w, want_json=False)
    else:
        run("build-weights", "--host-gem", HOST_GEM, "--edits", EDITS,
            "--condition-id", condition_id, "--out", w, want_json=False)

    run("solve", "--ground", "universal", "--atom-pairs", ATOM_PAIRS,
        "--direction", direction_path, "--weights", w, "--element", element,
        "--source", source_mnxm, "--leak", leak, "--out", out, want_json=False)
    return json.loads(out.read_text())


def rel_change(base_draw, ko_draw, mnxm):
    b = base_draw.get(mnxm)
    if b is None:
        return None, None, None
    k = ko_draw.get(mnxm, 0.0)
    if b == 0.0:
        return b, k, (0.0 if k == 0.0 else float("inf"))
    return b, k, (k - b) / b


def score_field(base_draw, ko_draw, floor):
    rows = []
    for m, b in base_draw.items():
        if b <= floor:
            continue
        k = ko_draw.get(m, 0.0)
        rows.append((m, b, k, (k - b) / b))
    rows.sort(key=lambda r: r[3])
    return rows


def n_negative(rows):
    return sum(1 for r in rows if r[3] < 0)


def rank_of(rows, mnxm):
    # Where the target sits in the depletion ranking, reported as a tie band rather
    # than a single index: on a null condition every metabolite ties at 0.0 and a bare
    # rank would read as rank 1 of N, i.e. as a perfect call.
    hit = next((r for r in rows if r[0] == mnxm), None)
    if hit is None:
        return None
    v = hit[3]
    n_below = sum(1 for r in rows if r[3] < v)
    n_tied = sum(1 for r in rows if r[3] == v)
    n = len(rows)
    return dict(rel_change=v, n_scored=n, n_more_depleted=n_below, n_tied=n_tied,
                frac_beaten=(n - n_below - n_tied) / n if n else float("nan"))


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--conditions", nargs="*", default=DEFAULT_CONDITIONS)
    p.add_argument("--element", default="C")
    p.add_argument("--leak", type=float, default=1e-6)
    p.add_argument("--floor", type=float, default=1e-15,
                   help="ignore metabolites whose background draw is below this")
    p.add_argument("--top", type=int, default=40,
                   help="rows per condition in out/top_depleted.tsv; the default clears "
                        "the largest negative set here (26) so no faller is truncated")
    args = p.parse_args()

    CACHE.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    unknown = [c for c in args.conditions if c not in PANEL]
    if unknown:
        raise SystemExit(f"no PANEL entry for {unknown}; add the target's names first")

    direction_path = build_direction_ratios(CACHE / "direction_ratios.parquet")
    names = chem_names()

    source = run("resolve-metabolite", "--name", SOURCE_NAME,
                 "--exact-names", *SOURCE_ALIASES, "--atom-pairs", ATOM_PAIRS,
                 "--chem-prop", CHEM_PROP, "--element", args.element)
    targets = {}
    for cid in args.conditions:
        tname, exact, _role = PANEL[cid]
        if tname in targets:
            continue
        targets[tname] = run("resolve-metabolite", "--name", tname,
                             "--exact-names", *exact, "--atom-pairs", ATOM_PAIRS,
                             "--chem-prop", CHEM_PROP, "--element", args.element)

    base = solve_condition("__base__", direction_path, source["mnxm"],
                           args.element, args.leak)
    kos = {cid: solve_condition(cid, direction_path, source["mnxm"],
                                args.element, args.leak)
           for cid in args.conditions}

    base_draw = base["draw"]
    extraction = pd.read_csv(EXTRACTION, sep="\t").set_index("obs_id")
    expectations = pd.read_csv(EXPECTATIONS, sep="\t")

    panel_rows, matrix_rows, top_rows, mech_rows = [], [], [], []
    for cid in args.conditions:
        ko = kos[cid]
        ko_draw = ko["draw"]
        assert not (set(ko_draw) - set(base_draw)), \
            f"{cid}: knockout graph gained nodes -- a deletion cannot do that"
        rows = score_field(base_draw, ko_draw, args.floor)
        n_neg = n_negative(rows)
        ex = extraction.loc[cid]
        del_mnxr = [r for r in str(ex.del_mnxr or "").split(",") if r and r != "nan"]

        for tname, t in targets.items():
            r = rank_of(rows, t["mnxm"])
            is_own = PANEL[cid][0] == tname
            b, k, rc = rel_change(base_draw, ko_draw, t["mnxm"])
            matrix_rows.append(dict(
                condition_id=cid, role=PANEL[cid][2], target=tname,
                target_mnxm=t["mnxm"], is_required=int(is_own),
                base_draw=b, ko_draw=k, rel_change=rc,
                in_background_graph=int(t["mnxm"] in base_draw),
                in_knockout_graph=int(t["mnxm"] in ko_draw),
                n_more_depleted=None if r is None else r["n_more_depleted"],
                n_tied=None if r is None else r["n_tied"],
                n_scored=None if r is None else r["n_scored"],
                frac_beaten=None if r is None else r["frac_beaten"],
            ))
            if is_own:
                panel_rows.append(dict(
                    condition_id=cid, role=PANEL[cid][2], gene=cid.split(":")[1],
                    required=tname, target_mnxm=t["mnxm"],
                    n_reactions_deleted=len(del_mnxr), del_mnxr=";".join(del_mnxr),
                    base_draw=b, ko_draw=k, rel_change=rc,
                    n_more_depleted=None if r is None else r["n_more_depleted"],
                    n_tied=None if r is None else r["n_tied"],
                    n_scored=None if r is None else r["n_scored"],
                    frac_beaten=None if r is None else r["frac_beaten"],
                    n_negative=n_neg, required_is_negative=int(rc is not None and rc < 0),
                    n_aam_gap=ko["n_aam_gap"], converged=ko["converged"],
                ))

        for m, b, k, rc in rows[:args.top]:
            top_rows.append(dict(condition_id=cid, mnxm=m, name=names.get(m, ""),
                                 base_draw=b, ko_draw=k, rel_change=rc))

        for e in expectations[expectations.condition_id == cid].itertuples(index=False):
            if e.element != args.element:
                continue
            b, k, rc = rel_change(base_draw, ko_draw, e.mnxm)
            mech_rows.append(dict(condition_id=cid, mnxm=e.mnxm,
                                  name=names.get(e.mnxm, ""), expected_dir=e.expected_dir,
                                  basis=e.basis, base_draw=b, ko_draw=k, rel_change=rc,
                                  in_background_graph=int(e.mnxm in base_draw),
                                  agrees=None if rc is None else int(rc < 0)))

    def write(df, name):
        path = OUT / name
        df.to_csv(path, sep="\t", index=False)
        print(f"[panel] wrote {path}", file=sys.stderr)
        return df

    panel = write(pd.DataFrame(panel_rows), "panel_targets.tsv")
    matrix = write(pd.DataFrame(matrix_rows), "panel_matrix.tsv")
    write(pd.DataFrame(top_rows), "top_depleted.tsv")
    mech = write(pd.DataFrame(mech_rows), "mechanical_expectations.tsv")

    summary = dict(
        source=SOURCE_NAME, source_mnxm=source["mnxm"], element=args.element,
        ground="universal", leak=args.leak, floor=args.floor,
        host="e_coli_k12", reference_basis="tier4 (not the canonical MNXref release)",
        n_metabolites_background=len(base_draw),
        n_scored_background=len(score_field(base_draw, base_draw, args.floor)),
        base_converged=base["converged"], base_n_aam_gap=base["n_aam_gap"],
        cholmod_available=base["cholmod_available"],
        conditions=args.conditions,
        targets={k: v["mnxm"] for k, v in targets.items()},
        mechanical_rows=len(mech),
        mechanical_agree=int(mech.agrees.sum()) if len(mech) and mech.agrees.notna().any() else 0,
        mechanical_scoreable=int(mech.agrees.notna().sum()) if len(mech) else 0,
    )
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, sort_keys=True))
    print(json.dumps(summary, indent=1, sort_keys=True))
    print("\n== required metabolite, per condition ==", file=sys.stderr)
    print(panel.to_string(index=False), file=sys.stderr)
    print("\n== condition x target ==", file=sys.stderr)
    print(matrix.pivot(index="condition_id", columns="target",
                       values="rel_change").to_string(), file=sys.stderr)


if __name__ == "__main__":
    main()
