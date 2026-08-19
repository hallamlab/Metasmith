from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _repo_root(start: Path) -> Path:
    for d in (start, *start.parents):
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


REPO = _repo_root(Path(__file__).resolve())
GENOMES = REPO / "data" / "fabfos" / "originals" / "genomes"

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_epi300_identity import model_rules, rule_holds                 # noqa: E402

GEM_HOST = "e_coli_k12"

BW25113_MARKERS = {
    "dlacZ_WJ16":   ("lacZ", "loss"),
    "daraBAD_AH33": ("araB", "loss"),
    "daraBAD_AH33/A": ("araA", "loss"),
    "daraBAD_AH33/D": ("araD", "loss"),
    "drhaBAD_LD78": ("rhaB", "loss"),
    "drhaBAD_LD78/A": ("rhaA", "loss"),
    "drhaBAD_LD78/D": ("rhaD", "loss"),
    "hsdR514":      ("hsdR", "loss"),
    "lacIq":        ("lacI", "variant"),
    "rrnB_T14":     (None,   "unresolved"),
    "drecA":        ("recA", "loss"),
}

LW06_MARKERS = {
    "dldhA":     ("ldhA", "loss"),
    "dackA":     ("ackA", "loss"),
    "dfrdABCD":  ("frdA", "loss"),
    "dfrdABCD/B": ("frdB", "loss"),
    "dfrdABCD/C": ("frdC", "loss"),
    "dfrdABCD/D": ("frdD", "loss"),
    "dadhE":     ("adhE", "loss"),
}

EXPECTED = {
    "e_coli_bw25113": {"ARAI", "LACZ", "LYXI", "RBK_L1", "RMI", "RMK", "RMPA"},
    "e_coli_lw06": {"ARAI", "FRD2", "FRD3", "LACZ", "LDH_D", "LYXI", "RBK_L1",
                    "RMI", "RMK", "RMPA"},
}
EXPECTED_INVISIBLE = {"hsdR514", "lacIq", "drecA"}
EXPECTED_UNRESOLVED = {"rrnB_T14"}

EXPECTED_RESCUED = {
    "ACKr":   "purT/tdcD",
    "ALCD2x": "adhP",
    "ALCD19": "adhP",
    "ACALD":  "mhpF",
}

HETEROLOGOUS = ("pdcZm (pyruvate decarboxylase, Zymomonas mobilis)",
                "adhBZm (alcohol dehydrogenase II, Zymomonas mobilis)")


def resolve(markers: dict, by_symbol: dict) -> tuple[set, set, dict]:
    invisible, unresolved, resolved = set(), set(), {}
    for marker, (symbol, kind) in markers.items():
        base = marker.split("/")[0]
        if symbol is None:
            unresolved.add(base)
            print(f"  {marker:<16} -> (names no locus) UNRESOLVED")
            continue
        hits = by_symbol.get(symbol, [])
        if not hits:
            invisible.add(base)
            print(f"  {marker:<16} -> {symbol:<5} [{kind}] absent from the model "
                  f"-- costs nothing")
            continue
        if kind != "loss":
            invisible.add(base)
            print(f"  {marker:<16} -> {symbol:<5} [{kind}] {hits} -- still functional, "
                  f"not edited")
            continue
        resolved[marker] = hits
        print(f"  {marker:<16} -> {symbol:<5} [{kind}] {hits}")
    return invisible, unresolved, resolved


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--genomes", type=Path, default=GENOMES)
    ap.add_argument("--gpr", type=Path, default=None,
                    help="a built ref::gpr_table_gem directory; if given, each host's "
                         "table is asserted to differ from K-12's by exactly its edit list")
    a = ap.parse_args()

    gems = sorted((a.genomes / GEM_HOST / "GEM").glob("*.json"))
    if len(gems) != 1:
        print(f"FAIL: expected one model under {GEM_HOST}/GEM, found "
              f"{[p.name for p in gems]}")
        return 1
    m = json.loads(gems[0].read_text())
    genes = {g["id"]: (g.get("name") or "") for g in m["genes"]}
    by_symbol: dict[str, list[str]] = {}
    for gid, name in genes.items():
        if name:
            by_symbol.setdefault(name, []).append(gid)
    rules = model_rules(gems[0])
    full = set(genes)
    print(f"{gems[0].stem}: {len(genes):,} genes, {len(rules):,} reactions\n")

    problems: list[str] = []
    found: dict[str, set] = {}
    rescued: dict[str, str] = {}

    print("BW25113 (Datsenko & Wanner 2000; the tolerance study adds delta-recA)")
    inv_bw, unres_bw, res_bw = resolve(BW25113_MARKERS, by_symbol)
    broken_bw = {g for hits in res_bw.values() for g in hits}

    print("\nLW06 (BW25113 DldhA DackA DfrdABCD DadhE; ATCC BAA-2466)")
    inv_lw, unres_lw, res_lw = resolve(LW06_MARKERS, by_symbol)
    broken_lw = broken_bw | {g for hits in res_lw.values() for g in hits}

    for host, broken in (("e_coli_bw25113", broken_bw), ("e_coli_lw06", broken_lw)):
        lost = [(rid, rule) for rid, rule in rules
                if rule_holds(rule, full) and not rule_holds(rule, full - broken)]
        kept = [(rid, rule) for rid, rule in rules
                if any(g in rule.replace("(", " ").replace(")", " ").split()
                       for g in broken)
                and (rid, rule) not in lost]
        found[host] = {rid for rid, _ in lost}
        print(f"\n  {host}: {len(broken)} model genes removed -> {len(lost)} reactions "
              f"go dark")
        for rid, rule in lost:
            print(f"     lost {rid:<10} {rule}")
        for rid, rule in kept:
            print(f"     kept {rid:<10} {rule}   (an isozyme carries it)")
            rescued.setdefault(rid, rule)
        if found[host] != EXPECTED[host]:
            problems.append(
                f"{host}'s lost-reaction set changed: found {sorted(found[host])}, "
                f"declared {sorted(EXPECTED[host])}. benchmark/host_gpr_gem.py drops a "
                f"FIXED list on the strength of this measurement, so the two have to "
                f"agree.")

    invisible = inv_bw | inv_lw
    unresolved = unres_bw | unres_lw
    if invisible != EXPECTED_INVISIBLE:
        problems.append(f"the invisible-marker set changed: found {sorted(invisible)}, "
                        f"declared {sorted(EXPECTED_INVISIBLE)}")
    if unresolved != EXPECTED_UNRESOLVED:
        problems.append(f"the unresolved-marker set changed: found {sorted(unresolved)}, "
                        f"declared {sorted(EXPECTED_UNRESOLVED)}")
    missing_rescue = set(EXPECTED_RESCUED) - set(rescued)
    if missing_rescue:
        problems.append(
            f"reactions declared rescued by an isozyme are no longer surviving the "
            f"deletion: {sorted(missing_rescue)}. LW06 making ethanol without its "
            f"engineered pathway is a stated finding of this benchmark; if that stopped "
            f"being true the report has to change with it.")

    print(f"\n  LW06 keeps {sorted(EXPECTED_RESCUED)} despite DackA and DadhE "
          f"({', '.join(f'{k} on {v}' for k, v in sorted(EXPECTED_RESCUED.items()))}) "
          f"-- so this model can make ethanol in LW06 without pdcZm/adhBZm.")
    print(f"  NOT MEASURED HERE: the attTn7 insertion, {'; '.join(HETEROLOGOUS)}. "
          f"host_gpr_gem.py only subtracts; those edges enter as study GPR rows.")

    if a.gpr is not None:
        import pandas as pd
        base = a.gpr / "hosts" / GEM_HOST / "gpr_gem.parquet"
        if not base.exists():
            problems.append(f"--gpr given but {base} is missing")
        else:
            da = pd.read_parquet(base)
            for host in ("e_coli_bw25113", "e_coli_lw06"):
                pb = a.gpr / "hosts" / host / "gpr_gem.parquet"
                if not pb.exists():
                    problems.append(f"--gpr given but {pb} is missing")
                    continue
                db = pd.read_parquet(pb)
                only_base = set(da["intermediate_id"]) - set(db["intermediate_id"])
                only_host = set(db["intermediate_id"]) - set(da["intermediate_id"])
                in_universe = sorted(
                    da.loc[da["intermediate_id"].isin(found[host])
                           & da["in_atom_universe"], "intermediate_id"].unique())
                print(f"\n  gpr tables: {len(da):,} ({GEM_HOST}) vs {len(db):,} "
                      f"({host}) rows; only in K-12's: {sorted(only_base)}; only in "
                      f"{host}'s: {sorted(only_host)}")
                print(f"  of the edit, inside the atom universe: {in_universe}")
                if only_base != EXPECTED[host] or only_host:
                    problems.append(
                        f"{host}'s table differs from K-12's by {sorted(only_base)} / "
                        f"{sorted(only_host)} rather than by the declared edit list "
                        f"{sorted(EXPECTED[host])}")

    print()
    for p in problems:
        print(f"FAIL: {p}")
    if not problems:
        print(f"BW25113's genotype costs iML1515 exactly {sorted(EXPECTED['e_coli_bw25113'])} "
              f"-- seven sugar-catabolic reactions neither paper's medium feeds -- and "
              f"LW06 adds exactly {sorted(EXPECTED['e_coli_lw06'] - EXPECTED['e_coli_bw25113'])}. "
              f"Seven deleted genes, three dark reactions.")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
