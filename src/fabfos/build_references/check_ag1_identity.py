from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
GENOMES = REPO / "data" / "fabfos" / "originals" / "genomes"

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_epi300_identity import model_rules, rule_holds                 # noqa: E402

GEM_HOST = "e_coli_dh1"
BORROWER = "e_coli_ag1"

MARKERS = {
    "recA1":  ("recA", "loss"),
    "endA1":  ("endA", "loss"),
    "gyrA96": ("gyrA", "variant"),
    "thi-1":  (None,   "unresolved"),
    "hsdR17": ("hsdR", "loss"),
    "supE44": ("glnV", "gain"),
    "relA1":  ("relA", "loss"),
}

EXPECTED_INVISIBLE = {"recA1", "endA1", "gyrA96", "hsdR17", "supE44"}
EXPECTED_UNRESOLVED = {"thi-1"}
EXPECTED_LOST_REACTIONS = {"GTPDPK"}

THIAMINE = ("thiB", "thiC", "thiD", "thiE", "thiF", "thiG", "thiH", "thiI", "thiK",
            "thiL", "thiM", "thiP", "thiQ")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--genomes", type=Path, default=GENOMES)
    ap.add_argument("--gpr", type=Path, default=None,
                    help="a built ref::gpr_table_gem directory; if given, AG1's table is "
                         "asserted to differ from DH1's by exactly the edit list")
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
    print(f"{gems[0].stem}: {len(genes):,} genes, {len(rules):,} reactions\n")

    invisible, resolved, unresolved = set(), {}, set()
    for marker, (symbol, kind) in MARKERS.items():
        if symbol is None:
            unresolved.add(marker)
            print(f"  {marker:<7} -> (no locus named by the genotype) UNRESOLVED")
            continue
        hits = by_symbol.get(symbol, [])
        if not hits:
            invisible.add(marker)
            print(f"  {marker:<7} -> {symbol:<5} [{kind}] absent from the model "
                  f"-- costs nothing")
            continue
        if kind != "loss":
            invisible.add(marker)
            print(f"  {marker:<7} -> {symbol:<5} [{kind}] {hits} -- still functional, "
                  f"not edited")
            continue
        resolved[marker] = hits
        print(f"  {marker:<7} -> {symbol:<5} [{kind}] {hits}")

    broken = {gid for hits in resolved.values() for gid in hits}
    full = set(genes)
    lost = [(rid, rule) for rid, rule in rules
            if rule_holds(rule, full) and not rule_holds(rule, full - broken)]
    kept = [(rid, rule) for rid, rule in rules
            if any(g in rule.split() for g in broken) and (rid, rule) not in lost]
    print(f"\n  reactions that go dark when {sorted(broken)} are removed: {len(lost)}")
    for rid, rule in lost:
        print(f"     lost {rid:<10} {rule}")
    for rid, rule in kept:
        print(f"     kept {rid:<10} {rule}   (an isozyme carries it)")

    thi_genes = {gid for sym in THIAMINE for gid in by_symbol.get(sym, [])}
    thi_rxns = [rid for rid, rule in rules if any(g in rule.split() for g in thi_genes)]
    print(f"\n  thi-1 is unresolved; the thiamine module it lies in is "
          f"{len(thi_genes)} model genes over {len(thi_rxns)} reactions "
          f"({sorted(thi_rxns)}) -- none of them edited")

    problems = []
    if invisible != EXPECTED_INVISIBLE:
        problems.append(f"the invisible-marker set changed: found {sorted(invisible)}, "
                        f"declared {sorted(EXPECTED_INVISIBLE)}")
    if unresolved != EXPECTED_UNRESOLVED:
        problems.append(f"the unresolved-marker set changed: found {sorted(unresolved)}, "
                        f"declared {sorted(EXPECTED_UNRESOLVED)}")
    found_lost = {rid for rid, _ in lost}
    if found_lost != EXPECTED_LOST_REACTIONS:
        problems.append(
            f"the lost-reaction set changed: found {sorted(found_lost)}, declared "
            f"{sorted(EXPECTED_LOST_REACTIONS)}. benchmark/host_gpr_gem.py drops a FIXED "
            f"list on the strength of this measurement, so the two have to agree.")

    if a.gpr is not None:
        import pandas as pd
        pa = a.gpr / "hosts" / GEM_HOST / "gpr_gem.parquet"
        pb = a.gpr / "hosts" / BORROWER / "gpr_gem.parquet"
        if not (pa.exists() and pb.exists()):
            problems.append(f"--gpr given but {pa} or {pb} is missing")
        else:
            da, db = pd.read_parquet(pa), pd.read_parquet(pb)
            only_dh1 = set(da["intermediate_id"]) - set(db["intermediate_id"])
            only_ag1 = set(db["intermediate_id"]) - set(da["intermediate_id"])
            in_universe = bool(da.loc[da["intermediate_id"].isin(found_lost),
                                      "in_atom_universe"].any())
            print(f"\n  gpr tables: {len(da):,} ({GEM_HOST}) vs {len(db):,} ({BORROWER}) "
                  f"rows; reactions only in DH1's: {sorted(only_dh1)}; only in AG1's: "
                  f"{sorted(only_ag1)}")
            print(f"  the edited reaction is inside the atom universe: {in_universe} "
                  f"-- so unlike EPI300's borrow, this one moves the network")
            if only_dh1 != EXPECTED_LOST_REACTIONS or only_ag1:
                problems.append(
                    f"AG1's table differs from DH1's by {sorted(only_dh1)} / "
                    f"{sorted(only_ag1)} rather than by the declared edit list "
                    f"{sorted(EXPECTED_LOST_REACTIONS)}")

    print()
    for p in problems:
        print(f"FAIL: {p}")
    if not problems:
        print(f"AG1's genotype costs DH1's model exactly "
              f"{sorted(EXPECTED_LOST_REACTIONS)}: five of the seven markers name no "
              f"gene in it, thi-1 names no locus at all, and relA1 loses GTPDPK while "
              f"GDPDPK survives on spoT. AG1 needs thiamine in the medium and this file "
              f"does not model that.")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
