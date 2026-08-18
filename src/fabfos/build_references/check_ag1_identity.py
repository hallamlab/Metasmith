"""What does AG1 borrowing DH1's model cost? Measure it, marker by marker.

    PATH="/home/tony/lib/miniforge3/envs/msm-fabfos/bin:$PATH" \\
        python build_references/check_ag1_identity.py [--gpr <dir>]

The ASKA library lives in E. coli AG1, and the eydallin cohort is read against it. AG1
has no assembly at NCBI and no published model, so it borrows DH1's -- the sibling
situation to EPI300 borrowing DH10B's, with one difference that changes the whole
method: there is no AG1 GENOME to compare. `check_epi300_identity.py` measures a borrow
by joining two annotations; here there is only one, plus a sentence.

THE SENTENCE IS THE INPUT. Qimron et al. PNAS 2006 (`data/fabfos/originals/aska/`, Materials
and Methods) state it exactly: "The ASKA collection was constructed by using E. coli
K-12 AG1 (Stratagene), which is a derivative of DH1: recA1, endA1, gyrA96, thi-1,
hsdR17 (rK-mK+), supE44, and relA1." Seven markers, and the question this script
answers is which of them the MODEL can see -- because a marker naming no gene in
iECDH1ME8569_1439 cannot change the reaction space however important it is to the
strain.

WHAT AN ALLELE DOES IS PART OF THE INPUT, not a detail. A genotype string lists markers,
not knockouts: `relA1` is a null, `gyrA96` is a resistance allele whose gyrase still
works, and `supE44` is a gained suppressor tRNA. Treating the list as seven deletions is
the easy mistake, and for gyrA it would assert a strain with no DNA gyrase -- which is
not viable, and which nothing downstream could catch. MARKERS carries the kind.

WHAT IT FINDS, and each of the three outcomes means something different:

  recA1, endA1, gyrA96, hsdR17, supE44   name no gene in the model at all. Homologous
                                         recombination, endonuclease I, gyrase,
                                         restriction and a tRNA suppressor are not
                                         metabolism; five of the seven markers are
                                         therefore free, and that is measured here
                                         rather than assumed from what they sound like.
  relA1                                  names `relA`, which carries two reactions.
                                         GDPDPK is `relA or spoT` and survives; GTPDPK
                                         is relA alone and goes dark. So the marker
                                         costs exactly one reaction -- and unlike
                                         EPI300's seven, it is INSIDE the atom
                                         universe, so this borrow does move the network.
  thi-1                                  is a classical allele, not a locus. It is a
                                         thiamine auxotrophy whose molecular lesion the
                                         genotype string does not name, and the
                                         thiamine module in this model is nine
                                         reactions over eight genes. Guessing one would
                                         put a fabricated deletion in the background of
                                         every eydallin condition. It is therefore NOT
                                         in the edit list, and the consequence is stated
                                         instead: AG1 needs thiamine in the medium,
                                         which is a claim about the MEDIUM and belongs
                                         wherever the medium is declared.

A marker that is real and invisible to the model, and a marker that is real and
unresolved, are different kinds of absence from the edit list. Both are printed.
"""
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

# The genotype, as the paper writes it -> the gene each marker names and WHAT THE ALLELE
# DOES. The symbol mapping is standard (`supE44` is the historical name for `glnV`, the
# rest drop their allele number); the second field is the interpretive step, and leaving
# it out is what makes "apply the genotype" mean "delete seven genes".
#
#   loss       the product is non-functional. recA1, endA1 and relA1 are the classical
#              null-phenotype alleles a cloning strain is built for, and hsdR17 removes
#              restriction.
#   variant    the product is ALTERED AND STILL WORKS. gyrA96 is a nalidixic-acid
#              resistance allele -- DNA gyrase is essential, so a strain carrying it as a
#              deletion would not be alive. Withholding its rows would claim AG1 has no
#              gyrase, which is both false and the kind of false a downstream solver
#              cannot notice.
#   gain       supE44/glnV is an amber-suppressor tRNA: a gained function, and not a
#              protein at all, so no protein table can express it either way.
#   unresolved thi-1 names no locus -- see the header.
MARKERS = {
    "recA1":  ("recA", "loss"),
    "endA1":  ("endA", "loss"),
    "gyrA96": ("gyrA", "variant"),
    "thi-1":  (None,   "unresolved"),
    "hsdR17": ("hsdR", "loss"),
    "supE44": ("glnV", "gain"),
    "relA1":  ("relA", "loss"),
}

# What the measurement below currently says. Declared so a CHANGE is a failure rather
# than a quietly different number -- the same contract check_epi300_identity.py has with
# host_gpr_gem.py, which drops a FIXED list on the strength of this.
EXPECTED_INVISIBLE = {"recA1", "endA1", "gyrA96", "hsdR17", "supE44"}
EXPECTED_UNRESOLVED = {"thi-1"}
EXPECTED_LOST_REACTIONS = {"GTPDPK"}

# The module thi-1 breaks somewhere, printed with its cost so the size of what is being
# left alone is on the record rather than implied.
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
            # An altered-but-working product removes nothing. Reported rather than
            # skipped: "this marker names a model gene and edits none of it" is a
            # different fact from "this marker names no model gene".
            invisible.add(marker)
            print(f"  {marker:<7} -> {symbol:<5} [{kind}] {hits} -- still functional, "
                  f"not edited")
            continue
        resolved[marker] = hits
        print(f"  {marker:<7} -> {symbol:<5} [{kind}] {hits}")

    # WHAT THE EDIT COSTS THE NETWORK, measured by evaluating every rule twice -- once
    # over the model's own gene set, once with the marked genes removed -- and diffing
    # the live reactions. A gene ORed with an isozyme takes nothing with it; one alone on
    # a reaction takes it out. Which of the two `relA` is differs per reaction, and that
    # is the entire finding here.
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

    # The module the unresolved marker breaks, so that "left out of the edit list" is a
    # readable size rather than a shrug.
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
