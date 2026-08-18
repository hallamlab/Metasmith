"""What do the SCALEs hosts cost iML1515? Measure it, locus by locus.

    PATH="/home/tony/lib/miniforge3/envs/build-refs-cobra/bin:$PATH" \\
        python src/fabfos/build_references/check_lw06_identity.py [--gpr <dir>]

Two hosts, one measurement. The SCALEs tolerance study selects in BW25113 delta-recA and
the SCALEs production study selects in LW06, which its Materials and Methods define
verbatim as `BW25113 DldhA DackA DfrdABCD DadhE attTn7::PLlacO-1 pdcZm adhBZm AmpR`
(ATCC BAA-2466). So LW06's edit list is BW25113's plus its own, and the two are computed
together here because computing them apart is how they drift.

THIS IS THE AG1 SITUATION WITH ONE HALF FIXED. Like AG1, LW06 has no assembly at NCBI, so
a sequenced parent stands in for it -- BW25113, GCF_050858555.1. Unlike AG1, that parent
IS sequenced and IS in the host set, so the sequence stand-in costs nothing that can be
measured against a genome. What cannot be measured that way is the MODEL borrow, and that
is what this file is for.

BW25113 READS AGAINST iML1515 BECAUSE IT IS K-12, not because it borrows a stranger's
model. Datsenko and Wanner built it from BD792/MG1655 stock; what separates it from
MG1655 is `lacIq rrnB_T14 dlacZ_WJ16 hsdR514 daraBAD_AH33 drhaBAD_LD78`, six catabolic
loci and a restriction marker. Calling that a borrow would overstate the distance.

WHAT COUNTING DELETED GENES WOULD GET WRONG, in both directions:

  BW25113   six loci name seven model genes and darken SEVEN reactions -- ARAI, LACZ,
            LYXI, RBK_L1, RMI, RMK, RMPA. But `hsdR514` names no gene in the model, and
            neither does the tolerance host's `recA`, so two of the markers are free.
            All seven lost reactions are arabinose, rhamnose and lactose catabolism, and
            neither paper's medium contains any of those sugars -- so the edit is real
            and inert under these selections. Inert is not empty: EPI300's list is empty
            because its seven reactions are out of the atom universe, which is a
            different argument and produces a different list.

  LW06      four deletions span SEVEN genes and darken only THREE reactions. frdABCD is
            a four-gene AND clause, so FRD2 and FRD3 go together; ldhA is alone on
            LDH_D. The other two deletions cost nothing: ACKr is `ackA or purT or tdcD`
            and survives on the isozymes, and adhE is ORed with adhP on both ALCD2x and
            ALCD19 and with mhpF on ACALD.

            So iML1515 says LW06 can still make ethanol without its engineered pathway.
            That is a finding about what a curated GEM can express about a strain
            engineering, not a bug to be tuned away -- forcing the expected topology by
            deleting the surviving reactions would assert a genome nobody sequenced.

THE HETEROLOGOUS HALF IS OUT OF SCOPE HERE AND SAYS SO. `attTn7::pdcZm adhBZm` ADDS a
Zymomonas pyruvate decarboxylase and alcohol dehydrogenase, and host_gpr_gem.py only
subtracts. Those edges ride in as study GPR rows and concatenate at solve time; this file
measures the subtraction only, and the printed summary names the gap so a reader does not
mistake a clean pass for a complete strain.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _repo_root(start: Path) -> Path:
    """Walk up until a directory holding `data/fabfos` is found.

    check_ag1_identity.py hardcodes `.parent.parent`, which resolved correctly only from
    the pre-src layout and now points at `src/fabfos`. Counting hops breaks whenever the
    file moves; looking for the thing being addressed does not.
    """
    for d in (start, *start.parents):
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


REPO = _repo_root(Path(__file__).resolve())
GENOMES = REPO / "data" / "fabfos" / "originals" / "genomes"

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_epi300_identity import model_rules, rule_holds                 # noqa: E402

GEM_HOST = "e_coli_k12"          # iML1515 -- BW25113's own lineage, not a borrow

# marker as the source writes it -> (model gene symbol, what the allele does).
# Same three kinds check_ag1_identity.py uses, and here for the same reason: a genotype
# string lists markers, not knockouts. `lacIq` is a PROMOTER-UP allele of a repressor --
# treating it as a deletion would assert the opposite of what it does.
BW25113_MARKERS = {
    "dlacZ_WJ16":   ("lacZ", "loss"),
    "daraBAD_AH33": ("araB", "loss"),
    "daraBAD_AH33/A": ("araA", "loss"),
    "daraBAD_AH33/D": ("araD", "loss"),
    "drhaBAD_LD78": ("rhaB", "loss"),
    "drhaBAD_LD78/A": ("rhaA", "loss"),
    "drhaBAD_LD78/D": ("rhaD", "loss"),
    "hsdR514":      ("hsdR", "loss"),
    "lacIq":        ("lacI", "variant"),   # over-expressed repressor, still a repressor
    "rrnB_T14":     (None,   "unresolved"),  # an rRNA terminator: not a protein at all
    "drecA":        ("recA", "loss"),      # the tolerance study's host, BW25113 delta-recA
}

# LW06's own four deletions, on top of every BW25113 marker above.
LW06_MARKERS = {
    "dldhA":     ("ldhA", "loss"),
    "dackA":     ("ackA", "loss"),
    "dfrdABCD":  ("frdA", "loss"),
    "dfrdABCD/B": ("frdB", "loss"),
    "dfrdABCD/C": ("frdC", "loss"),
    "dfrdABCD/D": ("frdD", "loss"),
    "dadhE":     ("adhE", "loss"),
}

# What the measurement below currently says, declared so a CHANGE is a failure rather
# than a quietly different number -- the same contract check_epi300_identity.py and
# check_ag1_identity.py have with host_gpr_gem.py, which drops FIXED lists on the
# strength of these.
EXPECTED = {
    "e_coli_bw25113": {"ARAI", "LACZ", "LYXI", "RBK_L1", "RMI", "RMK", "RMPA"},
    "e_coli_lw06": {"ARAI", "FRD2", "FRD3", "LACZ", "LDH_D", "LYXI", "RBK_L1",
                    "RMI", "RMK", "RMPA"},
}
# Three markers name no gene iML1515 carries -- restriction, the lac repressor and
# homologous recombination are not metabolism -- and one names no locus at all.
EXPECTED_INVISIBLE = {"hsdR514", "lacIq", "drecA"}
EXPECTED_UNRESOLVED = {"rrnB_T14"}

# Reactions LW06's deletions touch and DO NOT take, with the isozyme that carries each.
# Declared rather than merely printed, because "adhE is deleted and the model still makes
# ethanol" is the single most surprising thing this file reports and a silent change to it
# would be a silent change to what the production arm is measuring.
EXPECTED_RESCUED = {
    "ACKr":   "purT/tdcD",
    "ALCD2x": "adhP",
    "ALCD19": "adhP",
    "ACALD":  "mhpF",
}

# The heterologous insertion this file cannot express, named so its absence is a stated
# scope boundary rather than an omission.
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

    # WHAT THE EDIT COSTS THE NETWORK, by evaluating every rule twice -- once over the
    # model's own gene set, once with the marked genes removed -- and diffing the live
    # reactions. A gene ORed with an isozyme takes nothing with it; one alone on a
    # reaction, or inside an AND clause, takes it out.
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
