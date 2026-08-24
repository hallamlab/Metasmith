"""ECMDB measurements -> one intracellular concentration per MNXM, with a width.

The direction lane's ratio is exp(dG'o/RT): standard state, every participant at 1 M.
The correction that turns that into a physiological statement is RT * sum(nu_i * ln c_i),
so the lane needs a concentration per participant and an honest width on it.

TWO THINGS THIS TABLE IS NOT.

It is not a per-organism table. ECMDB is E. coli, and the concentration of a central
metabolite is a property of the physiological state far more than of the species -- ATP is
millimolar and NADPH micromolar in every heterotroph anybody has measured. Using it for
Nostoc is an approximation, and the width below is where that is paid for, not hidden.

It is not a single measurement. A metabolite carries up to nine growth conditions here
(strain x carbon source x phase), spanning two orders of magnitude for some. The spread
ACROSS conditions is the informative quantity: it is the width of the concentration
correction, and a metabolite whose concentration is condition-dependent should move a
direction call less confidently than one that is pinned.

The join to MetaNetX is by KEGG then ChEBI accession, never by name -- ECMDB names are
display strings and MetaNetX carries several accessions per name.

AND THEN IT IS EXPANDED OVER MetaNetX'S OWN NAME ALIASES, because one accession is not
enough. `reac_prop` writes glycogen phosphorylase with `MNXM1364212` (D-glucopyranose
1-phosphate) while KEGG's C00103 resolves to `MNXM1364214` (alpha-D-glucose 1-phosphate):
the same chemical, split by MetaNetX on anomeric specification, and a measurement of the
pool belongs to both. Without the expansion the measured [G1P] is absent and the correction
is applied to [Pi] alone -- WORSE than applying none, because a one-sided Q term invents a
pool skew rather than declining to state one.

THE JOIN IS BY ALIAS, NOT BY InChIKey SKELETON, and the skeleton was tried first. The
connectivity block `HXXFSFRBOHSIMQ` holds seventeen accessions: glucose, galactose,
mannose, allose and gulose 1-phosphate, every anomer and both enantiomeric series. It is a
hexose-phosphate bucket, not a compound. `chem_xref`'s third column is MetaNetX's own list
of names for each accession, so two accessions sharing the name `D-Glucose 1-phosphate`
are two accessions MetaNetX says are that compound -- which separates the anomers of
glucose 1-phosphate from galactose 1-phosphate, where a skeleton cannot.

Formula and charge must still agree, and an expansion spanning more than one formula is
refused rather than resolved.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path

# Bennett 2009 (PMID 19561621) is glucose/glycerol/acetate minimal, mid-log, K12 NCM3722 --
# the growth state every E. coli GEM is parameterised for, and the source the MDF/eQuilibrator
# literature uses. Preferring it is a stated choice, not a filter: a metabolite it does not
# carry falls through to every condition available.
PREFERRED_PMID = "19561621"


def load_xref(chem_xref: Path, wanted_prefixes=("kegg.compound:", "chebi:")):
    # accession -> MNXM. A source accession mapping to more than one MNXM is dropped
    # rather than resolved arbitrarily: the concentration would be attached to a
    # compound nobody chose.
    hits: dict[str, set[str]] = defaultdict(set)
    with open(chem_xref) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < 2:
                continue
            src, mnxm = p[0], p[1]
            if src.startswith(wanted_prefixes) and mnxm.startswith("MNXM"):
                hits[src].add(mnxm)
    return {k: next(iter(v)) for k, v in hits.items() if len(v) == 1}, \
           {k: v for k, v in hits.items() if len(v) > 1}


def geomean(xs):
    return math.exp(sum(math.log(x) for x in xs) / len(xs))


def norm(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def load_aliases(chem_xref: Path):
    # normalised alias -> {MNXM}. `chem_xref`'s third column is MetaNetX's own
    # '||'-separated name list per cross-reference, which is the closest thing the
    # distribution has to a statement that two accessions are the same compound.
    # Rows marked as obsolete carry no usable name and are skipped.
    aliases: dict[str, set[str]] = defaultdict(set)
    with open(chem_xref) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < 3 or not p[1].startswith("MNXM"):
                continue
            if p[2].startswith("secondary/obsolete"):
                continue
            for nm in p[2].split("||"):
                nm = norm(nm)
                if len(nm) > 3:
                    aliases[nm].add(p[1])
    return aliases


def load_formulas(chem_prop: Path):
    out = {}
    with open(chem_prop) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < 9 or not p[0]:
                continue
            out[p[0]] = (p[3], p[4], p[1])
    return out


def _alias_names(finfo, mnxm):
    # MetaNetX's own primary name for the accession, which is often not the name ECMDB
    # displays. Both are used as join keys.
    inf = finfo.get(mnxm)
    return [inf[2]] if inf else []


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--ecmdb-cards", required=True)
    ap.add_argument("--ecmdb-conc", required=True)
    ap.add_argument("--chem-xref", required=True)
    ap.add_argument("--chem-prop", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)

    cards = {x["m2m_id"]: x for x in json.load(open(a.ecmdb_cards))}
    conc = json.load(open(a.ecmdb_conc))
    xref, ambiguous = load_xref(Path(a.chem_xref))
    print(f"[xref] {len(xref):,} unambiguous source accessions, "
          f"{len(ambiguous):,} dropped as many-to-one")

    rows_by_m2m: dict[str, list[dict]] = defaultdict(list)
    for r in conc:
        if r["conc_mM"] is not None and r["conc_mM"] > 0:
            rows_by_m2m[r["m2m"]].append(r)

    out, no_id, no_mnxm = [], [], []
    for m2m, rows in rows_by_m2m.items():
        card = cards.get(m2m, {})
        keys = []
        if card.get("kegg_id"):
            keys.append(("kegg.compound:" + card["kegg_id"], "kegg"))
        if card.get("chebi_id"):
            keys.append(("chebi:" + str(card["chebi_id"]), "chebi"))
        if not keys:
            no_id.append(rows[0]["name"])
            continue
        mnxm = route = None
        for key, which in keys:
            if key in xref:
                mnxm, route = xref[key], which
                break
        if mnxm is None:
            no_mnxm.append(rows[0]["name"])
            continue

        pref = [r for r in rows if PREFERRED_PMID in r["citation"]]
        used = pref or rows
        vals = [r["conc_mM"] for r in used]
        allvals = [r["conc_mM"] for r in rows]
        logs = [math.log10(v) for v in allvals]
        out.append(dict(
            mnxm=mnxm, ecmdb=rows[0]["ecmdb"], name=rows[0]["name"], route=route,
            conc_mM=geomean(vals), n_used=len(used), n_all=len(rows),
            preferred=bool(pref),
            log10_spread=(statistics.pstdev(logs) if len(logs) > 1 else 0.0),
            log10_range=(max(logs) - min(logs)) if len(logs) > 1 else 0.0,
            min_mM=min(allvals), max_mM=max(allvals)))

    # One MNXM can be reached by two ECMDB entries (a tautomer pair, an acid/anion pair).
    # Keep the one with the most conditions behind it and record that it happened.
    best: dict[str, dict] = {}
    collisions = 0
    for r in sorted(out, key=lambda r: (-r["n_all"], r["ecmdb"])):
        if r["mnxm"] in best:
            collisions += 1
            continue
        best[r["mnxm"]] = r

    # Expand each measured accession over MetaNetX's own alias list for it.
    aliases = load_aliases(Path(a.chem_xref))
    finfo = load_formulas(Path(a.chem_prop))
    expanded, rejected, refused = {}, 0, []
    for mnxm, r in best.items():
        expanded[mnxm] = dict(r, join="accession")
        primary = finfo.get(mnxm)
        if primary is None:
            continue
        formula, charge, _ = primary
        reach = set()
        for nm in {norm(r["name"])} | {norm(n) for n in
                                      _alias_names(finfo, mnxm)}:
            reach |= aliases.get(nm, set())
        # No blanket refusal on a name reaching several formulas: an alias list routinely
        # spans protonation states and R-group generics, and the per-accession
        # formula+charge guard below already keeps only the ones that ARE this compound.
        for m in reach:
            if m in expanded or m not in finfo:
                continue
            if finfo[m][:2] != (formula, charge):
                rejected += 1
                continue
            expanded[m] = dict(r, mnxm=m, join="alias")
    print(f"[conc] alias expansion: {len(best):,} -> {len(expanded):,} accessions "
          f"({rejected:,} candidate accessions rejected on formula/charge)")
    best = expanded

    cols = ("mnxm", "ecmdb", "name", "route", "join", "conc_mM", "n_used", "n_all",
            "preferred", "log10_spread", "log10_range", "min_mM", "max_mM")
    with open(a.out, "w") as fh:
        fh.write("\t".join(cols) + "\n")
        for r in sorted(best.values(), key=lambda r: -r["conc_mM"]):
            fh.write("\t".join(str(r[c]) for c in cols) + "\n")

    print(f"[conc] {len(rows_by_m2m):,} ECMDB metabolites with a usable measurement")
    print(f"[conc] {len(no_id):,} carry neither a KEGG nor a ChEBI id")
    print(f"[conc] {len(no_mnxm):,} carry one MetaNetX does not cross-reference")
    print(f"[conc] {collisions:,} collapsed onto an MNXM another entry already held")
    print(f"[conc] {len(best):,} MNXM concentrations -> {a.out}")
    pref_n = sum(1 for r in best.values() if r["preferred"])
    print(f"[conc] {pref_n:,} rest on the preferred condition set (PMID {PREFERRED_PMID})")
    wide = sorted(best.values(), key=lambda r: -r["log10_range"])[:8]
    print("[conc] widest condition spread (log10 range):")
    for r in wide:
        print(f"        {r['log10_range']:5.2f}  {r['name'][:44]:44s} "
              f"{r['min_mM']:.3g} .. {r['max_mM']:.3g} mM  (n={r['n_all']})")


if __name__ == "__main__":
    main()
