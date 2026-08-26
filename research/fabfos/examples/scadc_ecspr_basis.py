#!/usr/bin/env python3
"""The basis the SCADC fosmid measurement runs on: the deployed bake, decoded and named.

    python research/fabfos/examples/scadc_ecspr_basis.py            # names + axes
    python research/fabfos/examples/scadc_ecspr_basis.py --out DIR  # elsewhere

Writes `metabolite_names.parquet`, `terminals.json` and `axes.json` under
`data/scratch/scadc_ecspr/basis/`. The two decoded bake tables are NOT written here --
they are the shared cache `bake_pairs` maintains, and this prints where they landed.

THE ARM MEASURES 83 AXES, NOT ONE. Eighty-two come from `ecspr_axes` -- set4's 79 plus
the three glycerol-backbone axes added beside it -- and each is one source and one sink
on one element. The eighty-third is the published four-hub merged axis, carried forward
unchanged so a rebuilt path can be checked against a number that already exists.

TWO DIFFERENT CHECKS, BECAUSE THE TWO SETS ARE DIFFERENT KINDS OF THING. The four hubs
are carried as NAMES and resolved against the bake, because an id is a fact about one
MetaNetX release and this arm has already outlived one. set4 IS ids -- that is what the
frozen file contains, and no name in it is authoritative -- so its endpoints get the
other half of the same guard: every one must be a node in its own element's pair table,
and any that is not is named. A conditions table naming metabolites the graph has never
heard of measures a run of structural zeros that looks exactly like a biological finding.

TIER4 IS RETIRED, AND THIS FILE IS WHERE THAT LANDED. The generation this replaces read
`data/fabfos/benchmark/reference_tier4/atom_pairs_tier4.parquet` and joined the bake's
direction table through `vocab` by hand. That pin was dropped when `metabolism_bake`
became the atom-pair basis; the path does not exist, so the old script could not run at
all -- and the measurement it produced is on a basis nothing else in this tree shares.
Decoding goes through `bake_pairs`/`bake_identity` instead, which stamps every cache with
the bake it came from and refuses to serve one decoded from another.

`metabolite_names` IS A FUNCTION OF THE BAKE and has to be rebuilt with it: it is
chem_prop restricted to the metabolites the bake actually pairs, so a metabolite a new
bake reaches is simply absent from an older copy. It exists so a terminal can be resolved
by NAME and checked present, which is the check the retired generation did not have on
its own conditions table.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks"))
sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/eydallin"))

import bake_identity  # noqa: E402
import bake_pairs  # noqa: E402
import ecspr_axes  # noqa: E402

CHEM_PROP = REPO / "data/fabfos/originals/metanetx/4.5/chem_prop.tsv"
DEFAULT_OUT = REPO / "data/scratch/scadc_ecspr/basis"
ELEMENT = "C"
ELEMENTS = ("C", "N", "P", "S")

# Glucose in, and the four sinks merged into one ground. Carried as NAMES, because an id
# is a fact about one MetaNetX release and this arm has already outlived one: the ids the
# retired generation used are recorded beside each entry and are compared, not assumed.
SOURCE = ("D-glucose", "glucose")
SINKS = (
    ("acetyl_CoA", ("acetyl-CoA",), "MNXM1104266"),
    ("malonyl_CoA", ("malonyl-CoA",), "MNXM1106093"),
    ("oxaloacetate", ("oxaloacetate",), "MNXM46"),
    ("AMP", ("AMP",), "MNXM728294"),
)

LEGACY_AXIS = "legacy__glucose__4hubs__C"

# The ids the lipid extension introduces, re-resolved by NAME as a drift check. Only
# these: set4's own ids are the frozen file's content and there is no name to check them
# against, while these three axes were authored here and a name exists for each.
NAMED_CHECKS = (("DHAP", ("dihydroxyacetone phosphate",), "MNXM77"),)

_FORMULA_RE = re.compile(r"([A-Z][a-z]?)(\d*)")


def _parse_formula(formula: str) -> dict:
    # A `*` RESIDUE IS KEPT, counting its explicit atoms only. `C28H49N3O10PS*2` is an
    # acyl-ACP whose carrier body MetaNetX leaves unspecified; dropping the whole formula
    # for that takes most of the ACP, polymer and protein-bound series out of the name
    # table. The retired scadc builder dropped them and the nostoc one does not.
    if not isinstance(formula, str) or not formula:
        return {}
    counts = {}
    for el, n in _FORMULA_RE.findall(formula):
        if not el:
            continue
        counts[el] = counts.get(el, 0) + (int(n) if n else 1)
    return counts


def _rel(p: Path) -> str:
    # `--out` may point outside the worktree, so a repo-relative name is a nicety rather
    # than a guarantee.
    try:
        return str(Path(p).relative_to(REPO))
    except ValueError:
        return str(p)


def _write(df: pd.DataFrame, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".writing")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, dest)
    return dest


def _chem_prop() -> pd.DataFrame:
    return pd.read_csv(CHEM_PROP, sep="\t", comment="#", usecols=[0, 1, 3], dtype=str,
                       names=["id", "name", "reference", "formula", "charge", "mass",
                              "inchi", "inchikey", "smiles"])


def _universe(pairs: pd.DataFrame, element: str | None = None) -> set:
    p = pairs if element is None else pairs[pairs.element == element]
    return set(p.substrate) | set(p["product"])


def build_names(pairs: pd.DataFrame, out: Path) -> pd.DataFrame:
    # Over EVERY element's universe, not carbon's. The arm reads N, P and S axes now, and
    # a name table cut to carbon silently has no row for L-glutamine on nitrogen.
    universe = _universe(pairs)
    chem = _chem_prop()
    chem = chem[chem.id.isin(universe)]
    rows = [(m, nm, f, el, n)
            for m, nm, f in zip(chem.id, chem.name, chem.formula)
            for el, n in _parse_formula(f).items()]
    names = pd.DataFrame(rows, columns=["mnxm", "name", "formula", "element", "n_atoms"])
    dest = _write(names, out / "metabolite_names.parquet")
    skipped = chem.id.nunique() - names.mnxm.nunique()
    print(f"metabolite_names: {len(names):,} rows over {names.mnxm.nunique():,} "
          f"metabolites ({skipped:,} of the universe carry no parseable formula) "
          f"-> {_rel(dest)}")
    return names


# MetaNetX carries many synonym rows per metabolite, so an exact-name lookup returns a
# SET rather than an id -- hence the incidence tie-break, which is the rule the nostoc
# lane uses. Printing the whole candidate set is what makes the choice reviewable.
def resolve(chem: pd.DataFrame, universe: set, names: tuple, label: str) -> str:
    hits = chem[chem.name.isin(names) & chem.id.isin(universe)]
    if hits.empty:
        raise SystemExit(f"no candidate for {label} among {names} in the bake's "
                         f"{ELEMENT} pair universe")
    counts = hits.id.value_counts()
    chosen = str(counts.idxmax())
    others = sorted(set(hits.id) - {chosen})
    print(f"  {label:14s} {chosen}"
          + (f"   (also matched {others})" if others else ""))
    return chosen


def build_terminals(pairs: pd.DataFrame, out: Path) -> dict:
    universe = _universe(pairs, ELEMENT)
    chem = _chem_prop()
    print(f"\nterminals, resolved by name against the bake's {ELEMENT} universe "
          f"({len(universe):,} metabolites over "
          f"{pairs[pairs.element == ELEMENT].mnxr.nunique():,} reactions):")
    source = resolve(chem, universe, SOURCE, "source glucose")
    sinks, moved = {}, []
    for key, names, retired in SINKS:
        mnxm = resolve(chem, universe, names, f"sink {key}")
        sinks[key] = mnxm
        if mnxm != retired:
            moved.append((key, retired, mnxm))

    # THE COMPARISON IS THE POINT, not a formality. A conditions table authored by
    # guesswork names metabolites the graph has never heard of and measures a run of
    # structural zeros that looks exactly like a biological finding -- so a terminal that
    # has moved since the retired generation is reported rather than silently adopted.
    if moved:
        print("\n  TERMINALS THAT MOVED since the retired generation:")
        for key, was, now in moved:
            print(f"    {key}: {was} -> {now}")
    else:
        print("\n  every terminal resolves to the id the retired generation used")

    doc = {"bake_identity": bake_identity.identity(), "element": ELEMENT,
           "source": source, "sinks": sinks,
           "moved_since_retired": [dict(sink=k, was=w, now=n) for k, w, n in moved]}
    dest = out / "terminals.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(doc, indent=2) + "\n")
    print(f"\nterminals -> {_rel(dest)}")
    return doc


def build_axes(pairs: pd.DataFrame, terminals: dict, out: Path) -> pd.DataFrame:
    axes = ecspr_axes.load()
    chem = _chem_prop()

    # Every endpoint must be a node in ITS OWN element's pair table. An axis whose source
    # or sink the graph does not carry is not a weak measurement, it is a structural zero
    # on every construction -- indistinguishable in the output from a real null.
    print(f"\n{len(axes)} axes from `ecspr_axes`, checked against each element's "
          f"pair universe:")
    absent = []
    for el in sorted(set(axes.element)):
        uni = _universe(pairs, el)
        rows = axes[axes.element == el]
        want = set(rows.src_mnxm) | set(rows.dst_mnxm)
        gone = sorted(want - uni)
        print(f"  {el}: {len(rows):2d} axes over {len(want):2d} metabolites, "
              f"universe {len(uni):,}"
              + (f"   ABSENT {gone}" if gone else "   all present"))
        absent += [(el, m) for m in gone]
    if absent:
        raise SystemExit(
            f"{len(absent)} axis endpoint(s) are not nodes in their own element's pair "
            f"table on this bake: {absent[:6]}. They would measure a structural zero on "
            f"every construction, which reads exactly like a biological null")

    # The drift check the four hubs get, applied to the ids this repository authored.
    print("\nlipid-extension ids, re-resolved by name:")
    for label, names, declared in NAMED_CHECKS:
        got = resolve(chem, _universe(pairs, ELEMENT), names, label)
        if got != declared:
            raise SystemExit(
                f"{label} resolves to {got} on this bake, but the lipid extension "
                f"declares {declared}. Fix the extension rather than the check")

    rows = [dict(axis_id=r.axis_id, element=r.element, source=r.src_mnxm,
                 sink_hub=r.dst_mnxm, category=r.category, axis_set=r.axis_set,
                 module_name=r.module_name) for r in axes.itertuples()]
    # THE PUBLISHED AXIS RIDES ALONG, so the rebuilt path can be checked against a number
    # that already exists. Its sinks are merged into one ground, which is why `sink_hub`
    # is a joined field on every row rather than a single id.
    rows.append(dict(axis_id=LEGACY_AXIS, element=terminals["element"],
                     source=terminals["source"],
                     sink_hub="|".join(terminals["sinks"].values()),
                     category="legacy", axis_set="legacy",
                     module_name="D-glucose -> 4 biomass hubs (merged)"))
    df = pd.DataFrame(rows)

    doc = {"bake_identity": bake_identity.identity(), "axes": rows}
    dest = out / "axes.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(doc, indent=2) + "\n")
    print(f"\n{len(df)} axes ("
          + ", ".join(f"{k} {v}" for k, v in df.axis_set.value_counts().items())
          + f") -> {_rel(dest)}")
    return df


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--out", default=str(DEFAULT_OUT))
    a = p.parse_args(argv)
    out = Path(a.out)

    print(f"bake identity: {bake_identity.identity()}")
    ap, dr = bake_pairs.atom_pairs(), bake_pairs.direction_ratios()
    print(f"  atom_pairs -> {_rel(ap)}")
    print(f"  direction  -> {_rel(dr)}")

    pairs = pd.read_parquet(ap)
    pairs = pairs[pairs.element.isin(ELEMENTS)]
    build_names(pairs, out)
    terminals = build_terminals(pairs, out)
    build_axes(pairs, terminals, out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
