#!/usr/bin/env python3
"""The basis the SCADC fosmid measurement runs on: the deployed bake, decoded and named.

    python research/fabfos/examples/scadc_ecspr_basis.py            # names + terminals
    python research/fabfos/examples/scadc_ecspr_basis.py --out DIR  # elsewhere

Writes `metabolite_names.parquet` and `terminals.json` under
`data/scratch/scadc_ecspr/basis/`. The two decoded bake tables are NOT written here --
they are the shared cache `bake_pairs` maintains, and this prints where they landed.

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

CHEM_PROP = REPO / "data/fabfos/originals/metanetx/4.5/chem_prop.tsv"
DEFAULT_OUT = REPO / "data/scratch/scadc_ecspr/basis"
ELEMENT = "C"

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


def build_names(pairs: pd.DataFrame, out: Path) -> pd.DataFrame:
    universe = set(pairs.substrate) | set(pairs["product"])
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
          f"-> {dest.relative_to(REPO)}")
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
    universe = set(pairs.substrate) | set(pairs["product"])
    chem = _chem_prop()
    print(f"\nterminals, resolved by name against the bake's {ELEMENT} universe "
          f"({len(universe):,} metabolites over {pairs.mnxr.nunique():,} reactions):")
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
    print(f"\nterminals -> {dest.relative_to(REPO)}")
    return doc


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--out", default=str(DEFAULT_OUT))
    a = p.parse_args(argv)
    out = Path(a.out)

    print(f"bake identity: {bake_identity.identity()}")
    ap, dr = bake_pairs.atom_pairs(), bake_pairs.direction_ratios()
    print(f"  atom_pairs -> {ap.relative_to(REPO)}")
    print(f"  direction  -> {dr.relative_to(REPO)}")

    pairs = pd.read_parquet(ap)
    pairs = pairs[pairs.element == ELEMENT]
    build_names(pairs, out)
    build_terminals(pairs, out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
