#!/usr/bin/env python3
"""Assemble the SCADC ECSPr reference basis + conditions table (plan T1).

Adapts what already exists locally, under different names/tiers, into the
schema `data_types/ecspr.yml` declares:

    atom_pairs         <- data/benchmark/reference_tier4/atom_pairs_tier4.parquet  (copied as-is)
    direction_ratios   <- data/processed/metabolism_bake/{direction,vocab}.parquet (joined)
    metabolite_names   <- data/originals/metanetx/4.5/chem_prop.tsv               (parsed formula)
    conditions         <- hand-picked source/sink hubs, resolved by exact name match

NOT the deployed reference basis (no `.awm/data/ref/derived/mnxref-4_5/` locally):
the tier-4 atom_pairs stand in for it, resolved above.
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "data" / "fabfos" / "runs" / "scadc_ecspr" / "refs"
OUT.mkdir(parents=True, exist_ok=True)

ATOM_PAIRS_TIER4 = ROOT / "data" / "fabfos" / "benchmark" / "reference_tier4" / "atom_pairs_tier4.parquet"
BAKE_DIR = ROOT / "data" / "fabfos" / "processed" / "metabolism_bake"
CHEM_PROP = ROOT / "data" / "fabfos" / "originals" / "metanetx" / "4.5" / "chem_prop.tsv"

FORMULA_RE = re.compile(r"([A-Z][a-z]?)(\d*)")


def parse_formula(formula: str) -> dict:
    """`"C6H12O6"` -> `{"C": 6, "H": 12, "O": 6}`. Skips malformed/blank formulas."""
    if not isinstance(formula, str) or not formula or "*" in formula:
        return {}
    counts = {}
    for el, n in FORMULA_RE.findall(formula):
        if not el:
            continue
        counts[el] = counts.get(el, 0) + (int(n) if n else 1)
    return counts


def build_atom_pairs():
    df = pd.read_parquet(ATOM_PAIRS_TIER4)
    out = OUT / "atom_pairs.parquet"
    df.to_parquet(out)
    print(f"atom_pairs: {len(df)} rows -> {out} (tier4, copied as-is)")
    return df


def build_direction_ratios():
    direction = pd.read_parquet(BAKE_DIR / "direction.parquet")
    vocab = pd.read_parquet(BAKE_DIR / "vocab.parquet")
    rxn_vocab = vocab[vocab.kind == "rxn"][["code", "symbol"]].rename(
        columns={"code": "rxn", "symbol": "mnxr"})
    df = direction.merge(rxn_vocab, on="rxn", how="inner")
    df = df[df.mnxr != "EMPTY"]
    df = df.assign(dG_prime=pd.NA, sigma=pd.NA, dir_method="bake_staged", dir_confidence=pd.NA)
    df = df[["mnxr", "ratio", "dG_prime", "sigma", "dir_tier", "dir_method", "dir_confidence"]]
    out = OUT / "direction_ratios.parquet"
    df.to_parquet(out)
    print(f"direction_ratios: {len(df)} rows -> {out} "
          f"(metabolism_bake direction.parquet joined through vocab.parquet)")
    return df


def build_metabolite_names(atom_pairs: pd.DataFrame):
    universe = set(atom_pairs["substrate"].unique()) | set(atom_pairs["product"].unique())
    chem = pd.read_csv(CHEM_PROP, sep="\t", comment="#",
                        names=["id", "name", "reference", "formula", "charge", "mass",
                               "inchi", "inchikey", "smiles"])
    chem = chem[chem.id.isin(universe)]
    rows = []
    skipped = 0
    for mnxm, name, formula in zip(chem.id, chem.name, chem.formula):
        counts = parse_formula(formula)
        if not counts:
            skipped += 1
            continue
        for element, n_atoms in counts.items():
            rows.append(dict(mnxm=mnxm, name=name, formula=formula,
                              element=element, n_atoms=n_atoms))
    df = pd.DataFrame(rows)
    out = OUT / "metabolite_names.parquet"
    df.to_parquet(out)
    print(f"metabolite_names: {len(df)} rows ({df.mnxm.nunique()} metabolites, "
          f"{skipped} skipped for missing/malformed formula) -> {out}")
    return df


def resolve_hub(chem_names: pd.DataFrame, universe: set, exact_names: list[str], label: str) -> str:
    hits = chem_names[chem_names.name.isin(exact_names) & chem_names.id.isin(universe)]
    if hits.empty:
        raise SystemExit(f"no candidate for {label} among {exact_names} in the atom_pairs universe")
    # Pick the most-connected candidate -- the one actually load-bearing in the
    # atom-transfer graph, not just the first alphabetical match.
    counts = hits.id.value_counts()
    chosen = counts.idxmax()
    row = hits[hits.id == chosen].iloc[0]
    print(f"{label}: chose {row.id} ({row['name']!r}, formula={row.formula}) "
          f"among {sorted(hits.id.unique())}")
    return chosen


def build_conditions(atom_pairs: pd.DataFrame):
    universe = set(atom_pairs["substrate"].unique()) | set(atom_pairs["product"].unique())
    chem = pd.read_csv(CHEM_PROP, sep="\t", comment="#",
                        names=["id", "name", "reference", "formula", "charge", "mass",
                               "inchi", "inchikey", "smiles"])
    source_hub = resolve_hub(chem, universe, ["D-glucose", "glucose"], "source (glucose)")

    sinks = [
        ("acetyl-CoA", ["acetyl-CoA"]),
        ("malonyl-CoA", ["malonyl-CoA"]),
        ("oxaloacetate", ["oxaloacetate"]),
        ("AMP", ["AMP"]),
    ]
    rows = []
    for i, (label, names) in enumerate(sinks):
        sink_hub = resolve_hub(chem, universe, names, f"sink ({label})")
        rows.append(dict(condition_id=f"scadc_C{i+1}_{label.replace('-', '_')}",
                          element="C", source_hub=source_hub, sink_hub=sink_hub,
                          media="glucose_minimal"))
    df = pd.DataFrame(rows)
    out = ROOT / "data" / "fabfos" / "runs" / "scadc_ecspr" / "conditions.parquet"
    df.to_parquet(out)
    print(f"conditions: {len(df)} rows -> {out}")
    print(df)
    return df


if __name__ == "__main__":
    pairs = build_atom_pairs()
    build_direction_ratios()
    build_metabolite_names(pairs)
    build_conditions(pairs)
