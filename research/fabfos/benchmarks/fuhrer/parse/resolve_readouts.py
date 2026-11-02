#!/usr/bin/env python3
"""Every KEGG compound the Fuhrer screen puts on an ion, carried through MetaNetX to a node
of the atom-pair graph -- or to the reason it is not one.

    PYTHONPATH=$PWD/src mamba run -n ecspr python \\
        research/fabfos/benchmarks/fuhrer/parse/resolve_readouts.py
    ... --publish     # writes data/fabfos/runs/fuhrer_clones/parse/readout_resolution.tsv

**CAUTION** RESOLVE THROUGH THE CROSS-REFERENCE TABLE, NEVER BY COMPOUND NAME. The eydallin
arm records that a name lookup returns the wrong MetaNetX identifier for ADP-glucose and for
glucose-1-phosphate, and the failure presents as `absent from the graph` rather than as an
error -- a sweep against the wrong node runs to completion and returns a plausible null.
`kegg.compound:<id>` in `chem_xref.tsv` is the only route used here.

THIS TABLE IS WHAT MAKES THE GENOME-WIDE SWEEP'S DENOMINATOR HONEST. The interesting number
is not how many readouts resolve, it is how many do not and at which step, because a sweep
that quietly considers only what happened to resolve reports coverage it never had. Four
outcomes per (ion, compound), and every ion keeps a row even with no annotation at all:

    no_annotation    the ion carries no KEGG compound within 3 mDa
    no_xref          the compound has no MetaNetX cross-reference at this release
    resolved         it has exactly one, and `node_gem` / `node_denovo` say whether that
                     identifier carries carbon atoms in each channel's background
    ambiguous_xref   it has more than one, which no rule here breaks

NODE-HOOD IS PER CHANNEL AND THE TWO DISAGREE. `gpr_gem.parquet` populates
`in_atom_universe`; `gpr_denovo.parquet` leaves it null, so the de-novo background is
recomputed locally with transport excluded. Defaulting the null either way is wrong in a
different direction on each channel -- true admits transport and reconnects everything,
false admits nothing and reads as `the organism cannot reach it`.

THE ION IS THE READOUT, THE COMPOUND IS A GUESS. Flow-injection mass spectrometry does not
separate isomers, so one ion routinely carries several KEGG compounds at the same mass.
`n_compound` is carried on every row for that reason: a resolved identifier on an ion with
six candidates is not the same evidence as a resolved identifier on an ion with one.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin"))

import bake_pairs                                                        # noqa: E402
from ecspr.model.build import load_pairs                                 # noqa: E402

ION_KEY = ROOT / "data/fabfos/runs/fuhrer_clones/parse/screen/ion_key.tsv"
CHEM_XREF = ROOT / "data/fabfos/originals/metanetx/4.5/chem_xref.tsv"
OUT = ROOT / "data/fabfos/runs/fuhrer_clones/parse/readout_resolution.tsv"

HOST = "e_coli_bw25113"
ELEMENT = "C"
CHANNELS = ("gem", "denovo")

COLS = ("mode", "ion_index", "n_compound", "kegg_id", "kegg_name", "mnxm", "mnx_name",
        "resolution", "node_gem", "node_denovo", "published_auc", "z_cutoff")


def kegg_to_mnxm() -> dict[str, list[tuple[str, str]]]:
    """`C-id -> [(MNXM, name)]`. `keggC:` rows duplicate `kegg.compound:` ones and carry
    the obsolete `M_C…` spellings besides, so only the canonical prefix is read."""
    out: dict[str, list[tuple[str, str]]] = {}
    with CHEM_XREF.open() as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            source, mnxm, name = line.rstrip("\n").split("\t")[:3]
            if not source.startswith("kegg.compound:"):
                continue
            out.setdefault(source.split(":", 1)[1], []).append(
                (mnxm, name.split("||")[0]))
    return out


def atom_universe() -> set:
    sys.path.insert(0, str(ROOT / "src/fabfos/build_references/resources/buildlib"))
    import bench_universe as bu                                          # noqa: E402
    bake = ROOT / "data/fabfos/processed/metabolism_bake"
    universe, stats = bu.atom_universe(
        bake / "vocab.parquet", bake / "atom_pairs.parquet",
        exclude=bu.transport_mnxrs(
            bu.reac_prop_path(ROOT / "data/fabfos/originals/metanetx")))
    print(bu.universe_line(stats, "fuhrer"), file=sys.stderr)
    return universe


def channel_nodes(pairs: pd.DataFrame, channel: str, universe: set) -> set[str]:
    p = ROOT / f"data/fabfos/runs/{HOST}/gpr/gpr_{channel}.parquet"
    df = pd.read_parquet(p, columns=["mnxr", "in_atom_universe"])
    keep = (df.in_atom_universe.fillna(False).infer_objects(copy=False).astype(bool)
            if channel == "gem" else df.mnxr.astype(str).isin(universe))
    weights = set(df[keep].mnxr.dropna().astype(str))
    sub = pairs[pairs.mnxr.isin(weights)]
    nodes = set(sub.substrate.astype(str)) | set(sub["product"].astype(str))
    print(f"[resolve] {channel}: {len(weights):,} in-universe reactions, "
          f"{len(nodes):,} carbon-bearing metabolite nodes")
    return nodes


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"write {OUT.relative_to(ROOT)}")
    a = ap.parse_args()

    ions = pd.read_csv(ION_KEY, sep="\t", dtype=str).fillna("")
    xref = kegg_to_mnxm()
    pairs = load_pairs(bake_pairs.atom_pairs(), element=ELEMENT)
    universe = atom_universe()
    nodes = {c: channel_nodes(pairs, c, universe) for c in CHANNELS}

    rows: list[dict] = []
    for r in ions.to_dict("records"):
        ids = [i for i in r["kegg_ids"].split(";") if i]
        names = r["kegg_names"].split(";") if r["kegg_names"] else []
        if not ids:
            rows.append({"mode": r["mode"], "ion_index": r["ion_index"], "n_compound": 0,
                         "kegg_id": "", "kegg_name": "", "mnxm": "", "mnx_name": "",
                         "resolution": "no_annotation", "node_gem": "", "node_denovo": "",
                         "published_auc": "", "z_cutoff": ""})
            continue
        for kid, kname in zip(ids, names + [""] * (len(ids) - len(names))):
            hits = xref.get(kid, [])
            distinct = sorted({m for m, _ in hits})
            mnxm = distinct[0] if len(distinct) == 1 else ""
            rows.append({
                "mode": r["mode"], "ion_index": r["ion_index"], "n_compound": len(ids),
                "kegg_id": kid, "kegg_name": kname, "mnxm": mnxm,
                "mnx_name": next((n for m, n in hits if m == mnxm), ""),
                "resolution": ("no_xref" if not hits else
                               "resolved" if mnxm else "ambiguous_xref"),
                "node_gem": str(mnxm in nodes["gem"]) if mnxm else "",
                "node_denovo": str(mnxm in nodes["denovo"]) if mnxm else "",
                # The published AUC is per ION-ANNOTATION pair, and `ion_key.tsv` only
                # keeps the winning one, so it is attached only to the compound that won.
                "published_auc": r["best_auc"] if kid == r["best_auc_id"] else "",
                "z_cutoff": r["z_cutoff"] if kid == r["best_auc_id"] else "",
            })

    df = pd.DataFrame(rows, columns=list(COLS))
    census = df.resolution.value_counts()
    print(f"\n{len(ions):,} ions -> {len(df):,} (ion, compound) rows")
    for k in ("no_annotation", "no_xref", "ambiguous_xref", "resolved"):
        print(f"   {k:16} {census.get(k, 0):>6,}")
    ok = df[df.resolution == "resolved"]
    for c in CHANNELS:
        on = ok[ok[f"node_{c}"] == "True"]
        print(f"   of the resolved, {len(on):,} rows / "
              f"{on.mnxm.nunique():,} distinct identifiers are nodes on {c} "
              f"({ok.mnxm.nunique() - on.mnxm.nunique():,} resolve to a compound the "
              f"{c} background does not carry carbon for)")
    solo = ok[(ok.n_compound == 1) & (ok.node_gem == "True")]
    print(f"   {len(solo):,} ions are annotated to exactly ONE compound that is a curated "
          f"node -- the pool axis declaration draws from")

    if not a.publish:
        print(f"\n(dry run -- pass --publish to write {OUT.relative_to(ROOT)})")
        return 0
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.unlink(missing_ok=True)
    df.to_csv(OUT, sep="\t", index=False)
    print(f"\n-> {OUT.relative_to(ROOT)}: {len(df):,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
