#!/usr/bin/env python3
"""Turn the ASKA/FFA measurements into a study extraction on the shared schema.

The human reading of this paper is `ffa_response.tsv` in the acquisitions tier --
a measured titer per strain, read off the figure bars by `digitise_ffa.py` and
anchored on the values the paper states in prose. Everything this script does on
top of that is mechanical and had better be reproducible, which is why it is a
script rather than sixty hand-typed rows: resolve each overexpressed ORF to a
b-number, resolve that to the reactions iML1515 gives it, and classify the
measured direction against the strain's own panel control.

RESOLUTION GOES THROUGH B-NUMBERS, NEVER NAMES. The ASKA roster and the paper
both use the old *rfa* nomenclature, and iML1515 uses the *waa* one, so the
paper's headline hit `rfaY` is `waaY`/`b3625` in the model. Twelve of the sixty
tested ORFs -- every *waa*, every *lpt*, and `gppA`, `nepI`, `opgD` -- fail to
match the ASKA roster by name and resolve cleanly by synonym. Name matching
would have dropped the one gene the study is about, silently and without
changing any count that gets printed. `NC_000913.3.gbk` carries `/gene`,
`/gene_synonym` and `/locus_tag` for the whole chromosome and is the offline
resolution table for both the tested ORFs and the 4,123-clone null universe.

DIRECTION COMES FROM THE PAPER'S OWN ANNOTATION WHERE IT MADE ONE. A bar filled
pink or carrying asterisks is a significant increase the authors tested for; a
bar marked `ns` is one they tested and did not find. They only ever annotated
increases, so the bars far *below* their control -- `setB` at a quarter of F0 --
carry no mark at all, and calling those "unknown" would throw away the clearest
negatives in the study. Unannotated bars are therefore classified by fold change
against their own panel control on a fixed threshold, declared here and not
fitted to anything: >= 1.25 is up, <= 0.80 is down, between is flat. Replicate
spreads in these panels run 5-15% of the mean, so the band is comfortably
outside noise, and no unannotated bar in any panel reaches 1.25 -- the authors
did mark every increase they saw.

Writes the extraction the build tier copies as bytes:

    data/fabfos/benchmarks/_extractions/aska_ffa/extraction.tsv

and, beside this script, the resolution census that says how much of the study
the model can see at all:

    main/benchmarks/aska/out/orf_resolution.tsv
"""
from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
RESPONSE = ROOT / "data/fabfos/originals/benchmarks/aska/ffa/ffa_response.tsv"
GENOME = ROOT / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.gbk"
HOST_GEM = ROOT / "data/fabfos/benchmarks/hosts/e_coli_k12/gpr_gem.parquet"
EXTRACT = ROOT / "data/fabfos/benchmarks/_extractions/aska_ffa/extraction.tsv"

CITATION = ("Fang et al. Metab Eng 2025;92:13-21; "
            "doi:10.1016/j.ymben.2025.06.010")

UP, DOWN = 1.25, 0.80

EXTRACTION_COLS = (
    "obs_id", "strain_id", "figure", "background", "gene", "b_number", "mnxr",
    "role", "measured", "ffa_mg_L", "control_mg_L", "fold_change", "citation",
    "note",
)


def gene_to_bnumber(gbk: Path) -> dict[str, str]:
    """Every name and synonym on the chromosome, mapped to its locus tag.

    A primary `/gene` wins over a synonym, because a synonym of one gene is
    occasionally the primary name of another and the primary is what the model
    is keyed on.
    """
    text = gbk.read_text()
    block = re.compile(
        r'/gene="([^"]+)"\s*\n\s*/locus_tag="([^"]+)"'
        r'(?:\s*\n\s*/gene_synonym="([^"]*)")?', re.S)
    primary, alias = {}, {}
    for m in block.finditer(text):
        name, tag, syns = m.group(1), m.group(2), (m.group(3) or "")
        primary.setdefault(name, tag)
        for s in re.split(r";\s*", syns.replace("\n", " ")):
            s = s.strip()
            if s:
                alias.setdefault(s, tag)
    return {**alias, **primary}


def direction(fold: float, mark: str, colour: str) -> str:
    """up / down / flat -- the paper's call where it made one, else the band."""
    if colour == "pink" or (mark and mark != "ns"):
        return "up" if fold >= 1.0 else "down"
    if mark == "ns":
        return "flat"
    if fold >= UP:
        return "up"
    if fold <= DOWN:
        return "down"
    return "flat"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=EXTRACT)
    ap.add_argument("--census", type=Path, default=HERE / "out" / "orf_resolution.tsv")
    args = ap.parse_args()

    resp = pd.read_csv(RESPONSE, sep="\t", dtype=str, keep_default_na=False)
    resp["ffa"] = resp["ffa_mg_L"].astype(float)
    lookup = gene_to_bnumber(GENOME)
    gem = pd.read_parquet(HOST_GEM, columns=["feature_id", "mnxr"])
    rxns = defaultdict(list)
    for tag, grp in gem.groupby("feature_id"):
        rxns[tag] = sorted(set(grp["mnxr"]))

    # Every panel's control titer, so a strain's fold change is against the bar
    # it was actually drawn beside. The round-two panel is read against RF, not
    # against F0, and a titer is only comparable inside its own panel.
    control = {}
    for _, r in resp.iterrows():
        if r["strain_id"] == r["control_strain"]:
            control[(r["figure"], r["strain_id"])] = r["ffa"]

    rows, census = [], []
    for _, r in resp.iterrows():
        strain, fig = r["strain_id"], r["figure"]
        ctrl_id = r["control_strain"]
        ctrl = control.get((fig, ctrl_id))
        if ctrl is None:                       # Fig. 5a reads against its own parent
            hit = resp[(resp["figure"] == fig) & (resp["strain_id"] == ctrl_id)]
            ctrl = float(hit["ffa"].iloc[0]) if len(hit) else float("nan")
        fold = r["ffa"] / ctrl
        measured = direction(fold, r["significance"], r["bar_colour"])
        obs = f"aska_ffa:{fig}:{strain}"
        genes = [g for g in r["clones"].split("|") if g]
        dels = [g for g in r["deletions"].split("|") if g]
        note = (f"{r['note']}; bar {r['bar_colour'] or 'n/a'}, "
                f"mark {r['significance'] or 'none'}, "
                f"{r['ffa_source']} titer vs {ctrl_id} at {ctrl:.1f} mg/L")
        for role, names in (("add", genes), ("del", dels)):
            for gene in names:
                tag = lookup.get(gene, "")
                got = rxns.get(tag, []) if tag else []
                census.append(dict(
                    gene=gene, b_number=tag or "", n_reactions=len(got),
                    strains=obs))
                for mnxr in (got or [""]):
                    rows.append(dict(
                        obs_id=obs, strain_id=strain, figure=fig,
                        background=r["background"], gene=gene, b_number=tag,
                        mnxr=mnxr, role=role, measured=measured,
                        ffa_mg_L=f"{r['ffa']:.1f}", control_mg_L=f"{ctrl:.1f}",
                        fold_change=f"{fold:.3f}", citation=CITATION, note=note))
        if not genes and not dels:
            # The panel's own control bar: the strain every other bar in that
            # figure is a difference from, carrying no clone at all. Declared as
            # a control rather than emitted as an edge-less condition, so the
            # gate has something whose spread IS the solver's floor.
            rows.append(dict(
                obs_id=obs, strain_id=strain, figure=fig,
                background=r["background"], gene="", b_number="", mnxr="",
                role="control", measured="flat", ffa_mg_L=f"{r['ffa']:.1f}",
                control_mg_L=f"{ctrl:.1f}", fold_change=f"{fold:.3f}",
                citation=CITATION, note=note + "; panel control, no clone"))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(EXTRACTION_COLS), delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {args.out} ({len(rows)} rows, "
          f"{len({r['obs_id'] for r in rows})} conditions)")

    # Resolution only -- did the name resolve, and does iML1515 give it reactions.
    # Whether those reactions can carry an edge the benchmark counts is a question
    # about the BAKE, which this script does not open; `triage.py` answers it from
    # the built tier, where the atom universe is the one the tier actually used.
    cen = (pd.DataFrame(census)
           .groupby(["gene", "b_number"], as_index=False)
           .agg(n_reactions=("n_reactions", "max"),
                n_strains=("strains", "nunique"))
           .sort_values("gene"))
    args.census.parent.mkdir(parents=True, exist_ok=True)
    cen.to_csv(args.census, sep="\t", index=False)

    unresolved = cen[cen["b_number"] == ""]
    absent = cen[(cen["b_number"] != "") & (cen["n_reactions"] == 0)]
    print(f"ORFs: {len(cen)} distinct  {len(unresolved)} unresolved to a b-number  "
          f"{len(absent)} resolved but absent from iML1515  "
          f"{len(cen) - len(unresolved) - len(absent)} with reactions")
    if len(unresolved):
        print("  unresolved:", ", ".join(unresolved["gene"]))
    print(f"wrote {args.census}")

    # A reaction nominated twice by the SAME side of one condition would be a
    # doubling the uniform weighting cannot express, since it counts distinct
    # units and both clones of a two-clone strain share one. Grouped by role, so
    # that the complementation strain -- which deletes rfaY and adds it back on a
    # plasmid, and means exactly that -- is not counted as a collision.
    df = pd.DataFrame(rows)
    dupes = (df[df["mnxr"] != ""]
             .groupby(["obs_id", "role", "mnxr"]).size().rename("n").reset_index())
    dupes = dupes[dupes["n"] > 1]
    print(f"reactions nominated twice within one condition: {len(dupes)}"
          + (f"\n{dupes.to_string(index=False)}" if len(dupes) else ""))


if __name__ == "__main__":
    main()
