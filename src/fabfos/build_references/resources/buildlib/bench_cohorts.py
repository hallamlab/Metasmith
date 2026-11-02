from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd

COLUMNS = ("cohort", "condition_id", "gene_label", "uniprot", "action",
           "host_hint", "source_organism")

EXPECTED = {
    "laser":    ("extraction.tsv",
                 "LASER engineering records, extracted to one row per observation with a "
                 "genes_json column"),
    "keio":     ("extraction.tsv",
                 "Keio single-gene knockouts (Baba 2006), extracted to one row per "
                 "knockout with its b-number"),
    "eydallin": ("extraction.tsv",
                 "Eydallin 2010 screen hits, extracted from the PDF supplement"),
    "het":      ("heterologous_uniprot.tsv",
                 "LASER's heterologous gene labels resolved to UniProt, one row per "
                 "(label, source organism); produced by curate_het_screen.py"),
}

_NULLISH = {"", "none", "n/a", "na", "null", "not specified", "-"}


def nullish(s: str) -> bool:
    return (s or "").strip().lower() in _NULLISH


def is_native(source: str) -> bool:
    return nullish(source) or "coli" in (source or "").lower()


def _find(product: Path, key: str) -> Path:
    filename, what = EXPECTED[key]
    product = Path(product)
    hits = [product / filename] + sorted(product.rglob(filename))
    for h in hits:
        if h.exists():
            return h
    raise SystemExit(
        f"cohort source missing: {filename} is not under {product}.\n"
        f"  It is {what}.\n"
        f"  This is a NAMED refusal. Building without it would ship a conditions table "
        f"that looks complete and is missing an arm.")


def load_gof(laser: Path) -> pd.DataFrame:
    rows = []
    with open(_find(laser, "laser")) as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            gj = (row.get("genes_json") or "").strip()
            if not gj:
                continue
            for g in json.loads(gj):
                gene = (g.get("gene") or "").strip()
                src = (g.get("source") or "").strip()
                if nullish(gene):
                    continue
                native = is_native(src)
                rows.append(dict(
                    cohort="gof_native" if native else "gof_het",
                    condition_id=row.get("obs_id") or "",
                    gene_label=gene,
                    uniprot=None,
                    action=",".join(sorted(g.get("actions") or [])),
                    host_hint=row.get("host", ""),
                    source_organism="" if native else src,
                ))
    return pd.DataFrame(rows, columns=list(COLUMNS)).drop_duplicates()


def load_lof(keio: Path) -> pd.DataFrame:
    rows = []
    with open(_find(keio, "keio")) as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            gene = (row.get("gene_set") or "").strip()
            gene = gene.split(":")[0].strip()
            if not gene:
                continue
            rows.append(dict(
                cohort="lof",
                condition_id=row.get("obs_id") or f"KEIO:{gene}",
                gene_label=gene,
                uniprot=None,
                action="del",
                host_hint=row.get("host", ""),
                source_organism="",
            ))
    return pd.DataFrame(rows, columns=list(COLUMNS)).drop_duplicates()


def load_eydallin(eydallin: Path) -> pd.DataFrame:
    rows = []
    with open(_find(eydallin, "eydallin")) as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            gene = (r.get("gene_norm") or r.get("gene") or "").strip()
            if not gene:
                continue
            rows.append(dict(
                cohort="eydallin",
                condition_id=f"EYDALLIN:{gene}",
                gene_label=gene,
                uniprot=None,
                action=(r.get("phenotype") or "").strip(),
                host_hint="",
                source_organism="",
            ))
    return pd.DataFrame(rows, columns=list(COLUMNS)).drop_duplicates()


def load_het(het: Path) -> pd.DataFrame:
    rows = []
    with open(_find(het, "het")) as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            acc = (r.get("uniprot") or "").strip()
            gene = (r.get("gene") or "").strip()
            if not acc:
                acc = None
            rows.append(dict(
                cohort="gof_het",
                condition_id=(r.get("obs_id") or f"HET:{gene or acc}"),
                gene_label=gene or (acc or ""),
                uniprot=acc,
                action="add",
                host_hint="",
                source_organism=(r.get("source") or "").strip(),
            ))
    return pd.DataFrame(rows, columns=list(COLUMNS)).drop_duplicates()


def load_all_cohorts(*, laser, keio, eydallin, het) -> pd.DataFrame:
    gof = load_gof(Path(laser))
    lof = load_lof(Path(keio))
    eyd = load_eydallin(Path(eydallin))
    hets = load_het(Path(het))

    key = hets.dropna(subset=["uniprot"]).drop_duplicates(["gene_label", "source_organism"])
    lut = dict(zip(zip(key["gene_label"], key["source_organism"]), key["uniprot"]))
    mask = gof["cohort"] == "gof_het"
    gof.loc[mask, "uniprot"] = [
        lut.get((g, s)) for g, s in zip(gof.loc[mask, "gene_label"],
                                        gof.loc[mask, "source_organism"])
    ]

    known = set(zip(gof.loc[mask, "gene_label"], gof.loc[mask, "source_organism"]))
    extra = hets[[(g, s) not in known
                  for g, s in zip(hets["gene_label"], hets["source_organism"])]]
    out = pd.concat([gof, lof, eyd, extra], ignore_index=True)
    return out.drop_duplicates(subset=["cohort", "condition_id", "gene_label",
                                       "uniprot"]).reset_index(drop=True)
