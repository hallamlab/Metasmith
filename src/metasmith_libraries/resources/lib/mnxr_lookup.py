import argparse
import os
import sys

import pandas as pd

_p = argparse.ArgumentParser()
_p.add_argument("--ev-lib", required=True)
_p.add_argument("--reac-prop", required=True)
_p.add_argument("--reac-xref", required=True)
_p.add_argument("--ko2reaction", required=True)
_p.add_argument("--rhea2uniprot", required=True)
_p.add_argument("--rhea2uniprot-trembl", required=True)
_p.add_argument("--out", required=True)
A = _p.parse_args()

sys.path.insert(0, os.path.dirname(A.ev_lib))
import fabfos_evidence as fe

COLUMNS = ["id", "id_source", "mnxr", "evidence_quality"]


def reac_xref_rows(prefixes):
    rows = []
    with open(A.reac_xref) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            src, mnxr = parts[0], parts[1]
            if not mnxr.startswith("MNXR") or mnxr == "EMPTY":
                continue
            if src.startswith(prefixes):
                rows.append((src.split(":", 1)[1], mnxr))
    return rows


def kegg_r_to_mnxr():
    return pd.DataFrame(reac_xref_rows(("kegg.reaction:", "keggR:")),
                        columns=["kegg_r", "mnxr"]).drop_duplicates()


# MNXref writes every MetaCyc crossref twice, as `metacyc.reaction:` and the legacy
# `metacycR:`; on 4.5 both carry the same 20,215 keys, so the union is the same set as
# either and does not bet on a release keeping them in step. A frame id is a curated
# crossreference, hence `reviewed`.
def route_metacyc():
    df = pd.DataFrame(reac_xref_rows(("metacyc.reaction:", "metacycR:")),
                      columns=["id", "mnxr"]).drop_duplicates()
    df["id_source"] = "metacyc"
    df["evidence_quality"] = "reviewed"
    return df[COLUMNS]


# reac_prop's classifs column is curated with no reviewed/unreviewed split; marking it
# `unreviewed` would let a TrEMBL row outrank a curated EC in the keep-first dedup.
# Partial ECs ("3.6.3.-") stay verbatim, as CLEAN can emit them too.
def route_ec():
    df = fe.load_ec_to_mnxr(A.reac_prop).rename(columns={"ec": "id"})
    df["id_source"] = "ec"
    df["evidence_quality"] = "reviewed"
    return df[COLUMNS]


# KO -> KEGG reaction -> MNXR, NOT KO -> EC -> MNXR: through the ko_list's EC tags the
# kofam lane goes from 646 to 8,548 reactions on the three benchmark hosts, 91% of them
# reactions the EC lane already reaches, and the lanes stop being independent evidence.
def route_ko():
    # The REST body is headerless and namespace-prefixed: `ko:K00001<TAB>rn:R00623`.
    ko_r = pd.read_csv(A.ko2reaction, sep="\t", dtype=str, header=None,
                       names=["ko", "kegg_r"]).dropna()
    ko_r["ko"] = ko_r["ko"].str.split(":").str[-1]
    ko_r["kegg_r"] = ko_r["kegg_r"].str.split(":").str[-1]
    if not ko_r["ko"].str.match(r"^K\d+$").all():
        bad = ko_r.loc[~ko_r["ko"].str.match(r"^K\d+$"), "ko"].head(5).tolist()
        raise SystemExit(f"[bridge] ko column does not look like KO ids: {bad}")

    r_mnxr = kegg_r_to_mnxr()
    df = (ko_r.merge(r_mnxr, on="kegg_r", how="inner")[["ko", "mnxr"]]
              .drop_duplicates().rename(columns={"ko": "id"}))
    n_unmapped = int((~ko_r["kegg_r"].isin(set(r_mnxr["kegg_r"]))).sum())
    print(f"[bridge] ko: {len(df):,} rows; reac_xref carries "
          f"{r_mnxr['kegg_r'].nunique():,} KEGG reactions, {n_unmapped:,} KO links "
          f"point at one it does not have", flush=True)
    df["id_source"] = "ko"
    df["evidence_quality"] = "reviewed"
    return df[COLUMNS]


# Swiss-Prot rows are `reviewed` and TrEMBL rows `unreviewed`. The label pool cuts on
# exactly that column, and the keep-first dedup below relies on it.
def route_uniprot(tmp):
    fe.build_uniprot_bridge(A.reac_xref, A.rhea2uniprot, A.rhea2uniprot_trembl, tmp)
    d = fe.read_parquet(tmp, columns=["uniprot_accession", "mnxr", "evidence_quality"])
    d = d.rename(columns={"uniprot_accession": "id"})
    d["id_source"] = "uniprot"
    return d[COLUMNS]


def main():
    frames = [route_ec(), route_ko(), route_metacyc(),
              route_uniprot("_uniprot_bridge.parquet")]
    for f, name in zip(frames, ("ec", "ko", "metacyc", "uniprot")):
        print(f"[bridge] {name:8s} {len(f):>12,} rows  {f['id'].nunique():>9,} ids  "
              f"{f['mnxr'].nunique():>7,} MNXR", flush=True)
    df = pd.concat(frames, ignore_index=True)
    n0 = len(df)

    # Consumers slice by `id` alone, so an id shared by two sources would mix
    # namespaces' claims into one lane. That needs a decision, not a dedup.
    per_id = df.groupby("id")["id_source"].nunique()
    clashes = per_id[per_id > 1]
    if len(clashes):
        raise SystemExit(
            f"{len(clashes):,} ids appear in more than one id_source "
            f"(e.g. {list(clashes.index[:5])}). `id` is no longer unambiguous.")

    # "reviewed" sorts before "unreviewed", so keep-first retains the stronger claim.
    df = (df.sort_values(["id", "mnxr", "evidence_quality"], kind="mergesort")
            .drop_duplicates(subset=["id", "mnxr"], keep="first")
            .reset_index(drop=True))
    fe.write_parquet(df, A.out)
    print(f"[bridge] {n0:,} -> {len(df):,} rows after dedup to distinct (id, mnxr)",
          flush=True)
    print(f"[bridge] evidence_quality {df['evidence_quality'].value_counts().to_dict()}",
          flush=True)
    print(f"[bridge] wrote {A.out} ({os.path.getsize(A.out)/1e6:.1f} MB)", flush=True)


main()
