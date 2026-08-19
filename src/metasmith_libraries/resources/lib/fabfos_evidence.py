"""ECSPr Layer-2 evidence: MNXR bridges + 4-lane readers + evidence-table compiler.

Faithful port of the scadc `03_layer2_evidence/` pipeline into a single
path-parameterized CLI so a metasmith transform can call it (no hardcoded scadc
paths). Ported verbatim (method-preserving) from:
  - _lanes.py                     (bridge loaders + the 4 lane readers, 8-col schema)
  - 00_build_uniprot_to_mnxr.py   (UniProt->MNXR Rhea DR bridge)
  - 11_build_evidence_table_dlec.py (the dl_ec 4-channel compiler)

Every lane reader emits the unified 11-column schema declared below as SCHEMA_COLS.
The declarations there -- the channel vocabulary, the raw_score direction/range
contract, and `validate_gpr` -- are the GPR table's definition, and both mappers in
`transforms/fabfos/` read them from here rather than restating them.

The compiler folds the four fresh-annotation lanes into one evidence table (the
canonical `dl_ec` variant); ECSPr consumes it as functional_annotation::evidence_table.

SUBCOMMANDS
  build-uniprot-bridge  reac_xref + rhea2uniprot{,_trembl}  -> uniprot_to_mnxr.parquet
  build-ec-bridge       reac_prop.tsv                       -> ec_to_mnxr.tsv
  build-mnxr-lookup     the ko/ec/uniprot trio              -> mnxr_lookup.parquet
  compile               per-lane annotator outputs + bridges -> evidence_table.parquet

ko_to_mnxr is a reused reference table (staged input), not built here.

`build-mnxr-lookup` folds the trio into the single `ref::mnxr_lookup` the lanes
now read; the per-bridge loaders above stay for reading the trio directly.

GATES: the lane INPUTS are produced fresh upstream by the heavy annotator
transforms (kofamscan, EZpred/ESM-C dl_ec, DIAMOND-vs-UniRef50, embed-transfer).
This module only needs pandas/pyarrow + the MetaNetX/Rhea reference tables (present
on disk); it recomputes NOTHING that must be reused (the metaG null lives elsewhere).
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

SCHEMA_COLS = [
    "source", "orf", "channel", "mnxr",
    "intermediate_id", "intermediate_name",
    "raw_score", "score_kind", "projection_via",
    "evidence_quality", "lane_set",
]


SCHEMA_EXTENSIONS = {
    "attribution": ("build_id", "host", "unit_id"),
    "feature": ("feature_kind", "feature_name", "gpr_rule"),
    "universe": ("in_atom_universe",),
    "cohort": ("condition_id", "cohort", "action", "source_organism"),
}


GRAIN_KEY = ["source", "orf", "channel", "intermediate_id", "mnxr"]


def grain_key(extensions=()) -> list:
    key = list(GRAIN_KEY)
    if "cohort" in extensions:
        key.extend(("condition_id", "action"))
    return key


def extensions_of(df) -> tuple:
    cols = set(df.columns)
    found = []
    for name, block in SCHEMA_EXTENSIONS.items():
        have = [c for c in block if c in cols]
        if not have:
            continue
        if len(have) != len(block):
            raise SystemExit(f"[gpr] block {name!r} is half present: has {have}, "
                             f"missing {[c for c in block if c not in cols]}. A block "
                             f"is a unit; a table with part of one is not describable")
        found.append(name)
    return tuple(found)


def is_unified(df) -> bool:
    return list(df.columns) == schema_for(extensions_of(df))


def schema_for(extensions=()) -> list:
    cols = list(SCHEMA_COLS)
    for b in extensions:
        if b not in SCHEMA_EXTENSIONS:
            raise SystemExit(f"[gpr] unknown schema extension {b!r}; "
                             f"known: {sorted(SCHEMA_EXTENSIONS)}")
        cols.extend(SCHEMA_EXTENSIONS[b])
    return cols

LANE_SETS = {
    "chosen_4": ("kofam", "clean", "uniref50", "pbert"),
    "full_7": ("kofam", "clean", "deepec", "ezpred", "uniref50", "pbert", "esmc"),
}
CHANNELS = tuple(sorted(set(c for cs in LANE_SETS.values() for c in cs)))

ASSERTION_CHANNELS = {
    "gem_gpr": "presence",
    "manual_gpr": "presence",
    "curated_insertion": "presence",
    "bridge": "bridge",
}
CURATED = "curated"

SCORE_KINDS = {
    "hmm_bitscore": (0.0, None),
    "clean_maxsep_inv": (0.0, 1.0),
    "blast_bsr": (0.0, 4.0),
    "knn_vote": (0.0, 1.0),
    "softmax": (0.0, 1.0),
    "presence": (1.0, 1.0),
    "bridge": (1.0, 1.0),
}

CHANNEL_SCORE_KIND = {
    "kofam": "hmm_bitscore",
    "clean": "clean_maxsep_inv",
    "uniref50": "blast_bsr",
    "pbert": "knn_vote",
    "esmc": "knn_vote",
    "deepec": "presence",
    "ezpred": "softmax",
}

EVIDENCE_QUALITY = ("reviewed", "unreviewed", "unknown", "synthetic")

_MNXR_RE = re.compile(r"^MNXR\d+$")
_MNXR_COMPOSED_RE = re.compile(r"^(?:[A-Za-z0-9_.-]+:)?MNXR\d+$|^BRIDGE:[^\s]+$")

DL_EC_SCORE_FLOOR = 0.3
EMBED_SCORE_FLOOR = 0.2


def clean_distance_to_score(d):
    return 1.0 / (1.0 + d)


RETIRED_CHANNELS = {
    "clean_ec": "clean",
    "uniref50_dr": "uniref50",
    "dl_ec": "ezpred",
}

_LEGACY_CORE = {
    "feature_id": "orf",
    "evidence_id": "intermediate_id",
    "evidence_name": "intermediate_name",
}
_ORF_SOURCES = ("feature_id", "feature_name", "evidence_id")


def normalise_channel(ch: str) -> str:
    ch = str(ch)
    if ch.startswith("denovo_"):
        ch = ch[len("denovo_"):]
    return RETIRED_CHANNELS.get(ch, ch)


def to_unified(df, extensions=("attribution", "feature", "universe")):
    import pandas as pd

    out = df.rename(columns=_LEGACY_CORE).copy()

    orf = None
    for c in _ORF_SOURCES:
        col = _LEGACY_CORE.get(c, c)
        if col in out.columns:
            orf = out[col] if orf is None else orf.fillna(out[col])
    if orf is None or orf.isna().any():
        raise SystemExit(f"[gpr] {int(orf.isna().sum()) if orf is not None else 'every'} "
                         f"row(s) name no nominator in any of {_ORF_SOURCES}")
    out["orf"] = orf.astype(str)

    out["channel"] = out["channel"].map(normalise_channel)
    lanes = sorted(set(out["channel"]) - set(ASSERTION_CHANNELS))
    unknown = sorted(set(lanes) - set(CHANNELS))
    if unknown:
        raise SystemExit(f"[gpr] channel(s) {unknown} are neither a declared lane nor "
                         f"an assertion; the vocabulary is {list(CHANNELS)} plus "
                         f"{sorted(ASSERTION_CHANNELS)}")

    out["score_kind"] = out["channel"].map(
        lambda c: "presence" if c in ASSERTION_CHANNELS else CHANNEL_SCORE_KIND[c])
    assertion = out["channel"].isin(ASSERTION_CHANNELS)
    bad = out.loc[assertion & (out["raw_score"].astype(float) != 1.0)]
    if len(bad):
        raise SystemExit(f"[gpr] {len(bad):,} assertion rows carry a raw_score that is "
                         f"not 1.0, e.g. {sorted(set(bad['raw_score']))[:3]} -- an "
                         f"assertion is a presence claim, not a ranked one")

    if "unit_id" in out.columns and out["unit_id"].nunique() != 1:
        raise SystemExit(f"[gpr] {out['unit_id'].nunique()} unit_ids in one table "
                         f"{sorted(out['unit_id'].unique())[:4]}; `source` names one "
                         f"artifact, so this table is really several")
    out["source"] = out["unit_id"].astype(str) if "unit_id" in out.columns else ""
    out["lane_set"] = _lane_set_for(set(lanes))
    if "evidence_quality" not in out.columns:
        out["evidence_quality"] = "unknown"
    if "projection_via" not in out.columns:
        out["projection_via"] = ""
    out["projection_via"] = out["projection_via"].fillna("")
    out["intermediate_id"] = out["intermediate_id"].fillna("")
    out["intermediate_name"] = out["intermediate_name"].fillna("")

    want = schema_for(extensions)
    missing = [c for c in want if c not in out.columns]
    if missing:
        raise SystemExit(f"[gpr] the source table has no {missing} to carry into "
                         f"extension blocks {list(extensions)}")
    return out[want].reset_index(drop=True)


def read_gpr(paths, extensions=None):
    import pandas as pd

    if isinstance(paths, (str, bytes)) or hasattr(paths, "__fspath__"):
        paths = [paths]
    frames = []
    for path in paths:
        df = pd.read_parquet(path)
        ext = tuple(extensions) if extensions is not None else extensions_of(df)
        if not is_unified(df) or (extensions is not None
                                  and list(df.columns) != schema_for(ext)):
            df = to_unified(df, ext)
        frames.append(df)
    if len(frames) == 1:
        return frames[0]
    return pd.concat(frames, ignore_index=True)


def _lane_set_for(lanes: set) -> str:
    if not lanes:
        return CURATED
    for name, members in LANE_SETS.items():
        if lanes == set(members):
            return name
    raise SystemExit(
        f"[gpr] lanes {sorted(lanes)} are not a declared set. Known: "
        + "; ".join(f"{k}={list(v)}" for k, v in LANE_SETS.items())
        + ". A table carrying some of a set is the failure this schema exists to name.")


def validate_gpr(df, lane_set: str, orf_ids, source: str, extensions=(),
                 composed: bool = False):
    import numpy as np

    if lane_set == CURATED:
        expected = ()
    else:
        expected = LANE_SETS.get(lane_set)
        if expected is None:
            raise SystemExit(f"[gpr] unknown lane_set {lane_set!r}; known: "
                             f"{sorted(LANE_SETS) + [CURATED]}")

    want = schema_for(extensions)
    if list(df.columns) != want:
        raise SystemExit(
            f"[gpr] column set/order is not the schema for extensions {list(extensions)}.\n"
            f"      got:      {list(df.columns)}\n"
            f"      expected: {want}")

    if len(df) == 0:
        raise SystemExit(
            "[gpr] the table is empty. Every lane joined to nothing, which is a "
            "staging or id-space failure, not a biological finding -- 157 fosmid "
            "inserts do not encode zero recognisable enzymes")

    assertion = df["channel"].isin(ASSERTION_CHANNELS)
    core = [c for c in SCHEMA_COLS if c != "mnxr"]
    nulls = {c: int(df[c].isna().sum()) for c in core if df[c].isna().any()}
    n_bad_mnxr = int((df["mnxr"].isna() & ~assertion).sum())
    if n_bad_mnxr:
        nulls["mnxr (on lane rows)"] = n_bad_mnxr
    if nulls:
        raise SystemExit(f"[gpr] null values in {nulls} -- every core column is "
                         f"non-null by contract, except `mnxr` on an assertion row")

    allowed = set(expected) | set(ASSERTION_CHANNELS)

    unknown = set(df["channel"].unique()) - allowed
    if unknown:
        raise SystemExit(
            f"[gpr] channels not in lane_set {lane_set}: {sorted(unknown)}; "
            f"the vocabulary is {list(expected)} plus the assertion channels "
            f"{sorted(ASSERTION_CHANNELS)}")

    bad_assert = sorted(
        f"{c}:{k}" for c, k in
        df.loc[assertion, ["channel", "score_kind"]].drop_duplicates().itertuples(index=False)
        if k != ASSERTION_CHANNELS[c])
    if bad_assert:
        raise SystemExit(f"[gpr] assertion channel(s) carry the wrong score_kind "
                         f"{bad_assert}; the declared kinds are {ASSERTION_CHANNELS}")

    bad_kind = set(df["score_kind"].unique()) - set(SCORE_KINDS)
    if bad_kind:
        raise SystemExit(f"[gpr] unknown score_kind(s) {sorted(bad_kind)}; known: {sorted(SCORE_KINDS)}")
    for ch, kind in df.groupby("channel")["score_kind"].agg(lambda s: sorted(set(s))).items():
        want = ASSERTION_CHANNELS.get(ch) or CHANNEL_SCORE_KIND[ch]
        if kind != [want]:
            raise SystemExit(f"[gpr] channel {ch!r} carries score_kind {kind}, expected ['{want}']")
    s = df["raw_score"].astype(float)
    if not np.isfinite(s).all():
        n = int((~np.isfinite(s)).sum())
        raise SystemExit(
            f"[gpr] {n:,} raw_score values are NaN or infinite. The downstream "
            f"share-of-sum reads a NaN lane total as zero and silently falls back "
            f"to a uniform split, so this must never reach the table")
    tol = 1e-6
    for kind, grp in df.groupby("score_kind"):
        lo, hi = SCORE_KINDS[kind]
        v = grp["raw_score"].astype(float)
        if lo is not None and v.min() < lo - tol:
            raise SystemExit(f"[gpr] score_kind {kind!r}: min {v.min():.9g} below the declared floor {lo}")
        if hi is not None and v.max() > hi + tol:
            raise SystemExit(f"[gpr] score_kind {kind!r}: max {v.max():.9g} above the declared ceiling {hi}")

    named = df.loc[df["mnxr"].notna()]
    pattern = _MNXR_COMPOSED_RE if composed else _MNXR_RE
    bad_mnxr = named.loc[~named["mnxr"].astype(str).str.match(pattern), "mnxr"].unique()
    if len(bad_mnxr):
        raise SystemExit(f"[gpr] {len(bad_mnxr):,} non-MNXR reaction ids, e.g. {list(bad_mnxr[:5])}")
    bad_eq = set(df["evidence_quality"].unique()) - set(EVIDENCE_QUALITY)
    if bad_eq:
        raise SystemExit(f"[gpr] unknown evidence_quality {sorted(bad_eq)}; known: {list(EVIDENCE_QUALITY)}")
    want_ls = {lane_set} | ({"bridge"} if composed else set())
    if not set(df["lane_set"].unique()) <= want_ls or lane_set not in set(df["lane_set"]):
        raise SystemExit(f"[gpr] lane_set column carries {sorted(set(df['lane_set']))}, "
                         f"expected {sorted(want_ls)}")
    if set(df["source"].unique()) != {source}:
        raise SystemExit(f"[gpr] source column carries {sorted(set(df['source']))}, expected [{source!r}]")

    key = grain_key(extensions)
    dupes = int(df.duplicated(subset=key).sum())
    if dupes:
        raise SystemExit(f"[gpr] {dupes:,} rows duplicate the grain key {key}")

    ids = set(orf_ids) if orf_ids is not None else None
    stray = (set(df["orf"].unique()) - ids) if ids is not None else set()
    if stray:
        raise SystemExit(
            f"[gpr] {len(stray):,} ORF ids are not in the input FASTA, e.g. "
            f"{sorted(stray)[:5]}. The lanes annotate different ORF sets, or one "
            f"lane's tool rewrote the ids (CLEAN splits on whitespace, DIAMOND does "
            f"not) -- a join on gene id across lanes is meaningless until this is 0")

    for ch in expected:
        if int((df["channel"] == ch).sum()) == 0:
            raise SystemExit(
                f"[gpr] channel {ch!r} contributed 0 rows. A lane that produces "
                f"nothing is a broken join or an unstaged reference; writing the "
                f"table anyway hides which of the {len(expected)} lanes failed")

    n_ids = len(ids) if ids is not None else df["orf"].nunique()
    print(f"[gpr] {len(df):,} rows | source={source} | lane_set={lane_set} | "
          f"{df['orf'].nunique():,} of {n_ids:,} nominators | "
          f"{df['mnxr'].nunique():,} MNXR", flush=True)
    present_assertions = [c for c in sorted(ASSERTION_CHANNELS)
                          if (df["channel"] == c).any()]
    for ch in tuple(expected) + tuple(present_assertions):
        g = df[df["channel"] == ch]
        v = g["raw_score"].astype(float)
        print(f"[gpr]   {ch:9s} {len(g):>9,} rows  {g['orf'].nunique():>7,} ORFs "
              f"({100.0*g['orf'].nunique()/max(n_ids,1):5.1f}% cover)  "
              f"{g['mnxr'].nunique():>6,} MNXR  "
              f"score[{g['score_kind'].iat[0]}] min={v.min():.4g} "
              f"med={v.median():.4g} max={v.max():.4g}", flush=True)
    print(f"[gpr]   evidence_quality {df['evidence_quality'].value_counts().to_dict()}", flush=True)
    return df


def load_ko_to_mnxr(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t")
    df = df.rename(columns={"ko": "ko", "mnx_r": "mnxr"})
    df = df[["ko", "mnxr"]].drop_duplicates()
    return df


def load_ec_to_mnxr(path: Path) -> pd.DataFrame:
    rows = []
    with open(path) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 5:
                continue
            mnxr, classifs = parts[0], parts[3]
            if not mnxr.startswith("MNXR") or not classifs:
                continue
            for ec in classifs.split(";"):
                ec = ec.strip()
                if not ec:
                    continue
                rows.append((ec, mnxr))
    return pd.DataFrame(rows, columns=["ec", "mnxr"]).drop_duplicates()


def load_uniprot_to_mnxr(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


MNXR_LOOKUP_SOURCES = ("ko", "ec", "uniprot")


def load_mnxr_lookup(path: Path, id_source: str) -> pd.DataFrame:
    if id_source not in MNXR_LOOKUP_SOURCES:
        raise ValueError(f"unknown id_source {id_source!r}; expected one of {MNXR_LOOKUP_SOURCES}")
    df = pd.read_parquet(
        path, columns=["id", "mnxr"], filters=[("id_source", "==", id_source)],
    )
    if df.empty:
        raise ValueError(f"mnxr_lookup at {path} carries no {id_source!r} rows")
    key = {"uniprot": "uniprot_accession"}.get(id_source, id_source)
    return df.rename(columns={"id": key}).drop_duplicates()


_BOILERPLATE_RE = re.compile(r"\s+(n=\d+|Tax=.+?|RepID=\S+)(?=\s|$)")


def _clean_stitle(stitle: str) -> str:
    if not isinstance(stitle, str):
        return ""
    s = stitle
    if s.startswith("UniRef50_"):
        head, _, rest = s.partition(" ")
        s = rest
    return _BOILERPLATE_RE.sub("", s).strip()


def _finish(df: pd.DataFrame, source: str, channel: str, lane_set: str) -> pd.DataFrame:
    df["source"] = source
    df["channel"] = channel
    df["score_kind"] = CHANNEL_SCORE_KIND[channel]
    df["lane_set"] = lane_set
    if "evidence_quality" not in df.columns:
        df["evidence_quality"] = "reviewed"
    if "intermediate_name" not in df.columns:
        df["intermediate_name"] = ""
    df["intermediate_name"] = df["intermediate_name"].fillna("")
    return df[SCHEMA_COLS]


def read_kofam(path, source: str, ko_to_mnxr: pd.DataFrame,
               lane_set: str = "chosen_4") -> pd.DataFrame:
    if path is None or not Path(path).exists():
        return pd.DataFrame(columns=SCHEMA_COLS)
    df = pd.read_csv(path)
    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["hmm_threshold"] = pd.to_numeric(df["hmm_threshold"], errors="coerce")
    df = df[df["score"].notna() & df["hmm_threshold"].notna()]
    df = df[df["score"] >= df["hmm_threshold"]]
    if "fosmid" in df.columns:
        df["orf_id"] = df["fosmid"].astype(str) + "_" + df["orf"].astype(str)
    elif "contig" in df.columns:
        df["orf_id"] = df["contig"].astype(str) + "_" + df["orf"].astype(str)
    else:
        df["orf_id"] = df["orf"].astype(str)
    df = df[["orf_id", "ko", "score", "description"]].rename(
        columns={"orf_id": "orf", "score": "raw_score", "description": "intermediate_name"}
    )
    df = df.merge(ko_to_mnxr, on="ko", how="inner")
    df["intermediate_id"] = df["ko"]
    df["projection_via"] = "kegg.reaction"
    return _finish(df, source, "kofam", lane_set)


def read_dl_ec(path, source: str, ec_to_mnxr: pd.DataFrame,
               lane_set: str = "full_7") -> pd.DataFrame:
    if path is None or not Path(path).exists():
        return pd.DataFrame(columns=SCHEMA_COLS)
    df = pd.read_parquet(
        path,
        columns=["sequence_id", "ec_number", "score"],
        filters=[("head_kind", "==", "enzyme"), ("score", ">=", DL_EC_SCORE_FLOOR)],
    )
    df = df[df["ec_number"].astype(str).str.match(r"^\d+\.\d+\.\d+\.\d+$", na=False)]
    df = df.merge(ec_to_mnxr, left_on="ec_number", right_on="ec", how="inner")
    df["intermediate_id"] = df["ec_number"]
    df = df.rename(columns={"sequence_id": "orf", "score": "raw_score"})
    df["orf"] = df["orf"].str.replace(r"-(\d+)$", r"_\1", regex=True)
    df["projection_via"] = "ec"
    return _finish(df, source, "ezpred", lane_set)


_BLAST6_BSR_COLS = [
    "qseqid", "sseqid", "pident", "length", "mismatch", "gapopen",
    "qstart", "qend", "sstart", "send", "evalue", "bitscore", "stitle", "bsr",
]


def read_uniref50(path, source: str, uniprot_to_mnxr: pd.DataFrame,
                  lane_set: str = "chosen_4") -> pd.DataFrame:
    if path is None or not Path(path).exists():
        return pd.DataFrame(columns=SCHEMA_COLS)
    df = pd.read_csv(path, sep="\t", header=None, names=_BLAST6_BSR_COLS, dtype=str)
    df["evalue"] = pd.to_numeric(df["evalue"], errors="coerce")
    df["bitscore"] = pd.to_numeric(df["bitscore"], errors="coerce")
    df["bsr"] = pd.to_numeric(df["bsr"], errors="coerce")
    df = (df.sort_values(["qseqid", "evalue", "bitscore"], ascending=[True, True, False])
            .drop_duplicates(subset=["qseqid"], keep="first"))
    df["uniprot_accession"] = df["sseqid"].str.replace(r"^UniRef50_", "", regex=True)
    df["intermediate_name"] = df["stitle"].apply(_clean_stitle)
    df = df[["qseqid", "uniprot_accession", "intermediate_name", "bsr"]].rename(
        columns={"qseqid": "orf", "bsr": "raw_score"})
    df["orf"] = df["orf"].str.replace(r"-(\d+)$", r"_\1", regex=True)
    cols = ["uniprot_accession", "dr_source", "mnxr"]
    if "evidence_quality" in uniprot_to_mnxr.columns:
        cols.append("evidence_quality")
    joined = df.merge(uniprot_to_mnxr[cols], on="uniprot_accession", how="inner")
    joined["intermediate_id"] = joined["uniprot_accession"]
    joined["projection_via"] = joined["dr_source"]
    return _finish(joined, source, "uniref50", lane_set)


def read_embed_transfer(path, source: str, _bridge=None,
                        lane_set: str = "chosen_4") -> pd.DataFrame:
    if path is None or not Path(path).exists():
        return pd.DataFrame(columns=SCHEMA_COLS)
    df = pd.read_parquet(path)
    df = df[df["channel"].isin(("pbert", "pbert_transfer"))
            & (df["raw_score"] >= EMBED_SCORE_FLOOR)].copy()
    df["evidence_quality"] = "reviewed"
    return _finish(df, source, "pbert", lane_set)


def _load_rhea_to_mnxr(reac_xref: Path) -> pd.DataFrame:
    rows = []
    with open(reac_xref) as fh:
        for line in fh:
            if line.startswith("#") or line.startswith("EMPTY"):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            src, mnxr = parts[0], parts[1]
            if not src.startswith("rhea:") or not mnxr.startswith("MNXR"):
                continue
            rows.append((int(src[len("rhea:"):]), mnxr))
    return pd.DataFrame(rows, columns=["rhea_id", "mnxr"])


def _load_rhea2uniprot(path: Path, evidence_quality: str) -> pd.DataFrame:
    df = pd.read_csv(
        path, sep="\t",
        dtype={"RHEA_ID": "int64", "DIRECTION": "string", "MASTER_ID": "int64", "ID": "string"},
    )
    df = df.rename(columns={"RHEA_ID": "rhea_id", "DIRECTION": "direction", "ID": "uniprot_accession"})
    df = df[["rhea_id", "direction", "uniprot_accession"]]
    df["evidence_quality"] = evidence_quality
    return df


def build_uniprot_bridge(reac_xref: Path, rhea_swiss: Path, rhea_trembl: Path, out: Path):
    print("[bridge] rhea -> MNXR from reac_xref...", flush=True)
    rhea_mnxr = _load_rhea_to_mnxr(reac_xref)
    print(f"         {len(rhea_mnxr):,} rows ({rhea_mnxr['mnxr'].nunique():,} MNXRs)", flush=True)
    frames = [_load_rhea2uniprot(rhea_swiss, "reviewed")]
    if rhea_trembl is not None and Path(rhea_trembl).exists():
        print("[bridge] rhea2uniprot TrEMBL (big)...", flush=True)
        frames.append(_load_rhea2uniprot(rhea_trembl, "unreviewed"))
    all_u = pd.concat(frames, ignore_index=True)
    joined = all_u.merge(rhea_mnxr, on="rhea_id", how="inner")
    out_df = pd.DataFrame({
        "uniprot_accession": joined["uniprot_accession"],
        "protein_name": "", "gene_name": "",
        "dr_source": "rhea",
        "external_id": "rhea:" + joined["rhea_id"].astype(str),
        "mnxr": joined["mnxr"],
        "evidence_quality": joined["evidence_quality"],
        "direction": joined["direction"],
    })
    out_df = (out_df.sort_values(["uniprot_accession", "mnxr", "evidence_quality"])
                    .drop_duplicates(subset=["uniprot_accession", "external_id", "mnxr"], keep="first"))
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out, index=False)
    print(f"[bridge] wrote {len(out_df):,} rows ({out_df['uniprot_accession'].nunique():,} UniProts) -> {out}", flush=True)


def build_mnxr_lookup(ko: Path, ec: Path, uniprot: Path, out: Path):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    frames = []
    for path, source, col in ((ko, "ko", "ko"), (ec, "ec", "ec")):
        df = pd.read_csv(path, sep="\t")[[col, "mnxr"]].drop_duplicates()
        df.columns = ["id", "mnxr"]
        df["id_source"] = source
        df["evidence_quality"] = ""
        frames.append(df[["id", "id_source", "mnxr", "evidence_quality"]])
        print(f"[lookup] {source}: {len(df):,} distinct pairs", flush=True)

    up = pq.read_table(uniprot, columns=["uniprot_accession", "mnxr", "evidence_quality", "dr_source"])
    print(f"[lookup] uniprot: {up.num_rows:,} rows in", flush=True)
    seen = pc.unique(up.column("dr_source")).to_pylist()
    if seen != ["rhea"]:
        raise ValueError(f"dr_source is not constant 'rhea' ({seen}); it cannot be dropped")
    up = up.drop_columns(["dr_source"])
    up = up.group_by(["uniprot_accession", "mnxr"]).aggregate([("evidence_quality", "min")])
    up = pa.table({
        "id": up.column("uniprot_accession"),
        "id_source": pa.array(["uniprot"] * up.num_rows, pa.string()),
        "mnxr": up.column("mnxr"),
        "evidence_quality": up.column("evidence_quality_min"),
    })
    print(f"[lookup] uniprot: {up.num_rows:,} distinct pairs", flush=True)

    table = pa.concat_tables([pa.Table.from_pandas(f, preserve_index=False) for f in frames] + [up])
    table = table.sort_by([("id_source", "ascending"), ("id", "ascending")])
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out, compression="zstd")
    print(f"[lookup] wrote {table.num_rows:,} rows -> {out}", flush=True)
    return table.num_rows


def compile_evidence(source, kofam, dl_ec, uniref50, embed,
                     ko_to_mnxr_path, ec_to_mnxr_path, uniprot_to_mnxr_path, out):
    print("[compile] loading bridges...", flush=True)
    ko_to_mnxr = load_ko_to_mnxr(ko_to_mnxr_path) if ko_to_mnxr_path else pd.DataFrame(columns=["ko", "mnxr"])
    if ec_to_mnxr_path:
        ec_to_mnxr = pd.read_csv(ec_to_mnxr_path, sep="\t")[["ec", "mnxr"]].drop_duplicates()
    else:
        ec_to_mnxr = pd.DataFrame(columns=["ec", "mnxr"])
    if uniprot_to_mnxr_path and Path(uniprot_to_mnxr_path).exists():
        uniprot_to_mnxr = load_uniprot_to_mnxr(uniprot_to_mnxr_path)
    else:
        print("[compile] uniprot_to_mnxr absent -> uniref50 lane skipped", flush=True)
        uniprot_to_mnxr = pd.DataFrame(columns=["uniprot_accession", "dr_source", "mnxr"])

    lanes = [
        ("kofam", read_kofam, kofam, ko_to_mnxr),
        ("ezpred", read_dl_ec, dl_ec, ec_to_mnxr),
        ("uniref50", read_uniref50, uniref50, uniprot_to_mnxr),
        ("pbert", read_embed_transfer, embed, None),
    ]
    frames = []
    for name, reader, path, bridge in lanes:
        if not path:
            continue
        f = reader(path, source, bridge)
        print(f"[compile] {name}: {len(f):,} rows ({f['orf'].nunique():,} ORFs, {f['mnxr'].nunique():,} MNXRs)", flush=True)
        frames.append(f)
    if not frames:
        raise SystemExit("no lane inputs provided; nothing to compile")
    out_df = pd.concat(frames, ignore_index=True)
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out, index=False)
    print(f"[compile] wrote {len(out_df):,} rows -> {out}", flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build-uniprot-bridge", help="UniProt->MNXR via Rhea DR")
    b.add_argument("--reac-xref", type=Path, required=True)
    b.add_argument("--rhea-swiss", type=Path, required=True)
    b.add_argument("--rhea-trembl", type=Path, default=None)
    b.add_argument("--out", type=Path, required=True)

    e = sub.add_parser("build-ec-bridge", help="EC(level-4)->MNXR from reac_prop classifs")
    e.add_argument("--reac-prop", type=Path, required=True)
    e.add_argument("--out", type=Path, required=True)

    m = sub.add_parser("build-mnxr-lookup", help="ko+ec+uniprot bridges -> one mnxr_lookup")
    m.add_argument("--ko-to-mnxr", type=Path, required=True)
    m.add_argument("--ec-to-mnxr", type=Path, required=True)
    m.add_argument("--uniprot-to-mnxr", type=Path, required=True)
    m.add_argument("--out", type=Path, required=True)

    c = sub.add_parser("compile", help="fold lanes -> evidence_table.parquet")
    c.add_argument("--source", default="fosmid")
    c.add_argument("--kofam", type=Path, default=None)
    c.add_argument("--dl-ec", type=Path, default=None)
    c.add_argument("--uniref50", type=Path, default=None)
    c.add_argument("--embed", type=Path, default=None)
    c.add_argument("--ko-to-mnxr", type=Path, default=None)
    c.add_argument("--ec-to-mnxr", type=Path, default=None)
    c.add_argument("--uniprot-to-mnxr", type=Path, default=None)
    c.add_argument("--out", type=Path, required=True)

    a = ap.parse_args()
    if a.cmd == "build-uniprot-bridge":
        build_uniprot_bridge(a.reac_xref, a.rhea_swiss, a.rhea_trembl, a.out)
    elif a.cmd == "build-ec-bridge":
        df = load_ec_to_mnxr(a.reac_prop)
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(a.out, sep="\t", index=False)
        print(f"[bridge] wrote {len(df):,} ec->mnxr rows ({df['ec'].nunique():,} ECs) -> {a.out}", flush=True)
    elif a.cmd == "build-mnxr-lookup":
        build_mnxr_lookup(a.ko_to_mnxr, a.ec_to_mnxr, a.uniprot_to_mnxr, a.out)
    elif a.cmd == "compile":
        compile_evidence(a.source, a.kofam, a.dl_ec, a.uniref50, a.embed,
                         a.ko_to_mnxr, a.ec_to_mnxr, a.uniprot_to_mnxr, a.out)


if __name__ == "__main__":
    main()
