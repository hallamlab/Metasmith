"""GPR table from an annotation table: the lanes of one lane set, mapped to MNXR and weighted.

Reads `annotation_table.py`'s output, keeps the lanes `--lane-set` names, applies each
lane's abstain, joins its terms to reactions through `ref::mnxr_lookup` and turns its own
score into nats through `ref::lane_calibration`. The schema, the lane sets, the cut-offs and
the calibration are `lib::fabfos_evidence.py`'s; this file only says how each lane's terms
reach a reaction.

Every lane abstains before the table exists: a threshold applied by a reader is a threshold
the published table did not have.
"""
import pandas as pd

from annotation_table import load_evidence, orf_ids


def _args():
    import argparse
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--annotation-table", nargs="+", required=True, help="lane tables")
    p.add_argument("--bridge", required=True)
    p.add_argument("--calibration", required=True)
    p.add_argument("--ev-lib", required=True)
    p.add_argument("--lane-set", required=True)
    p.add_argument("--orfs", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--source", required=True)
    return p.parse_args()


_FULL_EC = r"^\d+\.\d+\.\d+\.\d+$"


def bridge_slice(path, src, wanted):
    # The bridge is 30.5 million rows over three id spaces. Both cuts are pushed into the
    # parquet reader, so the read is bounded by this table's own ids, not by the reference.
    if len(wanted) == 0:
        return pd.DataFrame(columns=["term", "mnxr", "evidence_quality"])
    df = fe.read_parquet(
        path, columns=["id", "mnxr", "evidence_quality"],
        filters=[("id_source", "==", src), ("id", "in", sorted(set(wanted)))])
    return df.drop_duplicates().rename(columns={"id": "term"})


def bridged(calls, src, projection_via):
    # evidence_quality comes from the bridge, whose keep-first dedup already retained the
    # stronger of a Swiss-Prot and a TrEMBL claim per (id, mnxr).
    df = calls.merge(bridge_slice(A.bridge, src, calls["term"].unique()), on="term",
                     how="inner")
    df["intermediate_id"] = df["term"]
    df["intermediate_name"] = df["description"]
    df["projection_via"] = projection_via
    return df


def of(ann, namespace):
    return ann[ann["namespace"] == namespace].copy()


def lane_kofam(ann):
    d = of(ann, "ko")
    return bridged(d[d["score"].notna() & (d["score"] >= 1.0)], "ko", "kegg.reaction")


def lane_clean(ann):
    d = of(ann, "ec")
    keep = (d["score"].notna() & (d["score"] >= fe.CLEAN_MIN_SCORE)
            & d["term"].astype(str).str.match(_FULL_EC, na=False))
    print("[gpr] clean: " + str(len(d)) + " calls, "
          + str(int((d["score"].notna() & (d["score"] < fe.CLEAN_MIN_SCORE)).sum()))
          + " below the abstain at " + str(fe.CLEAN_MIN_SCORE), flush=True)
    return bridged(d[keep], "ec", "ec")


def lane_uniref50(ann):
    # The best hit per ORF. Within one query a higher bsr is a higher bitscore, since both
    # share that query's self-score, so the tiebreak is the bitscore's.
    d = of(ann, "uniref50")
    d = (d[d["score"].notna()]
         .sort_values(["orf", "evalue", "score"], ascending=[True, True, False])
         .drop_duplicates(subset=["orf"], keep="first"))
    d["term"] = d["term"].str.replace(r"^UniRef50_", "", regex=True)
    return bridged(d, "uniprot", "rhea")


def lane_pbert(ann):
    d = of(ann, "mnxr")
    d["mnxr"] = d["term"]
    # intermediate_id names the DONOR neighbour: label transfer IS the projection. The
    # landmarks are the bridge's `reviewed` cut by construction, so every label inherits it.
    d["intermediate_id"] = d["via"]
    d["intermediate_name"] = None
    d["projection_via"] = "embedding_knn"
    d["evidence_quality"] = "reviewed"
    return d


def lane_deepec(ann):
    d = of(ann, "ec")
    d = d[d["term"].astype(str).str.match(_FULL_EC, na=False)]
    # A score-less tool asserts PRESENCE, and 1.0 is how that is spelled. It carries no
    # information, so `fe.calibrate` gives the whole lane UNCALIBRATED_LLR.
    d["score"] = 1.0
    return bridged(d, "ec", "ec")


def lane_eggnog(ann):
    ko = of(ann, "ko")
    ec = of(ann, "ec")
    ec = ec[ec["term"].astype(str).str.match(_FULL_EC, na=False)]
    return pd.concat([bridged(ko, "ko", "kegg.reaction"), bridged(ec, "ec", "ec")],
                     ignore_index=True)


# channel -> how its annotation-table rows reach an MNXR. A channel here is a lane of
# the annotation table under the same name.
REACTION_LANES = {
    "kofam": lane_kofam,
    "clean": lane_clean,
    "uniref50": lane_uniref50,
    "pbert": lane_pbert,
    "deepec": lane_deepec,
    "eggnog": lane_eggnog,
}


def finish(df, channel):
    # `score` is the lane's own scale and is spent here, the last place it exists.
    df = df.copy()
    df["source"] = A.source
    df["channel"] = channel
    df["llr"] = fe.calibrate(CAL, channel, df["score"].to_numpy(dtype=float))
    df["llr_route"] = fe.llr_route_for(CAL, channel)
    df["lane_set"] = A.lane_set
    df["intermediate_name"] = df["intermediate_name"].fillna("")
    if "evidence_quality" not in df.columns:
        df["evidence_quality"] = "reviewed"
    # THE SYMBOL IS THE FASTA'S. A lane names an EC or a KO and never learns what the gene
    # is called, and `intermediate_name` must not stand in for it: it holds an EC
    # description, a different namespace from the one `gene` consolidates.
    df["gene"] = df["orf"].map(GENES).astype("object")
    df["gene"] = df["gene"].where(df["gene"].notna(), None)
    return df[fe.SCHEMA_COLS]


def main():
    global GENES
    ids, GENES = orf_ids(A.orfs)
    if not ids:
        raise SystemExit("[gpr] the input ORF FASTA has no records")
    lanes = fe.LANE_SETS.get(A.lane_set)
    if lanes is None:
        raise SystemExit("[gpr] unknown lane set " + repr(A.lane_set) + "; known: "
                         + repr(sorted(fe.LANE_SETS)))
    unmapped = [c for c in lanes if c not in REACTION_LANES]
    if unmapped:
        raise SystemExit("[gpr] lane set " + A.lane_set + " names " + repr(unmapped)
                         + ", which this mapper has no route to a reaction for")
    ann = fe.read_lane_tables(A.annotation_table)
    frames = [finish(REACTION_LANES[ch](ann[ann["lane"] == ch]), ch) for ch in lanes]
    gpr = pd.concat(frames, ignore_index=True)
    key = fe.GRAIN_KEY
    gpr = gpr.drop_duplicates(subset=key).sort_values(key, kind="mergesort").reset_index(drop=True)
    # Refuse before writing: an empty lane, an ORF-id mismatch and an unstaged reference
    # all used to produce a zero-row parquet and report success.
    fe.validate_gpr(gpr, A.lane_set, ids, A.source)
    fe.write_parquet(gpr, A.out)
    print("[gpr] wrote " + str(len(gpr)) + " rows -> " + A.out, flush=True)


if __name__ == "__main__":
    A = _args()
    fe = load_evidence(A.ev_lib)
    # `ref::lane_calibration` is a requirement rather than a default, because a table built
    # with no calibration is an UNWEIGHTED table and must not be mistaken for a weighted one.
    CAL = fe.load_calibration(A.calibration)
    if CAL is None:
        raise SystemExit(
            "[gpr] --calibration " + A.calibration + " holds no lane_calibration.json. "
            "Every lane would fall back to a flat " + str(fe.UNCALIBRATED_LLR) + " nats, "
            "a table that says every annotation call is worth the same")
    main()
