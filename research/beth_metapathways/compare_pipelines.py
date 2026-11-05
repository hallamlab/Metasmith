"""Compare the original MetaPathways run (mp) with the port (port), per assembly.

  compare_pipelines.py assembly --sample S   one assembly into <out>/per_assembly/S.*.tsv
  compare_pipelines.py combine               every per-assembly result into the summary tables and comparison.md

An assembly is ready when its tables folder holds summary.tsv. Adding an assembly is one
`assembly` run plus a `combine`.
"""
import argparse
import html
import json
import re
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

B = Path("/scratch/phyberos/beth_mp")
MNXR_LOOKUP = Path("/scratch/phyberos/fabfos_refs/processed/mnxr_lookup/mnxr_lookup.parquet")
CLEAN_PGDB_FLOOR = 0.02
EZPRED_FLOOR = 0.5
MP_MIN_AA = 60
MP_LANES = ["swissprot", "uniref50", "uniref90", "cazy", "eggnog", "metacyc"]
PORT_HOMOLOGY = ["diamond_uniref50", "kofam", "eggnog_mapper", "interproscan"]
UNINFORMATIVE = re.compile(r"hypothetical|uncharacteri[sz]ed|unknown function|unannotated|^\s*<|^\s*$", re.I)
EC4 = re.compile(r"^\d+\.\d+\.\d+\.n?\d+$")

EC_LANES = ["clean", "deepec", "ezpred", "kofam", "proteinbert", "esm_c", "gpr7", "gpr7_2plus", "port_pgdb"]
RXN_LANES = ["gpr7", "gpr7_2plus", "proteinbert", "esm_c", "kofam", "clean", "port_pgdb"]
CAUSE = {
    "clean": "method: ML EC predictor (CLEAN, score >= 0.02, the new pipeline's Pathway Tools input) vs homology + name matching",
    "deepec": "method: ML EC predictor (DeepEC) vs homology + name matching",
    "ezpred": "method: ML EC predictor (EZpred, full EC, p >= 0.5) vs homology + name matching",
    "kofam": "method and reference: KEGG HMM profiles (KofamScan) vs FAST homology to Swiss-Prot/UniRef/MetaCyc",
    "proteinbert": "method: protein language model transfer from Swiss-Prot 2026_02 vs FAST homology to 2025_04 references",
    "esm_c": "method: protein language model transfer from Swiss-Prot 2026_02 vs FAST homology to 2025_04 references",
    "gpr7": "method: 7-method consensus (top reaction) vs homology + name matching",
    "gpr7_2plus": "method: 7-method consensus, top reaction backed by 2 or more methods, vs homology + name matching",
    "port_pgdb": "PGDB inputs: new pipeline gives Pathway Tools CLEAN EC numbers only; original gives product names + EC from homology",
}


def ecs(text):
    out = set()
    for tok in re.split(r"[,;\s|]+", str(text or "")):
        tok = re.sub(r"^(EC[:-])", "", tok.strip(), flags=re.I)
        if EC4.match(tok):
            out.add(tok)
    return out


def mp_orf(orf):
    m = re.search(r"C(\d+)-G(\d+)$", str(orf)) or re.search(r"(\d+)_(\d+)$", str(orf))
    return f"C{m.group(1)}-G{m.group(2)}" if m else str(orf)


def ec3(s):
    return {e.rsplit(".", 1)[0] for e in s}


def read_long(path, lanes, columns=("feature_id", "rank", "id", "desc", "conf"), rank1=False):
    filt = [("lane", "in", lanes)] + ([("rank", "=", 1)] if rank1 else [])
    return pq.read_table(path, columns=list(columns) + ["lane"], filters=filt).to_pandas()


class Lookup:
    def __init__(self, path):
        t = pq.read_table(path, columns=["id", "id_source", "mnxr"], filters=[("id_source", "in", ["metacyc", "ec", "ko"])]).to_pandas()
        self.metacyc = dict(zip(t.loc[t.id_source == "metacyc", "id"], t.loc[t.id_source == "metacyc", "mnxr"]))
        self.ko = t[t.id_source == "ko"].groupby("id")["mnxr"].agg(set).to_dict()
        ec = t[t.id_source == "ec"]
        self.ec_to = ec.groupby("id")["mnxr"].agg(set).to_dict()
        self.to_ec = ec.groupby("mnxr")["id"].agg(lambda s: {e for e in s if EC4.match(e)}).to_dict()

    def rxn(self, metacyc_ids):
        return {self.metacyc.get(r, "metacyc:" + r) for r in metacyc_ids if r}

    def mnxr_ec(self, mnxrs):
        return set().union(*(self.to_ec.get(m, set()) for m in mnxrs)) if mnxrs else set()


def classify(a, b, partial):
    if not a and not b:
        return "neither"
    if not b:
        return "mp_only"
    if not a:
        return "port_only"
    if a & b:
        return "agree"
    if partial and ec3(a) & ec3(b):
        return "partial"
    return "disagree"


def mp_dir(sample):
    d = B / "mp4/runs" / sample / sample
    return d if d.is_dir() else B / "mp4/out" / sample


def pgdb(table, pipeline):
    p = table / f"pgdb_{pipeline}" / "pgdb_genes.parquet"
    m = table / f"pgdb_{pipeline}" / "pgdb_manifest.tsv"
    genes = pq.read_table(p, columns=["entity", "entity_type", "orf_id", "reaction", "ec", "pathway", "pathway_name"]).to_pandas() if p.exists() else None
    man = pd.read_csv(m, sep="\t", dtype=str, keep_default_na=False) if m.exists() else None
    return genes, man


def per_assembly(sample, tables_root, out, lookup_path):
    T = tables_root / sample
    lk = Lookup(lookup_path)
    wide_path, long_path = T / "annotations_wide.parquet", T / "annotations_long.parquet"
    names = set(pq.ParquetFile(wide_path).schema_arrow.names)
    want = ["feature_id", "feature_type", "length", "bin", "pipelines", "coord_status", "mp_id", "port_id"]
    want += [c for lane in MP_LANES + PORT_HOMOLOGY + ["mp_product", "amrfinderplus", "deeptfactor"]
             for c in (f"{lane}_id", f"{lane}_desc") if c in names]
    w = pq.read_table(wide_path, columns=want, filters=[("feature_type", "=", "CDS")]).to_pandas()
    col = lambda c: w[c].fillna("").astype(str) if c in w else pd.Series("", index=w.index)
    is_mp, is_port = w.pipelines.str.contains("mp"), w.pipelines.str.contains("port")
    mpw, pw = w[is_mp], w[is_port]
    matched = w[w.coord_status.eq("match") & is_mp & is_port]
    m = {"sample": sample}

    st = lambda frame: frame.coord_status.value_counts()
    smp, sport = st(mpw), st(pw[~pw.index.isin(matched.index)])
    m.update(cds_mp=len(mpw), cds_port=len(pw), cds_exact=len(matched),
             cds_same_stop_mp=int(smp.get("same_stop_diff_start", 0)), cds_same_stop_port=int(sport.get("same_stop_diff_start", 0)),
             cds_overlap_mp=int(smp.get("overlap", 0)), cds_overlap_port=int(sport.get("overlap", 0)),
             cds_unique_mp=int(smp.get("unique", 0)), cds_unique_port=int(sport.get("unique", 0)))
    m["pct_exact_of_mp"] = round(100 * len(matched) / max(len(mpw), 1), 2)
    m["pct_exact_of_port"] = round(100 * len(matched) / max(len(pw), 1), 2)
    mm = T / "cds_coordinate_mismatches.tsv"
    if mm.exists():
        d = pd.read_csv(mm, sep="\t", usecols=["status", "pipeline", "start_delta", "end_delta"])
        ss = d[d.status == "same_stop_diff_start"]
        m["same_stop_median_start_shift_bp"] = float(ss.start_delta.abs().median()) if len(ss) else 0.0
    aa = mpw.length.astype(int) // 3 - 1
    m["mp_cds_below_60aa"] = int((aa < MP_MIN_AA).sum())
    m["mp_cds_below_60aa_unique_mp"] = int(((aa < MP_MIN_AA) & mpw.coord_status.ne("match")).sum())

    hit = lambda frame, lanes: pd.concat([col(f"{l}_id").loc[frame.index] != "" for l in lanes], axis=1).any(axis=1) if lanes else pd.Series(False, index=frame.index)
    named = lambda c: ~col(c).str.replace(r"\s+n=\d+\s+Tax=.*$", "", regex=True).str.contains(UNINFORMATIVE)
    m["mp_any_hit"] = int(hit(mpw, MP_LANES).sum())
    m["mp_named_function"] = int((named("mp_product_desc") & (col("mp_product_id") != "")).loc[mpw.index].sum())
    m["port_any_homology"] = int(hit(pw, PORT_HOMOLOGY).sum())
    m["port_named_function"] = int(((named("diamond_uniref50_desc") & (col("diamond_uniref50_id") != "")) | (col("kofam_id") != "")
                                    | (named("eggnog_mapper_desc") & (col("eggnog_mapper_id") != ""))).loc[pw.index].sum())
    m["port_interproscan"] = int((col("interproscan_id") != "").loc[pw.index].sum())

    u_mp, u_port = col("uniref50_id").loc[matched.index], col("diamond_uniref50_id").loc[matched.index]
    strip = lambda s: s.str.replace(r"\s+n=\d+\s+Tax=.*$", "", regex=True).str.strip().str.lower()
    n_mp, n_port = strip(col("uniref50_desc").loc[matched.index]), strip(col("diamond_uniref50_desc").loc[matched.index])
    both = (u_mp != "") & (u_port != "")
    m.update(uniref50_matched_both=int(both.sum()), uniref50_matched_mp_only=int(((u_mp != "") & (u_port == "")).sum()),
             uniref50_matched_port_only=int(((u_mp == "") & (u_port != "")).sum()),
             uniref50_same_cluster=int((both & (u_mp == u_port)).sum()), uniref50_same_name=int((both & (n_mp == n_port)).sum()))

    mp2feat = dict(zip(matched.mp_id, matched.feature_id))
    port2feat = dict(zip(matched.port_id, matched.feature_id))

    mp_ec, mp_rx = {}, {}
    pt = mp_dir(sample) / "results/annotation_table" / f"{sample}.ptinput.tsv"
    if pt.exists():
        p = pd.read_csv(pt, sep="\t", dtype=str, keep_default_na=False, usecols=["orf_id", "ec", "rxn"])
        for orf, e, r in zip(p.orf_id.map(mp_orf), p.ec, p.rxn):
            mp_ec.setdefault(orf, set()).update(ecs(e))
            mp_rx.setdefault(orf, set()).update(lk.rxn(r.split("|")))
    m["mp_ptinput"] = int(pt.exists())
    gm, man_mp = pgdb(T, "mp")
    gp, man_port = pgdb(T, "port")
    if gm is not None:
        g = gm[gm.reaction != ""]
        for orf, r, e in zip(g.orf_id.map(mp_orf), g.reaction, g.ec):
            mp_ec.setdefault(orf, set()).update(ecs(e))
            mp_rx.setdefault(orf, set()).add(lk.metacyc.get(r, "metacyc:" + r))
    m["mp_with_ec"] = sum(1 for o in mpw.mp_id if mp_ec.get(o))
    m["mp_with_reaction"] = sum(1 for o in mpw.mp_id if mp_rx.get(o))

    port_ec, port_rx = {k: {} for k in EC_LANES}, {k: {} for k in RXN_LANES}
    put = lambda d, k, v: d.setdefault(k, set()).update(v)
    for f, i, c in read_long(long_path, ["clean"]).pipe(lambda d: d[d.conf >= CLEAN_PGDB_FLOOR])[["feature_id", "id", "conf"]].itertuples(index=False):
        put(port_ec["clean"], f, ecs(i))
        put(port_rx["clean"], f, set().union(*(lk.ec_to.get(e, set()) for e in ecs(i))))
    for f, i in read_long(long_path, ["deepec"])[["feature_id", "id"]].itertuples(index=False):
        put(port_ec["deepec"], f, ecs(i))
    ez = read_long(long_path, ["ezpred"])
    ez = ez[(ez["desc"] == "enzyme") & (ez.conf >= EZPRED_FLOOR)]
    for f, i in ez[["feature_id", "id"]].itertuples(index=False):
        put(port_ec["ezpred"], f, ecs(i))
    ko = read_long(long_path, ["kofam"], columns=("feature_id", "id", "desc", "extras"))
    ko = ko[ko.extras.map(lambda s: json.loads(s or "{}").get("best") == "*")]
    for f, i, d in ko[["feature_id", "id", "desc"]].itertuples(index=False):
        mm_ = re.search(r"\[EC:([^\]]+)\]", d or "")
        put(port_ec["kofam"], f, ecs(mm_.group(1)) if mm_ else set())
        put(port_rx["kofam"], f, lk.ko.get(i, set()))
    for lane in ("proteinbert", "esm_c"):
        t = read_long(long_path, [lane], columns=("feature_id", "rank", "id", "desc"))
        top = t[t["rank"] == 1].set_index("feature_id")["desc"]
        t = t[t["desc"].values == top.reindex(t.feature_id).values]
        for f, g in t.groupby("feature_id")["id"]:
            put(port_rx[lane], f, set(g))
            put(port_ec[lane], f, lk.mnxr_ec(set(g)))
    t = read_long(long_path, ["gpr7"], columns=("feature_id", "id", "extras"), rank1=True)
    t["n"] = t.extras.map(lambda s: json.loads(s or "{}").get("n", 1))
    for lane, sub in (("gpr7", t), ("gpr7_2plus", t[t.n >= 2])):
        for f, g in sub.groupby("feature_id")["id"]:
            put(port_rx[lane], f, set(g))
            put(port_ec[lane], f, lk.mnxr_ec(set(g)))
    if gp is not None:
        g = gp[(gp.reaction != "") & gp.orf_id.isin(port2feat)]
        for orf, r, e in zip(g.orf_id, g.reaction, g.ec):
            f = port2feat[orf]
            put(port_ec["port_pgdb"], f, ecs(e))
            put(port_rx["port_pgdb"], f, {lk.metacyc.get(r, "metacyc:" + r)})
    port_feats = set(pw.feature_id)
    for lane in EC_LANES:
        m[f"port_with_ec_{lane}"] = sum(1 for f in port_ec[lane] if f in port_feats and port_ec[lane][f])
    if gp is not None:
        m["port_with_ec_port_pgdb"] = int(gp.loc[gp.reaction != "", "orf_id"].nunique())

    rows = []
    for level, lanes, mp_sets, port_sets in (("ec", EC_LANES, mp_ec, port_ec), ("reaction", RXN_LANES, mp_rx, port_rx)):
        for lane in lanes:
            ps = port_sets[lane]
            counts = {"agree": 0, "partial": 0, "disagree": 0, "mp_only": 0, "port_only": 0, "neither": 0}
            for orf, f in mp2feat.items():
                counts[classify(mp_sets.get(orf, set()), ps.get(f, set()), level == "ec")] += 1
            both_n = counts["agree"] + counts["partial"] + counts["disagree"]
            rows.append(dict(sample=sample, level=level, port_lane=lane, matched_cds=len(mp2feat), **counts,
                             pct_agree_when_both=round(100 * counts["agree"] / both_n, 1) if both_n else None,
                             likely_cause=CAUSE[lane]))
    ec_tab = pd.DataFrame(rows)

    prow = []
    if gm is not None or gp is not None:
        ents = sorted(set(man_mp.entity if man_mp is not None else []) | set(man_port.entity if man_port is not None else []),
                      key=lambda e: (e != "community", e))
        for e in ents:
            def sets(g, man):
                if g is None:
                    return set(), set(), "MISSING", "", {}
                s = g[g.entity == e]
                mrow = man[man.entity == e] if man is not None else pd.DataFrame()
                status = mrow.status.iloc[0] if len(mrow) else "MISSING"
                npw = mrow.pathways.iloc[0] if len(mrow) else ""
                pw_names = dict(zip(s.pathway, s.pathway_name))
                return set(s.pathway[s.pathway != ""]), set(s.reaction[s.reaction != ""]), status, npw, pw_names
            a_pw, a_rx, a_st, a_n, a_names = sets(gm, man_mp)
            b_pw, b_rx, b_st, b_n, b_names = sets(gp, man_port)
            jac = lambda a, b: round(len(a & b) / len(a | b), 3) if a | b else None
            names = {**b_names, **a_names}
            plain = lambda x: html.unescape(re.sub(r"<[^>]+>", "", names.get(x) or x))
            ex = lambda s: "; ".join(sorted(plain(x) for x in s)[:5])
            prow.append(dict(sample=sample, entity=e, entity_type="community" if e == "community" else "mag",
                             mp_status=a_st, port_status=b_st, mp_pathways_in_pgdb=a_n, port_pathways_in_pgdb=b_n,
                             mp_pathways=len(a_pw), port_pathways=len(b_pw), shared_pathways=len(a_pw & b_pw),
                             mp_only_pathways=len(a_pw - b_pw), port_only_pathways=len(b_pw - a_pw), pathway_jaccard=jac(a_pw, b_pw),
                             mp_reactions=len(a_rx), port_reactions=len(b_rx), shared_reactions=len(a_rx & b_rx), reaction_jaccard=jac(a_rx, b_rx),
                             examples_mp_only=ex(a_pw - b_pw), examples_port_only=ex(b_pw - a_pw)))
    pw_tab = pd.DataFrame(prow)

    s = pd.read_csv(T / "summary.tsv", sep="\t", dtype=str, keep_default_na=False)
    s["value"] = pd.to_numeric(s.value, errors="coerce").fillna(0)
    get = lambda metric, item: int(s.loc[(s.metric == metric) & (s.item == item), "value"].sum())
    ecls = s[s.metric == "element_class"]
    for k in ("plasmid", "virus", "provirus", "MGE", "integron", "BGC"):
        m[f"port_elements_{k}"] = int(ecls[ecls.item.str.split(";").map(lambda x: k in x)].value.sum())
    for ft in ("ncRNA", "tmRNA", "regulatory_region", "CRISPR", "promoter"):
        m[f"port_features_{ft}"] = get("features", ft)
    for lane in ("amrfinderplus", "pathofact", "deeptfactor", "vfdb", "bacmet", "megares", "tcdb"):
        m[f"port_cds_{lane}"] = get("lane_features", lane)
    lp = pd.read_csv(T / "lane_presence.tsv", sep="\t", dtype=str, keep_default_na=False)
    m["lanes_not_ok"] = "; ".join(f"{r.lane}={r.status}" for r in lp.itertuples() if r.status not in ("ok",))
    m["mag_count"] = int((pw_tab.entity_type == "mag").sum()) if len(pw_tab) else 0

    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([m]).to_csv(out / f"{sample}.metrics.tsv", sep="\t", index=False)
    ec_tab.to_csv(out / f"{sample}.ec.tsv", sep="\t", index=False)
    pw_tab.to_csv(out / f"{sample}.pathways.tsv", sep="\t", index=False)
    print(f"{sample}: {len(mp2feat)} matched CDS, {len(pw_tab)} PGDB entities", flush=True)


def rng(s, fmt="{:.0f}"):
    s = pd.to_numeric(s, errors="coerce").dropna()
    if not len(s):
        return "n/a"
    lo, hi = fmt.format(s.min()), fmt.format(s.max())
    return lo if lo == hi else f"{lo} to {hi}"


def combine(out):
    per = out / "per_assembly"
    met = pd.concat([pd.read_csv(p, sep="\t") for p in sorted(per.glob("*.metrics.tsv"))], ignore_index=True)
    ec = pd.concat([pd.read_csv(p, sep="\t") for p in sorted(per.glob("*.ec.tsv"))], ignore_index=True)
    pw = pd.concat([pd.read_csv(p, sep="\t") for p in sorted(per.glob("*.pathways.tsv")) if p.stat().st_size > 1], ignore_index=True)
    pct = lambda a, b: (100 * met[a] / met[b].where(met[b] > 0)).round(1)
    met["pct_mp_any_hit"] = pct("mp_any_hit", "cds_mp")
    met["pct_port_any_homology"] = pct("port_any_homology", "cds_port")
    met["pct_mp_named"] = pct("mp_named_function", "cds_mp")
    met["pct_port_named"] = pct("port_named_function", "cds_port")
    met["pct_mp_with_ec"] = pct("mp_with_ec", "cds_mp")
    met["pct_port_with_ec_pgdb"] = pct("port_with_ec_port_pgdb", "cds_port")
    met["pct_port_with_ec_gpr7"] = pct("port_with_ec_gpr7", "cds_port")
    met["pct_port_interproscan"] = pct("port_interproscan", "cds_port")
    met["pct_uniref50_same_name"] = (100 * met.uniref50_same_name / met.uniref50_matched_both.where(met.uniref50_matched_both > 0)).round(1)
    com = pw[pw.entity == "community"].set_index("sample")
    mags = pw[(pw.entity_type == "mag") & (pw.mp_status == "ok") & (pw.port_status == "ok")]
    met = met.set_index("sample")
    for c in ("mp_pathways", "port_pathways", "shared_pathways", "pathway_jaccard", "reaction_jaccard"):
        met[f"community_{c}"] = com[c].reindex(met.index)
    met["mag_pathway_jaccard_mean"] = mags.groupby("sample").pathway_jaccard.mean().round(3).reindex(met.index)
    g7 = ec[(ec.level == "ec") & (ec.port_lane == "gpr7")].set_index("sample")
    cl = ec[(ec.level == "ec") & (ec.port_lane == "clean")].set_index("sample")
    met["ec_gpr7_pct_agree_when_both"] = g7.pct_agree_when_both.reindex(met.index)
    met["ec_clean_pct_agree_when_both"] = cl.pct_agree_when_both.reindex(met.index)
    met = met.reset_index()
    met.to_csv(out / "per_assembly.tsv", sep="\t", index=False)
    pw.to_csv(out / "pathways_overlap.tsv", sep="\t", index=False)
    ec.to_csv(out / "ec_agreement.tsv", sep="\t", index=False)
    (out / "comparison.md").write_text(report(met, ec, pw))
    print(f"combined {len(met)} assemblies", flush=True)


def fmt(v, f="{:.0f}"):
    return "" if pd.isna(v) else f.format(v)


def report(met, ec, pw):
    n = len(met)
    tot = lambda c: int(met[c].sum())
    eca = ec[ec.level == "ec"].groupby("port_lane")[["agree", "partial", "disagree", "mp_only", "port_only", "neither"]].sum()
    both = eca[["agree", "partial", "disagree"]].sum(axis=1)
    agree_pct = (100 * eca.agree / both).round(0)
    com = pw[pw.entity == "community"]
    mags = pw[(pw.entity_type == "mag") & (pw.mp_status == "ok") & (pw.port_status == "ok")]
    label = {"clean": "CLEAN", "deepec": "DeepEC", "ezpred": "EZpred", "kofam": "KofamScan", "proteinbert": "ProteinBERT",
             "esm_c": "ESM-C", "gpr7": "7-method consensus", "gpr7_2plus": "7-method consensus, 2+ methods agree", "port_pgdb": "Pathway Tools input"}
    rows = "\n".join(
        f"| {r.sample} | {r.cds_mp:,} / {r.cds_port:,} | {fmt(r.pct_exact_of_mp, '{:.1f}%')} | {fmt(r.pct_mp_named, '{:.0f}%')} / {fmt(r.pct_port_named, '{:.0f}%')} "
        f"| {fmt(r.pct_mp_with_ec, '{:.0f}%')} / {fmt(r.pct_port_with_ec_pgdb, '{:.0f}%')} | {fmt(r.community_mp_pathways)} / {fmt(r.community_port_pathways)} "
        f"| {fmt(r.community_pathway_jaccard, '{:.2f}')} | {fmt(r.mag_pathway_jaccard_mean, '{:.2f}')} |"
        for r in met.sort_values("sample").itertuples())
    lanes = "\n".join(f"| {label[l]} | {int(both[l]):,} | {agree_pct[l]:.0f}% | {100 * eca.loc[l, 'partial'] / both[l]:.0f}% "
                      f"| {int(eca.loc[l, 'mp_only']):,} | {int(eca.loc[l, 'port_only']):,} |"
                      for l in EC_LANES if l in eca.index and both[l])
    return f"""# The new pipeline against the original MetaPathways: what changed

{n} assemblies compared. Each assembly ran through the original MetaPathways (4.0.3) and through
the new pipeline. Numbers below are ranges across the assemblies.

## What you need to know

- **The genes are the same.** Both pipelines use the same gene finder (Prodigal, metagenome
  mode). {rng(met.pct_exact_of_mp, "{:.1f}")}% of the original's genes are called with identical
  coordinates by the new pipeline, so gene-level results can be matched one to one. Most of the
  rest are weak, low-confidence calls that only one pipeline makes; a few share the stop codon
  but start elsewhere.
- **The original leaves short proteins unannotated.** It skips proteins under {MP_MIN_AA} amino
  acids ({rng(100 * met.mp_cds_below_60aa / met.cds_mp, "{:.1f}")}% of its genes). The new
  pipeline annotates every protein.
- **Named functions are close.** The share of genes with a named (not "hypothetical") function
  is {rng(met.pct_mp_named)}% in the original and {rng(met.pct_port_named)}% in the new pipeline,
  partly because of the skipped short proteins. Where both searched UniRef50, the top hit has the same name for
  {rng(met.pct_uniref50_same_name)}% of genes. The UniRef50 release and the search program both
  differ, so this residue cannot be split between them.
- **Enzyme (EC) calls differ much more, and that is a method change.** The original gives an EC
  number to {rng(met.pct_mp_with_ec)}% of genes; the new pipeline's Pathway Tools input gives one
  to {rng(met.pct_port_with_ec_pgdb)}%. The new pipeline predicts enzymes with machine-learning
  models rather than only by similarity to known proteins. Where both pipelines give the same
  gene an EC number and at least two of the new pipeline's seven methods agree, it matches the
  original for {agree_pct.get('gpr7_2plus', float('nan')):.0f}% of genes. Calls that rest on a single method are much less
  reliable: the overall agreement falls to {agree_pct.get('gpr7', float('nan')):.0f}%, and one method (ESM-C) agrees
  only {agree_pct.get('esm_c', float('nan')):.0f}% of the time while giving an EC number to nearly every gene. **Use the
  calls that two or more methods agree on.**
- **Pathway lists overlap but are not the same.** The community databases hold
  {rng(com.mp_pathways)} pathways (original) and {rng(com.port_pathways)} (new); the share of
  pathways in common (shared / either) is {rng(com.pathway_jaccard, "{:.2f}")}. Per MAG it is
  {rng(mags.pathway_jaccard, "{:.2f}")}. Both used the same Pathway Tools and MetaCyc release
  (27.0), so the difference comes from what each pipeline feeds Pathway Tools: product names
  plus EC numbers in the original, EC numbers from one predictor (CLEAN) in the new pipeline.
- **The new pipeline adds annotation types the original lacks**: mobile elements (plasmids,
  viruses, insertion sequences, integrons), non-coding RNAs, antibiotic and metal resistance
  genes, biosynthetic gene clusters, protein domains (InterProScan, {rng(met.pct_port_interproscan)}%
  of genes), transcription factors and promoters.

## Per assembly

Pairs are original / new. "Exact" is the share of the original's genes called identically.
"Pathway overlap" is shared pathways divided by pathways in either database.

| Assembly | Genes | Exact | Named function | With EC | Community pathways | Pathway overlap | MAG overlap (mean) |
|---|---|---|---|---|---|---|---|
{rows}

## EC agreement by method, on genes both pipelines called identically

"Agree" counts genes where both sides gave an EC number and at least one matches exactly.
"Close" means only the first three EC digits match.

| New-pipeline method | Genes with EC on both sides | Agree | Close | Original only | New only |
|---|---|---|---|---|---|
{lanes}

## Why they differ

- **Gene finder.** Same program and mode; the new pipeline runs it on batches of contigs in
  parallel. The genes only one side calls are mostly low-confidence calls.
- **Reference release.** The original searched UniProt 2025_04 (Swiss-Prot, UniRef50, UniRef90).
  The new pipeline's UniRef50 file is a later download (release not recorded), and its
  language-model methods use Swiss-Prot 2026_02. This affects named functions a little.
- **Method.** Most EC differences are here: the new pipeline predicts EC numbers with models
  (CLEAN, DeepEC, EZpred, ProteinBERT, ESM-C) and KEGG profiles, and these predict for proteins
  with no close known relative. They therefore call far more genes than the original.
- **Pathway Tools input.** The original passes product names and lets Pathway Tools match them
  to reactions; the new pipeline passes only CLEAN's EC numbers. This explains most of the
  pathway differences. The original also merges community genes that share a product name; this
  changes gene counts, not the pathway list.

Tables: `per_assembly.tsv`, `ec_agreement.tsv` and `pathways_overlap.tsv` next to this file.
"""


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["assembly", "combine", "ready"])
    ap.add_argument("--sample")
    ap.add_argument("--tables-root", type=Path, default=B / "tables/v1")
    ap.add_argument("--out-dir", type=Path, default=B / "tables/v1/comparison")
    ap.add_argument("--mnxr-lookup", type=Path, default=MNXR_LOOKUP)
    a = ap.parse_args()
    if a.mode == "ready":
        for d in sorted(a.tables_root.iterdir()):
            if (d / "summary.tsv").exists() and (d / "annotations_long.parquet").exists():
                print(d.name)
    elif a.mode == "assembly":
        if not a.sample:
            ap.error("--sample is required")
        per_assembly(a.sample, a.tables_root, a.out_dir / "per_assembly", a.mnxr_lookup)
    else:
        combine(a.out_dir)


if __name__ == "__main__":
    sys.exit(main())
