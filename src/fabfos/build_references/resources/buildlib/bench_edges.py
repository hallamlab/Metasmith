from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

ROUTES = ("curated", "curated_override", "host_gem", "metacyc_rxn", "bridge_uniprot",
          "bridge_ec", "bridge_ko", "web")

EDGE_COLS = ("cohort", "condition_id", "gene_label", "mnxr", "action", "action_raw",
             "route", "route_key", "strength")

EDGE_KEY = ("cohort", "condition_id", "mnxr", "action")

ROW_KEY = ("cohort", "condition_id", "gene_label", "mnxr", "action", "route", "route_key")

_ADD_TOKENS = {"add", "reg+"}
_DEL_TOKENS = {"del", "reg-"}

_ARM_ACTION = {"lof": "del", "eydallin": "del"}

_EC4 = re.compile(r"^\d+\.\d+\.\d+\.\d+$")


def split_ids(cell) -> list:
    if cell is None or (isinstance(cell, float) and cell != cell):
        return []
    return [x.strip() for x in str(cell).replace(";", ",").split(",") if x.strip()]


def decode_entities(s) -> str:
    if s is None or (isinstance(s, float) and s != s):
        return ""
    t = str(s)
    for ent, ch in (("&alpha;", "alpha"), ("&beta;", "beta"), ("&gamma;", "gamma"),
                    ("&delta;", "delta"), ("&omega;", "omega"), ("&epsilon;", "epsilon"),
                    ("&prime;", "'"), ("&amp;", "&")):
        t = t.replace(ent, ch)
    return t


def norm_name(s) -> str:
    return re.sub(r"\s+", " ", decode_entities(s).strip()).lower()


def norm_ec(tok) -> str | None:
    if tok is None:
        return None
    t = str(tok).strip()
    if t.upper().startswith("EC-"):
        t = t[3:]
    if t.upper().startswith("EC:"):
        t = t[3:]
    t = t.strip()
    return t if _EC4.match(t) else None


def organism_match(a, b) -> bool:
    a, b = norm_name(a), norm_name(b)
    if not a or not b:
        return False
    if a == b:
        return True
    ta, tb = a.replace(".", ". ").split(), b.replace(".", ". ").split()
    if len(ta) < 2 or len(tb) < 2 or ta[1] != tb[1]:
        return False
    return ta[0][0] == tb[0][0]


def canonical_action(cohort: str, action_raw) -> str:
    if cohort in _ARM_ACTION:
        return _ARM_ACTION[cohort]
    toks = {t.strip() for t in str(action_raw or "").split(",") if t.strip()}
    if toks & _DEL_TOKENS and not toks & _ADD_TOKENS:
        return "del"
    if toks & _ADD_TOKENS:
        return "add"
    return "mut" if toks else "add"


def _frame(rows) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=list(EDGE_COLS))


def curated_edges(extracts, entries: pd.DataFrame) -> pd.DataFrame:
    extracts = Path(extracts)
    per_obs = {}
    strength = {}
    for study, cols in (("laser", (("add", "add_mnxr"), ("del", "del_mnxr"))),
                        ("keio", (("del", "del_mnxr"),))):
        src = extracts / study / "extraction.tsv"
        if not src.exists():
            raise SystemExit(
                f"[edges] curated route: {src} is absent.\n"
                f"  It is the study's extraction, and it carries the reaction ids the "
                f"benchmark was curated with.\n"
                f"  This is a NAMED refusal. Building without it would ship an edge "
                f"table that looks complete and is missing its strongest route.")
        df = pd.read_csv(src, sep="\t", dtype=str)
        for r in df.to_dict("records"):
            obs = str(r.get("obs_id") or "").strip()
            if not obs:
                continue
            for action, col in cols:
                ids = split_ids(r.get(col))
                if ids:
                    per_obs.setdefault(obs, {}).setdefault(action, set()).update(ids)
            prov = str(r.get("mapping_provenance") or "").strip()
            if prov:
                strength[obs] = prov

    rows = []
    for e in entries.itertuples(index=False):
        for action, ids in per_obs.get(e.condition_id, {}).items():
            for mnxr in ids:
                rows.append((e.cohort, e.condition_id, None, mnxr, action, action,
                             "curated", e.condition_id,
                             strength.get(e.condition_id, "curated_source_table")))
    return _frame(rows).drop_duplicates()


_NOTE = re.compile(r"^(?:(?P<gene>.+?):)?(?P<verb>[a-z_]+)(?:\((?P<arg>.*)\))?$")

_NOTE_VERBS = {"override_select_mnxr", "override_select_ec", "override_reject",
               "override_confirm", "conflict", "species_fallback", "gated_by_record_ec"}


def parse_mapping_notes(cell) -> list:
    text = decode_entities(cell)
    out = []
    for chunk in (c.strip() for c in text.split(";")):
        if not chunk:
            continue
        m = _NOTE.match(chunk)
        if not m:
            raise SystemExit(
                f"[edges] mapping_notes: cannot parse {chunk!r}.\n"
                f"  Expected `[gene:]verb[(arg)]`. A note this parser skips is "
                f"indistinguishable from an observation that carried none, which is why "
                f"this refuses instead.")
        verb, arg = m.group("verb"), (m.group("arg") or "").strip()
        if verb not in _NOTE_VERBS:
            raise SystemExit(
                f"[edges] mapping_notes: unknown verb {verb!r} in {chunk!r}. The curator's "
                f"vocabulary has grown; teach this parser rather than let it drop the note.")
        out.append(((m.group("gene") or "").strip(), verb, arg))
    return out


def override_edges(extracts, entries: pd.DataFrame) -> tuple:
    src = Path(extracts) / "laser" / "extraction.tsv"
    df = pd.read_csv(src, sep="\t", dtype=str)
    by_obs = {}
    ec_over = {}
    for r in df.to_dict("records"):
        obs = str(r.get("obs_id") or "").strip()
        for gene, verb, arg in parse_mapping_notes(r.get("mapping_notes")):
            if verb == "override_select_mnxr":
                for mnxr in split_ids(arg.replace("/", ",")):
                    by_obs.setdefault(obs, []).append((norm_name(gene), mnxr))
            elif verb == "override_select_ec":
                ec = norm_ec(arg)
                if ec:
                    ec_over.setdefault((obs, norm_name(gene)), set()).add(ec)

    rows = []
    for e in entries.itertuples(index=False):
        for gene, mnxr in by_obs.get(e.condition_id, ()):
            if gene and norm_name(e.gene_label) != gene:
                continue
            act = canonical_action(e.cohort, e.action)
            rows.append((e.cohort, e.condition_id, e.gene_label, mnxr, act, e.action,
                         "curated_override", f"{e.condition_id}:{gene}", "override"))
    return _frame(rows).drop_duplicates(), ec_over


def host_gem_edges(gpr_gem, entries: pd.DataFrame) -> pd.DataFrame:
    g = gpr_gem if isinstance(gpr_gem, pd.DataFrame) else pd.read_parquet(gpr_gem)
    by_sym = {}
    nominator = g["orf"] if "orf" in g.columns else g["feature_id"]
    for fid, fname, mnxr in zip(nominator, g["feature_name"], g["mnxr"]):
        if not isinstance(mnxr, str) or not mnxr:
            continue
        for k in (fid, fname):
            if isinstance(k, str) and k.strip():
                by_sym.setdefault(norm_name(k), set()).add(mnxr)

    rows = []
    for e in entries.itertuples(index=False):
        if not _native(e.source_organism):
            continue
        key = norm_name(e.gene_label)
        for mnxr in sorted(by_sym.get(key, ())):
            rows.append((e.cohort, e.condition_id, e.gene_label, mnxr,
                         canonical_action(e.cohort, e.action), e.action,
                         "host_gem", key, "gem_gpr"))
    return _frame(rows).drop_duplicates()


def _native(source) -> bool:
    s = str(source or "").strip().lower()
    return s in ("", "none", "n/a", "na", "null", "not specified", "-") or "coli" in s


def load_pairings(pairings_path) -> pd.DataFrame:
    p = Path(pairings_path)
    if not p.exists():
        raise SystemExit(
            f"[edges] metacyc route: {p} is absent.\n"
            f"  It is LASER's `Gene-Reaction Pairings.txt`, 3,370 rows of gene, species "
            f"and MetaCyc reaction frame id.\n"
            f"  This is a NAMED refusal.")
    df = pd.read_csv(p, sep="\t", dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]
    need = {"Gene Name", "Species", "Unique ID", "Discovered EC Number"}
    missing = need - set(df.columns)
    if missing:
        raise SystemExit(f"[edges] {p}: columns {sorted(missing)} are absent -- the "
                         f"upstream table's layout has changed")
    return df


def metacyc_edges(pairings: pd.DataFrame, metacyc_map: dict,
                  entries: pd.DataFrame) -> pd.DataFrame:
    by_gs = {}
    for gene, sp, uid in zip(pairings["Gene Name"], pairings["Species"],
                             pairings["Unique ID"]):
        mnxrs = metacyc_map.get(str(uid).strip())
        if mnxrs:
            by_gs.setdefault(norm_name(gene), []).append((norm_name(sp), set(mnxrs)))

    rows = []
    for e in entries.itertuples(index=False):
        cands = by_gs.get(norm_name(e.gene_label))
        if not cands:
            continue
        want = e.source_organism or "Escherichia coli"
        act = canonical_action(e.cohort, e.action)
        key = norm_name(e.gene_label) + "@" + norm_name(want)
        for sp, mnxrs in cands:
            if not organism_match(sp, want):
                continue
            for mnxr in mnxrs:
                rows.append((e.cohort, e.condition_id, e.gene_label, mnxr, act, e.action,
                             "metacyc_rxn", key, "metacyc_frame"))
    return _frame(rows).drop_duplicates()


def bridge_slice(bridge_path, id_source: str) -> dict:
    b = pd.read_parquet(bridge_path, columns=["id", "id_source", "mnxr"])
    b = b[b["id_source"] == id_source]
    return b.groupby("id")["mnxr"].apply(set).to_dict()


def uniprot_edges(up_map: dict, entries: pd.DataFrame, web_accessions=(),
                  native_accessions=None) -> pd.DataFrame:
    web = {str(a).strip() for a in web_accessions if str(a).strip()}
    native = native_accessions or {}
    rows = []
    for e in entries.itertuples(index=False):
        acc = e.uniprot
        if acc is None or (isinstance(acc, float) and acc != acc):
            if not (_native(e.source_organism) and native):
                continue
            acc = native.get(norm_name(e.gene_label))
            if not acc:
                continue
        mnxrs = up_map.get(str(acc).strip())
        if not mnxrs:
            continue
        route = "web" if str(acc).strip() in web else "bridge_uniprot"
        act = canonical_action(e.cohort, e.action)
        for mnxr in mnxrs:
            rows.append((e.cohort, e.condition_id, e.gene_label, mnxr, act, e.action,
                         route, str(acc).strip(), "bridge"))
    return _frame(rows).drop_duplicates()


def ec_index(extracts, pairings: pd.DataFrame, het_path, ec_overrides=None) -> dict:
    out = {}

    def add(gene, tok):
        ec = norm_ec(tok)
        if ec:
            out.setdefault(norm_name(gene), set()).add(ec)

    L = pd.read_csv(Path(extracts) / "laser" / "extraction.tsv", sep="\t", dtype=str)
    for cell in L["genes_json"].fillna(""):
        if not str(cell).strip():
            continue
        for g in json.loads(cell):
            for tok in str(g.get("ec") or "").split():
                add(g.get("gene"), tok)

    K = pd.read_csv(Path(extracts) / "keio" / "extraction.tsv", sep="\t", dtype=str)
    for gene, ecs in zip(K["gene_set"].fillna(""), K["EC"].fillna("")):
        for tok in str(ecs).replace(",", ";").split(";"):
            add(str(gene).split(":")[0], tok)

    for gene, ecs in zip(pairings["Gene Name"], pairings["Discovered EC Number"]):
        for tok in str(ecs).split():
            add(gene, tok)

    het = Path(het_path) / "heterologous_uniprot.tsv"
    if het.exists():
        H = pd.read_csv(het, sep="\t", dtype=str).fillna("")
        for gene, reason in zip(H["gene"], H["reason"]):
            if ":" in reason and reason.split(":", 1)[0].startswith("ec"):
                add(gene, reason.split(":", 1)[1])

    for (_obs, gene), ecs in (ec_overrides or {}).items():
        for ec in ecs:
            add(gene, ec)
    return out


def ec_edges(ec_map: dict, gene_ecs: dict, entries: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for e in entries.itertuples(index=False):
        act = canonical_action(e.cohort, e.action)
        for ec in sorted(gene_ecs.get(norm_name(e.gene_label), ())):
            for mnxr in ec_map.get(ec, ()):
                rows.append((e.cohort, e.condition_id, e.gene_label, mnxr, act, e.action,
                             "bridge_ec", ec, "bridge"))
    return _frame(rows).drop_duplicates()


def ko_edges(ko_map: dict, gene_kos: dict, entries: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for e in entries.itertuples(index=False):
        act = canonical_action(e.cohort, e.action)
        for ko in sorted(gene_kos.get(norm_name(e.gene_label), ())):
            for mnxr in ko_map.get(ko, ()):
                rows.append((e.cohort, e.condition_id, e.gene_label, mnxr, act, e.action,
                             "bridge_ko", ko, "bridge"))
    return _frame(rows).drop_duplicates()


def layer(frames: dict, universe=None) -> tuple:
    offered = {}
    report = []
    for route in ROUTES:
        df = frames.get(route)
        if df is None or not len(df):
            report.append(dict(route=route, offered=0, out_of_universe=0,
                               out_of_universe_ids=[], added=0, entries=0, mnxr=0))
            offered[route] = _frame([])
            continue
        n0 = len(df)
        gone = []
        if universe is not None:
            bad = ~df["mnxr"].isin(universe)
            gone = sorted(set(df.loc[bad, "mnxr"]))
            df = df[~bad]
        offered[route] = df
        report.append(dict(route=route, offered=n0, out_of_universe=n0 - len(df),
                           out_of_universe_ids=gone, added=0, entries=0, mnxr=0))

    agreed = {}
    for route in ROUTES:
        df = offered[route]
        if not len(df):
            continue
        for k in zip(*(df[c] for c in EDGE_KEY)):
            agreed.setdefault(k, set()).add(route)

    claimed = set()
    kept = []
    for route, rec in zip(ROUTES, report):
        df = offered[route]
        if not len(df):
            continue
        keys = list(zip(*(df[c] for c in EDGE_KEY)))
        mask = [k not in claimed for k in keys]
        add = df[pd.Series(mask, index=df.index)]
        claimed.update(k for k, m in zip(keys, mask) if m)
        rec["added"] = len(add)
        if len(add):
            rec["entries"] = int(add[["cohort", "condition_id"]].drop_duplicates().shape[0])
            rec["mnxr"] = int(add["mnxr"].nunique())
            kept.append(add)

    out = (pd.concat(kept, ignore_index=True) if kept
           else _frame([]))
    if len(out):
        out["agreed_by"] = ["+".join(sorted(agreed.get(k, {r})))
                            for k, r in zip(zip(*(out[c] for c in EDGE_KEY)),
                                            out["route"])]
        out["n_agree"] = out["agreed_by"].str.count(r"\+") + 1

        rank = {r: i for i, r in enumerate(ROUTES)}
        entry_top = (out.assign(_r=out["route"].map(rank))
                        .groupby(["cohort", "condition_id"])["_r"].min())
        out["in_entry_set"] = [
            rank[r] == entry_top.get((c, i), -1)
            for r, c, i in zip(out["route"], out["cohort"], out["condition_id"])]
    else:
        out["agreed_by"] = pd.Series(dtype=str)
        out["n_agree"] = pd.Series(dtype=int)
        out["in_entry_set"] = pd.Series(dtype=bool)

    if len(out):
        dup = out[out.duplicated(subset=list(ROW_KEY), keep=False)]
        if len(dup):
            raise SystemExit(
                f"[edges] {len(dup):,} rows share a row key -- the same route is "
                f"asserting the same gene-to-reaction edge twice. "
                f"First few:\n{dup.head(5).to_string()}")
        by_edge = out.groupby(list(EDGE_KEY))["route"].nunique()
        if (by_edge > 1).any():
            bad = by_edge[by_edge > 1]
            raise SystemExit(
                f"[edges] {len(bad):,} edges are supplied by more than one route -- the "
                f"layering restriction did not hold. First few:\n{bad.head(5)}")
        prim = (out[out["in_entry_set"]].groupby("route")[["cohort", "condition_id"]]
                .apply(lambda d: d.drop_duplicates().shape[0]).to_dict())
        for rec in report:
            rec["primary_entries"] = prim.get(rec["route"], 0)
    return out.reset_index(drop=True), report


def report_lines(report: list, tag: str = "edges") -> list:
    lines = []
    for r in report:
        lines.append(f"[{tag}] {r['route']:<18} offered {r['offered']:>7,}  "
                     f"added {r['added']:>7,}  {r['entries']:>4,} entries  "
                     f"{r['mnxr']:>5,} reactions"
                     + (f"  primary for {r['primary_entries']:>4,}"
                        if r.get("primary_entries") is not None else ""))
        if r["out_of_universe"]:
            ids = r["out_of_universe_ids"]
            shown = ids[:8]
            more = f" and {len(ids) - len(shown):,} more" if len(ids) > len(shown) else ""
            lines.append(f"[{tag}]   {r['out_of_universe']:,} of those name a reaction "
                         f"MetaNetX does not define and are DROPPED: {shown}{more}")
    return lines
