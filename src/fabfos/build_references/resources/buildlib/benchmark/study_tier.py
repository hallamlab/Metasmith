import argparse as _argparse
import ast as _ast

_p = _argparse.ArgumentParser()
_p.add_argument("--bridge", required=True)
_p.add_argument("--channel", required=True)
_p.add_argument("--condition-cols", required=True)
_p.add_argument("--direction", required=True)
_p.add_argument("--elements", required=True)
_p.add_argument("--ev-lib", required=True)
_p.add_argument("--extensions", required=True)
_p.add_argument("--extract", required=True)
_p.add_argument("--hosts-gem", required=True)
_p.add_argument("--lane-set", required=True)
_p.add_argument("--metanetx", required=True)
_p.add_argument("--out", required=True)
_p.add_argument("--pairs", required=True)
_p.add_argument("--studies", required=True)
_p.add_argument("--universe-m", required=True)
_p.add_argument("--vocab", required=True)
_p.add_argument("--y-cols", required=True)
A = _p.parse_args()
_LIT_condition_cols = _ast.literal_eval(A.condition_cols)
_LIT_elements = _ast.literal_eval(A.elements)
_LIT_extensions = _ast.literal_eval(A.extensions)
_LIT_studies = _ast.literal_eval(A.studies)
_LIT_y_cols = _ast.literal_eval(A.y_cols)

import json, os, sys
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(A.universe_m))
import bench_universe as bu
sys.path.insert(0, os.path.dirname(A.ev_lib))
import fabfos_evidence as fe

EXTRACT = Path(A.extract)
OUT     = Path(A.out)
HOSTS   = Path(A.hosts_gem) / "hosts"
EXTENSIONS = _LIT_extensions
GPR_COLS = fe.schema_for(EXTENSIONS)
CONDITION_COLS = _LIT_condition_cols
Y_COLS = _LIT_y_cols
ELEMENTS = _LIT_elements
STUDIES = _LIT_studies
CHANNEL = A.channel
LANE_SET = A.lane_set

# ---- the atom universe, decoded straight off the vocab -------------------------------
# Same argument as host_gpr_gem.py: this reads `atom_pairs.rxn`, a plain vocab code, and
# no packed node column -- so the three files being ONE artifact is what has to hold, and
# the encoder's version gate does not apply. The version is carried into BUILD.json.
def bake_identity(path):
    md = pq.read_schema(path).metadata or {}
    if b"ecspr_bake" not in md:
        raise SystemExit(f"[study] {path} carries no ecspr_bake identity block")
    return md[b"ecspr_bake"]

blocks = {p: bake_identity(p) for p in (A.vocab, A.pairs, A.direction)}
if len(set(blocks.values())) != 1:
    raise SystemExit("[study] the three bake files are not one artifact")
ident = json.loads(next(iter(blocks.values())).decode())
V = pd.read_parquet(A.vocab)
rxn_symbol = V[V["kind"] == "rxn"].set_index("code")["symbol"]
transport = bu.transport_mnxrs(bu.reac_prop_path(A.metanetx))
universe, u_stats = bu.atom_universe(V, A.pairs, exclude=transport)
print(bu.universe_line(u_stats, "study") +
      f"  (bake v{ident['bake_version']})", flush=True)

# Which reactions the de-novo lanes could even nominate -- used to REPORT reach per
# study, never to filter. A study whose edges the lanes cannot see is a finding about
# the method, which is the thing the benchmark exists to measure.
reachable = set(pd.read_parquet(A.bridge, columns=["mnxr"])["mnxr"].unique())

# ---- reaction -> the metabolites its atoms flow INTO, per element --------------------
# STRUCTURE, not a result. The atom-pair table states which atom of which metabolite
# becomes which; reading the head side of a reaction's pairs is reading the model, and
# it is what lets Y name the metabolites an addition is expected to move without ever
# consulting a solve.
#
# BUILT OVER THE UNIVERSE, not over the whole pairs table, for the same reason the
# universe excludes transport: a transporter's heads are its energetic coupling's
# products (ADP, Pi), so a condition naming one would be scored on whether the method
# moved ATP -- an expectation shared with every other transporter in the model.
P = pd.read_parquet(A.pairs, columns=["element", "rxn", "head_met"])
met_symbol = V[V["kind"] == "met"].set_index("code")["symbol"]
ELEMENT_ORDER = list(ident["element_order"])
P["el"] = P["element"].map(lambda i: ELEMENT_ORDER[int(i)]
                           if 0 <= int(i) < len(ELEMENT_ORDER) else None)
P["rxn_s"] = P["rxn"].map(rxn_symbol)
P["met_s"] = P["head_met"].map(met_symbol)
P = P[P["rxn_s"].isin(universe)]
products_of = {}
for (rx, el), grp in P.dropna(subset=["el", "rxn_s", "met_s"]).groupby(["rxn_s", "el"]):
    products_of.setdefault(rx, {})[el] = sorted(set(grp["met_s"]))
print(f"[study] product map: {len(products_of):,} reactions over "
      f"{ELEMENT_ORDER}", flush=True)

# Each host's own reaction set, so a condition can be told whether the edge it names is
# ALREADY in the background network. That distinction is what separates an addition from
# a dosage change, and getting it wrong is how an in-base addition becomes a silent
# no-op: a reaction the host already carries must enter as a PARALLEL copy, never as an
# overwrite. This project has hit that once already.
host_mnxr = {}
for d in sorted(p for p in HOSTS.glob("*") if p.is_dir()):
    g = pd.read_parquet(d / "gpr_gem.parquet", columns=["mnxr"])
    host_mnxr[d.name] = set(g["mnxr"].unique())
print(f"[study] host background: "
      f"{ {h: len(v) for h, v in host_mnxr.items()} }", flush=True)


def split_ids(cell):
    if cell is None or (isinstance(cell, float) and np.isnan(cell)):
        return []
    return [x.strip() for x in str(cell).replace(";", ",").split(",") if x.strip()]


def rows_obs_mnxr(df, spec):
    """One row per observation, reactions in add_mnxr / del_mnxr list columns.

    `orf` names the whole gene SET and feature_kind is `curated_set`: the extraction
    attributes reactions to the OBSERVATION, not to a gene within it, and the schema's
    nominator column is never null. See the module docstring.
    """
    out = []
    for _, r in df.iterrows():
        cid = str(r.get("obs_id") or "").strip()
        if not cid:
            continue
        genes = str(r.get("gene_set") or "")
        for action, col in (("add", "add_mnxr"), ("del", "del_mnxr")):
            for mnxr in split_ids(r.get(col)):
                out.append(dict(
                    orf=genes or cid, feature_kind="curated_set", feature_name=genes,
                    mnxr=mnxr, intermediate_id=cid, intermediate_name=genes,
                    condition_id=cid, action=action,
                    source_organism=str(r.get("host") or ""),
                    measured=str(r.get("measured") or "unknown").strip().lower(),
                    citation=str(r.get("doi") or ""),
                    note=str(r.get("note") or ""),
                ))
    return out


def rows_gene_del(df, spec):
    """One row per knocked-out gene. Reactions come from del_mnxr where the extraction
    resolved them; where it did not, the gene is still emitted with a NULL mnxr so the
    conditions table can name it -- an absent row would make the condition look
    edge-less rather than host-resolved, and the edges are the host's, named by symbol."""
    out = []
    for _, r in df.iterrows():
        gene = str(r.get("gene_set") or r.get("gene") or "").strip()
        gene = gene.split(":")[0] if gene else gene
        if not gene:
            continue
        cid = str(r.get("obs_id") or "").strip() or f"{spec['cohort']}:{gene}"
        mnxrs = split_ids(r.get("del_mnxr")) or [None]
        for mnxr in mnxrs:
            out.append(dict(
                orf=gene, feature_kind="curated_gene",
                feature_name=str(r.get("function_supplTableS1") or r.get("subsystem") or ""),
                mnxr=mnxr, intermediate_id=cid,
                intermediate_name=str(r.get("b_number") or r.get("gene_norm") or ""),
                condition_id=cid, action="del", source_organism="",
                # THE LOF ARM IS UNIFORM. A knockout removes a route; that is a
                # CONDUCTANCE claim, not a growth claim, and it never was.
                measured="down",
                citation=str(r.get("doi") or r.get("citation") or ""),
                note=str(r.get("note") or r.get("phenotype") or ""),
            ))
    return out


def rows_gene_ovx_row(df, spec):
    """One row per OVEREXPRESSED gene -- `gene_del`'s mirror, for an ASKA-style screen.

    Same extraction shape, opposite perturbation: a clone ADDS a copy of its gene, so
    the condition's rows are `action=add`. That is the only structural difference, and
    it is the one that matters -- a knockout removes a route and an overexpression adds
    conductance to one, and nothing downstream can tell them apart after the fact.

    DIRECTION COMES FROM THE PAPER'S OWN LABEL, through the map the study declares.
    `gene_del` can assert `measured=down` for every row because a knockout removing a
    route is uniform; a screen that reports genes in BOTH directions cannot, and reading
    58 deficient genes as 58 confirmations of `down` while the other 28 say `up` would
    make the answer key agree with itself no matter what happened. A label the map does
    not cover yields NO direction rather than a guess -- B5 drops a condition whose
    direction is unknown, which is the honest shape of "we do not know".
    """
    directions = spec.get("directions") or {}
    out = []
    for _, r in df.iterrows():
        gene = str(r.get("gene_set") or r.get("gene") or "").strip()
        gene = gene.split(":")[0] if gene else gene
        if not gene:
            continue
        cid = str(r.get("obs_id") or "").strip() or f"{spec['cohort']}:{gene}"
        label = str(r.get("phenotype") or "").strip()
        mnxrs = split_ids(r.get("add_mnxr") or r.get("del_mnxr")) or [None]
        for mnxr in mnxrs:
            out.append(dict(
                orf=gene, feature_kind="curated_gene",
                feature_name=str(r.get("function_supplTableS1") or r.get("subsystem") or ""),
                mnxr=mnxr, intermediate_id=cid,
                intermediate_name=str(r.get("b_number") or r.get("gene_norm") or ""),
                condition_id=cid, action="add", source_organism="",
                measured=directions.get(label, ""),
                citation=str(r.get("doi") or r.get("citation") or ""),
                note=label or str(r.get("note") or ""),
            ))
    return out


def cell(r, key):
    """A field as a clean string, or ''. `str(nan or "")` is `'nan'` -- NaN is TRUTHY,
    so the usual `or ""` idiom silently turns a missing reaction into the literal
    reaction id 'nan', which then joins to nothing and reads as an unmapped edge."""
    v = r.get(key)
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return ""
    return str(v).strip()


def read_ragged(src, elements):
    """Read a contrast extraction, repairing rows that are one field short.

    THE CONTRAST TABLES ARE RAGGED and pandas does not say so. Their `add` rows carry no
    src/sink metabolites and were written with three tabs where the header wants four
    columns, so every field right of `sink_name` shifts left by one: `element` reads the
    direction, `expected_dir` reads the citation, and the citation reads the note. That
    is not a parse warning -- it is a table that looks fine and means something else.

    The repair pads the src/sink block, which is the only block that can be short, and
    then VALIDATES: `element` must be one of the elements and `expected_dir` one of the
    directions. A repair that does not produce valid values is refused rather than
    guessed at, because a wrong repair is indistinguishable from a right one downstream.
    """
    lines = src.read_text().splitlines()
    header = lines[0].split("\t")
    n, gaps = len(header), 0
    try:
        i_el, i_dir = header.index("element"), header.index("expected_dir")
        i_src = header.index("src_mnxm")
    except ValueError:
        return pd.read_csv(src, sep="\t", dtype=str, keep_default_na=False,
                           na_values=[""]), 0
    rows = []
    for line in lines[1:]:
        if not line.strip():
            continue
        f = line.split("\t")
        if len(f) < n:
            gaps += 1
            f = f[:i_src] + [""] * (n - len(f)) + f[i_src:]
        f = (f + [""] * n)[:n]
        rows.append(f)
    df = pd.DataFrame(rows, columns=header)
    bad_el = sorted(set(df["element"]) - set(elements) - {""})
    bad_dir = sorted(set(df["expected_dir"]) - {"+", "-", "0", ""})
    if bad_el or bad_dir:
        raise SystemExit(
            f"[study] {src}: after repairing {gaps} short row(s), element carries "
            f"{bad_el} and expected_dir carries {bad_dir}. The columns are still not "
            f"where the header says they are, and a table that parses into the wrong "
            f"columns is worse than one that fails to parse.")
    return df, gaps


def rows_gene_row(df, spec):
    """One row per (gene, reaction) claim -- the contrast cohorts, already attributed."""
    out = []
    for i, r in df.iterrows():
        gene = cell(r, "gene")
        role = cell(r, "role") or "add"
        mnxr = cell(r, "mnxr") or None
        # `role=target` names an AXIS, not an edge -- src/sink metabolites and no
        # reaction. It belongs to Y. `role=control` is a DECLARED control, and the
        # curator's judgement about which rows those are is the whole reason these four
        # cohorts were chosen; treating one as an addition puts a control in the
        # measurement.
        if role in ("target", "control"):
            continue
        cid = f"{spec['cohort']}:{gene}:{i}"
        out.append(dict(
            orf=gene, feature_kind="curated_gene",
            feature_name=cell(r, "ec"),
            mnxr=mnxr, intermediate_id=cid, intermediate_name=cell(r, "ec"),
            condition_id=cid, action="del" if role == "del" else "add",
            source_organism="",
            # These tables carry no separate `measured` column: `expected_dir` IS the
            # curator's reading of what the paper reported, not a prediction someone
            # made about the pipeline. That is the same rule, not an exception to it.
            measured={"+": "up", "-": "down", "0": "flat"}.get(
                cell(r, "expected_dir"), "unknown"),
            citation=cell(r, "citation"),
            note=cell(r, "note"),
        ))
    return out


def rows_gene_ovx(df, spec):
    """One row per (strain, gene, reaction) -- THE STRAIN IS THE CONDITION.

    An overexpression screen measures a strain, and a strain carries one or two
    named ORFs whose every reaction belongs to that one measurement. So the
    condition id is the extraction's own `obs_id` rather than the row number:
    grouping by row would turn one clone carrying twenty-one reactions into
    twenty-one conditions of one reaction each, and score each of them against
    the same single titer.

    Attribution is per gene, because here the curator could make it -- the
    extraction resolves each ORF to its own b-number and that b-number's own
    reactions. `orf` is therefore the b-number, which is also what the
    null pool is drawn on: a drawn clone and a tested clone have to be the same
    kind of thing or the comparison is between two different questions.

    An ORF the model has no reaction for still emits a row with a null mnxr. The
    strain was built and measured, and a gene the network cannot see is a result
    about the method's reach, not an absence of data -- dropping it would shrink
    the denominator to the cases that were already going to work.
    """
    out = []
    for _, r in df.iterrows():
        cid = cell(r, "obs_id")
        role = cell(r, "role") or "add"
        if not cid or role == "control":
            continue
        gene = cell(r, "gene")
        out.append(dict(
            # The b-number where the extraction resolved one, else the symbol: the
            # nominator column is never null, and the strain named SOMETHING.
            orf=cell(r, "b_number") or gene or cid, feature_kind="curated_gene",
            feature_name=gene, mnxr=cell(r, "mnxr") or None,
            intermediate_id=cid, intermediate_name=cell(r, "strain_id"),
            condition_id=cid, action="del" if role == "del" else "add",
            source_organism="",
            measured=cell(r, "measured") or "unknown",
            citation=cell(r, "citation"), note=cell(r, "note"),
        ))
    return out


def declared_controls(df, spec, study):
    """`role=control` rows, kept as declared controls rather than dropped."""
    out = []
    for i, r in df.iterrows():
        if cell(r, "role") != "control":
            continue
        # An extraction that names its own observations keeps its own id -- a
        # control is one of the study's strains, and renaming it here would
        # break the join back to the measurement it was read beside.
        cid = cell(r, "obs_id") or f"{study}:CTRL:{cell(r, 'gene') or i}"
        out.append(dict(
            condition_id=cid,
            element=cell(r, "element"), note=cell(r, "note"),
            citation=cell(r, "citation"),
            # `expected_dir` where the contrast tables carry one; otherwise the
            # measured direction, which is the same claim under another name.
            measured_dir=cell(r, "expected_dir") or {
                "up": "+", "down": "-", "flat": "0"}.get(cell(r, "measured"), ""),
        ))
    return out


READERS = {"obs_mnxr": rows_obs_mnxr, "gene_del": rows_gene_del,
            "gene_ovx_row": rows_gene_ovx_row,
            "gene_row": rows_gene_row, "gene_ovx": rows_gene_ovx}
DECLARES = ("gene_row", "gene_ovx")

summary = []
for study, spec in sorted(STUDIES.items()):
    # The elements THIS study is scored on. A subset of ELEMENTS, never a superset --
    # an element the bake does not carry has no network to be scored on.
    els = tuple(spec.get("elements", ELEMENTS))
    if not set(els) <= set(ELEMENTS):
        raise SystemExit(f"[study] {study}: elements {els} are not a subset of "
                         f"{ELEMENTS}, which is what the bake carries")
    src = EXTRACT / study / "extraction.tsv"
    if not src.exists():
        raise SystemExit(f"[study] no extraction for {study} at {src}")
    if spec["reader"] == "gene_row":
        df, gaps = read_ragged(src, ELEMENTS)
        if gaps:
            print(f"[study] {study}: repaired {gaps} short row(s) in the extraction "
                  f"-- three tabs where the header wants four columns", flush=True)
    else:
        df = pd.read_csv(src, sep="\t", dtype=str, keep_default_na=False,
                         na_values=[""])
    raw = READERS[spec["reader"]](df, spec)
    declared = declared_controls(df, spec, study) if spec["reader"] in DECLARES else []

    d = OUT / study
    d.mkdir(parents=True, exist_ok=True)
    # Copied as BYTES. A re-extraction that disagreed with the deployed one would move
    # the benchmark without failing anything.
    (d / "extraction.tsv").write_bytes(src.read_bytes())

    host = spec["host"]
    background = host_mnxr.get(host, set())
    gpr = pd.DataFrame(raw)
    if gpr.empty:
        raise SystemExit(f"[study] {study}: the extraction produced no rows")
    # `source` names the artifact these rows came out of, which for a curated tier is
    # the study's own extraction.
    gpr["source"] = study
    gpr["build_id"] = f"manual_{study}"
    gpr["host"] = host
    gpr["unit_id"] = study
    gpr["channel"] = CHANNEL
    gpr["score_kind"] = fe.ASSERTION_CHANNELS[CHANNEL]
    gpr["lane_set"] = LANE_SET
    # NOT ASSERTED. The extractions carry no quality grade, and inventing one would let a
    # downstream filter act on a distinction no curator drew.
    gpr["evidence_quality"] = "unknown"
    # A curator asserts that an edge IS claimed, not how strongly -- the same argument
    # the GEM table's uniform 1.0 rests on. The evidence-weighted line is gpr_denovo.
    gpr["raw_score"] = np.float32(1.0)
    gpr["projection_via"] = "curated"
    # Nullable, because a row with no reaction has no answer to "is it in the atom
    # universe" and `False` would be one. Under pandas 2 a None into a bool column
    # upcast silently; pandas 3 refuses outright, so the dtype is declared rather
    # than left to whichever version happens to be installed.
    gpr["in_atom_universe"] = gpr["mnxr"].isin(universe).astype("boolean")
    gpr.loc[gpr["mnxr"].isna(), "in_atom_universe"] = pd.NA
    gpr["cohort"] = spec["cohort"]
    # No boolean rule: a curated claim is per gene or per observation, and inventing one
    # would make this look like the same kind of claim a GEM makes.
    gpr["gpr_rule"] = None
    meta = gpr[["condition_id", "measured", "citation", "note"]].drop_duplicates(
        subset=["condition_id"])
    gpr = gpr[list(GPR_COLS)]
    gpr = gpr.sort_values(fe.grain_key(EXTENSIONS), kind="mergesort",
                          na_position="last").reset_index(drop=True)
    # No `orf_ids`: a curated row names a gene the paper reported, and whether the host's
    # proteome carries it under that symbol is what the reach columns below report.
    fe.validate_gpr(gpr, LANE_SET, None, study, EXTENSIONS)
    gpr.to_parquet(d / "gpr_manual.parquet", index=False, compression="zstd")

    named = gpr[gpr["mnxr"].notna()]
    in_base = set(named["mnxr"]) & background
    covered = set(named["mnxr"]) & reachable

    # ---- conditions -------------------------------------------------------------
    # THE JOIN. A condition may only be emitted if this study's GPR table knows the edge
    # set it names -- so the conditions are built FROM the table rather than beside it.
    conds = []
    by_cond = gpr.groupby("condition_id", sort=True)
    for cid, g in by_cond:
        n_add = int((g["action"] == "add").sum())
        n_del = int((g["action"] == "del").sum())
        m = meta[meta["condition_id"] == cid]
        measured = (m["measured"].iloc[0] if len(m) else "unknown")
        # A reaction outside the atom universe cannot carry an edge the benchmark
        # counts, so a condition made only of those is a structural control -- a
        # property of the reactions, not of a path between two chosen endpoints. The
        # prior endpoint-pair test relabelled 27 real effects as controls.
        #
        # SINCE TRANSPORT LEFT THE UNIVERSE, a purely-transport condition lands here
        # too, and that is the intended reading: its only atom pairs are the ATP
        # hydrolysis every transporter shares, so "does the method move this" has the
        # same answer for all of them and none of it is about the species that crossed.
        known = g[g["mnxr"].notna()]
        structural = len(known) > 0 and not known["in_atom_universe"].astype(bool).any()
        for el in els:
            conds.append(dict(
                study=study, condition_id=cid, cohort=spec["cohort"], arm=spec["arm"],
                tier="primary", host=host, element=el,
                n_add=n_add, n_del=n_del,
                is_control=int(structural), control_kind="structural" if structural else "",
                read_against=f"{study}:BASELINE",
                # DIRECTION COMES FROM WHAT WAS MEASURED, never from what someone
                # predicted -- `expected` is populated on 10 of 382 GOF rows, so using it
                # would silently shrink the answer key to the cases already anticipated.
                # "unknown" carries no direction: "we don't know" and "we expect no
                # movement" are different claims and are stored differently.
                measured_dir={"up": "+", "down": "-", "flat": "0"}.get(measured, ""),
                citation=(m["citation"].iloc[0] if len(m) else ""),
                note=(m["note"].iloc[0] if len(m) else ""),
            ))

    # The unperturbed host. Declared, not inferred: it is the zero point every result in
    # this study is a difference from, and a study without one has nothing to diff.
    for el in els:
        conds.append(dict(
            study=study, condition_id=f"{study}:BASELINE", cohort=spec["cohort"],
            arm=spec["arm"], tier="control", host=host, element=el,
            n_add=0, n_del=0, is_control=1, control_kind="baseline", read_against="",
            measured_dir="0", citation="", note="the unperturbed host network"))

    # The curator's OWN declared controls. Emitted only for the element the curator
    # named, because a control declared on the P axis says nothing about C.
    for c in declared:
        for el in ([c["element"]] if c["element"] in els else list(els)):
            conds.append(dict(
                study=study, condition_id=c["condition_id"], cohort=spec["cohort"],
                arm=spec["arm"], tier="control", host=host, element=el,
                n_add=0, n_del=0, is_control=1, control_kind="declared",
                read_against=f"{study}:BASELINE", measured_dir=c["measured_dir"],
                citation=c["citation"], note=c["note"]))

    # An ON-PATH control: the study's own edge with the most atom-pair coverage in the
    # background network. It must return large, and picking it from the study's own rows
    # rather than from a fixed list keeps it meaningful when the study is revised.
    on_path = named[named["in_atom_universe"].astype(bool) & named["mnxr"].isin(background)]
    if len(on_path):
        pick = sorted(on_path["mnxr"].unique())[0]
        for el in els:
            conds.append(dict(
                study=study, condition_id=f"{study}:ONPATH", cohort=spec["cohort"],
                arm=spec["arm"], tier="control", host=host, element=el,
                n_add=0, n_del=1, is_control=1, control_kind="on_path",
                read_against=f"{study}:BASELINE", measured_dir="-", citation="",
                note=f"deleting {pick}, an in-base atom-mapped edge -- must move"))

    cdf = pd.DataFrame(conds, columns=list(CONDITION_COLS))
    cdf.to_csv(d / "conditions.tsv", sep="\t", index=False)

    # ---- Y ----------------------------------------------------------------------
    # An addition's expected direction attaches to the metabolites its reactions' atoms
    # flow INTO -- the products. A deletion's attaches to the same set, because removing
    # the reaction removes their route. Which metabolites those are is read off the
    # compiled atom-pair table, which is STRUCTURE: it says which atom of which
    # metabolite becomes which, and says nothing about what any solve returned.
    yrows = []
    for cid, g in by_cond:
        m = meta[meta["condition_id"] == cid]
        measured = (m["measured"].iloc[0] if len(m) else "unknown")
        # A condition whose direction is UNKNOWN gets no expectation at all -- not a
        # zero. "We don't know" and "we expect no movement" are different claims and are
        # stored differently; collapsing them scores ignorance as a correct null.
        if measured not in ("up", "down", "flat"):
            continue
        base = {"up": "+", "down": "-", "flat": "0"}[measured]
        for _, r in g.iterrows():
            mnxr = r["mnxr"]
            if mnxr is None or (isinstance(mnxr, float) and np.isnan(mnxr)):
                continue
            for el, heads in products_of.get(mnxr, {}).items():
                # The study's own elements, for the same reason the conditions are: an
                # expectation on an element the paper did not read is not an expectation.
                if el not in els:
                    continue
                for mnxm in heads:
                    yrows.append(dict(
                        condition_id=cid, element=el, mnxm=mnxm,
                        expected_dir=base, basis="reaction_product", tier="mechanical"))

    ydf = pd.DataFrame(yrows, columns=list(Y_COLS))
    if len(ydf):
        # One row per (condition, element, metabolite). A metabolite reached by two of a
        # condition's reactions is one expectation, not two -- and if the two disagree
        # the condition says nothing about it, which is a null claim rather than a
        # coin-flip.
        agg = (ydf.groupby(["condition_id", "element", "mnxm"], sort=True)
               ["expected_dir"].agg(lambda v: v.iloc[0] if v.nunique() == 1 else ""))
        ydf = agg.reset_index()
        n_conflict = int((ydf["expected_dir"] == "").sum())
        ydf = ydf[ydf["expected_dir"] != ""].copy()
        ydf["basis"] = "reaction_product"
        ydf["tier"] = "mechanical"
        ydf = ydf[list(Y_COLS)]
    else:
        n_conflict = 0
    (d / "Y").mkdir(parents=True, exist_ok=True)
    ydf.to_csv(d / "Y" / "expectations.tsv", sep="\t", index=False)
    (d / "Y" / "DEFAULT.md").write_text(
        "# The sparse default\n\n"
        "Any `(condition_id, element, mnxm)` triple ABSENT from `expectations.tsv` is "
        "`expected_dir=0`. Storing the zeros would be "
        f"{len(cdf['condition_id'].unique()) * len(els) * 1000:,}-ish rows of "
        "nothing.\n\n"
        "A condition whose measured direction is UNKNOWN contributes NO rows at all -- "
        "it is not a row of zeros. \"We don't know\" and \"we expect no movement\" are "
        "different claims; collapsing them scores ignorance as a correct null.\n\n"
        "Nothing on this file's input path is an ECSPr result. It reads the paper's "
        "extraction and the compiled atom-pair table, which states which atom of which "
        "metabolite becomes which -- structure, not a score.\n")
    print(f"[study] {study:11s} Y: {len(ydf):,} expectations over "
          f"{ydf['mnxm'].nunique() if len(ydf) else 0:,} metabolites, "
          f"{n_conflict:,} dropped as self-contradictory", flush=True)

    kinds = sorted(set(cdf["control_kind"]) - {""})
    print(f"[study] {study:11s} {len(gpr):>6,} gpr rows  "
          f"{gpr['condition_id'].nunique():>4,} conditions x {'+'.join(els)}  "
          f"{named['mnxr'].nunique():>5,} MNXR  "
          f"{len(in_base):>4,} in base  {len(covered):>4,} lane-reachable  "
          f"controls {kinds}", flush=True)

    attribution = "per observation" if spec["reader"] == "obs_mnxr" else "per gene"
    # Empty for a study on the full element set, so those READMEs are unchanged by the
    # narrowing having become expressible at all.
    narrowed = "" if len(els) == len(ELEMENTS) else (
        f"Scored on **{'+'.join(els)} only**. The paper's readout is a named compound "
        f"rather than growth, so its measured direction is a claim about that element and "
        f"about no other; replicating it onto the rest would assert directions nobody "
        f"measured, and nothing downstream could tell those apart from the one that was.\n\n")
    (d / "README.md").write_text(
        f"# `{study}`\n\n"
        f"Cohort `{spec['cohort']}`, arm `{spec['arm']}`, read against host "
        f"`{host}`.\n\n"
        f"`extraction.tsv` is the human reading of the paper and is the PRIMARY "
        f"artifact -- everything else here is a projection of it. It is copied as bytes "
        f"from the acquisition; nothing recomputes it, because there is no transform "
        f"that turns a PDF supplement into rows.\n\n"
        f"| | |\n|---|---|\n"
        f"| gpr rows | {len(gpr):,} |\n"
        f"| conditions | {gpr['condition_id'].nunique():,} x {len(els)} element"
        f"{'s' if len(els) > 1 else ''} |\n"
        f"| distinct MNXR | {named['mnxr'].nunique():,} |\n"
        f"| already in the {host} background | {len(in_base):,} |\n"
        f"| reachable through the de-novo bridge | {len(covered):,} |\n"
        f"| controls | {', '.join(kinds)} |\n\n"
        f"{narrowed}"
        f"Reaction attribution is `{attribution}`. Where the extraction resolved "
        f"reactions to the "
        f"observation rather than to a gene, `orf` names the gene SET and `feature_kind` "
        f"is `curated_set`: splitting the list across the observation's genes would "
        f"manufacture an attribution the curator never made.\n\n"
        f"Coverage is REPORTED, never enforced. A study whose edges the annotation lanes "
        f"cannot see is a finding about the method, which is what the benchmark exists "
        f"to measure.\n")

    summary.append(dict(study=study, **spec, y_rows=len(ydf), gpr_rows=len(gpr),
                        conditions=int(gpr["condition_id"].nunique()),
                        mnxr=int(named["mnxr"].nunique()),
                        unresolved_rows=int(gpr["mnxr"].isna().sum()),
                        in_base=len(in_base), lane_reachable=len(covered),
                        control_kinds=kinds))

# `universe` beside `bake` for the same reason host_gpr_gem carries it: which reactions
# the benchmark was willing to count is not recoverable from the bake identity, and a
# control labelled against one universe means something else against another.
(OUT / "BUILD.json").write_text(json.dumps(
    dict(bake=ident, universe=u_stats, channel=CHANNEL, elements=list(ELEMENTS),
         studies=summary), indent=2))
print(f"[study] {len(summary)} studies -> {OUT}/<study>/", flush=True)
