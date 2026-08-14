"""B3 -- the per-study evaluation set: one folder per publication, one file schema.

Replaces the single all-cohorts condition GPR. Per-study folders mean per-study tables,
so a study's edges can be revised without rebuilding the others -- which is the whole
reason the cut is by publication rather than by processing stage. The join the single
table protected still holds and moves inside each study: A CONDITION MAY ONLY BE EMITTED
IF THAT STUDY'S GPR TABLE KNOWS THE EDGE SET IT NAMES. Building the two independently
and joining later produces a conditions table that looks complete and silently drops
rows at scoring time.

    <study>/extraction.tsv       the human reading of the paper, copied verbatim
    <study>/gpr_manual.parquet   its edges, on the host GPR schema + 4 condition columns
    <study>/conditions.tsv       how those rows compose onto a host, and the controls
    <study>/Y/expectations.tsv   the answer key, per (condition, element, metabolite)
    <study>/README.md            what the cohort is and what its acquisition was

Y IS KEYED ON METABOLITES, NOT EDGES, because the measurement it is scored against
changed. The probe used to report a conductance between two chosen terminals, so an
expectation could only be about an edge on a shared panel; universal leakage reports an
effective current for EVERY metabolite, so an expectation is about a metabolite. Storage
stays sparse under one declared default: any (condition, element, metabolite) triple
absent from the table is `0`.

NOTHING ON Y'S INPUT PATH MAY BE AN ECSPr RESULT. A key derived from the implementation
cannot fail, and an expectation set to "whatever the incumbent returns" is not an
expectation. The line is precise and the loose version is wrong: Y may read the
network's STRUCTURE -- which metabolites a reaction's atoms flow into is a fact about
the model -- but never a RESULT. `audit_y_inputs` enforces that mechanically over the
paths this step opens rather than by convention.

THE SHARED SCHEMA IS THE ANSWER TO "HOW DOES A CONDITION COMPOSE ONTO A HOST". The host's
rows are the background, a condition's `action=add` rows are appended and its
`action=del` rows removed, and no reshaping step stands between them. A network is never
stored -- each x in X is constructed at solve time from the conditions table plus the two
GPR tables, which is what keeps the derivation expressed rather than frozen.

WHERE THE CURATOR ATTRIBUTED PER GENE, SO DOES THE TABLE; WHERE THEY DID NOT, IT SAYS SO.
The LASER extraction resolves reactions per OBSERVATION -- `genes_json` names each gene's
action but no gene carries its own MNXR -- so those rows are `feature_kind=curated_set`
with a null `feature_id`. Splitting an observation's reaction list across its genes would
manufacture an attribution the curator never made, and every downstream per-gene count
would then be reading an invention.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
extract   = model.AddRequirement(lib.GetType("bench::study_extractions"))
bridge    = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
# `in_atom_universe` is a fact about the BAKE, so the bake is an input -- same argument
# as benchmark/host_gpr_gem.py. A guessed value is worse than an absent one.
pairs     = model.AddRequirement(lib.GetType("ref::atom_pairs"))
vocab     = model.AddRequirement(lib.GetType("ref::metabolism_vocab"))
direction = model.AddRequirement(lib.GetType("ref::direction_ratios"))
# The transport flag, which the bake does not carry: `ref::atom_pairs` says which
# reactions were MAPPED, not which of them the benchmark counts. Read from MetaNetX's own
# reac_prop.tsv rather than threaded through `ref::mnxr_lookup`, whose schema several
# consumers read.
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
# The same universe definition host_gpr_gem uses, shared rather than inlined twice: a
# condition's rows and a host's rows CONCATENATE, so the two cannot be allowed to mean
# different things by `in_atom_universe`.
universe_m = model.AddRequirement(lib.GetType("buildlib::bench_universe.py"))
hosts_gem = model.AddRequirement(lib.GetType("ref::gpr_table_gem"))
out       = model.AddProduct(lib.GetType("bench::study_benchmark"))

CHANNEL = "manual_gpr"

# The 14 host columns, so a condition's rows and a host's rows concatenate without
# reshaping, plus the four that scope a row to a condition.
GPR_COLS = (
    "build_id", "host", "unit_id", "feature_id", "feature_kind", "feature_name",
    "mnxr", "channel", "evidence_id", "evidence_name", "raw_score",
    "projection_via", "in_atom_universe", "gpr_rule",
    "condition_id", "cohort", "action", "source_organism",
)

# Y's own schema. `basis` says HOW the metabolite was derived and `tier` how much the
# claim is worth, because a mechanically-derived product and a curator-named pathway
# endpoint are different strengths of evidence and averaging them hides that.
Y_COLS = ("condition_id", "element", "mnxm", "expected_dir", "basis", "tier")

# Files Y is allowed to open. Anything else -- a solve output, a scored matrix, an
# analysis report -- makes the key unfalsifiable. Checked by name at build time.
Y_INPUT_NAMES = ("extraction.tsv", "atom_pairs.parquet", "vocab.parquet",
                 "gpr_manual.parquet")

CONDITION_COLS = (
    "study", "condition_id", "cohort", "arm", "tier", "host", "element",
    "n_add", "n_del", "is_control", "control_kind", "read_against",
    "measured_dir", "citation", "note",
)

# A condition is scored PER ELEMENT, because the atom-resolved network is built per
# element and a condition's expected direction is an element-specific claim.
ELEMENTS = ("C", "N", "P", "S")

# The seven studies, and how each one's extraction is shaped. A fact in this file for the
# same reason the host set is a fact in acquire/genomes.py: a caller who could pass a
# different study set could make two runs of "the benchmark" mean different things.
#
# `reader` names the extraction's shape, not the paper:
#   obs_mnxr  -- one row per observation, reactions in add_mnxr/del_mnxr list columns
#   gene_del  -- one row per knocked-out gene
#   gene_row  -- one row per (gene, reaction) claim, already attributed
STUDIES = {
    "laser":      dict(reader="obs_mnxr", cohort="gof",      arm="gof", host="e_coli_k12"),
    "keio":       dict(reader="gene_del", cohort="lof",      arm="lof", host="e_coli_k12"),
    "eydallin":   dict(reader="gene_del", cohort="eydallin", arm="lof", host="e_coli_k12"),
    "aromatic":   dict(reader="gene_row", cohort="aromatic", arm="gof", host="e_coli_epi300"),
    "pg_anionic": dict(reader="gene_row", cohort="pg_anionic", arm="gof", host="e_coli_epi300"),
    "forsberg":   dict(reader="gene_row", cohort="forsberg", arm="gof", host="e_coli_epi300"),
    "fa_supply":  dict(reader="gene_row", cohort="fa_supply", arm="gof", host="e_coli_epi300"),
}

# THREE KINDS OF CONTROL, because each catches a different failure, and they are
# DECLARED per study rather than inferred.
#
#   baseline    the unperturbed host. The zero point every result is a difference from.
#   structural  a perturbation that cannot reach the network -- must return zero.
#   on_path     a perturbation known to matter -- must return large.
#
# The structural control's reachability test is the one to be careful with. The prior
# implementation compared ENDPOINT PAIRS, so a reaction adding only parallel edges read
# as unreachable: 27 real effects were relabelled controls, manufacturing a noise floor
# out of signal. Here a structural control is a reaction with NO atom-pair coverage --
# it cannot carry an edge at all, which is a property of the reaction rather than of a
# path between two chosen nodes.
CONTROL_KINDS = ("baseline", "structural", "on_path", "declared")

DRIVER = r'''
import json, os, sys
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname("{universe_m}"))
import bench_universe as bu

EXTRACT = Path("{extract}")
OUT     = Path("{out}")
HOSTS   = Path("{hosts_gem}") / "hosts"
GPR_COLS = {gpr_cols}
CONDITION_COLS = {condition_cols}
Y_COLS = {y_cols}
ELEMENTS = {elements}
STUDIES = {studies}
CHANNEL = "{channel}"

# ---- the atom universe, decoded straight off the vocab -------------------------------
# Same argument as host_gpr_gem.py: this reads `atom_pairs.rxn`, a plain vocab code, and
# no packed node column -- so the three files being ONE artifact is what has to hold, and
# the encoder's version gate does not apply. The version is carried into BUILD.json.
def bake_identity(path):
    md = pq.read_schema(path).metadata or {{}}
    if b"ecspr_bake" not in md:
        raise SystemExit(f"[study] {{path}} carries no ecspr_bake identity block")
    return md[b"ecspr_bake"]

blocks = {{p: bake_identity(p) for p in ("{vocab}", "{pairs}", "{direction}")}}
if len(set(blocks.values())) != 1:
    raise SystemExit("[study] the three bake files are not one artifact")
ident = json.loads(next(iter(blocks.values())).decode())
V = pd.read_parquet("{vocab}")
rxn_symbol = V[V["kind"] == "rxn"].set_index("code")["symbol"]
transport = bu.transport_mnxrs(bu.reac_prop_path("{metanetx}"))
universe, u_stats = bu.atom_universe(V, "{pairs}", exclude=transport)
print(bu.universe_line(u_stats, "study") +
      f"  (bake v{{ident['bake_version']}})", flush=True)

# Which reactions the de-novo lanes could even nominate -- used to REPORT reach per
# study, never to filter. A study whose edges the lanes cannot see is a finding about
# the method, which is the thing the benchmark exists to measure.
reachable = set(pd.read_parquet("{bridge}", columns=["mnxr"])["mnxr"].unique())

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
P = pd.read_parquet("{pairs}", columns=["element", "rxn", "head_met"])
met_symbol = V[V["kind"] == "met"].set_index("code")["symbol"]
ELEMENT_ORDER = list(ident["element_order"])
P["el"] = P["element"].map(lambda i: ELEMENT_ORDER[int(i)]
                           if 0 <= int(i) < len(ELEMENT_ORDER) else None)
P["rxn_s"] = P["rxn"].map(rxn_symbol)
P["met_s"] = P["head_met"].map(met_symbol)
P = P[P["rxn_s"].isin(universe)]
products_of = {{}}
for (rx, el), grp in P.dropna(subset=["el", "rxn_s", "met_s"]).groupby(["rxn_s", "el"]):
    products_of.setdefault(rx, {{}})[el] = sorted(set(grp["met_s"]))
print(f"[study] product map: {{len(products_of):,}} reactions over "
      f"{{ELEMENT_ORDER}}", flush=True)

# Each host's own reaction set, so a condition can be told whether the edge it names is
# ALREADY in the background network. That distinction is what separates an addition from
# a dosage change, and getting it wrong is how an in-base addition becomes a silent
# no-op: a reaction the host already carries must enter as a PARALLEL copy, never as an
# overwrite. This project has hit that once already.
host_mnxr = {{}}
for d in sorted(p for p in HOSTS.glob("*") if p.is_dir()):
    g = pd.read_parquet(d / "gpr_gem.parquet", columns=["mnxr"])
    host_mnxr[d.name] = set(g["mnxr"].unique())
print(f"[study] host background: "
      f"{{ {{h: len(v) for h, v in host_mnxr.items()}} }}", flush=True)


def split_ids(cell):
    if cell is None or (isinstance(cell, float) and np.isnan(cell)):
        return []
    return [x.strip() for x in str(cell).replace(";", ",").split(",") if x.strip()]


def rows_obs_mnxr(df, spec):
    """One row per observation, reactions in add_mnxr / del_mnxr list columns.

    feature_id is NULL and feature_kind is `curated_set`: the extraction attributes
    reactions to the OBSERVATION, not to a gene within it. See the module docstring.
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
                    feature_id=None, feature_kind="curated_set", feature_name=genes,
                    mnxr=mnxr, evidence_id=cid, evidence_name=genes,
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
        cid = str(r.get("obs_id") or "").strip() or f"{{spec['cohort']}}:{{gene}}"
        mnxrs = split_ids(r.get("del_mnxr")) or [None]
        for mnxr in mnxrs:
            out.append(dict(
                feature_id=gene, feature_kind="curated_gene",
                feature_name=str(r.get("function_supplTableS1") or r.get("subsystem") or ""),
                mnxr=mnxr, evidence_id=cid,
                evidence_name=str(r.get("b_number") or r.get("gene_norm") or ""),
                condition_id=cid, action="del", source_organism="",
                # THE LOF ARM IS UNIFORM. A knockout removes a route; that is a
                # CONDUCTANCE claim, not a growth claim, and it never was.
                measured="down",
                citation=str(r.get("doi") or r.get("citation") or ""),
                note=str(r.get("note") or r.get("phenotype") or ""),
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
    bad_el = sorted(set(df["element"]) - set(elements) - {{""}})
    bad_dir = sorted(set(df["expected_dir"]) - {{"+", "-", "0", ""}})
    if bad_el or bad_dir:
        raise SystemExit(
            f"[study] {{src}}: after repairing {{gaps}} short row(s), element carries "
            f"{{bad_el}} and expected_dir carries {{bad_dir}}. The columns are still not "
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
        cid = f"{{spec['cohort']}}:{{gene}}:{{i}}"
        out.append(dict(
            feature_id=gene, feature_kind="curated_gene",
            feature_name=cell(r, "ec"),
            mnxr=mnxr, evidence_id=cid, evidence_name=cell(r, "ec"),
            condition_id=cid, action="del" if role == "del" else "add",
            source_organism="",
            # These tables carry no separate `measured` column: `expected_dir` IS the
            # curator's reading of what the paper reported, not a prediction someone
            # made about the pipeline. That is the same rule, not an exception to it.
            measured={{"+": "up", "-": "down", "0": "flat"}}.get(
                cell(r, "expected_dir"), "unknown"),
            citation=cell(r, "citation"),
            note=cell(r, "note"),
        ))
    return out


def declared_controls(df, spec, study):
    """`role=control` rows, kept as declared controls rather than dropped."""
    out = []
    for i, r in df.iterrows():
        if cell(r, "role") != "control":
            continue
        out.append(dict(
            condition_id=f"{{study}}:CTRL:{{cell(r, 'gene') or i}}",
            element=cell(r, "element"), note=cell(r, "note"),
            citation=cell(r, "citation"),
            measured_dir=cell(r, "expected_dir"),
        ))
    return out


READERS = {{"obs_mnxr": rows_obs_mnxr, "gene_del": rows_gene_del,
            "gene_row": rows_gene_row}}

summary = []
for study, spec in sorted(STUDIES.items()):
    src = EXTRACT / study / "extraction.tsv"
    if not src.exists():
        raise SystemExit(f"[study] no extraction for {{study}} at {{src}}")
    if spec["reader"] == "gene_row":
        df, gaps = read_ragged(src, ELEMENTS)
        if gaps:
            print(f"[study] {{study}}: repaired {{gaps}} short row(s) in the extraction "
                  f"-- three tabs where the header wants four columns", flush=True)
    else:
        df = pd.read_csv(src, sep="\t", dtype=str, keep_default_na=False,
                         na_values=[""])
    raw = READERS[spec["reader"]](df, spec)
    declared = declared_controls(df, spec, study) if spec["reader"] == "gene_row" else []

    d = OUT / study
    d.mkdir(parents=True, exist_ok=True)
    # Copied as BYTES. A re-extraction that disagreed with the deployed one would move
    # the benchmark without failing anything.
    (d / "extraction.tsv").write_bytes(src.read_bytes())

    host = spec["host"]
    background = host_mnxr.get(host, set())
    gpr = pd.DataFrame(raw)
    if gpr.empty:
        raise SystemExit(f"[study] {{study}}: the extraction produced no rows")
    gpr["build_id"] = f"manual_{{study}}"
    gpr["host"] = host
    gpr["unit_id"] = study
    gpr["channel"] = CHANNEL
    # A curator asserts that an edge IS claimed, not how strongly -- the same argument
    # the GEM table's uniform 1.0 rests on. The evidence-weighted line is gpr_denovo.
    gpr["raw_score"] = np.float32(1.0)
    gpr["projection_via"] = "curated"
    gpr["in_atom_universe"] = gpr["mnxr"].isin(universe)
    gpr.loc[gpr["mnxr"].isna(), "in_atom_universe"] = None
    gpr["cohort"] = spec["cohort"]
    # No boolean rule: a curated claim is per gene or per observation, and inventing one
    # would make this look like the same kind of claim a GEM makes.
    gpr["gpr_rule"] = None
    meta = gpr[["condition_id", "measured", "citation", "note"]].drop_duplicates(
        subset=["condition_id"])
    gpr = gpr[list(GPR_COLS)]
    gpr = gpr.sort_values(["condition_id", "action", "feature_id", "mnxr"],
                          kind="mergesort", na_position="last").reset_index(drop=True)
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
        for el in ELEMENTS:
            conds.append(dict(
                study=study, condition_id=cid, cohort=spec["cohort"], arm=spec["arm"],
                tier="primary", host=host, element=el,
                n_add=n_add, n_del=n_del,
                is_control=int(structural), control_kind="structural" if structural else "",
                read_against=f"{{study}}:BASELINE",
                # DIRECTION COMES FROM WHAT WAS MEASURED, never from what someone
                # predicted -- `expected` is populated on 10 of 382 GOF rows, so using it
                # would silently shrink the answer key to the cases already anticipated.
                # "unknown" carries no direction: "we don't know" and "we expect no
                # movement" are different claims and are stored differently.
                measured_dir={{"up": "+", "down": "-", "flat": "0"}}.get(measured, ""),
                citation=(m["citation"].iloc[0] if len(m) else ""),
                note=(m["note"].iloc[0] if len(m) else ""),
            ))

    # The unperturbed host. Declared, not inferred: it is the zero point every result in
    # this study is a difference from, and a study without one has nothing to diff.
    for el in ELEMENTS:
        conds.append(dict(
            study=study, condition_id=f"{{study}}:BASELINE", cohort=spec["cohort"],
            arm=spec["arm"], tier="control", host=host, element=el,
            n_add=0, n_del=0, is_control=1, control_kind="baseline", read_against="",
            measured_dir="0", citation="", note="the unperturbed host network"))

    # The curator's OWN declared controls. Emitted only for the element the curator
    # named, because a control declared on the P axis says nothing about C.
    for c in declared:
        for el in ([c["element"]] if c["element"] in ELEMENTS else list(ELEMENTS)):
            conds.append(dict(
                study=study, condition_id=c["condition_id"], cohort=spec["cohort"],
                arm=spec["arm"], tier="control", host=host, element=el,
                n_add=0, n_del=0, is_control=1, control_kind="declared",
                read_against=f"{{study}}:BASELINE", measured_dir=c["measured_dir"],
                citation=c["citation"], note=c["note"]))

    # An ON-PATH control: the study's own edge with the most atom-pair coverage in the
    # background network. It must return large, and picking it from the study's own rows
    # rather than from a fixed list keeps it meaningful when the study is revised.
    on_path = named[named["in_atom_universe"].astype(bool) & named["mnxr"].isin(background)]
    if len(on_path):
        pick = sorted(on_path["mnxr"].unique())[0]
        for el in ELEMENTS:
            conds.append(dict(
                study=study, condition_id=f"{{study}}:ONPATH", cohort=spec["cohort"],
                arm=spec["arm"], tier="control", host=host, element=el,
                n_add=0, n_del=1, is_control=1, control_kind="on_path",
                read_against=f"{{study}}:BASELINE", measured_dir="-", citation="",
                note=f"deleting {{pick}}, an in-base atom-mapped edge -- must move"))

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
        base = {{"up": "+", "down": "-", "flat": "0"}}[measured]
        for _, r in g.iterrows():
            mnxr = r["mnxr"]
            if mnxr is None or (isinstance(mnxr, float) and np.isnan(mnxr)):
                continue
            for el, heads in products_of.get(mnxr, {{}}).items():
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
        f"{{len(cdf['condition_id'].unique()) * len(ELEMENTS) * 1000:,}}-ish rows of "
        "nothing.\n\n"
        "A condition whose measured direction is UNKNOWN contributes NO rows at all -- "
        "it is not a row of zeros. \"We don't know\" and \"we expect no movement\" are "
        "different claims; collapsing them scores ignorance as a correct null.\n\n"
        "Nothing on this file's input path is an ECSPr result. It reads the paper's "
        "extraction and the compiled atom-pair table, which states which atom of which "
        "metabolite becomes which -- structure, not a score.\n")
    print(f"[study] {{study:11s}} Y: {{len(ydf):,}} expectations over "
          f"{{ydf['mnxm'].nunique() if len(ydf) else 0:,}} metabolites, "
          f"{{n_conflict:,}} dropped as self-contradictory", flush=True)

    kinds = sorted(set(cdf["control_kind"]) - {{""}})
    print(f"[study] {{study:11s}} {{len(gpr):>6,}} gpr rows  "
          f"{{gpr['condition_id'].nunique():>4,}} conditions x {{len(ELEMENTS)}} elements  "
          f"{{named['mnxr'].nunique():>5,}} MNXR  "
          f"{{len(in_base):>4,}} in base  {{len(covered):>4,}} lane-reachable  "
          f"controls {{kinds}}", flush=True)

    attribution = "per observation" if spec["reader"] == "obs_mnxr" else "per gene"
    (d / "README.md").write_text(
        f"# `{{study}}`\n\n"
        f"Cohort `{{spec['cohort']}}`, arm `{{spec['arm']}}`, read against host "
        f"`{{host}}`.\n\n"
        f"`extraction.tsv` is the human reading of the paper and is the PRIMARY "
        f"artifact -- everything else here is a projection of it. It is copied as bytes "
        f"from the acquisition; nothing recomputes it, because there is no transform "
        f"that turns a PDF supplement into rows.\n\n"
        f"| | |\n|---|---|\n"
        f"| gpr rows | {{len(gpr):,}} |\n"
        f"| conditions | {{gpr['condition_id'].nunique():,}} x {{len(ELEMENTS)}} elements |\n"
        f"| distinct MNXR | {{named['mnxr'].nunique():,}} |\n"
        f"| already in the {{host}} background | {{len(in_base):,}} |\n"
        f"| reachable through the de-novo bridge | {{len(covered):,}} |\n"
        f"| controls | {{', '.join(kinds)}} |\n\n"
        f"Reaction attribution is `{{attribution}}`. Where the extraction resolved "
        f"reactions to the "
        f"observation rather than to a gene, `feature_id` is null and `feature_kind` is "
        f"`curated_set`: splitting the list across the observation's genes would "
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
print(f"[study] {{len(summary)}} studies -> {{OUT}}/<study>/", flush=True)
'''


# Anything that looks like a solve output, a scored matrix or an analysis report. A key
# derived from the implementation cannot fail, so the refusal is by NAME over the paths
# this step actually opens rather than by convention -- the deployed builder enforced the
# same rule the same way, and it is the only reason to trust the key at all.
Y_FORBIDDEN_MARKS = ("solve", "score", "ieff", "conductance", "baseline", "axes",
                     "result", "report", "ecspr_out")


def audit_y_inputs(context) -> list:
    """Every path this step opens, checked against the rule that Y may read STRUCTURE
    but never a RESULT. Structure is a fact about the model; a score is the thing under
    test."""
    opened = {
        "extraction": str(context.Input(extract).container),
        "atom_pairs": str(context.Input(pairs).container),
        "vocab": str(context.Input(vocab).container),
        "direction": str(context.Input(direction).container),
        "bridge": str(context.Input(bridge).container),
        "hosts_gem": str(context.Input(hosts_gem).container),
        # The transport flag and the module that reads it. Listed because the audit is
        # over the paths this step OPENS -- a new input that skips the list makes the
        # check narrower without making it fail.
        "metanetx": str(context.Input(metanetx).container),
        "bench_universe": str(context.Input(universe_m).container),
    }
    bad = []
    for name, path in opened.items():
        low = path.lower()
        hit = [m for m in Y_FORBIDDEN_MARKS if m in low]
        if hit:
            bad.append(f"{name} at {path} matches {hit}")
    return bad


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    tainted = audit_y_inputs(context)
    if tainted:
        raise SystemExit(
            "Y's input path reads something that is or may be an ECSPr RESULT:\n  "
            + "\n  ".join(tainted)
            + "\nA key derived from the implementation cannot fail. Y may read the "
              "network's structure -- which metabolites a reaction's atoms flow into is "
              "a fact about the model -- but never a score.")
    driver = DRIVER.format(
        extract=context.Input(extract).container,
        bridge=context.Input(bridge).container,
        vocab=context.Input(vocab).container,
        pairs=context.Input(pairs).container,
        direction=context.Input(direction).container,
        hosts_gem=context.Input(hosts_gem).container,
        metanetx=context.Input(metanetx).container,
        universe_m=context.Input(universe_m).container,
        gpr_cols=repr(GPR_COLS), condition_cols=repr(CONDITION_COLS),
        y_cols=repr(Y_COLS),
        elements=repr(ELEMENTS), studies=repr(STUDIES), channel=CHANNEL,
        out=iout.container,
    )
    context.LocalShell("cat > _study_tier.py << 'PYEOF'\n" + driver + "\nPYEOF\n")
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd="python3 _study_tier.py") \
        .ifVirtualEnvDo(env=image, cmd="python3 _study_tier.py")

    # A folder missing a file is a named failure; a folder with an extra file is a
    # schema violation. Both are checked, because a schema nothing enforces is a
    # convention.
    WANT = {"extraction.tsv", "gpr_manual.parquet", "conditions.tsv", "README.md", "Y"}
    problems = []
    for study in STUDIES:
        d = iout.local / study
        if not d.is_dir():
            problems.append(f"{study}: no folder")
            continue
        have = {p.name for p in d.glob("*")}
        if have - WANT:
            problems.append(f"{study}: unexpected {sorted(have - WANT)}")
        if WANT - have:
            problems.append(f"{study}: missing {sorted(WANT - have)}")
    for p in problems:
        Log.Error(f"study_tier: {p}")
    Log.Info(f"study_tier: {len(STUDIES) - len({p.split(':')[0] for p in problems})}"
             f"/{len(STUDIES)} studies clean")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=not problems and (iout.local / "BUILD.json").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=extract,
    labels=["local"],
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=1)),
)
