"""B3 -- ONE GPR table over every benchmark condition.

Carries the edges each condition adds or deletes, across four cohorts:
gof_native, gof_het, lof, eydallin. Together with the per-host GPR tables it holds
every edge any condition needs, which is what lets each x in X be CONSTRUCTED at
run time from the conditions table instead of being stored as a network.

SEVEN ROUTES, LAYERED, NOT ONE. This step reached 203 of 845 entries for as long as it
existed, because it re-derived every edge from a single route -- gene label to UniProt
accession to the bridge -- while the reaction ids the benchmark was curated with sat
unread in the extraction's own `add_mnxr` / `del_mnxr` columns and LASER's own
gene-to-reaction table sat unread beside them. `buildlib::bench_edges.py` holds the
routes and the layering; what belongs here is which givens they read and what the
emitted table looks like.

A LOWER ROUTE CLAIMS ONLY WHAT NOTHING ABOVE IT CLAIMED, and every route that
independently produced an edge is still recorded in `agreed_by`. Precedence decides who
supplies an edge; agreement is the accuracy evidence and would be erased by precedence if
it were not written down separately.

The per-protein belief weight is the deployed scheme and is not reinvented here:
each protein carries a total belief of 1.0 split equally across the lanes that
annotated it, so a protein resolved by one lane and one resolved by three carry
identical mass -- the contrast downstream measures topology, not how many lanes
happened to fire. Within a lane the share splits across nominations by raw score,
then spreads evenly across each nomination's fanout. Fanout is left uncapped:
dilution is the designed answer to promiscuous EC fanout, and capping is a
scoring-time policy that does not belong in an annotation artifact.

THE WEIGHT IS COMPUTED PER ROUTE, OVER THAT ROUTE'S OWN OFFER, and that is what keeps it
comparable to what shipped. An accession's fanout is a property of the accession, so
computing it over the edges the accession offered -- rather than over the subset a
stronger route left it -- means the `bridge_uniprot` numbers here are the numbers this
step has always emitted. Routes are not summed against each other; each conserves 1.0 per
unit on its own, asserted.

EVERY COHORT ROW IS EMITTED, with or without a reaction. A row that reached no route
carries a null `mnxr` and a `route` saying which kind of nothing it is: `host_gpr` for a
native gene whose edges are the host's and are resolved at network construction,
`unresolved_heterologous` for a foreign protein nothing resolved, `unresolved` otherwise.
An absent row would make a condition look edge-less rather than host-resolved, and it is
also how 11 entries used to vanish while the guard downstream printed `0 dropped`.

PROVISIONAL contract -- the literature inputs are confirmed while porting
benchmark_v4.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
# THE EXTRACTIONS, not the downloads. What the cohort loaders and the curated route read
# is the per-study `extraction.tsv` this tree normalised all seven studies onto, which is
# one folder and one schema.
extracts  = model.AddRequirement(lib.GetType("bench::study_extractions"))
het       = model.AddRequirement(lib.GetType("raw::het_screen_records"))
# The upstream LASER checkout, for `inputs/Gene-Reaction Pairings.txt` alone: 3,370 rows
# of gene, species and MetaCyc reaction frame id, which is the one route here that is
# neither a curator's assignment nor a projection through an annotation id space.
laser     = model.AddRequirement(lib.GetType("raw::laser_records"))
bridge    = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
# The reaction universe an emitted edge has to be a member of. Read from MetaNetX's own
# reac_prop.tsv rather than off the bridge: the bridge holds only reactions carrying an
# EC, KO, MetaCyc or UniProt key, so checking membership against it fails 151 curated ids
# that MetaNetX defines perfectly well. Same file supplies the transport flag.
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
# The host's own curated model, which is what makes a native gene's reactions readable
# HERE rather than only at network construction. Y is derived by walking an entry's
# reactions, so "resolved against the host GPR later" and "has no answer key" are the same
# state -- and that is why Eydallin's expectation table is empty.
hosts_gem = model.AddRequirement(lib.GetType("ref::gpr_table_gem"))
weights   = model.AddRequirement(lib.GetType("buildlib::bench_evidence_weights.py"))
cohorts_m = model.AddRequirement(lib.GetType("buildlib::bench_cohorts.py"))
edges_m   = model.AddRequirement(lib.GetType("buildlib::bench_edges.py"))
universe_m = model.AddRequirement(lib.GetType("buildlib::bench_universe.py"))
out       = model.AddProduct(lib.GetType("bench::condition_gpr"))

# The four arms. gof_het is the heterologous / metagenomic screening arm and is the only
# one whose proteins are not the host's own, which is why it is the only one that can
# project through the bridge's accession slice at all.
COHORTS = ("gof_native", "gof_het", "lof", "eydallin")

DRIVER = r'''
import os, sys
import pandas as pd

sys.path.insert(0, os.path.dirname("{lib}"))
from bench_cohorts import load_all_cohorts, is_native
from bench_evidence_weights import nomination_contributions, assert_conservation
import bench_edges as be
import bench_universe as bu

genes = load_all_cohorts(laser="{extracts}/laser", keio="{extracts}/keio",
                         eydallin="{extracts}/eydallin", het="{het}")
print(f"[cond_gpr] {{len(genes):,}} cohort entries over "
      f"{{genes['cohort'].nunique()}} cohorts", flush=True)
for c, g in genes.groupby("cohort"):
    print(f"           {{c:12s}} {{len(g):5,}} named, "
          f"{{int(g['uniprot'].notna().sum()):5,}} with a UniProt accession", flush=True)

entries = genes[["cohort", "condition_id"]].drop_duplicates()
print(f"[cond_gpr] {{len(entries):,}} entries "
      f"({{genes['condition_id'].nunique():,}} distinct observations)", flush=True)

# ---- the universe an edge has to be a member of ------------------------------------
reac_prop = bu.reac_prop_path("{metanetx}")
universe  = bu.all_mnxrs(reac_prop)
transport = bu.transport_mnxrs(reac_prop)
print(f"[cond_gpr] MetaNetX defines {{len(universe):,}} reactions, {{len(transport):,}} "
      f"of them transport", flush=True)

# ---- the routes --------------------------------------------------------------------
b = pd.read_parquet("{bridge}", columns=["id", "id_source", "mnxr"])
print(f"[cond_gpr] bridge id spaces {{b['id_source'].value_counts().to_dict()}}", flush=True)
slice_of = lambda s: (b[b["id_source"] == s].groupby("id")["mnxr"].apply(set).to_dict())

pairings = be.load_pairings("{laser}/inputs/Gene-Reaction Pairings.txt")
overrides, ec_over = be.override_edges("{extracts}", entries=genes)
gene_ecs = be.ec_index("{extracts}", pairings, "{het}", ec_overrides=ec_over)
print(f"[cond_gpr] {{len(gene_ecs):,}} gene labels carry a four-level EC", flush=True)

# The literature lane's accessions, if the curation ran one. Auto-accepted -- see
# curate_het_screen.py -- but under their OWN route, because an accession a query
# returned and an accession read out of a paper's methods section are both accessions and
# a reader has to be able to tell which they are looking at.
web_acc = set()
native_acc = {{}}
for _f in ("heterologous_uniprot.tsv", "native_uniprot.tsv"):
    _hp = os.path.join("{het}", _f)
    if not os.path.exists(_hp):
        continue
    _h = pd.read_csv(_hp, sep="\t", dtype=str).fillna("")
    if "reason" in _h.columns:
        web_acc |= set(_h.loc[_h["reason"].str.startswith("web"), "uniprot"]) - {{""}}
    if _f == "native_uniprot.tsv":
        # A host gene the host's own GEM does not carry. Keyed on the label because that
        # is what the cohort frame has -- these rows never had an accession, which is
        # precisely why their entries had no reaction at all.
        native_acc = {{be.norm_name(g): u
                       for g, u in zip(_h["gene"], _h["uniprot"]) if u.strip()}}
print(f"[cond_gpr] {{len(native_acc):,}} native gene labels carry a recovered accession; "
      f"{{len(web_acc):,}} accessions came from the literature lane", flush=True)

# EVERY STUDY IN THIS LANE IS PINNED TO ONE HOST, declared rather than read off
# `host_hint` -- that column carries strain names (MG1655, BW25113, BL21(DE3)) and the
# per-study builder pins laser, keio and eydallin to e_coli_k12. Inferring it per row
# would give two observations of the same study different background chemistry.
HOST = "e_coli_k12"
_gem = os.path.join("{hosts_gem}", "hosts", HOST, "gpr_gem.parquet")
if not os.path.exists(_gem):
    raise SystemExit(
        f"[cond_gpr] {{_gem}} is absent.\\n"
        f"  It is {{HOST}}'s curated GEM GPR table, and it is the only route by which a "
        f"native gene -- the LOF and screen arms in their entirety -- reaches a "
        f"reaction at all.\\n"
        f"  This is a NAMED refusal.")

frames = {{
    "curated":          be.curated_edges("{extracts}", entries=genes),
    "curated_override": overrides,
    "host_gem":         be.host_gem_edges(_gem, entries=genes),
    "metacyc_rxn":      be.metacyc_edges(pairings, slice_of("metacyc"), entries=genes),
    "bridge_uniprot":   be.uniprot_edges(slice_of("uniprot"), entries=genes,
                                         web_accessions=web_acc,
                                         native_accessions=native_acc),
    "bridge_ec":        be.ec_edges(slice_of("ec"), gene_ecs, entries=genes),
    "bridge_ko":        be.ko_edges(slice_of("ko"), {{}}, entries=genes),
}}
# `uniprot_edges` tags literature-derived accessions as `web` rather than emitting them
# twice, so the web route is that frame's own slice.
_u = frames["bridge_uniprot"]
frames["web"] = _u[_u["route"] == "web"]
frames["bridge_uniprot"] = _u[_u["route"] == "bridge_uniprot"]

# ---- the per-route weight, over each route's OWN offer ------------------------------
# Computed BEFORE layering, so an accession's fanout stays a property of the accession
# rather than of what a stronger route happened to leave it. This is what keeps the
# bridge_uniprot weights identical to the ones this step has always emitted.
contrib = {{}}
for route, df in frames.items():
    if not len(df):
        continue
    ev = df.copy()
    # The unit a route normalises over: the accession where it keyed on one, the gene
    # where it keyed on a gene, the observation where the curator attributed to the
    # observation as a whole.
    ev["orf"] = ev["route_key"].astype(str)
    ev["channel"] = route
    ev["intermediate_id"] = ev["route_key"].astype(str)
    # Every route here asserts a mapping rather than scoring a hit, so nominations enter
    # at equal score and the split is by fanout alone. Inventing a score would be
    # inventing evidence strength for a crosswalk.
    ev["raw_score"] = 1.0
    c = nomination_contributions(ev)
    assert_conservation(c, f"condition_gpr/{{route}}")
    contrib[route] = (c.groupby(["orf", "mnxr"], sort=False)["contrib"].sum()
                      .rename("E_full").reset_index())

edges, report = be.layer(frames, universe=universe)
for line in be.report_lines(report, "cond_gpr"):
    print(line, flush=True)

n_transport = int(edges["mnxr"].isin(transport).sum()) if len(edges) else 0
if n_transport:
    per = (edges[edges["mnxr"].isin(transport)]["route"].value_counts().to_dict())
    print(f"[cond_gpr] {{n_transport:,}} emitted edges name a TRANSPORT reaction "
          f"({{per}}). They are kept -- the benchmark's atom universe excludes transport "
          f"at scoring time, and excluding it here would hide a curated assignment "
          f"rather than a scoring policy.", flush=True)

# ---- attach the weight, then emit ---------------------------------------------------
edges["E_full"] = float("nan")
for route, w in contrib.items():
    m = edges["route"] == route
    if not m.any():
        continue
    j = edges.loc[m, ["route_key", "mnxr"]].copy()
    j["route_key"] = j["route_key"].astype(str)
    merged = j.merge(w, left_on=["route_key", "mnxr"], right_on=["orf", "mnxr"],
                     how="left")
    edges.loc[m, "E_full"] = merged["E_full"].to_numpy()

meta_cols = ["cohort", "condition_id", "gene_label", "action", "host_hint",
             "source_organism", "uniprot"]
meta = genes[meta_cols].drop_duplicates()
edges = edges.rename(columns={{"gene_label": "_gl", "action": "edge_action",
                              "action_raw": "action"}})
edges = edges.merge(meta, on=["cohort", "condition_id", "action"], how="left",
                    suffixes=("", "_m"))
# `gene_label` is null on the curated route by construction (the curator attributed to the
# observation, not to a gene); where the join supplied one it is the observation's gene
# set, which is a different claim, so the route's own value wins.
edges["gene_label"] = edges["_gl"].where(edges["_gl"].notna(), edges["gene_label"])
edges = edges.drop(columns=["_gl"]).drop_duplicates()

gpr = pd.DataFrame({{
    "protein_uid": edges["uniprot"].where(edges["uniprot"].notna(),
                                          edges["gene_label"].where(
                                              edges["gene_label"].notna(),
                                              edges["condition_id"])),
    "mnxr": edges["mnxr"],
    "E_full": edges["E_full"],
    "lanes": edges["route"],
    "n_lanes": edges["n_agree"],
    "cohort": edges["cohort"],
    "condition_id": edges["condition_id"],
    "gene_label": edges["gene_label"],
    "action": edges["action"],
    "host_hint": edges["host_hint"],
    "source_organism": edges["source_organism"],
    "route": edges["route"],
    "route_key": edges["route_key"],
    "edge_action": edges["edge_action"],
    "agreed_by": edges["agreed_by"],
    "n_agree": edges["n_agree"],
    "strength": edges["strength"],
    "in_entry_set": edges["in_entry_set"],
}})

# ---- every cohort row that reached NO route, named ----------------------------------
reached = set(zip(gpr["cohort"], gpr["condition_id"], gpr["gene_label"]))
rest = genes[[(c, i, g) not in reached
              for c, i, g in zip(genes["cohort"], genes["condition_id"],
                                 genes["gene_label"])]].copy()
# A native gene's edges ARE the host's, named by symbol and resolved against
# ref::gpr_table_{{gem,denovo}} at network construction; that is a routing fact, not a
# missing value. A heterologous protein has no such fallback -- the host does not carry
# the gene -- so routing it to the host GPR promises an edge set that can never be
# supplied, and it is emitted as an explicit unresolved state instead.
def _kind(cohort, src, acc):
    if is_native(src):
        return "host_gpr"
    return "unresolved_heterologous"
rest["_kind"] = [ _kind(c, s, u) for c, s, u
                  in zip(rest["cohort"], rest["source_organism"], rest["uniprot"]) ]
rest_rows = pd.DataFrame({{
    "protein_uid": rest["uniprot"].where(rest["uniprot"].notna(), rest["gene_label"]),
    "mnxr": None, "E_full": float("nan"),
    "lanes": rest["_kind"], "n_lanes": 0,
    "cohort": rest["cohort"], "condition_id": rest["condition_id"],
    "gene_label": rest["gene_label"], "action": rest["action"],
    "host_hint": rest["host_hint"], "source_organism": rest["source_organism"],
    "route": rest["_kind"], "route_key": rest["gene_label"],
    "edge_action": [be.canonical_action(c, a)
                    for c, a in zip(rest["cohort"], rest["action"])],
    "agreed_by": "", "n_agree": 0, "strength": "", "in_entry_set": False,
}})
print(f"[cond_gpr] {{len(rest_rows):,}} cohort rows reached no route: "
      f"{{rest['_kind'].value_counts().to_dict()}}", flush=True)

allrows = pd.concat([gpr, rest_rows], ignore_index=True)
allrows = allrows.sort_values(["cohort", "condition_id", "protein_uid", "mnxr"],
                              na_position="last").reset_index(drop=True)
allrows.to_parquet("{out}", index=False, compression="zstd")

# ---- what the table reaches, per entry ----------------------------------------------
prim = allrows[allrows["mnxr"].notna() & allrows["in_entry_set"]]
sz = prim.groupby(["cohort", "condition_id"])["mnxr"].nunique()
print(f"[cond_gpr] the AUTHORITATIVE edge set (in_entry_set) is the best route that "
      f"reached each entry: {{len(sz):,}} entries, median {{int(sz.median()) if len(sz) else 0}} "
      f"reactions, max {{int(sz.max()) if len(sz) else 0}}", flush=True)
with_edge = allrows[allrows["mnxr"].notna()][["cohort", "condition_id"]].drop_duplicates()
print(f"[cond_gpr] wrote {{len(allrows):,}} rows "
      f"({{int(allrows['mnxr'].notna().sum()):,}} carry an MNXR directly, "
      f"{{int(allrows['mnxr'].isna().sum()):,}} do not)", flush=True)
print(f"[cond_gpr] ENTRIES WITH >=1 REACTION: {{len(with_edge):,}} / {{len(entries):,}}",
      flush=True)
for c, g in entries.groupby("cohort"):
    n = int((with_edge["cohort"] == c).sum())
    print(f"           {{c:12s}} {{n:5,}} / {{len(g):5,}}", flush=True)

zero = (set(zip(entries["cohort"], entries["condition_id"]))
        - set(zip(with_edge["cohort"], with_edge["condition_id"])))
if zero:
    why = {{}}
    for c, i in sorted(zero):
        k = tuple(sorted(set(allrows[(allrows["cohort"] == c)
                                     & (allrows["condition_id"] == i)]["route"])))
        why.setdefault(k or ("absent",), []).append(f"{{c}}/{{i}}")
    print(f"[cond_gpr] {{len(zero):,}} entries still carry NO reaction. By reason:",
          flush=True)
    for k, v in sorted(why.items(), key=lambda x: -len(x[1])):
        shown = v[:8]
        more = f" and {{len(v) - len(shown):,}} more" if len(v) > len(shown) else ""
        print(f"           {{'+'.join(k):<26}} {{len(v):4,}}  {{shown}}{{more}}", flush=True)

agree = allrows[allrows["mnxr"].notna()]["agreed_by"].value_counts()
print(f"[cond_gpr] route agreement (edges by the set of routes that independently "
      f"produced them):", flush=True)
for k, v in agree.head(12).items():
    print(f"           {{k:<45}} {{v:6,}}", flush=True)
'''


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    driver = DRIVER.format(
        lib=context.Input(weights).container,
        extracts=context.Input(extracts).container,
        het=context.Input(het).container,
        laser=context.Input(laser).container,
        bridge=context.Input(bridge).container,
        metanetx=context.Input(metanetx).container,
        hosts_gem=context.Input(hosts_gem).container,
        out=iout.container,
    )
    context.LocalShell("cat > _condition_gpr.py << 'PYEOF'\n" + driver + "\nPYEOF\n")
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd="python3 _condition_gpr.py") \
        .ifVirtualEnvDo(env=image, cmd="python3 _condition_gpr.py")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=4)),
)
