"""B1 -- the GPR a curated genome-scale model asserts, for every host at once.

ONE JOB OVER THE HOST SET, writing `hosts/<host>/gpr_gem.parquet`. It used to be one
job per model, which was right when the acquisition tier typed one product per file
and a scatter fanned an accession out to three hosts. The tier now delivers the host
set as ONE folder, and per-host jobs would need the planner to split it -- which it
does by binding a requirement to ONE concrete type, so three sibling per-host subtypes
collapse to whichever it picked while the task count still reads correct. Keying inside
the product instead keeps the host set where acquire/genomes.py already declares it.

Deliberately a SEPARATE FILE from the de-novo table rather than merged with it. The two
are independent lines of evidence and the comparison between them is the point;
conflating them into one table destroys the only thing they are jointly good for.

EPI300 BORROWS DH10B'S MODEL, and the borrow happens HERE rather than in the
acquisition. Writing DH10B's JSON a second time under EPI300 would assert a download
that never happened and put identical bytes at two paths claiming two provenances.

AG1 BORROWS DH1'S THE SAME WAY, and needs the borrow more: it has no assembly at NCBI
either, so there is no genome to compare and the licence is the genotype Qimron et al.
state. `check_ag1_identity.py` is that measurement, and unlike EPI300's it is not free
-- see EDIT_LIST.

THE BORROW IS NOT FREE, and earlier work here recorded that it was. `proV` and `fhuA`
are PSEUDOGENES in EPI300 -- both in AND clauses, so seven transport reactions go dark.
That is the whole edit list; nothing else in the model's 1,327 genes differs. It was
missed because a pseudogene is a CDS with no `/protein_id`, so a check keyed on the
protein accession never sees one. `check_epi300_identity.py` is the measurement, and it
asserts this exact list rather than asserting emptiness -- a number that drifts without
failing is the failure mode both directions have already produced here.

Needs cobra to open the model, which is also why this cannot run inside the pinned
ecspr image -- that image carries no cobra.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::cobra.env"))
# The whole host set: genomes/<host>/{genome,GEM}/. One folder, so the host set is a
# fact in acquire/genomes.py rather than a caller's argument.
genomes   = model.AddRequirement(lib.GetType("fabfos_data::genomes"))
bridge    = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
# `in_atom_universe` means "this reaction has atom-pair coverage, so an edge can exist
# for it". That is a fact about the BAKE, so the bake is an input. Without it the column
# could only be guessed, and a guessed `in_atom_universe` is worse than an absent one
# because the consumer trusts it.
pairs     = model.AddRequirement(lib.GetType("ref::atom_pairs"))
vocab     = model.AddRequirement(lib.GetType("ref::metabolism_vocab"))
direction = model.AddRequirement(lib.GetType("ref::direction_ratios"))
# What `in_atom_universe` MEANS for a benchmark row, shared with the study tier so the
# two cannot answer the same question differently. It is the bake's coverage less
# transport; see the module for why the filter is here and not at the bake.
universe_m = model.AddRequirement(lib.GetType("buildlib::bench_universe.py"))
out       = model.AddProduct(lib.GetType("ref::gpr_table_gem"))

CHANNEL = "gem_gpr"

# The frozen 14-column host GPR schema, from the pre-library hosts.py. Shared with the
# de-novo table so the two lines of evidence can be compared column for column -- which is
# the only thing they are jointly good for, and the reason they stay separate files.
GPR_COLS = (
    "build_id", "host", "unit_id", "feature_id", "feature_kind", "feature_name",
    "mnxr", "channel", "evidence_id", "evidence_name", "raw_score",
    "projection_via", "in_atom_universe", "gpr_rule",
)

# host -> the host whose GEM it uses. A host mapping to itself has its own published
# model; a host mapping to another BORROWS it, and the borrow is licensed by a measured
# edit list rather than by convenience. Coupled to acquire/genomes.py's GEM_FOR_HOST.
GEM_SOURCE = {
    "e_coli_k12":    "e_coli_k12",
    "e_coli_dh10b":  "e_coli_dh10b",
    "e_coli_epi300": "e_coli_dh10b",
    "e_coli_dh1":    "e_coli_dh1",
    # AG1 IS THE SECOND BORROW AND IT IS NOT LIKE THE FIRST. The ASKA library lives in
    # AG1, which NCBI does not have at all -- there is no genome to compare, only the
    # genotype Qimron et al. state: a DH1 derivative carrying recA1, endA1, gyrA96,
    # thi-1, hsdR17, supE44 and relA1. `check_ag1_identity.py` measures what those cost
    # the model, and W3110 is deliberately NOT here: it is where a clone's sequence
    # comes from, not a strain this tree reads a model against.
    "e_coli_ag1":    "e_coli_dh1",
}

# host -> the borrowed model's reactions that strain cannot carry, by the MODEL's own
# reaction id. A fact about two genomes, declared here for the same reason the host set
# is declared in acquire/genomes.py: a caller who could pass a different edit list could
# make two runs of "the reference build" mean different things.
#
# EMPTY FOR EPI300, AND THE MEASUREMENT BEHIND IT IS NOT. `proV` and `fhuA` are
# pseudogenes in EPI300 and intact in DH10B, both sit in AND clauses, and seven of the
# model's reactions therefore go dark: PROabcpp, CRNabcpp, CRNDabcpp, CTBTabcpp (the ProU
# osmoprotectant ABC transporter) and FE3HOXtonex, FECRMtonex, FEOXAMtonex (the
# ferrichrome TonB-dependent receptor -- a broken fhuA being the classic
# T1-phage-resistance marker in a cloning strain). check_epi300_identity.py still asserts
# that exact set, because a difference that drifts without failing is the failure mode
# this whole check exists for.
#
# ALL SEVEN ARE TRANSPORT, WHICH IS WHY THE LIST IS EMPTY RATHER THAN THOSE SEVEN. The
# benchmark excludes transport from the atom universe (buildlib::bench_universe.py), so
# all seven are already marked out of universe and carry no edge either way. Editing them
# out would move no measurement while making the two tables differ, so the honest table is
# DH10B's read faithfully under the EPI300 tag.
#
# AG1 IS NOT EMPTY, AND THE CONTRAST IS THE POINT. Its genotype is seven markers and
# `check_ag1_identity.py` measures each against this model: five of them -- recA1, endA1,
# gyrA96, hsdR17, supE44 -- name no gene in it, so they are free. `relA1` names `relA`,
# which carries two reactions: GDPDPK is `relA or spoT` and survives on the isozyme,
# GTPDPK is relA alone and goes dark. One reaction, and it is INSIDE the atom universe,
# so this borrow moves the network where EPI300's could not.
#
# `thi-1` IS DELIBERATELY ABSENT. It is a classical thiamine-auxotrophy allele rather
# than a locus, and the module it lies in is 12 genes over 10 reactions here. Picking one
# would put a fabricated deletion into the background of every eydallin condition, and a
# fabricated deletion is worse than a known gap because nothing downstream can tell.
# What the marker really says is that AG1 needs thiamine in the medium, which is a claim
# about the medium and belongs where the medium is declared.
EDIT_LIST = {
    "e_coli_ag1": ("GTPDPK",),
}

DRIVER = r'''
import json, os, sys
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname("{universe_m}"))
# Both halves come from the installed ecspr: `model` builds the network, `bake`
# reads the compiled tables. Only bench_universe is still a staged flat file.
from ecspr.model.build import crosswalk_gem, load_model
from ecspr.bake import encoding as refs
import bench_universe as bu

GENOMES = Path("{genomes}")
OUT     = Path("{out}")
CHANNEL = "{channel}"
GPR_COLS = {gpr_cols}
GEM_SOURCE = {gem_source}
EDIT_LIST = {edit_list}

# The MetaNetX source folder is <release>/reac_xref.tsv; the release is read off disk
# rather than pinned here, because the acquisition names the directory after what the
# server served and hardcoding a number here would silently target the wrong snapshot.
mnx = Path("{metanetx}")
rels = sorted(p for p in mnx.glob("*") if p.is_dir())
if len(rels) != 1:
    raise SystemExit(f"[gem_gpr] expected one MetaNetX release under {{mnx}}, found "
                     f"{{[p.name for p in rels]}}")
reac_xref = rels[0] / "reac_xref.tsv"
if not reac_xref.exists():
    raise SystemExit(f"[gem_gpr] no reac_xref.tsv under {{rels[0]}}")

# THE IDENTITY ASSERTION IS KEPT AND THE VERSION GATE IS NOT, and the difference is
# exactly what this step reads. `refs.assert_same_bake` refuses any bake whose
# `bake_version` is not the one that encoder speaks, because it is the entry point for
# code that decodes PACKED NODE CODES -- and reading `met_bits`/`rank_bits` from one
# version with another's layout silently resolves every atom to the wrong metabolite.
# This step touches no node column. It reads `atom_pairs.rxn`, a plain code into the
# vocab table, whose (kind, code, symbol) shape is the same in both versions.
#
# What DOES have to hold is that the three files are one artifact, so that is asserted
# directly: their `ecspr_bake` blocks must be byte-identical. The version is carried into
# BUILD.json rather than silently accepted -- a table built against a v1 bake and one
# built against a v2 bake are different claims about the same reactions.
def bake_identity(path):
    md = pq.read_schema(path).metadata or {{}}
    if b"ecspr_bake" not in md:
        raise SystemExit(f"[gem_gpr] {{path}} carries no ecspr_bake identity block")
    return md[b"ecspr_bake"]

blocks = {{p: bake_identity(p) for p in ("{vocab}", "{pairs}", "{direction}")}}
if len(set(blocks.values())) != 1:
    raise SystemExit(
        f"[gem_gpr] the three bake files do not carry the same identity block: "
        f"{{ {{k: v[:64] for k, v in blocks.items()}} }}. They are ONE artifact -- "
        f"reading atom_pairs against another bake's vocab decodes every code to the "
        f"wrong reaction without raising.")
ident = json.loads(next(iter(blocks.values())).decode())

transport = bu.transport_mnxrs(bu.reac_prop_path(mnx))
universe, u_stats = bu.atom_universe("{vocab}", "{pairs}", exclude=transport)
print(bu.universe_line(u_stats, "gem_gpr") +
      f"  (bake v{{ident['bake_version']}}, vocab {{ident['vocab_sha256'][:16]}})",
      flush=True)

reachable = set(pd.read_parquet("{bridge}", columns=["mnxr"])["mnxr"].unique())

def gem_json_for(src_host):
    d = GENOMES / src_host / "GEM"
    js = sorted(d.glob("*.json"))
    if len(js) != 1:
        raise SystemExit(f"[gem_gpr] expected one model JSON under {{d}}, found "
                         f"{{[p.name for p in js]}}")
    return js[0]

# The crosswalk is a pure function of (model, reac_xref, universe), so two hosts sharing
# a model share it exactly. Computed once per SOURCE model rather than once per host --
# not for speed, but so that the EPI300/DH10B tables cannot diverge through two runs of
# the same resolver.
crosswalks = {{}}
summary = []
for host, src_host in sorted(GEM_SOURCE.items()):
    if src_host not in crosswalks:
        path = gem_json_for(src_host)
        m = load_model(str(path))
        xw, stats = crosswalk_gem(m, str(reac_xref), universe)
        crosswalks[src_host] = (m, xw, stats)
        print(f"[gem_gpr] {{src_host}} / {{getattr(m, 'id', '?')}}: "
              f"{{stats['n_reactions']:,}} reactions -> {{stats['n_resolved']:,}} resolved "
              f"({{xw['mnxr'].nunique():,}} distinct MNXR), {{stats['n_unresolved']:,}} "
              f"unresolved, {{stats['n_aam_gap']:,}} outside the atom universe", flush=True)

    m, xw, stats = crosswalks[src_host]
    gem_id = getattr(m, "id", None) or "unknown"
    by_rxn = dict(zip(xw["rxn_id"], zip(xw["mnxr"], xw["source"], xw["in_universe"])))
    build_id = f"gem_{{gem_id}}"
    rows = []
    n_ruleless = 0
    dropped = EDIT_LIST.get(host, ())
    n_dropped = 0
    for r in m.reactions:
        if r.id in dropped:
            # A reaction the model asserts and THIS strain cannot carry. Dropped rather
            # than kept-and-flagged, because every consumer of this table treats a row as
            # "the host has this reaction" -- a flag nobody reads is worse than absence.
            n_dropped += 1
            continue
        hit = by_rxn.get(r.id)
        if hit is None:
            # No candidate MNXR at all: it can never carry an edge, and a row with a null
            # reaction would put an unjoinable key in the table.
            continue
        mnxr, via, in_universe = hit
        rule = (r.gene_reaction_rule or "").strip()
        genes = list(r.genes)
        common = dict(
            build_id=build_id, host=host, unit_id=gem_id, mnxr=mnxr, channel=CHANNEL,
            evidence_id=r.id, evidence_name=r.name or None,
            # A curated model asserts that a reaction is PRESENT, not how much evidence
            # there is for it, so weighting it by anything would be inventing a quantity.
            # The evidence-weighted line is gpr_denovo.
            raw_score=1.0, projection_via=via,
            in_atom_universe=bool(in_universe),
            # CARRIED, not evaluated. A reference table has no perturbation, so evaluating
            # the boolean rule here would bake in one condition -- and the consumer must
            # remember that cobra.GPR.eval takes the KNOCKED-OUT set, not the active one.
            # Handing it the active set inverts the question, and a previous run of that
            # logic reported more reactions live after a knockout than before.
            gpr_rule=rule or None,
        )
        if not genes:
            # Exchanges, diffusion and spontaneous chemistry have no gene to attribute,
            # but they are live in every condition. Dropping them would make every gene
            # set look like starvation.
            n_ruleless += 1
            rows.append(dict(common, feature_id=None, feature_kind="ruleless",
                             feature_name=None))
            continue
        for g in genes:
            rows.append(dict(common, feature_id=g.id, feature_kind="gem_gene",
                             feature_name=g.name or None))

    df = pd.DataFrame(rows, columns=list(GPR_COLS))
    df["raw_score"] = df["raw_score"].astype(np.float32)
    df["in_atom_universe"] = df["in_atom_universe"].astype(bool)
    df = df.sort_values(["feature_kind", "feature_id", "mnxr", "evidence_id"],
                        kind="mergesort", na_position="last").reset_index(drop=True)
    d = OUT / "hosts" / host
    d.mkdir(parents=True, exist_ok=True)
    df.to_parquet(d / "gpr_gem.parquet", index=False, compression="zstd")

    gene_rows = df[df["feature_kind"] == "gem_gene"]
    gem_mnxr = set(df["mnxr"].unique())
    print(f"[gem_gpr] {{host}}: {{len(df):,}} rows  "
          f"{{gene_rows['feature_id'].nunique():,}} genes  {{df['mnxr'].nunique():,}} MNXR  "
          f"{{n_ruleless:,}} ruleless  "
          f"{{int(df['in_atom_universe'].sum()):,}} rows in the atom universe", flush=True)
    if dropped:
        print(f"[gem_gpr] {{host}}: {{n_dropped}}/{{len(dropped)}} edit-list reactions "
              f"dropped from the borrowed model", flush=True)
        if n_dropped != len(dropped):
            raise SystemExit(
                f"[gem_gpr] {{host}}: the edit list names {{len(dropped)}} reactions but "
                f"only {{n_dropped}} are in the model. An edit list that does not apply "
                f"is a claim about a model this is not.")
    if n_ruleless == 0:
        raise SystemExit(f"[gem_gpr] {{host}}: no ruleless reactions emitted -- a "
                         f"genome-scale model always has exchanges and spontaneous "
                         f"chemistry, so this means they were dropped rather than absent")
    # How much of this model's reactome the de-novo lanes could even nominate. REPORTED,
    # not enforced: the two tables are independent lines of evidence and the comparison
    # between them is the point, so a gap here is a finding about lane reach rather than
    # a build error.
    print(f"[gem_gpr] {{host}}: {{len(gem_mnxr & reachable):,}} of {{len(gem_mnxr):,}} GEM "
          f"reactions are also reachable through the de-novo bridge "
          f"({{len(gem_mnxr & reachable)/max(1,len(gem_mnxr)):.1%}})", flush=True)
    summary.append(dict(host=host, gem_host=src_host, gem_id=gem_id,
                        edit_list=list(dropped), rows=len(df),
                        genes=int(gene_rows["feature_id"].nunique()),
                        mnxr=int(df["mnxr"].nunique()), ruleless=n_ruleless,
                        in_atom_universe=int(df["in_atom_universe"].sum()),
                        bridge_reachable=len(gem_mnxr & reachable)))

# The bake this column was computed against, beside the tables it labels. `in_atom_universe`
# is only meaningful relative to one bake, so a table that does not carry the bake's
# identity cannot be compared with one built later. `universe` records the SECOND thing it
# is relative to: which reactions the benchmark was willing to count, which is the bake's
# coverage less transport and therefore not recoverable from the bake identity alone.
(OUT / "BUILD.json").write_text(json.dumps(
    dict(bake=ident, universe=u_stats, channel=CHANNEL, gem_source=GEM_SOURCE,
         edit_list=EDIT_LIST, hosts=summary), indent=2))
print(f"[gem_gpr] {{len(summary)}} hosts -> {{OUT}}/hosts/<host>/gpr_gem.parquet", flush=True)
'''


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    driver = DRIVER.format(
        universe_m=context.Input(universe_m).container,
        genomes=context.Input(genomes).container,
        metanetx=context.Input(metanetx).container,
        vocab=context.Input(vocab).container, pairs=context.Input(pairs).container,
        direction=context.Input(direction).container,
        bridge=context.Input(bridge).container,
        channel=CHANNEL, gpr_cols=repr(GPR_COLS), gem_source=repr(GEM_SOURCE),
        edit_list=repr(EDIT_LIST),
        out=iout.container,
    )
    context.LocalShell("cat > _host_gpr_gem.py << 'PYEOF'\n" + driver + "\nPYEOF\n")
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd="python3 _host_gpr_gem.py") \
        .ifVirtualEnvDo(env=image, cmd="python3 _host_gpr_gem.py")

    made = sorted((iout.local / "hosts").glob("*/gpr_gem.parquet")) \
        if (iout.local / "hosts").exists() else []
    Log.Info(f"gem_gpr: {len(made)}/{len(GEM_SOURCE)} host tables")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(made) == len(GEM_SOURCE) and (iout.local / "BUILD.json").exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=genomes,
    labels=["local"],
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=1)),
)
