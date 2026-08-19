import argparse as _argparse
import ast as _ast

_p = _argparse.ArgumentParser()
_p.add_argument("--bridge", required=True)
_p.add_argument("--channel", required=True)
_p.add_argument("--direction", required=True)
_p.add_argument("--edit-list", required=True)
_p.add_argument("--ev-lib", required=True)
_p.add_argument("--extensions", required=True)
_p.add_argument("--gem-source", required=True)
_p.add_argument("--genomes", required=True)
_p.add_argument("--lane-set", required=True)
_p.add_argument("--metanetx", required=True)
_p.add_argument("--out", required=True)
_p.add_argument("--pairs", required=True)
_p.add_argument("--universe-m", required=True)
_p.add_argument("--vocab", required=True)
A = _p.parse_args()
_LIT_edit_list = _ast.literal_eval(A.edit_list)
_LIT_extensions = _ast.literal_eval(A.extensions)
_LIT_gem_source = _ast.literal_eval(A.gem_source)

import json, os, sys
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(A.universe_m))
sys.path.insert(0, os.path.dirname(A.ev_lib))
import fabfos_evidence as fe
# Both halves come from the installed ecspr: `model` builds the network, `bake`
# reads the compiled tables. Only bench_universe is still a staged flat file.
from ecspr.model.build import crosswalk_gem, load_model
from ecspr.bake import encoding as refs
import bench_universe as bu

GENOMES = Path(A.genomes)
OUT     = Path(A.out)
CHANNEL = A.channel
LANE_SET = A.lane_set
EXTENSIONS = tuple(_LIT_extensions)
GEM_SOURCE = _LIT_gem_source
EDIT_LIST = _LIT_edit_list

# The MetaNetX source folder is <release>/reac_xref.tsv; the release is read off disk
# rather than pinned here, because the acquisition names the directory after what the
# server served and hardcoding a number here would silently target the wrong snapshot.
mnx = Path(A.metanetx)
rels = sorted(p for p in mnx.glob("*") if p.is_dir())
if len(rels) != 1:
    raise SystemExit(f"[gem_gpr] expected one MetaNetX release under {mnx}, found "
                     f"{[p.name for p in rels]}")
reac_xref = rels[0] / "reac_xref.tsv"
if not reac_xref.exists():
    raise SystemExit(f"[gem_gpr] no reac_xref.tsv under {rels[0]}")

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
    md = pq.read_schema(path).metadata or {}
    if b"ecspr_bake" not in md:
        raise SystemExit(f"[gem_gpr] {path} carries no ecspr_bake identity block")
    return md[b"ecspr_bake"]

blocks = {p: bake_identity(p) for p in (A.vocab, A.pairs, A.direction)}
if len(set(blocks.values())) != 1:
    raise SystemExit(
        f"[gem_gpr] the three bake files do not carry the same identity block: "
        f"{ {k: v[:64] for k, v in blocks.items()} }. They are ONE artifact -- "
        f"reading atom_pairs against another bake's vocab decodes every code to the "
        f"wrong reaction without raising.")
ident = json.loads(next(iter(blocks.values())).decode())

transport = bu.transport_mnxrs(bu.reac_prop_path(mnx))
universe, u_stats = bu.atom_universe(A.vocab, A.pairs, exclude=transport)
print(bu.universe_line(u_stats, "gem_gpr") +
      f"  (bake v{ident['bake_version']}, vocab {ident['vocab_sha256'][:16]})",
      flush=True)

reachable = set(pd.read_parquet(A.bridge, columns=["mnxr"])["mnxr"].unique())

def gem_json_for(src_host):
    d = GENOMES / src_host / "GEM"
    js = sorted(d.glob("*.json"))
    if len(js) != 1:
        raise SystemExit(f"[gem_gpr] expected one model JSON under {d}, found "
                         f"{[p.name for p in js]}")
    return js[0]

# The crosswalk is a pure function of (model, reac_xref, universe), so two hosts sharing
# a model share it exactly. Computed once per SOURCE model rather than once per host --
# not for speed, but so that the EPI300/DH10B tables cannot diverge through two runs of
# the same resolver.
crosswalks = {}
summary = []
for host, src_host in sorted(GEM_SOURCE.items()):
    if src_host not in crosswalks:
        path = gem_json_for(src_host)
        m = load_model(str(path))
        xw, stats = crosswalk_gem(m, str(reac_xref), universe)
        crosswalks[src_host] = (m, xw, stats)
        print(f"[gem_gpr] {src_host} / {getattr(m, 'id', '?')}: "
              f"{stats['n_reactions']:,} reactions -> {stats['n_resolved']:,} resolved "
              f"({xw['mnxr'].nunique():,} distinct MNXR), {stats['n_unresolved']:,} "
              f"unresolved, {stats['n_aam_gap']:,} outside the atom universe", flush=True)

    m, xw, stats = crosswalks[src_host]
    gem_id = getattr(m, "id", None) or "unknown"
    by_rxn = dict(zip(xw["rxn_id"], zip(xw["mnxr"], xw["source"], xw["in_universe"])))
    build_id = f"gem_{gem_id}"
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
            build_id=build_id, host=host, unit_id=gem_id, source=gem_id,
            mnxr=mnxr, channel=CHANNEL, lane_set=LANE_SET,
            intermediate_id=r.id, intermediate_name=r.name or "",
            # A curated model's assertion is a presence claim; `evidence_quality` says
            # a human curated it, which is a stronger statement than any lane makes.
            score_kind="presence", evidence_quality="reviewed",
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
            # `orf` names whatever nominates the reaction and is never null. A ruleless
            # reaction nominates ITSELF -- there is no gene to name -- so the model
            # reaction id is the nominator, and belief conservation then groups these
            # per reaction instead of collapsing every one of them onto a single null.
            rows.append(dict(common, orf=r.id, feature_kind="ruleless",
                             feature_name=""))
            continue
        for g in genes:
            rows.append(dict(common, orf=g.id, feature_kind="gem_gene",
                             feature_name=g.name or ""))

    df = pd.DataFrame(rows, columns=fe.schema_for(EXTENSIONS))
    df["raw_score"] = df["raw_score"].astype(np.float32)
    df["in_atom_universe"] = df["in_atom_universe"].astype(bool)
    df = df.sort_values(fe.grain_key(EXTENSIONS), kind="mergesort",
                        na_position="last").reset_index(drop=True)
    fe.validate_gpr(df, LANE_SET, None, gem_id, EXTENSIONS)
    d = OUT / "hosts" / host
    d.mkdir(parents=True, exist_ok=True)
    df.to_parquet(d / "gpr_gem.parquet", index=False, compression="zstd")

    gene_rows = df[df["feature_kind"] == "gem_gene"]
    gem_mnxr = set(df["mnxr"].unique())
    print(f"[gem_gpr] {host}: {len(df):,} rows  "
          f"{gene_rows['orf'].nunique():,} genes  {df['mnxr'].nunique():,} MNXR  "
          f"{n_ruleless:,} ruleless  "
          f"{int(df['in_atom_universe'].sum()):,} rows in the atom universe", flush=True)
    if dropped:
        print(f"[gem_gpr] {host}: {n_dropped}/{len(dropped)} edit-list reactions "
              f"dropped from the borrowed model", flush=True)
        if n_dropped != len(dropped):
            raise SystemExit(
                f"[gem_gpr] {host}: the edit list names {len(dropped)} reactions but "
                f"only {n_dropped} are in the model. An edit list that does not apply "
                f"is a claim about a model this is not.")
    if n_ruleless == 0:
        raise SystemExit(f"[gem_gpr] {host}: no ruleless reactions emitted -- a "
                         f"genome-scale model always has exchanges and spontaneous "
                         f"chemistry, so this means they were dropped rather than absent")
    # How much of this model's reactome the de-novo lanes could even nominate. REPORTED,
    # not enforced: the two tables are independent lines of evidence and the comparison
    # between them is the point, so a gap here is a finding about lane reach rather than
    # a build error.
    print(f"[gem_gpr] {host}: {len(gem_mnxr & reachable):,} of {len(gem_mnxr):,} GEM "
          f"reactions are also reachable through the de-novo bridge "
          f"({len(gem_mnxr & reachable)/max(1,len(gem_mnxr)):.1%})", flush=True)
    summary.append(dict(host=host, gem_host=src_host, gem_id=gem_id,
                        edit_list=list(dropped), rows=len(df),
                        genes=int(gene_rows["orf"].nunique()),
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
print(f"[gem_gpr] {len(summary)} hosts -> {OUT}/hosts/<host>/gpr_gem.parquet", flush=True)
