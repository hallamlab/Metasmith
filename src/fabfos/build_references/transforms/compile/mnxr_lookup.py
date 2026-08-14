"""R5 -- the consolidated intermediate-id -> MNXR bridge.

One table replacing the former ko/ec/uniprot trio: `id, id_source, mnxr,
evidence_quality`. Measured before consolidating -- the three id spaces share no
ids, so a single `id` column is unambiguous and `id_source` is a label rather than
a disambiguator; deduping to distinct (id, mnxr) went 35,762,706 -> 30,467,712 rows
and is behaviour-preserving because the consumer already ran that dedup after its
join, and the duplicate rows differed only in columns nothing downstream reads.
82.5 MB against 251.5 MB across the three files it replaces.

`evidence_quality` is carried at a cost of 0.2 MB because "reviewed" sorts before
"unreviewed", so an existing keep-first dedup already retains the stronger claim.

Four routes into one table:
  ec       reac_prop classifs
  ko       KEGG REST's link/reaction/ko body -> reac_xref `kegg.reaction:` rows
  uniprot  rhea2uniprot{,_trembl} -> reac_xref `rhea:` rows
  metacyc  reac_xref `metacyc.reaction:` rows -- MetaCyc REACTION FRAME IDS

THE METACYC ROUTE IS AN ID SPACE, NOT A LANE. Nothing annotates a protein with a MetaCyc
frame id, so no de-novo lane emits one. What does carry them is curated literature: LASER
ships a 3,370-row gene-to-reaction table keyed on MetaCyc frame ids, and 97.7% of its rows
(96.5% of its distinct ids) land here. Measured on MNXref 4.5 the fanout is exactly one
reaction per frame id for all 20,215 keys -- as precise as an accession and reaching more
rows, which is why it belongs beside the other three rather than being joined ad hoc by
the one consumer that needs it today.

THIS IS EVERY ANNOTATION LANE'S TERMINUS, which is why it sits under the annotation
references rather than beside the metabolism bake. kofam emits KOs, CLEAN emits ECs,
the DIAMOND lane and the ProteinBERT pool emit UniProt accessions -- four different
id spaces, and this is the one table that turns all of them into reactions. The CLEAN
lane in particular has no reference artifact of its own: its weights are baked into
its image, so the `ec` route below is the whole of what the build owes it.

REQUIRES THREE SOURCE FOLDERS, NOT SEVEN FILES. Each `fabfos_data::` requirement is a
whole upstream distribution holding one release directory, resolved at run time -- the
release moves under a rolling upstream path and naming it here would silently pin the
bridge to whichever snapshot was current when this file was written.

The KEGG body is parsed HERE and not at acquisition. `acquire/kegg.py` saves the REST
response exactly as returned -- headerless, ids namespace-prefixed (`ko:K00001<TAB>
rn:R00623`) -- because an acquisition that strips prefixes and adds a header puts a
file in the originals tier that no upstream URL would return.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
metanetx   = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
kegg       = model.AddRequirement(lib.GetType("fabfos_data::kegg"))
rhea       = model.AddRequirement(lib.GetType("fabfos_data::rhea"))
# The EC route and the UniProt route already have builders in the shipped evidence
# library, and they are CALLED here rather than reimplemented: the run-side mapper reads
# this table through the same module, so a second copy of the parser is a second thing to
# keep in step with reac_prop's column layout.
ev_lib     = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
bridge     = model.AddProduct(lib.GetType("ref::mnxr_lookup"))


DRIVER = r'''
import os, sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, os.path.dirname("{ev_lib}"))
import fabfos_evidence as fe

MNX_ROOT  = "{metanetx}"
KEGG_ROOT = "{kegg}"
RHEA_ROOT = "{rhea}"
OUT       = "{out}"

COLUMNS = ["id", "id_source", "mnxr", "evidence_quality"]


def one_release(root, what):
    """The single release directory inside a fabfos_data:: source folder.

    Exactly one, asserted. Two staged releases is not a case to resolve by taking the
    newest: this table joins four id spaces from three sources, and a bridge built from
    two snapshots of one of them would be internally inconsistent with nothing on disk
    to show it.
    """
    subs = sorted(p for p in Path(root).iterdir() if p.is_dir())
    if len(subs) != 1:
        raise SystemExit(
            f"[bridge] {{what}}: expected exactly one release under {{root}}, found "
            f"{{len(subs)}} ({{[p.name for p in subs]}})")
    return subs[0]


MNX  = one_release(MNX_ROOT, "metanetx")
KEGG = one_release(KEGG_ROOT, "kegg")
RHEA = one_release(RHEA_ROOT, "rhea")
print(f"[bridge] metanetx {{MNX.name}} · kegg {{KEGG.name}} · rhea {{RHEA.name}}", flush=True)

REAC_PROP = str(MNX / "reac_prop.tsv")
REAC_XREF = str(MNX / "reac_xref.tsv")
KO_KEGG_R = str(KEGG / "ko2reaction.tsv")
RHEA_SP   = str(RHEA / "tsv" / "rhea2uniprot.tsv")
RHEA_TR   = str(RHEA / "tsv" / "rhea2uniprot_trembl.tsv.gz")

for p in (REAC_PROP, REAC_XREF, KO_KEGG_R, RHEA_SP, RHEA_TR):
    if not os.path.exists(p):
        raise SystemExit(f"[bridge] missing {{p}} -- the source folder's layout has changed")


def kegg_r_to_mnxr():
    """`kegg.reaction:R#####` rows of reac_xref. Both prefixes, as the deployed builder
    accepts both."""
    rows = []
    with open(REAC_XREF) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            src, mnxr = parts[0], parts[1]
            if not mnxr.startswith("MNXR") or mnxr == "EMPTY":
                continue
            if src.startswith("kegg.reaction:") or src.startswith("keggR:"):
                rows.append((src.split(":", 1)[1], mnxr))
    return pd.DataFrame(rows, columns=["kegg_r", "mnxr"]).drop_duplicates()


def route_metacyc():
    """MetaCyc reaction frame ids, from reac_xref.

    BOTH PREFIXES, UNIONED. MNXref writes every MetaCyc crossref twice, once as
    `metacyc.reaction:<FRAME>` and once as the legacy `metacycR:<FRAME>`; measured on 4.5
    the two carry the SAME 20,215 keys with no key exclusive to either, so the union is
    the same set as either alone and taking one arbitrarily would be a silent bet on a
    release that keeps them in step.

    `evidence_quality` is `reviewed`: a MetaCyc frame id is a curated crossreference with
    no reviewed/unreviewed split, the same argument route_ec makes.
    """
    rows = []
    with open(REAC_XREF) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            src, mnxr = parts[0], parts[1]
            if not mnxr.startswith("MNXR") or mnxr == "EMPTY":
                continue
            if src.startswith("metacyc.reaction:") or src.startswith("metacycR:"):
                rows.append((src.split(":", 1)[1], mnxr))
    df = pd.DataFrame(rows, columns=["id", "mnxr"]).drop_duplicates()
    df["id_source"] = "metacyc"
    df["evidence_quality"] = "reviewed"
    return df[COLUMNS]


def route_ec():
    """reac_prop classifs. Partial ECs ("3.6.3.-") are kept verbatim because that is how
    reac_prop writes them and the CLEAN lane can emit them too."""
    df = fe.load_ec_to_mnxr(REAC_PROP)
    df = df.rename(columns={{"ec": "id"}})
    df["id_source"] = "ec"
    # MetaNetX's classifs column is a curated assignment with no reviewed/unreviewed
    # split, so the whole route is `reviewed`. Marking it `unreviewed` would let a TrEMBL
    # row outrank a curated EC in the keep-first dedup.
    df["evidence_quality"] = "reviewed"
    return df[COLUMNS]


def route_ko():
    """KO -> KEGG reaction -> MNXR.

    NOT KO -> EC -> MNXR. Measured on the three hosts, routing through the ko_list's
    `[EC:...]` tag takes the kofam lane from 646 to 8,548 reactions and makes 91% of them
    reactions the EC lane already reaches -- the two lanes stop being independent
    evidence, which is the entire reason for having four of them.
    """
    # The REST body is headerless and namespace-prefixed: `ko:K00001<TAB>rn:R00623`.
    # Kept that way in the originals tier; stripped here.
    ko_r = pd.read_csv(KO_KEGG_R, sep="\t", dtype=str, header=None,
                       names=["ko", "kegg_r"]).dropna()
    ko_r["ko"] = ko_r["ko"].str.split(":").str[-1]
    ko_r["kegg_r"] = ko_r["kegg_r"].str.split(":").str[-1]
    if not ko_r["ko"].str.match(r"^K\d+$").all():
        bad = ko_r.loc[~ko_r["ko"].str.match(r"^K\d+$"), "ko"].head(5).tolist()
        raise SystemExit(f"[bridge] ko column does not look like KO ids: {{bad}}")

    r_mnxr = kegg_r_to_mnxr()
    df = (ko_r.merge(r_mnxr, on="kegg_r", how="inner")[["ko", "mnxr"]]
              .drop_duplicates().rename(columns={{"ko": "id"}}))
    n_unmapped = int((~ko_r["kegg_r"].isin(set(r_mnxr["kegg_r"]))).sum())
    print(f"[bridge] ko: {{len(df):,}} rows; reac_xref carries "
          f"{{r_mnxr['kegg_r'].nunique():,}} KEGG reactions, {{n_unmapped:,}} KO links "
          f"point at one it does not have", flush=True)
    df["id_source"] = "ko"
    df["evidence_quality"] = "reviewed"      # KEGG's REACTION block is curated
    return df[COLUMNS]


def route_uniprot(tmp):
    """rhea2uniprot{{,_trembl}} -> reac_xref `rhea:` rows, via the shipped builder.

    TrEMBL is ~10x SwissProt and is what gives the lane its reach on non-model ORFs, so
    it carries `unreviewed` and SwissProt carries `reviewed` -- which is the column the
    label pool later cuts on, and the reason the keep-first dedup below retains the
    stronger claim rather than an arbitrary one.
    """
    fe.build_uniprot_bridge(REAC_XREF, RHEA_SP, RHEA_TR, tmp)
    d = pd.read_parquet(tmp, columns=["uniprot_accession", "mnxr", "evidence_quality"])
    d = d.rename(columns={{"uniprot_accession": "id"}})
    d["id_source"] = "uniprot"
    return d[COLUMNS]


def main():
    frames = [route_ec(), route_ko(), route_metacyc(),
              route_uniprot("_uniprot_bridge.parquet")]
    for f, name in zip(frames, ("ec", "ko", "metacyc", "uniprot")):
        print(f"[bridge] {{name:8s}} {{len(f):>12,}} rows  {{f['id'].nunique():>9,}} ids  "
              f"{{f['mnxr'].nunique():>7,}} MNXR", flush=True)
    df = pd.concat(frames, ignore_index=True)
    n0 = len(df)

    # The id spaces share NO ids (measured before consolidating, and re-measured when
    # metacyc joined them), so `id` alone is unambiguous and `id_source` is a label rather
    # than a disambiguator. Asserted rather than assumed: if a future source collides, a
    # bare `id` join would silently mix two namespaces' claims into one lane.
    per_id = df.groupby("id")["id_source"].nunique()
    clashes = per_id[per_id > 1]
    if len(clashes):
        raise SystemExit(
            f"{{len(clashes):,}} ids appear in more than one id_source "
            f"(e.g. {{list(clashes.index[:5])}}). `id` is no longer unambiguous, so the "
            f"consumer's per-lane slice would mix namespaces -- this needs a decision, "
            f"not a dedup.")

    # Sort before the dedup so "reviewed" < "unreviewed" alphabetically and keep-first
    # retains the stronger claim. This is the whole reason evidence_quality is carried at
    # all (0.2 MB); dropping it would make the surviving row arbitrary.
    df = (df.sort_values(["id", "mnxr", "evidence_quality"], kind="mergesort")
            .drop_duplicates(subset=["id", "mnxr"], keep="first")
            .reset_index(drop=True))
    df.to_parquet(OUT, index=False)
    print(f"[bridge] {{n0:,}} -> {{len(df):,}} rows after dedup to distinct (id, mnxr)",
          flush=True)
    print(f"[bridge] evidence_quality {{df['evidence_quality'].value_counts().to_dict()}}",
          flush=True)
    print(f"[bridge] wrote {{OUT}} ({{os.path.getsize(OUT)/1e6:.1f}} MB)", flush=True)


main()
'''


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    ikeg = context.Input(kegg)
    irhe = context.Input(rhea)
    iev  = context.Input(ev_lib)
    iout = context.Output(bridge)

    driver = DRIVER.format(
        ev_lib=iev.container, metanetx=imnx.container, kegg=ikeg.container,
        rhea=irhe.container, out=iout.container,
    )
    context.LocalShell("cat > _mnxr_lookup.py << 'PYEOF'\n" + driver + "\nPYEOF\n")
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd="python3 _mnxr_lookup.py") \
        .ifVirtualEnvDo(env=image, cmd="python3 _mnxr_lookup.py")

    return ExecutionResult(
        manifest=[{bridge: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # NOT labels=["local"]. That label is right for `acquire/` -- a download needs the
    # login node's network -- and copying it here is what pinned every compile to the
    # login node under the slurm preset: `xlocalx` sets `executor = 'local'`, whose pool
    # slurm.nf declares as 8 cores / 8 GB, and Nextflow's local executor REFUSES a
    # process asking for more rather than queueing it. It also sets
    # errorStrategy='ignore' with no retry, so the refusal is silent and the workflow
    # goes green with the reference absent. Nothing in this transform touches the
    # network; it belongs on a compute node.
    resources=Resources(cpus=2, memory=Size.GB(32), duration=Duration(hours=2)),
)
