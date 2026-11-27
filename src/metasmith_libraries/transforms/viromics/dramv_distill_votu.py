import json

from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()

dram   = model.AddRequirement(lib.GetType("env::dram.env"))
python = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))

# One task per study, because a vOTU representative is a frozen contig and the frozen set
# is pooled. The genes are the frozen set's own (prodigal-gv) ORFs, which put each gene in
# its representative's frame already; only VirSorter2's genes, called on the per-sample
# contig batches, have to be moved there.
study  = model.AddRequirement(lib.GetType("viromics::contig_study"))
frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"), parents={study})
prov   = model.AddRequirement(lib.GetType("viromics::candidate_call_provenance"), parents={study})
votu   = model.AddRequirement(lib.GetType("viromics::votu_cluster_table"), parents={frozen})
orfs   = model.AddRequirement(lib.GetType("sequences::orfs"), parents={frozen})
# The lanes dram_annotations.render reads.
lanes  = [model.AddRequirement(lib.GetType(f"annotation::{k}_lane_table"), parents={orfs})
          for k in ("kofam", "uniref50", "interproscan", "refseq_viral", "dbcan", "merops", "vogdb")]
affi   = model.AddRequirement(lib.GetType("annotation::virsorter2_affi_contigs"), parents={study})
sheets = model.AddRequirement(lib.GetType("ref::dram_sheets"))
gpr_lib = model.AddRequirement(lib.GetType("lib::fabfos_gpr"))
ev_lib  = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))

annotations = model.AddProduct(lib.GetType("annotation::dramv_votu_annotations"))
amg_summary = model.AddProduct(lib.GetType("annotation::dramv_amg_summary"))
product     = model.AddProduct(lib.GetType("annotation::dramv_product_html"))
vmag_stats  = model.AddProduct(lib.GetType("annotation::dramv_vmag_stats"))

DISTILL = {amg_summary: "amg_summary.tsv", product: "product.html", vmag_stats: "vMAG_stats.tsv"}


def protocol(context: ExecutionContext):
    isheets = context.Input(sheets)
    igpr    = context.Input(gpr_lib)
    iann    = context.Output(annotations)
    iouts   = {t: context.Output(t) for t in DISTILL}

    affis = " ".join(str(f.container) for f in context.InputGroup(affi))

    with open("DRAM.config", "w") as fh:
        json.dump({
            "search_databases": {}, "database_descriptions": {}, "custom_dbs": None,
            "dram_sheets": {s: f"{isheets.container}/{s}.tsv"
                            for s in ("genome_summary_form", "amg_database")},
            "description_db": None, "dram_version": "1.5.0",
        }, fh)

    context.ExecWithEnv(env=python, cmd=f"""
        python3 {igpr.container}/dramv_annotations.py \
            --ev-lib {context.Input(ev_lib).container} \
            --frozen {context.Input(frozen).container} \
            --provenance {context.Input(prov).container} \
            --votu {context.Input(votu).container} \
            --orfs {context.Input(orfs).container} \
            --annotation-table {" ".join(str(context.Input(t).container) for t in lanes)} \
            --affi {affis} \
            --name votu_representatives \
            --out-annotations rendered.tsv --out-affi votu-affi-contigs.tab \
            --out-fasta votu_representatives.fna
    """)
    context.ExecWithEnv(env=dram, cmd=f"""
        export HOME=$PWD
        python {igpr.container}/dramv_flags.py \
            --annotations rendered.tsv --affi votu-affi-contigs.tab \
            --fasta votu_representatives.fna --sheets {isheets.container} \
            --out {iann.container}
        DRAM-v.py distill -i {iann.container} -o distill --config_loc DRAM.config
    """)
    context.LocalShell("\n".join(f"cp distill/{f} {iouts[t].local}" for t, f in DISTILL.items()))

    return ExecutionResult(
        manifest=[{annotations: iann.local, **{t: i.local for t, i in iouts.items()}}],
        success=iann.local.exists() and all(i.local.exists() for i in iouts.values()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(
        cpus=1,
        memory=Size.GB(32),
        duration=Duration(hours=8),
    ),
)
