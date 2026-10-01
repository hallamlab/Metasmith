# master_summary -- MASTER_SUMMARY (asv_pipeline.nf:5763). The script merges every whitelisted
# table it finds under four directories onto the ASV meta. Upstream's default whitelist names
# the lung study's indicator files, so this one lists every label's indicator tables instead,
# then upstream's network, clustermap and link tables.
#
# Upstream wrote the anchor table into the network directory, so it is staged there again.

from metasmith.python_api import *

lib        = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model      = Transform()
image      = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts    = model.AddRequirement(lib.GetType("lib::aspire"))
study      = model.AddRequirement(lib.GetType("aspire::study_metadata"))
am         = model.AddRequirement(lib.GetType("aspire::analysis_asv_meta"), parents={study})
counts     = model.AddRequirement(lib.GetType("aspire::analysis_counts"), parents={study})
cmaps      = model.AddRequirement(lib.GetType("aspire::clustermap_outputs"), parents={study})
indic      = model.AddRequirement(lib.GetType("aspire::indicspecies_results"), parents={study})
net        = model.AddRequirement(lib.GetType("aspire::network_outputs"), parents={study})
anchors    = model.AddRequirement(lib.GetType("aspire::module_asv_anchor_table"), parents={study})
magl       = model.AddRequirement(lib.GetType("aspire::asv_mag_outputs"), parents={study})
long       = model.AddProduct(lib.GetType("aspire::master_long"))
wide       = model.AddProduct(lib.GetType("aspire::master_count_wide"))
manifest   = model.AddProduct(lib.GetType("aspire::master_source_manifest"))
colmap     = model.AddProduct(lib.GetType("aspire::master_column_mapping"))
collisions = model.AddProduct(lib.GetType("aspire::master_column_collisions"))

FILES = {
    long: "ASV_master_long.tsv",
    wide: "ASV_master_count_wide.tsv",
    manifest: "ASV_master_source_manifest.tsv",
    colmap: "ASV_master_column_mapping.tsv",
    collisions: "ASV_master_column_collisions_original.tsv",
}
WHITELIST = ",".join([
    "spieceasi_node_features.csv", "spieceasi_modules_sub.tsv", "spieceasi_modules_all.tsv",
    "module_asv_anchor_table.tsv", "network_component_membership_POS_ALL.tsv",
    "network_component_membership_POS_SUB.tsv", "clustermap_ASV_ID_plot.tsv",
    "asv2mag_pairing.tsv", "asv2mag_summary.tsv", "asv2mag_genome_summary.tsv",
    "barrnap_16s_reference_catalog.tsv",
])


def protocol(context: ExecutionContext):
    outs = {p: context.Output(p) for p in FILES}
    s, ind = context.Input(scripts).container, context.Input(indic).container
    copies = "; ".join(f"cp res/{name} {outs[p].container}" for p, name in FILES.items())

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        cp -r --no-preserve=mode {context.Input(net).container} spieceasi
        cp {context.Input(anchors).container} spieceasi/module_asv_anchor_table.tsv
        isa=$(cd {ind} && ls *_indicator_species_summary.tsv *_indicator_species_results.tsv 2>/dev/null | paste -sd, -) || true
        python {s}/build_master_asv_summary.py --asv-meta {context.Input(am).container} \
            --asv-counts {context.Input(counts).container} \
            --clustermaps-dir {context.Input(cmaps).container} --indicspecies-dir {ind} \
            --spieceasi-dir spieceasi --asv-mag-dir {context.Input(magl).container} \
            --whitelist "${{isa:+$isa,}}{WHITELIST}" --outdir res --max-direct-cols 300
        {copies}
        rm -rf res spieceasi
    """)

    return ExecutionResult(
        manifest=[{p: o.local for p, o in outs.items()}],
        success=all(o.local.exists() for o in outs.values()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(
        cpus=4,
        memory=Size.GB(32),
        duration=Duration(hours=4),
    ),
)
