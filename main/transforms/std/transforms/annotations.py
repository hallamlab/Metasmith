from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

assembly         = model.AddRequirement(lib.GetType("std::assembly"))
cds              = model.AddRequirement(lib.GetType("std::coding_sequences"), parents={assembly})
gff              = model.AddRequirement(lib.GetType("std::gene_features"), parents={assembly})
bakta            = model.AddRequirement(lib.GetType("std::bakta_annotations"), parents={assembly})
kofamscan        = model.AddRequirement(lib.GetType("std::kofamscan_annotations"), parents={assembly})
cazy_raw         = model.AddRequirement(lib.GetType("std::cazy_raw"))
cazy             = model.AddRequirement(lib.GetType("std::cazy_annotations"), parents={assembly})
busco_raw        = model.AddRequirement(lib.GetType("std::busco_raw"))
busco            = model.AddRequirement(lib.GetType("std::busco_annotations"), parents={assembly})

cazy_visualize   = model.AddRequirement(lib.GetType("std::cazy_visualize_script"))
busco_visualize  = model.AddRequirement(lib.GetType("std::busco_visualize_script"))
qc_table         = model.AddRequirement(lib.GetType("std::qc_table_script"))
_helpers_script  = model.AddRequirement(lib.GetType("std::helpers_script"))

image         = model.AddRequirement(lib.GetType("std::oci_image_script_runner"))
out           = model.AddProduct(lib.GetType("std::functional_annotations"))

def protocol(context: ExecutionContext):
    assembly_path  = context.Get(assembly)
    bakta_path     = context.Get(bakta)
    cds_path       = context.Get(cds)
    gff_path       = context.Get(gff)
    kofamscan_path = context.Get(kofamscan)
    cazy_path      = context.Get(cazy)
    busco_path     = context.Get(busco)
    cazy_raw_path  = context.Get(cazy_raw)
    busco_path     = context.Get(busco)
    busco_raw_path = context.Get(busco_raw)
    out_path       = context.Get(out)

    cazy_visualize_script   = context.Get(cazy_visualize)
    busco_visualize_script  = context.Get(busco_visualize)
    qc_table_script         = context.Get(qc_table)

    context.ExecWithContainer(
        image,
        cmd = f"""
            cd {out_path.container.parent}
            mkdir annotations/
            cd annotations/

            mkdir raw_outputs/
            cd raw_outputs/
            cp {assembly_path.container} assembly.fasta
            cp {cds_path.container} coding_sequences.fasta
            cp {gff_path.container} gene_features.gff3
            cp {kofamscan_path.container} kofamscan.tsv
            cp -r {bakta_path.container} bakta/
            cp {busco_raw_path.container} busco.tsv
            cp {cazy_raw_path.container} cazy.tsv
            cd ../

            mkdir annotated_outputs/
            cp {cazy_path.container} annotated_outputs/cazy.tsv
            cp {busco_path.container} annotated_outputs/busco.tsv
            python {qc_table_script.container} \
                raw_outputs/assembly.fasta \
                raw_outputs/coding_sequences.fasta \
                raw_outputs/busco.tsv \
                raw_outputs/cazy.tsv \
                annotated_outputs/qc_table.tsv

            mkdir visualizations/
            python {cazy_visualize_script.container} annotated_outputs/cazy.tsv visualizations/cazy.html
            python {busco_visualize_script.container} annotated_outputs/busco.tsv visualizations/busco.html
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "annotations/",
    },
)
