from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads           = model.AddRequirement(lib.GetType("std::long_reads"))
assembly        = model.AddRequirement(lib.GetType("std::assembly"))
image_minimap2  = model.AddRequirement(lib.GetType("std::oci_image_minimap2"))
image_samtools  = model.AddRequirement(lib.GetType("std::oci_image_samtools"))
out_sam         = model.AddProduct(lib.GetType("std::sequence_alignment_map"))
out_bam         = model.AddProduct(lib.GetType("std::binary_alignment_map"))
out_bam_csi     = model.AddProduct(lib.GetType("std::binary_alignment_map_csi"))

def protocol(context: ExecutionContext):
    reads_path     = context.Get(reads)
    assembly_path  = context.Get(assembly)
    out_sam_path   = context.Get(out_sam)
    out_bam_path   = context.Get(out_bam)

    context.ExecWithContainer(
        image = image_minimap2,
        cmd = f"""
                minimap2 -ax map-pb {assembly_path.container} {reads_path.container} > {out_sam_path.container}
        """
    )

    cpus = context.params.get("cpus")
    cpus_string = ""
    if cpus is not None:
        cpus_string = f"-@ {cpus}"
    context.ExecWithContainer(
        image = image_samtools,
        cmd = f"""
                samtools view {cpus_string} -b {out_sam_path.container} \
                    | samtools sort {cpus_string} -o {out_bam_path.container} -O bam
                samtools index -c {out_bam_path.container}
        """
    )
    return ExecutionResult(success=context.Get(out_bam_csi).local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out_sam:      "alignments.sam",
        out_bam:      "alignments.bam",
        out_bam_csi:  "alignments.bam.csi"
    },
)
