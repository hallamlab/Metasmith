from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

assembly   = model.AddRequirement(lib.GetType("std::long_reads_assembly"))
bam   = model.AddRequirement(lib.GetType("std::binary_alignment_map"))
image   = model.AddRequirement(lib.GetType("std::oci_image_pilon"))
out     = model.AddProduct(lib.GetType("std::hybrid_assembly"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    assembly_path = context.Get(assembly).container / "assembly/assembly.fasta"
    bam_dir = context.Get(bam)

    # Assert a file matching the glob pattern exists
    bam_matches = list(bam_dir.container.glob("*.sorted.bam"))
    assert bam_matches, "No file in BAM input directory matches *.sorted.bam"
    bam_path = bam_matches[0]

    context.ExecWithContainer(
        image = image,
        cmd = f"""
                mkdir -p {out_path.container}
                cd {out_path.container}
                java -jar \
                    /pilon/pilon.jar \
                        --genome {assembly_path} \
                        --frags {bam_path} \
                        --output hybrid_assembly
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "pilon_out/",
    },
)
