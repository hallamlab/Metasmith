from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads   = model.AddRequirement(lib.GetType("std::short_reads_qc"))
image   = model.AddRequirement(lib.GetType("std::oci_image_spades"))
out     = model.AddProduct(lib.GetType("std::short_reads_assembly"))

def protocol(context: ExecutionContext):
    out_path = context.Output(out)
    reads_path = context.Input(reads)

    cpus_string = ""
    cpus = context.params.get("cpus")
    if cpus is not None:
        cpus_string = f"--threads {cpus}"

    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {out_path.container.parent}
                spades.py \
                    --isolate \
                    {cpus_string} \
                    --12 {reads_path.container} \
                    -o spades_out
                cp spades_out/contigs.fasta {out_path.container}
        """
    )
    return ExecutionResult(
        manifest=[{
            out: out_path.local,
        }],
        success=out_path.local.exists(),
    )

TransformInstance(
    protocol = protocol,
    group_by=reads,
    model = model,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=6),
    ),
)
