from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

via_miniasm = model.AddRequirement(lib.GetType("std::miniasm_estimate"))
reads   = model.AddRequirement(lib.GetType("std::long_reads_filtered"), parents={via_miniasm})
image   = model.AddRequirement(lib.GetType("std::oci_image_flye"))
out     = model.AddProduct(lib.GetType("std::long_reads_assembly"))

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
            flye \
                --pacbio-raw \
                {reads_path.container} \
                --out-dir long_reads_assembly/ \
                {cpus_string}
            cp long_reads_assembly/assembly.fasta {out_path.container}
        """
    )
    return ExecutionResult(
        manifest=[{
            out: out_path.local,
        }],
        success=out_path.local.exists()
    )

TransformInstance(
    protocol = protocol,
    group_by=reads,
    model = model,
    output_signature = {
        out: "assembly.fasta",
    },
    resources=Resources(
        cpus=8,
        memory=Size.GB(24),
        duration=Duration(hours=12),
    ),
)
