from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

forward = model.AddRequirement(lib.GetType("std::paired_reads_forward"))
reverse = model.AddRequirement(lib.GetType("std::paired_reads_reverse"))
image   = model.AddRequirement(lib.GetType("std::oci_image_fastp"))
out     = model.AddProduct(lib.GetType("std::short_reads_qc"))

def protocol(context: ExecutionContext):
    forward_path = context.Input(forward)
    reverse_path = context.Input(reverse)
    out_path     = context.Output(out)

    cpus = context.params.get("cpus")
    cpus_string = ""
    if cpus is not None:
        cpus_string = f"--thread {cpus}"

    context.ExecWithContainer(
        image = image,
        cmd = f"""
            cd {out_path.container.parent}
            fastp \
                -i {forward_path.container} \
                -I {reverse_path.container} \
                {cpus_string} \
                --detect_adapter_for_pe \
                --json fastp.json \
                --html fastp.html \
                --stdout 2> fastp.log | gzip > {out_path.container}
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
    group_by=forward,
    model = model,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
