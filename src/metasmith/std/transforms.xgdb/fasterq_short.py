from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

accession  = model.AddRequirement(lib.GetType("std::short_reads_accession"))
image      = model.AddRequirement(lib.GetType("std::oci_image_fasterq_dump"))
out        = model.AddProduct(lib.GetType("std::short_reads"))

def protocol(context: ExecutionContext):
    out_path = context.Output(out)
    accession_path = context.Input(accession)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {out_path.container.parent}
                fasterq-dump \
                    $(cat {accession_path.container}) \
                    --split-spot \
                    -Z > {out_path.container.name}

        """,
        shell="sh",
    )
    return ExecutionResult(
        manifest=[{
            out: out_path.local,
        }],
        success=out_path.local.exists()
    )

TransformInstance(
    protocol = protocol,
    resources=Resources(
        cpus=1,
        memory=Size.GB(16),
        duration=Duration(hours=12),
    ),
    group_by=accession,
    model = model,
    output_signature = {
        out: "reads.fq",
    },
)
