from pathlib import Path

from ...models.libraries import ExecutionContext, ExecutionResult, TransformInstance, TransformInstanceLibrary
from ...models.solver import Transform

base_path = Path(__file__).parent
lib = TransformInstanceLibrary.Load(base_path)
model = Transform()

reads  = model.AddRequirement(node=lib.GetType("std::short_reads"))
image  = model.AddRequirement(node=lib.GetType("std::oci_image_fastqc"))
out    = model.AddProduct(lib.GetType("std::read_stats"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    context.ExecWithContainer(
        image = image,
        cmd = f"""\
            mkdir -p {out_path.container}/fastqc && \
            fastqc \
                --noextract \
                -o {out_path.container}/fastqc \
                {context.Get(reads).container}
            """,
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "fastqc",
    },
)
