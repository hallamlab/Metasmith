from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads   = model.AddRequirement(lib.GetType("std::short_reads"))
image   = model.AddRequirement(lib.GetType("std::oci_image_trimmomatic"))
out     = model.AddProduct(lib.GetType("std::short_reads_filtered"))

def protocol(context: ExecutionContext):
    out_path = context.Get(out)
    reads_path = context.Get(reads)
    context.ExecWithContainer(
        image = image,
        cmd = f"""
                trimmomatic \
                    SE \
                    -phred33 \
                    {reads_path.container} \
                    {out_path.container} \
                    ILLUMINACLIP:/Trimmomatic-0.39/adapters/TruSeq3-SE.fa:2:30:10 \
                    LEADING:3 \
                    TRAILING:3 \
                    SLIDINGWINDOW:4:15 \
                    MINLEN:36
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "short_reads_filtered.fastq",
    },
)
