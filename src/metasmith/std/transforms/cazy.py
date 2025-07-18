from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reference = model.AddRequirement(lib.GetType("std::cazy_ref"))
cds  = model.AddRequirement(lib.GetType("std::coding_sequences"))
image   = model.AddRequirement(lib.GetType("std::oci_image_diamond"))
out     = model.AddProduct(lib.GetType("std::cazy_annotations"))

def protocol(context: ExecutionContext):
    cds_path = context.Get(cds)
    reference_path = context.Get(reference)
    out_path = context.Get(out)

    # cpus = context.params.get("cpus")
    cpus_string = ""
    # if cpus is not None:
    #     cpus_string = f"--threads {cpus}"

    context.ExecWithContainer(
        image = image,
        cmd = f"""
                cd {out_path.container.parent}
                diamond-aligner makedb \
                    --db cazy_db \
                    --in {reference_path.container}
                diamond-aligner blastp \
                    {cpus_string} \
                    --db cazy_db \
                    --out {out_path.container} \
                    --outfmt 6 \
                    --query {cds_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "cazy_alignment.tsv",
    },
)
