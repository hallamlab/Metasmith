from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reference       = model.AddRequirement(lib.GetType("std::cazy_ref"))
cds             = model.AddRequirement(lib.GetType("std::coding_sequences"))
image_diamond   = model.AddRequirement(lib.GetType("std::oci_image_diamond"))
image_runner    = model.AddRequirement(lib.GetType("std::oci_image_script_runner"))
script          = model.AddRequirement(lib.GetType("std::cazy_annotation_script"))
_helper_script   = model.AddRequirement(lib.GetType("std::helpers_script"))
raw_out         = model.AddProduct(lib.GetType("std::cazy_raw"))
out             = model.AddProduct(lib.GetType("std::cazy_annotations"))

def protocol(context: ExecutionContext):
    cds_path        = context.Input(cds)
    reference_path  = context.Input(reference)
    raw_out_path    = context.Input(raw_out)
    out_path        = context.Input(out)
    script_path     = context.Input(script)

    cpus_string = ""
    cpus = context.params.get("cpus")
    if cpus is not None:
        cpus_string = f"--threads {cpus}"

    context.ExecWithContainer(
        image = image_diamond,
        cmd = f"""
                cd {raw_out_path.container.parent}
                diamond-aligner makedb \
                    --db cazy_db \
                    --in {reference_path.container}
                diamond-aligner blastp \
                    {cpus_string} \
                    --db cazy_db \
                    --out {raw_out_path.container} \
                    --outfmt 6 \
                    --query {cds_path.container} \
                    --max-target-seqs 1
        """
    )
    context.ExecWithContainer(
        image = image_runner,
        cmd = f"""
                python {script_path.container} {raw_out_path.container} {out_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    group_by=cds,
    model = model,
    output_signature = {
        raw_out:  "cazy_raw.tsv",
        out:      "cazy_alignment.tsv",
    },
)
