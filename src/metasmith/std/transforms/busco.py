from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reference      = model.AddRequirement(lib.GetType("std::busco_ref"))
cds            = model.AddRequirement(lib.GetType("std::coding_sequences"))
map            = model.AddRequirement(lib.GetType("std::busco_map"))
image_diamond  = model.AddRequirement(lib.GetType("std::oci_image_diamond"))
image_runner   = model.AddRequirement(lib.GetType("std::oci_image_script_runner"))
script         = model.AddRequirement(lib.GetType("std::busco_annotation_script"))
raw_out        = model.AddProduct(lib.GetType("std::busco_raw"))
out            = model.AddProduct(lib.GetType("std::busco_annotations"))

def protocol(context: ExecutionContext):
    cds_path        = context.Get(cds)
    reference_path  = context.Get(reference)
    map_path        = context.Get(map)
    script_path     = context.Get(script)
    raw_out_path    = context.Get(raw_out)
    out_path        = context.Get(out)

    cpus_string = ""
    # cpus = context.params.get("cpus")
    # if cpus is not None:
    #     cpus_string = f"--threads {cpus}"

    context.ExecWithContainer(
        image = image_diamond,
        cmd = f"""
                cd {raw_out_path.container.parent}
                diamond-aligner makedb \
                    --db busco_db \
                    --in {reference_path.container}
                diamond-aligner blastp \
                    {cpus_string} \
                    --db busco_db \
                    --out {raw_out_path.container} \
                    --outfmt 6 \
                    --query {cds_path.container} \
                    --max-target-seqs 1
        """
    )
    context.ExecWithContainer(
        image = image_runner,
        cmd = f"""
                cp {script_path.container} /runner/busco.py
                uv run /runner/busco.py {raw_out_path.container} {map_path.container} {out_path.container}
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        raw_out:  "busco_raw.tsv",
        out:      "busco_alignment.tsv",
    },
)
