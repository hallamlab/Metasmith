# Bacterial models only: an archaeal bin's SSU still scores against them, and the one consumer,
# the ASPIRE ASV-MAG linker, keeps any feature named 16S whatever its score.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::barrnap.env"))
bin   = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"))
gff   = model.AddProduct(lib.GetType("binning_local::quality_bin_rrna_gff"))


def protocol(context: ExecutionContext):
    threads = context.params.get("cpus") or 1
    manifest = []
    for item in context.AsBatch():
        ibin, igff = item.Input(bin), item.Output(gff)
        context.ExecWithEnv(env=image, cmd=(
            f"barrnap --kingdom bac --threads {threads} --quiet {ibin.container} > {igff.container}"
        ))
        manifest.append({gff: igff.local})

    return ExecutionResult(
        manifest=manifest,
        success=all(m[gff].exists() for m in manifest),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=bin,
    batch_size=200,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
