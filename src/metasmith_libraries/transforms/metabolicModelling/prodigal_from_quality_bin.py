# prodigal_from_bin.py on one quality MAG from the aggregator's pool, so CarveMe models
# every quality MAG rather than one binner's bins.
#
# CAUTION a copy of prodigal_from_bin.py with one requirement changed: keep the protocol
# and the resources in step with it. gtdbtk_quality.py says why the pool is not retyped.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::pprodigal.env"))
bin_  = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"))
cds   = model.AddProduct(lib.GetType("sequences::bin_orfs"))

def protocol(context: ExecutionContext):
    ibin = context.Input(bin_)
    icds = context.Output(cds)

    cpus_string = ""
    cpus = context.params.get("cpus")
    if cpus is not None:
        cpus_string = f"-T {cpus}"

    _cmd = f"""\
            pprodigal \
                {cpus_string} \
                -p meta \
                -i {ibin.container} \
                -a {icds.container} \
                -f gff \
                -o bin_orfs.gff
            """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{cds: icds.local}],
        success=icds.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=bin_,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
