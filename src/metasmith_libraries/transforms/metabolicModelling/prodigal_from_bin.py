# Gene calling on ONE metagenomic bin, following `metagenomics/prodigal.py`'s
# shape but scoped to a single putative genome rather than a whole assembly.
# The product is `sequences::bin_orfs`, a DISTINCT sibling of `sequences::orfs`
# (see that type's comment in `data_types/sequences.yml`) -- the point of this
# transform is the type, not the tool: it is what lets
# `carveme_from_orfs.py`'s reconstruction pin to bin-derived ORFs and be
# unreachable from the whole-assembly or viral-frozen-set producers of plain
# `orfs`. No sharding here (unlike `prodigal.py`): a single bin's ORF count
# never approaches the shard-worthy sizes that whole-sample annotation fans
# out over.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::pprodigal.env"))
bin_  = model.AddRequirement(lib.GetType("sequences::bin_fasta"))
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
