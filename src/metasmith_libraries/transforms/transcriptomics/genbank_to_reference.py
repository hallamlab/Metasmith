from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
gbk   = model.AddRequirement(lib.GetType("sequences::gbk"))
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
helpers = model.AddRequirement(lib.GetType("lib::transcriptomics"))
fasta = model.AddProduct(lib.GetType("transcriptomics::organellar_reference"))
gff   = model.AddProduct(lib.GetType("transcriptomics::organellar_gff"))

def protocol(context: ExecutionContext):
    igbk   = context.Input(gbk)
    ihelp = context.Input(helpers)
    ifasta = context.Output(fasta)
    igff   = context.Output(gff)

    context.ExecWithEnv(
        env=image,
        cmd=f"python {ihelp.container}/genbank_to_reference.py {igbk.container} {ifasta.container} {igff.container}",
    )
    return ExecutionResult(
        manifest=[
            {fasta: ifasta.local},
            {gff: igff.local},
        ],
        success=ifasta.local.exists() and igff.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=gbk,
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(minutes=30),
    ),
)
