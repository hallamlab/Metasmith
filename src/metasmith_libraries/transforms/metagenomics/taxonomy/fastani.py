from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
pan     = model.AddRequirement(lib.GetType("pangenome::pangenome"))
asm     = model.AddRequirement(lib.GetType("sequences::putative_genome"), parents={pan})
image   = model.AddRequirement(lib.GetType("env::fastani.env"))
out     = model.AddProduct(lib.GetType("taxonomy::ani_table"))

def protocol(context: ExecutionContext):
    ipan = context.Input(pan)
    iasm = context.InputGroup(asm)
    iout = context.Output(out)

    genomes = "genomes.list"
    with open(genomes, "w") as f:
        for path in iasm:
            f.write(str(path.container)+"\n")

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--threads {threads}"
    context.ExecWithEnv().ifContainerDo(
        env = image,
        cmd = f"""
            fastANI {threads} --queryList {genomes} --refList {genomes} --output {iout.container} 
        """,
    )

    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
            },
        ],
        success=iout.local.exists()
    )

TransformInstance(
    protocol=protocol,
    model=model, # the contract
    group_by=pan,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=3),
    )
)
