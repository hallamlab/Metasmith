from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
trnascan = model.AddRequirement(lib.GetType("env::trnascan.env"))
python   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
asm      = model.AddRequirement(lib.GetType("sequences::assembly"))
rna_lib  = model.AddRequirement(lib.GetType("lib::rna_scan"))
out      = model.AddProduct(lib.GetType("annotation::trna_gff"))


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    irna = context.Input(rna_lib)
    iout = context.Output(out)

    threads = context.params.get("cpus", 8)
    # General mode (-G): a metagenome mixes bacteria, archaea and eukaryotes.
    context.ExecWithEnv(env=trnascan, cmd=f"""
        tRNAscan-SE -G --brief --thread {threads} -Q -o trnascan.txt {iasm.container}
    """)
    context.ExecWithEnv(env=python, cmd=f"""
        PYTHONPATH={irna.container} python3 {irna.container}/trna.py trnascan \
            --raw trnascan.txt --out {iout.container}
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=8,
        memory=Size.GB(8),
        duration=Duration(hours=12),
    ),
)
