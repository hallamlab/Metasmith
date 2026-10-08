# contaminant_set -- no .nf process. The default contaminant set: the literature list and the
# Biofactorial list. Each file's stem is the list name a contaminant hit reports.

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
lit   = model.AddRequirement(lib.GetType("aspire::contaminants_literature"))
biof  = model.AddRequirement(lib.GetType("aspire::contaminants_biofactorial"))
cset  = model.AddProduct(lib.GetType("aspire::contaminant_reference_set"))


def protocol(context: ExecutionContext):
    iset = context.Output(cset)
    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        mkdir -p {iset.container}
        cp -L {context.Input(lit).container} {iset.container}/literature.fasta
        cp -L {context.Input(biof).container} {iset.container}/biofactorial.fasta
    """)

    return ExecutionResult(
        manifest=[{cset: iset.local}],
        success=all((iset.local / f).exists() for f in ("literature.fasta", "biofactorial.fasta")),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=lit,
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=1),
    ),
)
