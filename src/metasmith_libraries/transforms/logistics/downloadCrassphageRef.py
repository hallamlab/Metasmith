from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))  # has wget
ref   = model.AddProduct(lib.GetType("annotation::crassphage_ref"))

# crAssphage reference genome (NCBI nuccore NC_024711.1) as a single FASTA.
EFETCH_URL = (
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    "?db=nuccore&id=NC_024711.1&rettype=fasta&retmode=text"
)


def protocol(context: ExecutionContext):
    iref = context.Output(ref)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            wget -q --no-check-certificate "{EFETCH_URL}" -O {iref.container}
        """,
    )

    return ExecutionResult(
        manifest=[{ref: iref.local}],
        success=iref.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(minutes=15),
    ),
)
