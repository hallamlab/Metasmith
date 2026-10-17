from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))
ref   = model.AddProduct(lib.GetType("annotation::crassphage_ref"))

ACCESSION = "NC_024711.1"
EFETCH_URL = (
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    f"?db=nuccore&id={ACCESSION}&rettype=fasta&retmode=text"
)


def protocol(context: ExecutionContext):
    iref = context.Output(ref)
    url = context.params.get("crassphage_url", EFETCH_URL)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(url, iref.local, label=f"crAssphage {ACCESSION}"),
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    # efetch answers an unknown or throttled request with 200 and an error body.
    with open(iref.local, errors="replace") as f:
        header = f.readline()
        bases = sum(len(line.strip()) for line in f if not line.startswith(">"))
    Log.Info(f"[{header.strip()}] carries {bases:,} bp")
    return ExecutionResult(
        manifest=[{ref: iref.local}],
        success=header.startswith(">") and bases > 90_000,
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
