from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::amrfinderplus.env"))
db    = model.AddProduct(lib.GetType("annotation::amrfinderplus_db"))

# `amrfinder_update` takes no version: it installs the newest release under the
# software's minor version and indexes it with `amrfinder_index`. This does the same
# two steps for a named release. A release lives under the minor version of the
# software that reads it, and 4.2 is amrfinderplus.env's 4.2.7.
BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pathogen/Antimicrobial_resistance/AMRFinderPlus/database"
MINOR = "4.2"
DB_VERSION = "2026-08-07.1"

# The list amrfinder_update.cpp fetches, plus one AMR_DNA pair per taxgroup that
# taxgroup.tsv gives nucleotide reference genes.
FILES = [
    "AMR.LIB", "AMRProt.fa", "AMRProt-mutation.tsv", "AMRProt-suppress.tsv",
    "AMRProt-susceptible.fa", "AMRProt-susceptible.tsv", "AMR_CDS.fa",
    "database_format_version.txt", "fam.tsv", "taxgroup.tsv", "version.txt", "changes.txt",
]


def protocol(context: ExecutionContext):
    idb = context.Output(db)
    base = context.params.get("amrfinder_db_url", BASE_URL).rstrip("/")
    minor = context.params.get("amrfinder_db_minor", MINOR)
    assert not isinstance(minor, float), f"amrfinder_db_minor [{minor}] was read as a number; quote it, e.g. \"{minor}\""
    minor = str(minor)
    version = str(context.params.get("amrfinder_db_version", DB_VERSION))
    url = f"{base}/{minor}/{version}"
    stage = Path("amrfinderdb")
    failed = ExecutionResult(manifest=[], success=False)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"mkdir -p {stage}",
        *(FetchCommand(f"{url}/{f}", stage/f) for f in FILES),
    ])) != 0:
        return failed

    taxgroups = []
    for line in open(stage/"taxgroup.tsv"):
        if line.startswith("#") or not line.strip():
            continue
        taxgroup, _, n = line.split("\t")[:3]
        if int(n) > 0:
            taxgroups.append(taxgroup)
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *(FetchCommand(f"{url}/AMR_DNA-{t}.{ext}", stage/f"AMR_DNA-{t}.{ext}")
          for t in taxgroups for ext in ("fa", "tsv")),
    ])) != 0:
        return failed

    # `amrfinder -V` reads the database the way a search does, so a release this
    # software cannot read fails here rather than in the first annotation.
    context.ExecWithEnv(
        env=image,
        cmd=f"""
            amrfinder_index {stage}
            amrfinder -d {stage} -V
        """,
    )

    stage.rename(idb.local)

    found = (idb.local/"version.txt").read_text().strip()
    Log.Info(f"AMRFinderPlus database [{found}] with {len(taxgroups)} point-mutation taxgroups")
    whole = found == version and all(
        (idb.local/f).is_file() and (idb.local/f).stat().st_size > 0
        for f in ("AMRProt.fa.phr", "AMR_CDS.fa.nhr", "AMR.LIB.h3m")
    )
    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=whole,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
