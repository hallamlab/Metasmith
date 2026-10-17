from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::metaphlan.env"))
out   = model.AddProduct(lib.GetType("ref::metaphlan_db"))

# Pinned rather than `latest`, which is now vJan26 and newer than MetaPhlAn 4.2.4;
# vJan25 is the database 4.2 shipped with, and strainphlan.py names it too.
INDEX = "mpa_vJan25_CHOCOPhlAnSGB_202503"
BASE  = "https://cmprod1.cibio.unitn.it/biobakery4/metaphlan_databases"
# Each is a .tar with its .md5 beside it.
ARCHIVES = [f"bowtie2_indexes/{INDEX}_bt2", INDEX]
BT2L = [f"{INDEX}.{n}.bt2l" for n in ("1", "2", "3", "4", "rev.1", "rev.2")]

# This leaves what `metaphlan --install` leaves, without the part that costs: install
# unpacks the SGB marker FASTA (4.8 GB as bz2), concatenates it, and deletes it once
# bowtie2-inspect has accepted the shipped index. The same bowtie2-inspect is the check
# here. `metaphlan --install` itself is not, because loading the .pkl peaks at 6.6 GB,
# which a login node should not spend on a check the md5 has already made.


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    base = context.params.get("metaphlan_url", BASE).rstrip("/")
    db   = iout.local
    failed = ExecutionResult(manifest=[], success=False)

    names = [Path(a).name for a in ARCHIVES]
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *(FetchCommand(f"{base}/{a}.md5", f"{n}.md5") for a, n in zip(ARCHIVES, names)),
    ])) != 0:
        return failed
    md5 = [open(f"{n}.md5").read().split()[0] for n in names]

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        *(FetchCommand(f"{base}/{a}.tar", f"{n}.tar", md5=m) for a, n, m in zip(ARCHIVES, names, md5)),
        f"mkdir -p {db}",
        FetchCommand(f"{base}/{INDEX}.nwk", db/f"{INDEX}.nwk"),
        *(f"tar -xf {n}.tar -C {db} --exclude={INDEX}_SGB.fna.bz2" for n in names),
        f"bunzip2 -f {db}/{INDEX}_VSG.fna.bz2",
        # The consumer runs --offline with no --index, which reads the index name from here.
        f"echo {INDEX} > {db}/mpa_latest",
        *(f"rm -f {n}.tar" for n in names),
    ])) != 0:
        return failed

    context.ExecWithEnv(env=image, cmd=f"bowtie2-inspect -n {iout.container}/{INDEX} > /dev/null")

    whole = all((db/f).is_file() and (db/f).stat().st_size > 0 for f in [
        *BT2L, f"{INDEX}.pkl", f"{INDEX}_VSG.fna", f"{INDEX}_VINFO.csv", f"{INDEX}.nwk", "mpa_latest",
    ])
    return ExecutionResult(manifest=[{out: db}], success=whole)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=12),
    ),
)
