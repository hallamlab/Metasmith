from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("ref::kraken2_db"))

# The release's own directory, file by file, rather than k2_standard_16gb_20240605.tar.gz:
# no 12 GB archive beside its 16 GB unpacking, and each file has its own md5.
BASE = "https://genome-idx.s3.amazonaws.com/kraken/standard_16gb_20240605"
MD5S = "standard16gb.md5"
FILES = [
    "hash.k2d", "opts.k2d", "taxo.k2d", "seqid2taxid.map", "ktaxonomy.tsv",
    "inspect.txt", "library_report.tsv",
    *(f"database{n}mers.kmer_distrib" for n in (50, 75, 100, 150, 200, 250, 300)),
]


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    base = context.params.get("kraken2_url", BASE).rstrip("/")
    failed = ExecutionResult(manifest=[], success=False)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{base}/{MD5S}", MD5S),
    ])) != 0:
        return failed
    md5 = {}
    for line in open(MD5S):
        toks = line.split()
        if len(toks) == 2:
            md5[toks[1]] = toks[0]
    missing = [f for f in FILES if f not in md5]
    assert not missing, f"{base}/{MD5S} lists no md5 for {missing}"

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"mkdir -p {iout.local}",
        *(FetchCommand(f"{base}/{f}", iout.local/f, md5=md5[f]) for f in FILES),
    ])) != 0:
        return failed

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=all((iout.local/f).is_file() and (iout.local/f).stat().st_size > 0 for f in FILES),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(2),
        duration=Duration(hours=6),
    ),
)
