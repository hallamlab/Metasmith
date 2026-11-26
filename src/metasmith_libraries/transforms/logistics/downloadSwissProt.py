import re

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("ref::swissprot_sequences"))

# current_release/ rolls forward; RELEASE.metalink names the release and carries the md5,
# reldate.txt dates it. The EBI mirror first: the US host has answered with zero-byte files.
RELEASE_DIR = "current_release/knowledgebase/complete"
MIRRORS = ["https://ftp.ebi.ac.uk/pub/databases/uniprot", "https://ftp.expasy.org/databases/uniprot",
           "https://ftp.uniprot.org/pub/databases/uniprot"]
FILES = ("RELEASE.metalink", "reldate.txt", "uniprot_sprot.fasta.gz")
URLS = {name: [f"{m}/{RELEASE_DIR}/{name}" for m in MIRRORS] for name in FILES}


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    failed = ExecutionResult(manifest=[], success=False)
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"mkdir -p {iout.local}",
        FetchCommand(URLS["RELEASE.metalink"], iout.local/"RELEASE.metalink"),
        FetchCommand(URLS["reldate.txt"], iout.local/"reldate.txt"),
    ])) != 0:
        return failed
    metalink = (iout.local/"RELEASE.metalink").read_text()
    release = re.search(r"<version>([^<]+)", metalink)
    md5 = re.search(r'file name="uniprot_sprot.fasta.gz">.*?<hash type="md5">([0-9a-f]+)',
                    metalink, re.S)
    assert release and md5, "RELEASE.metalink names no release or no md5 for uniprot_sprot.fasta.gz"
    (iout.local/"release").write_text(release.group(1) + "\n")
    Log.Info(f"Swiss-Prot {release.group(1)}, md5 {md5.group(1)}")

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(URLS["uniprot_sprot.fasta.gz"], iout.local/"uniprot_sprot.fasta.gz",
                     md5=md5.group(1)),
    ])) != 0:
        return failed
    return ExecutionResult(manifest=[{out: iout.local}],
                           success=all((iout.local/f).stat().st_size > 0 for f in FILES))


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    download_size=Size.GB(0.09),  # release 2026_04, 93,867,502 B by Content-Length, 2026-11-26
    resources=Resources(cpus=1, memory=Size.GB(1), duration=Duration(hours=1)),
)
