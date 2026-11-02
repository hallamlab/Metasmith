import re
from pathlib import Path
from urllib.parse import urlparse

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::phyloflash.env"))
out   = model.AddProduct(lib.GetType("ref::phyloflash_sources"))

# Pinned rather than phyloFlash_makedb.pl --remote, which takes SILVA's `current`
# (144 since Aug 2026) and has no option to name a release.
SILVA_URL  = ("https://www.arb-silva.de/fileadmin/silva_databases/release_138_2/Exports/"
              "SILVA_138.2_SSURef_NR99_tax_silva_trunc.fasta.gz")
UNIVEC_URL = "https://ftp.ncbi.nlm.nih.gov/pub/UniVec/UniVec"


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    # Run params, so a moved upstream file is fixed at launch rather than by a release.
    silva_url  = context.params.get("phyloflash_silva_url", SILVA_URL)
    univec_url = context.params.get("phyloflash_univec_url", UNIVEC_URL)

    # makedb reads the release from the file name, so refuse a name it cannot read
    # before fetching rather than after.
    silva_file = Path(urlparse(silva_url).path).name
    assert re.match(r"SILVA_[^_]+_", silva_file), \
        f"[{silva_file}] is not named SILVA_<release>_..., which makedb requires"

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(silva_url, silva_file, label="SILVA SSU"),
        FetchCommand(univec_url, "UniVec", label="UniVec"),
        f"gzip -t {silva_file}",
        f"mkdir -p {iout.local}",
        f"mv {silva_file} UniVec {iout.local}/",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    univec = iout.local/"UniVec"
    whole = (iout.local/silva_file).stat().st_size > 0 \
        and univec.stat().st_size > 0 and univec.read_bytes()[:1] == b">"
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=whole,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=2),
    ),
)
