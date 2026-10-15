import re
from pathlib import Path
from urllib.parse import urlparse

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::phyloflash.env"))
out   = model.AddProduct(lib.GetType("ref::phyloflash_db"))

# Pinned rather than phyloFlash_makedb.pl --remote, which takes SILVA's `current`
# (144 since Aug 2026) and has no option to name a release.
SILVA_URL  = ("https://www.arb-silva.de/fileadmin/silva_databases/release_138_2/Exports/"
              "SILVA_138.2_SSURef_NR99_tax_silva_trunc.fasta.gz")
UNIVEC_URL = "https://ftp.ncbi.nlm.nih.gov/pub/UniVec/UniVec"


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    threads = context.params.get('cpus')
    threads_arg = "" if threads is None else f"-CPUs {threads}"
    # Run params, so a moved upstream file is fixed at launch rather than by a release.
    silva_url  = context.params.get("phyloflash_silva_url", SILVA_URL)
    univec_url = context.params.get("phyloflash_univec_url", UNIVEC_URL)

    # makedb reads the release from the file name and builds into ./<release>/.
    silva_file = Path(urlparse(silva_url).path).name
    release = re.match(r"SILVA_([^_]+)_", silva_file)
    assert release, f"[{silva_file}] is not named SILVA_<release>_..., which makedb requires"
    release = release.group(1)

    # The image has no wget, so fetch on the host into the shared work directory.
    context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(silva_url, silva_file, label="SILVA SSU"),
        FetchCommand(univec_url, "UniVec", label="UniVec"),
    ]))

    # phyloFlash.pl -dbhome must name the release directory itself, and consumers
    # pass this product as -dbhome, so the release directory becomes the product.
    context.ExecWithEnv(env=image, cmd=f"""
        set -e
        phyloFlash_makedb.pl --silva_file {silva_file} --univec_file UniVec {threads_arg}
        mkdir -p $(dirname {iout.container})
        if [ -d {iout.container} ]; then rmdir {iout.container}; fi
        mv {release} {iout.container}
    """)
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=(iout.local / "ref" / "genome" / "1" / "summary.txt").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(
        cpus=4,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    ),
)
