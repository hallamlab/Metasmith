from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

image   = model.AddRequirement(lib.GetType("env::metabuli.env"))
sources = model.AddProduct(lib.GetType("ref::metabuli_sources"))

# Pinned, because `metabuli databases GTDB` is not. It resolves whatever the
# upstream index calls current, so two runs of the same transform can produce
# different databases and the taxonomy they assign is not comparable. gtdb232 is
# also the release env/metabuli.env's 1.2.0 floor exists for -- 1.1.0 cannot read
# its format at all -- and the release GTDB-Tk is pinned to in downloadGtdbDB.py,
# so bin-level and contig-level taxonomy agree.
DB_URL = "https://steineggerlab.s3.amazonaws.com/metabuli/gtdb232.tar.gz"


def protocol(context: ExecutionContext):
    isrc = context.Output(sources)
    staged = context.StagingDir()
    url = context.params.get("metabuli_db_url", DB_URL)

    # 634 GB packed. The archive lands in the staging dir, so a retry resumes the
    # `.part` where the attempt before it stopped, and buildMetabuliDB unpacks it on a
    # compute node: 744 GB more at the peak, 1.4 TB in all. Upstream publishes no
    # checksum, so gzip's CRC during the unpack is the check.
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(url, staged.local/"metabuli.tar.gz", label="metabuli"),
        f"mv {staged.local}/metabuli.tar.gz {isrc.local}",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{sources: isrc.local}],
        success=isrc.local.is_file() and isrc.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(hours=24),
    ),
)
