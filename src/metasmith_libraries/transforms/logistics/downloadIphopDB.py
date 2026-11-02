import shlex
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

image   = model.AddRequirement(lib.GetType("env::iphop.env"))
sources = model.AddProduct(lib.GetType("ref::iphop_sources"))

# iPHoP_db_Jun25_rw is GTDB r226 and is what iphop.env's version reads; see there for
# why no r232 host database is possible. A run param, so Aug23_rw (r214, iphop 1.3.3)
# or the small `iPHoP_db_rw_for-test` can be named at launch.
#
# Use `iPHoP_db_rw_for-test`, NOT the `iPHoP_db_for-test` the tool's own --help prints:
# that name 404s.
DB_VERSION = "iPHoP_db_Jun25_rw"
BASE_URL = "https://portal.nersc.gov/cfs/m342/iphop/db"
STREAMS = 4

# What `iphop download --split` 1.4.2 reads (modules/master_downloader.py), fetched here
# instead so a chunk lands resumably in the staging dir rather than restarting under a
# fresh work dir. The list is `<md5>  <file>` for the whole archive, then one line per
# 10 GB chunk, then a last line holding only the folder the archive unpacks to.
def _read_md5_list(path):
    whole, chunks, folder = None, [], None
    for line in open(path):
        tab = line.split()
        if len(tab) == 2:
            if whole is None: whole = tab
            else: chunks.append(tab)
        elif len(tab) == 1:
            folder = tab[0]
    return whole, chunks, folder


def protocol(context: ExecutionContext):
    isrc = context.Output(sources)
    staged = context.StagingDir()
    version = context.params.get("iphop_db_version", DB_VERSION)
    base = context.params.get("iphop_base_url", BASE_URL).rstrip("/")
    failed = ExecutionResult(manifest=[], success=False)

    md5_list = staged.local/f"{version}.tar.gz.md5"
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(f"{base}/{version}.tar.gz.md5", md5_list, label=f"{version} md5 list"),
    ])) != 0:
        return failed
    whole, chunks, folder = _read_md5_list(md5_list)
    if whole is None or folder is None:
        Log.Error(f"[{md5_list.name}] names no archive or no folder; is [{version}] a release iPHoP publishes?")
        return failed
    if not chunks:
        chunks = [whole]
    Log.Info(f"{version}: {len(chunks)} chunk(s), unpacking to [{folder}]")

    # One script per chunk, so xargs can run STREAMS of them and a failed chunk
    # fails the step. A chunk is promoted from `.part` only after its md5 matches.
    scripts = []
    for md5, name in chunks:
        script = Path(f"fetch_{name}.sh")
        script.write_text("set -euo pipefail\n" + FetchCommand(
            f"{base}/{name}", staged.local/name, md5=md5, label=name,
        ) + "\n")
        scripts.append(str(script))
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"printf '%s\\n' {' '.join(shlex.quote(s) for s in scripts)} | xargs -P {STREAMS} -n 1 bash",
        f"mkdir -p {isrc.local}",
        *(f"mv {staged.local/name} {isrc.local}/" for _, name in chunks),
        f"cp {md5_list} {isrc.local}/",
    ])) != 0:
        return failed

    return ExecutionResult(
        manifest=[{sources: isrc.local}],
        success=all((isrc.local/name).is_file() for _, name in chunks),
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
