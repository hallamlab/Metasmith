from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::vcontact3.env"))
db    = model.AddProduct(lib.GetType("ref::vcontact3_db"))

# vcontact3/databases.py fetches the archive with a hard-coded
# `curl -s -o <dest> <url>` and its own image ships no curl -- only wget and
# aria2c -- so `prepare_databases` dies on FileNotFoundError before it reaches
# the network. wget is a drop-in for the one call shape the tool makes, and it
# is a better one: `curl -s -o` exits 0 on a 404 and leaves the error page in
# the destination, which the tool would then hand to unpack_archive, whereas
# wget fails loudly and the tool's own returncode check catches it.
CURL_SHIM = r"""#!/bin/sh
dest=""; url=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) dest="$2"; shift 2 ;;
    -s|-S|-L|-f) shift ;;
    *) url="$1"; shift ;;
  esac
done
exec wget --quiet --tries=5 --timeout=60 -O "$dest" "$url"
"""


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    shim_dir = Path("_shim")
    shim_dir.mkdir(exist_ok=True)
    shim = shim_dir/"curl"
    shim.write_text(CURL_SHIM)
    shim.chmod(0o755)

    # This must run in vcontact3's OWN image, which is why the env is a
    # requirement rather than a convenience. From 3.0.5 `prepare_databases` only
    # offers releases the installed version can read, so fetching the same
    # database from anywhere else can stage one 3.1.4 will refuse at run time.
    _cmd = f"""
        export PATH="$PWD/{shim_dir}:$PATH"
        vcontact3 prepare_databases --get-version "latest" --set-location ./vcontact3_db
    """
    context.ExecWithEnv(env=image, cmd=_cmd)

    staged = Path("vcontact3_db")
    # The tool unpacks beside the archive and leaves it there, so keeping it
    # would double a two-gigabyte reference for nothing.
    for archive in list(staged.glob("*.tar.gz")) + list(staged.glob("*.tar.xz")):
        archive.unlink()
    versions = sorted(staged.glob("[0-9][0-9][0-9].json"))
    assert versions, f"vcontact3 staged no version json, only {sorted(p.name for p in staged.iterdir())}"
    Log.Info(f"vcontact3 database versions staged: {[p.stem for p in versions]}")
    staged.rename(idb.local)

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=idb.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=4),
    ),
)
