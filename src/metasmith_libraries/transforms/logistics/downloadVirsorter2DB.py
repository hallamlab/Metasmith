# VirSorter2's own setup, driven from inside its own image -- and that image has no
# downloader. `virsorter setup` is a snakemake workflow whose download rules shell out
# to `wget`, which is absent here, as is curl. Every rule therefore failed with
# "wget: command not found" and snakemake reported it as a rule error rather than as a
# missing dependency. The shim below answers `wget` with python3, which the image does
# have, honouring the flags those rules actually pass.
#
# Two details of that shim are load-bearing. The rules read the command's OUTPUT for
# the string "404 Not Found" and fall back to a second mirror when they see it, so a
# failure has to exit non-zero rather than write a short file; and `--tries N` and
# `--timeout N` arrive as two tokens, so a naive "skip anything starting with -" parser
# consumes the URL as a flag argument.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::virsorter2.env"))
db    = model.AddProduct(lib.GetType("annotation::virsorter2_db"))

WGET_SHIM = r'''#!/usr/bin/env python3
import sys, time, urllib.error, urllib.request

dest, url, args, i = None, None, sys.argv[1:], 0
while i < len(args):
    a = args[i]
    if a == "-O":
        dest, i = args[i + 1], i + 2
    elif a.startswith("-O") and len(a) > 2:
        dest, i = a[2:], i + 1
    elif a in ("--tries", "--timeout", "--waitretry", "-T", "-t"):
        i += 2
    elif a.startswith("-"):
        i += 1
    else:
        url, i = a, i + 1

if url is None:
    sys.exit(2)

for attempt in range(4):
    try:
        with urllib.request.urlopen(url, timeout=120) as r, open(dest or "index.html", "wb") as o:
            while chunk := r.read(1 << 20):
                o.write(chunk)
        sys.exit(0)
    except urllib.error.HTTPError as e:
        print(f"{e.code} {e.reason}", file=sys.stderr)
        if e.code == 404:
            print("404 Not Found")
            sys.exit(8)
    except Exception as e:
        print(e, file=sys.stderr)
    time.sleep(5 * (attempt + 1))
sys.exit(1)
'''


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    cpus = context.params.get("cpus", 4)

    shim_dir = Path("_shim")
    shim_dir.mkdir(exist_ok=True)
    shim = shim_dir/"wget"
    shim.write_text(WGET_SHIM)
    shim.chmod(0o755)

    # HOME points into the work directory rather than at /tmp: setup runs snakemake
    # with --use-conda, and while the environments land under --conda-prefix the
    # package cache follows HOME. On a host whose /tmp is a small shared tmpfs that
    # is how you fill it.
    context.ExecWithEnv(
        env=image,
        cmd=f"""
            export HOME="$PWD"
            export PATH="$PWD/{shim_dir}:$PATH"
            virsorter setup -d vs2_db -j {cpus}
        """,
    )

    Path("vs2_db").rename(idb.local)

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
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
