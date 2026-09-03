from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::metabuli.env"))
ref   = model.AddProduct(lib.GetType("ref::metabuli_ref"))

# Pinned, because `metabuli databases GTDB` is not. It resolves whatever the
# upstream index calls current, so two runs of the same transform can produce
# different databases and the taxonomy they assign is not comparable. gtdb232 is
# also the release env/metabuli.env's 1.2.0 floor exists for -- 1.1.0 cannot read
# its format at all -- and the release GTDB-Tk is pinned to in downloadGtdbDB.py,
# so bin-level and contig-level taxonomy agree.
DB_URL = "https://steineggerlab.s3.amazonaws.com/metabuli/gtdb232.tar.gz"


def protocol(context: ExecutionContext):
    idb = context.Output(ref)

    context.LocalShell(f"""
        set -euo pipefail
        mkdir -p {idb.local}
        cd {idb.local}
        wget -c --no-check-certificate -O gtdb232.tar.gz "{DB_URL}"
        tar xzf gtdb232.tar.gz
        rm -f gtdb232.tar.gz

        # The tarball's nesting has changed between releases, and metabuli is
        # given this directory verbatim -- so flatten to the level db.parameters
        # is on rather than assume either shape.
        if [ ! -f db.parameters ]; then
            inner=$(dirname "$(find . -maxdepth 3 -name db.parameters -print -quit)")
            [ -n "$inner" ] && [ "$inner" != "." ] && mv "$inner"/* . && rm -rf "$inner"
        fi
    """)

    # Assert the extracted database, not the command's exit code. The r1 campaign
    # burned a day on exactly this: its verify stage ran `metabuli databases` and
    # read the return value, which said nothing about whether any data landed. A
    # truncated tarball extracts to something that looks like a database and
    # classifies nothing.
    params = next(iter(sorted(idb.local.glob("**/db.parameters"))), None)
    return ExecutionResult(
        manifest=[{ref: idb.local}],
        success=params is not None,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=8),
    ),
)
