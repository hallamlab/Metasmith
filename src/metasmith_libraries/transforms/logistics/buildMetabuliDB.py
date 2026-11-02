from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

sources = model.AddRequirement(lib.GetType("ref::metabuli_sources"))
ref     = model.AddProduct(lib.GetType("ref::metabuli_ref"))


def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    idb = context.Output(ref)
    threads = context.params.get("cpus") or 4

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        "mkdir -p extract",
        f"tar -I 'pigz -p {threads}' -xf {isrc.local} -C extract",
        # The tarball's nesting has changed between releases (gtdb232 wraps everything
        # in gtdb+human+virus/), and metabuli is given this directory verbatim -- so
        # the directory holding db.parameters becomes the product, whatever its depth.
        "params=$(find extract -maxdepth 3 -name db.parameters -print -quit)",
        '[ -n "$params" ] || { echo "the archive holds no db.parameters" >&2; exit 1; }',
        f'mv "$(dirname "$params")" {idb.local}',
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    # Assert the extracted database, not the command's exit code. The r1 campaign
    # burned a day on exactly this: its verify stage ran `metabuli databases` and
    # read the return value, which said nothing about whether any data landed.
    # The alternatives are the ones `metabuli classify` 1.2.0 accepts at load.
    def present(*names):
        return all((idb.local/n).is_file() and (idb.local/n).stat().st_size > 0 for n in names)
    whole = present("db.parameters") \
        and (present("diffIdx", "info", "split") or present("deltaIdx.mtbl", "deltaIdxSplits.mtbl")) \
        and (present("taxonomyDB") or present("taxonomy/merged.dmp"))
    return ExecutionResult(
        manifest=[{ref: idb.local}],
        success=whole,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sources,
    labels=["noscratch"],
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=24),
    ),
)
