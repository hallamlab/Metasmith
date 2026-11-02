from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::diamond.env"))
fasta   = model.AddRequirement(lib.GetType("ref::uniref50_fasta"))
db      = model.AddProduct(lib.GetType("ref::uniref50_diamond_db"))


def protocol(context: ExecutionContext):
    ifasta = context.Input(fasta)
    idb = context.Output(db)
    threads = context.params.get("cpus") or 8

    # makedb holds one 1e9-letter block plus a 16-byte entry per sequence, whatever the
    # input's size: rebuilding the 2026-08 release (38.8 M sequences, 12.2 G letters)
    # peaked at 3.4 GB RSS with --threads 4, in 21 min.
    context.ExecWithEnv(env=image, cmd="\n".join([
        f"diamond makedb --in {ifasta.container} -d uniref50 --threads {threads}",
        "diamond dbinfo -d uniref50.dmnd",
        f"mv uniref50.dmnd {idb.container}",
    ]))

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=idb.local.is_file() and idb.local.stat().st_size > 0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=fasta,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
