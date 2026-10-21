from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

mp      = model.AddRequirement(lib.GetType("env::metapathways.env"))
pt      = model.AddRequirement(lib.GetType("env::pathologic.env"))
scripts = model.AddRequirement(lib.GetType("lib::metapathways_pgdb"))
batch   = model.AddRequirement(lib.GetType("engcyc::genome_batch"))
mpdb    = model.AddRequirement(lib.GetType("engcyc::metapathways_db"))
out     = model.AddProduct(lib.GetType("engcyc::pgdb_batch"))

# Node-local disk. MetaPathways and Nextflow write ~50 files per genome plus a work
# tree, which the shared filesystem's file quota cannot absorb across a large run.
LOCAL = ("${SLURM_TMPDIR:-${TMPDIR:-/tmp}}", "/lscratch")
# Pathway Tools finds its home through the passwd entry, not $HOME, and stops at
# start-up when that directory is absent, which `--no-home` makes it.
PT_HOME = (f"{LOCAL[0]}/ptools_home", "$HOME")


def protocol(context: ExecutionContext):
    iscripts = context.Input(scripts)
    ibatch   = context.Input(batch)
    impdb    = context.Input(mpdb)
    iout     = context.Output(out)

    cpus   = int(context.params.get("cpus", 16))
    mem_gb = int(float(context.params.get("memory", 64)))
    work   = f"{LOCAL[1]}/{ibatch.local.name}"
    lib_dir = iscripts.container

    context.ExecWithEnv(
        env=mp, binds=[LOCAL],
        cmd=f"""
            mkdir -p {work} {LOCAL[1]}/ptools_home
            bash {lib_dir}/annotate.sh {ibatch.container} {impdb.container} {work} \
                {cpus} {mem_gb - 2} {max(4, mem_gb // 4)} > {work}/annotate.tsv
        """,
    )
    context.ExecWithEnv(
        env=pt, binds=[LOCAL, PT_HOME],
        cmd=f"""
            export TMPDIR={work}/tmp
            mkdir -p $TMPDIR
            bash {lib_dir}/pathologic.sh {work}/pf {work}/pgdb {cpus} > {work}/pathologic.tsv
        """,
    )
    context.ExecWithEnv(
        env=mp, binds=[LOCAL],
        cmd=f"bash {lib_dir}/pack.sh {work} {iout.container}",
    )

    stats = iout.local / "stats.tsv"
    packed = list(iout.local.glob("*.tar.gz")) if iout.local.exists() else []
    if stats.exists():
        print(stats.read_text(), flush=True)
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=stats.exists() and len(packed) > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=batch,
    resources=Resources(
        cpus=16,
        memory=Size.GB(64),
        duration=Duration(hours=12),
    ),
)
