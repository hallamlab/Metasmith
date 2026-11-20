from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

pt      = model.AddRequirement(lib.GetType("env::pathologic.env"))
python  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"), parents={asm})
ann     = model.AddRequirement(lib.GetType("annotation::ptools_annotation_table"), parents={orfs})
mag     = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"), parents={asm})
pt_lib  = model.AddRequirement(lib.GetType("lib::pathologic"))
ev_lib  = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
archive = model.AddProduct(lib.GetType("annotation::bin_pgdb_archive"))
table   = model.AddProduct(lib.GetType("annotation::bin_pathway_table"))

LOCAL = ("${SLURM_TMPDIR:-${TMPDIR:-/tmp}}", "/lscratch")
# Pathway Tools finds its home through the passwd entry, not $HOME, and stops at
# start-up when that directory is absent, which `--no-home` makes it.
PT_HOME = (f"{LOCAL[0]}/ptools_home", "$HOME")


# The MAG takes the assembly's own ORF annotations on its contigs, so a MAG
# PGDB and the assembly PGDB name the same ORFs and nothing is re-predicted.
def protocol(context: ExecutionContext):
    iann     = context.Input(ann)
    imag     = context.Input(mag)
    ilib     = context.Input(pt_lib)
    iev      = context.Input(ev_lib)
    iarchive = context.Output(archive)
    itable   = context.Output(table)
    name     = imag.local.stem

    # The bind source must exist before apptainer mounts it, and only a container that binds LOCAL
    # expands it the way the Pathway Tools step will.
    context.ExecWithEnv(env=python, binds=[LOCAL], cmd=f"""
        mkdir -p {LOCAL[1]}/ptools_home
        PYTHONPATH=$(dirname {iev.container}) python3 {ilib.container}/build_pf.py --annotations {iann.container} --name {name} \
            --bin-fasta {imag.container} --out-dir ptin
    """)
    context.ExecWithEnv(env=pt, binds=[LOCAL, PT_HOME], cmd=f"""
        export TMPDIR={LOCAL[1]}/pathologic_{name}
        mkdir -p $TMPDIR
        bash {ilib.container}/run.sh ptin pgdb
    """)
    context.ExecWithEnv(env=python, cmd=f"""
        python3 {ilib.container}/pathway_table.py --pgdb pgdb --out {itable.container}
    """)
    context.LocalShell(f"tar czf {iarchive.local} pgdb pgdb.log")

    return ExecutionResult(
        manifest=[{archive: iarchive.local, table: itable.local}],
        success=iarchive.local.exists() and itable.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=mag,
    resources=Resources(
        cpus=1,
        memory=Size.GB(16),
        duration=Duration(hours=6),
    ),
)
