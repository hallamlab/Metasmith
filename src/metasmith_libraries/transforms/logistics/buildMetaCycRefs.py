from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
# LICENSED. The Pathway Tools image exists only when the user gave licensed::pathway_tools_package
# (or a prebuilt image), so these references, and the lanes reading them, stay off until then.
pt      = model.AddRequirement(lib.GetType("env::pathologic.env"))
mp4     = model.AddRequirement(lib.GetType("env::metapathways.env"))
diamond = model.AddRequirement(lib.GetType("env::diamond.env"))
pt_lib  = model.AddRequirement(lib.GetType("lib::pathologic"))
db      = model.AddProduct(lib.GetType("ref::metacyc_protein_diamond_db"))
rxn     = model.AddProduct(lib.GetType("ref::metacyc_monomer_reactions"))

LOCAL = ("${SLURM_TMPDIR:-${TMPDIR:-/tmp}}", "/lscratch")
# As pathologic.py: Pathway Tools finds its home through the passwd entry, and `--no-home` removes it.
PT_HOME = (f"{LOCAL[0]}/ptools_home", "$HOME")


# MetaPathways 4's own make_tables turns the export into MetaCyc-monomer-rxn-pairs.tsv, so the
# monomer-reaction pairs are MP4's and of the same release as the proteins.
def protocol(context: ExecutionContext):
    ilib = context.Input(pt_lib)
    idb, irxn = context.Output(db), context.Output(rxn)
    context.ExecWithEnv(env=pt, binds=[LOCAL], cmd=f"mkdir -p {LOCAL[1]}/ptools_home")
    context.ExecWithEnv(env=pt, binds=[LOCAL, PT_HOME], cmd=f"""
        export TMPDIR={LOCAL[1]}/metacyc_export
        mkdir -p $TMPDIR
        bash {ilib.container}/export_metacyc.sh export
    """)
    context.ExecWithEnv(env=mp4, cmd="""
        python3 -c 'import sys; from metapathways.metacyc_db import make_tables; make_tables(sys.argv[1], sys.argv[2])' export tables
    """)
    context.ExecWithEnv(env=diamond, cmd=f"""
        set -euo pipefail
        diamond makedb --in export/protseq.fsa -d metacyc
        mv metacyc.dmnd {idb.container}
        cp tables/MetaCyc-monomer-rxn-pairs.tsv {irxn.container}
        chmod 600 {idb.container} {irxn.container}
        rm -r export tables
    """)
    return ExecutionResult(manifest=[{db: idb.local, rxn: irxn.local}],
                           success=idb.local.exists() and irxn.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=pt,
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(hours=2)),
)
