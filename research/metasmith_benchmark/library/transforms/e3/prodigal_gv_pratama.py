# prodigal-gv on the frozen viral set (tool table: E3 prodigal-gv on viral contigs).
# Same command as the standard prodigal_gv.py, under E3's own product types. As sequences::orfs the
# viral genes also answer the metaSPAdes assembly's Prodigal target, because the frozen set descends from it.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

# checkv.env ships prodigal-gv 2.11.0-gv.
image  = model.AddRequirement(lib.GetType("env::checkv.env"))
frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
cds    = model.AddProduct(lib.GetType("e3::viral_orfs"))
gff    = model.AddProduct(lib.GetType("e3::viral_gff"))


def protocol(context: ExecutionContext):
    ifrozen = context.Input(frozen)
    outs = {p: context.Output(p) for p in (cds, gff)}
    context.ExecWithEnv(env=image, cmd=f"""
        prodigal-gv -p meta -i {ifrozen.container} -a viral_orfs.faa -f gff -o viral_orfs.gff
    """)
    context.LocalShell(f"cp viral_orfs.faa {outs[cds].local}")
    context.LocalShell(f"cp viral_orfs.gff {outs[gff].local}")
    return ExecutionResult(manifest=[{p: o.local for p, o in outs.items()}],
                           success=all(o.local.exists() for o in outs.values()))


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={cds: "viral_orfs.faa", gff: "viral_orfs.gff"},
    resources=Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=4)),
)
