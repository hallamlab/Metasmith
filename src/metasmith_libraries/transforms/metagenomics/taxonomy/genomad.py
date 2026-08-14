from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::genomad.env"))
# ~5 Mbp contig batch (w4_rebatch.py), sample-prefixed headers; per-sample
# regroup happens in w4_recompile.py. Sibling of assembly, not a subtype.
assembly = model.AddRequirement(lib.GetType("sequences::contig_batch"))
ref  = model.AddRequirement(lib.GetType("ref::genomad"))

virus_summary_out   = model.AddProduct(lib.GetType("taxonomy::genomad_virus_summary"))
plasmid_summary_out = model.AddProduct(lib.GetType("taxonomy::genomad_plasmid_summary"))


def protocol(context: ExecutionContext):
    iasm    = context.Input(assembly)
    idb     = context.Input(ref)
    ovirus  = context.Output(virus_summary_out)
    oplasmid = context.Output(plasmid_summary_out)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-t {threads}"

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"/usr/local/bin/_entrypoint.sh genomad end-to-end {iasm.container} genomad_output {idb.container} {threads} --cleanup",
    )

    prefix = Path(iasm.local).stem
    summary_dir = Path(f"genomad_output/{prefix}_summary")

    import shutil
    shutil.copy2(summary_dir / f"{prefix}_virus_summary.tsv", ovirus.local)
    shutil.copy2(summary_dir / f"{prefix}_plasmid_summary.tsv", oplasmid.local)

    return ExecutionResult(
        manifest=[
            {
                virus_summary_out: ovirus.local,
                plasmid_summary_out: oplasmid.local,
            },
        ],
        success=ovirus.local.exists() and oplasmid.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=assembly,
    resources=Resources(
        cpus=16,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    )
)
