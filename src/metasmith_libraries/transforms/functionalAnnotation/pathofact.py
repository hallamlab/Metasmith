import glob
from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::pathofact.env"))
asm = model.AddRequirement(lib.GetType("sequences::contig_batch"))
db = model.AddRequirement(lib.GetType("annotation::pathofact_db"))
out_amr = model.AddProduct(lib.GetType("annotation::pathofact_amr"))
out_vf = model.AddProduct(lib.GetType("annotation::pathofact_vf"))
out_tox = model.AddProduct(lib.GetType("annotation::pathofact_tox"))
out_mge = model.AddProduct(lib.GetType("annotation::pathofact_mge"))


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    idb = context.Input(db)
    oamr = context.Output(out_amr)
    ovf = context.Output(out_vf)
    otox = context.Output(out_tox)
    omge = context.Output(out_mge)

    threads = context.params.get("cpus", 16)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idb.external, "/pathofact_db")],
        cmd=f"""
            pathofact \
                --input {iasm.container} \
                --db /pathofact_db \
                --outdir pf_out \
                --threads {threads}
        """,
    )

    def _grab(pattern, dest, header):
        hits = sorted(glob.glob(pattern, recursive=True))
        if hits:
            context.LocalShell(f"cp {hits[0]} {dest.local}")
        else:
            Path(dest.local).write_text(header)

    _grab("pf_out/**/*AMR*pred*.tsv", oamr, "Contig\tORF\tARG\tprediction\n")
    _grab("pf_out/**/*[Vv]irulence*.tsv", ovf, "Contig\tORF\tVF\tprediction\n")
    _grab("pf_out/**/*[Tt]oxin*.tsv", otox, "Contig\tORF\ttoxin\tprediction\n")
    _grab("pf_out/**/*MGE*.tsv", omge, "Contig\tORF\tMGE\tprediction\n")

    context.LocalShell("rm -rf pf_out 2>/dev/null || true")

    return ExecutionResult(
        manifest=[{
            out_amr: oamr.local,
            out_vf: ovf.local,
            out_tox: otox.local,
            out_mge: omge.local,
        }],
        success=all(p.local.exists() for p in (oamr, ovf, otox, omge)),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=6),
    ),
)
