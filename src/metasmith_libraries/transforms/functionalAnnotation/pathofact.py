"""pathofact — PathoFact 2.0 integrated ARG + VF + toxin + MGE prediction.

Runs on sequences::contig_batch (~5 Mbp contig batches, w4_rebatch.py), calling
ORFs internally, and emits the four PathoFact prediction tables. Like the other
contig-level annotators (integron_finder, virsorter2, genomad) it fans out over
batches rather than whole assemblies; the contig id (SG<id>~k141_XXXXXX,
sample-prefixed) is carried in each output's Contig column, so w4_recompile.py
strips the prefix + regroups the per-batch tables per sample.

The exact in-container invocation below is the contract implemented by the
container build (container_builds/main/pathofact); its bundled DBs
(annotation::pathofact_db) are staged separately and bound at /pathofact_db.
"""
import glob
from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::pathofact.env"))
# ~5 Mbp contig batch (w4_rebatch.py), sample-prefixed headers; per-sample regroup
# happens in w4_recompile.py. Sibling of assembly, not a subtype.
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

    # Entrypoint wrapper `pathofact` (defined by the container build) runs the
    # Snakemake pipeline end-to-end and writes results under pf_out/.
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

    # PathoFact's Snakemake run leaves a large intermediate tree under pf_out
    # (prodigal/hmmer/signalp per-contig files). The 4 tables are copied out
    # above; drop the tree so the never-reaped nxf_work dir doesn't blow the
    # /scratch inode cap on the coarse multi-hundred-Mbp batches.
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
        # coarse ~240 Mbp batches (w4_batches_coarse): calibration PF(bp)=243+4.23e-5*bp
        # → ~2.9 h/batch (30 Mbp=1513s, 90 Mbp=4053s); MaxRSS 12.7 GB at 90 Mbp → 32 GB
        # gives ~2x headroom for the bigger batch; 6 h base = wall margin over the ~2.9 h.
        memory=Size.GB(32),
        duration=Duration(hours=6),
    ),
)
