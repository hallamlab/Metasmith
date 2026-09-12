# The scorer CAMI's own challenge uses (github.com/CAMI-challenge/AMBER).
# Requiring the shared binning::contig_to_bin_table rather than any one
# binner's subtype means this one transform matches metabat2, semibin2 and
# comebin alike, fanning out per binner (and per sample) for free -- the same
# trick checkm.py plays on sequences::putative_genome.
#
# --skip_gs drops AMBER's own gold-standard-vs-itself sanity row: with one
# tool scored per task instance there is nothing to compare it against, so it
# is dead weight rather than a self-check here.
#
# contig_to_bin_table is our own plain "contig\tbin" TSV (see metabat2.py /
# semibin2.py), not AMBER's Bioboxes prediction format -- confirmed by running
# amber.py against one directly: it logs the file "malformed" and exits 1
# rather than scoring it. The protocol below reformats a copy before calling
# amber.py; contig_gold_standard_table (built by cami_contig_truth.py) is
# already Bioboxes, so it needs no such step.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::amber.env"))
asm   = model.AddRequirement(lib.GetType("sequences::assembly"))
table = model.AddRequirement(lib.GetType("binning::contig_to_bin_table"), parents={asm})
gold  = model.AddRequirement(lib.GetType("binning::contig_gold_standard_table"), parents={asm})

out_results     = model.AddProduct(lib.GetType("binning::amber_results"))
out_bin_metrics = model.AddProduct(lib.GetType("binning::amber_bin_metrics"))


def protocol(context: ExecutionContext):
    itable = context.Input(table)
    igold = context.Input(gold)
    oresults = context.Output(out_results)
    obin_metrics = context.Output(out_bin_metrics)

    # binning::metabat2_contig_to_bin_table -> "metabat2", used only as AMBER's
    # display label and the genome/<label>/ subdirectory it writes into.
    label = context.GetMeta(table).type_name.split("::")[-1].removesuffix("_contig_to_bin_table")

    # amber.py matches gold standard and prediction by @SampleID -- reuse the
    # gold standard's own so the two agree.
    sample_id = None
    with open(igold.local) as f:
        for line in f:
            if line.startswith("@SampleID:"):
                sample_id = line.strip().split(":", 1)[1]
                break
    assert sample_id, f"gold standard [{igold.local}] has no @SampleID header"

    pred_path = Path("prediction.tsv")
    with open(itable.local) as fin, open(pred_path, "w") as fout:
        fin.readline()  # our own "contig\tbin" header, not Bioboxes'
        fout.write(f"@Version:0.9.1\n@SampleID:{sample_id}\n\n@@SEQUENCEID\tBINID\n")
        for line in fin:
            fout.write(line)

    work = "amber_out"
    _cmd = f"""
            amber.py -g {igold.container} -l {label} -o {work} --skip_gs \
                {pred_path}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    results_src = Path(work) / "results.tsv"
    bin_metrics_src = Path(work) / "genome" / label / "metrics_per_bin.tsv"
    assert results_src.exists(), (
        f"amber.py wrote no results.tsv for [{label}] -- check {work}/log.txt"
    )
    assert bin_metrics_src.exists(), (
        f"amber.py wrote no genome/{label}/metrics_per_bin.tsv -- check {work}/log.txt"
    )
    oresults.local.write_bytes(results_src.read_bytes())
    obin_metrics.local.write_bytes(bin_metrics_src.read_bytes())

    return ExecutionResult(
        manifest=[{out_results: oresults.local, out_bin_metrics: obin_metrics.local}],
        success=oresults.local.exists() and obin_metrics.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=table,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
