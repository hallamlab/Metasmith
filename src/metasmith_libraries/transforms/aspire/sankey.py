# sankey -- SANKEY (asv_pipeline.nf:3687), upstream's sankey_builder.py over a laid-out tree.
#
# Two renderings, as upstream: the samples that reached the clean table, and every sample
# of the sheet. The flows split by the sheet's first label.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
fate    = model.AddRequirement(lib.GetType("aspire::read_fate"), parents={study})
removed = model.AddRequirement(lib.GetType("aspire::counts_removed"), parents={study})
out     = model.AddProduct(lib.GetType("aspire::sankey_outputs"))


def protocol(context: ExecutionContext):
    iout, iscripts, istudy = context.Output(out), context.Input(scripts), context.Input(study)
    header = istudy.local.read_text().splitlines()[0].split("\t")
    sid_col, label = header[0], header[1]
    L = "sankey_layout"
    runs = ""
    for prefix, extra in (("read_fate_sankey", ""), ("read_fate_sankey_all_samples", "--all-samples")):
        runs += f"""
        python {iscripts.container}/sankey_builder.py --data-dir {iout.container} --sub-dir . \
            --metadata $PWD/{L}/metadata.tsv --sample-manifest $PWD/{L}/manifest.tsv \
            --samp-col "{sid_col}" --group1-col "{label}" --color-col Color \
            --fastq-stats $PWD/{L}/stats/fastq_stats.tsv --filtered-stats $PWD/{L}/stats/filtered_fastqs.tsv \
            --asv-raw $PWD/{L}/ASVs/ASV_counts.tsv --asv-decon $PWD/{L}/ASVs/ASV_target.decon.tsv \
            --asv-micro $PWD/{L}/ASVs/ASV_target.micro.tsv \
            --title "Read fate by {label}" --output-prefix {prefix} \
            --make-labeled --make-unlabeled {extra} --verbose
        """
    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        mkdir -p {iout.container}
        python {iscripts.container}/upstream_layout.py sankey --sheet {istudy.container} \
            --fate {context.Input(fate).container} --label "{label}" --out {L}
        {runs}
        rm -r {L}
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=(iout.local / "read_fate_sankey.html").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
