# indicspecies -- INDICSPECIES (asv_pipeline.nf:4518) with its two plot processes (4608, 4792).
#
# One multipatt run per label of the sample sheet, which is the survey node for an ASPIRE
# study. The settings are upstream's code defaults: this row reads no params, so a count
# table from outside ASPIRE can reach it. The one departure: a label with more than 8 levels
# gets only the single-group test (Dufrene & Legendre 1997), linear in its levels, since every
# combination of 15 levels over 189 samples outran a 12-hour task.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
survey  = model.AddRequirement(lib.GetType("amplicon::survey"))
md      = model.AddRequirement(lib.GetType("aspire::analysis_metadata"), parents={survey})
counts  = model.AddRequirement(lib.GetType("amplicon::abundance_table"), parents={survey})
results = model.AddProduct(lib.GetType("aspire::indicspecies_results"))
tables  = model.AddProduct(lib.GetType("aspire::indicspecies_tables"))
plots   = model.AddProduct(lib.GetType("aspire::indicspecies_plots"))
aligned = model.AddProduct(lib.GetType("aspire::indicspecies_aligned_plots"))

EXHAUSTIVE_LEVELS = 8


def protocol(context: ExecutionContext):
    iresults, itables = context.Output(results), context.Output(tables)
    iplots, ialigned = context.Output(plots), context.Output(aligned)
    iscripts = context.Input(scripts)
    header = context.Input(survey).local.read_text().splitlines()[0].split("\t")
    sid_col, labels = header[0], header[1:]
    assert labels, "the sample sheet has no label column"

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        mkdir -p all {iresults.container} {itables.container}
        Rscript {iscripts.container}/run_indicspecies.R \
            --data-wide {context.Input(counts).container} --data-long {context.Input(md).container} \
            --sample-col "{sid_col}" --group-cols "{','.join(labels)}" \
            --perms 9999 --seed 42 --q-threshold 0.05 --min-n 2 \
            --exhaustive-levels {EXHAUSTIVE_LEVELS} --outdir all
        cp all/*.tsv {itables.container}/
        cp all/skipped_labels.tsv {iresults.container}/
        for f in all/*_indicator_species_results.tsv all/*_indicator_species_summary.tsv; do
            [ -e "$f" ] && cp "$f" {iresults.container}/
        done
        python {iscripts.container}/plot_indicators.py --results {iresults.container} \
            --plots {iplots.container} --aligned {ialigned.container}
        rm -r all
    """)

    return ExecutionResult(
        manifest=[{results: iresults.local, tables: itables.local,
                   plots: iplots.local, aligned: ialigned.local}],
        success=(iresults.local / "skipped_labels.tsv").exists() and iplots.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=survey,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    ),
)
