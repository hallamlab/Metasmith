# curate -- MITO_DECONTAM (asv_pipeline.nf:3595) and FILTER_COUNTS (3637).
#
# mito_checker.py makes the non-target call unchanged, and curate.py applies it through
# filter_nontarget.py's own functions. FILTER_COUNTS's group-size cut is not applied: a rare
# label value is blanked by plot_metadata instead, so the counts do not depend on the labels.
# contaminant_hits.py reads the set's headers so each contaminant removal names its list and
# that entry's support; counts_removed itself keeps upstream's one `reason` column.

import yaml
from metasmith.python_api import *

lib       = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model     = Transform()
image     = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts   = model.AddRequirement(lib.GetType("lib::aspire"))
study     = model.AddRequirement(lib.GetType("aspire::study_metadata"))
fcounts   = model.AddRequirement(lib.GetType("aspire::asv_filtered_counts"), parents={study})
fseqs     = model.AddRequirement(lib.GetType("aspire::asv_filtered_seqs"), parents={study})
tax       = model.AddRequirement(lib.GetType("amplicon::asv_taxonomy"), parents={study})
master    = model.AddRequirement(lib.GetType("aspire::mitomaster_table"), parents={study})
mhits     = model.AddRequirement(lib.GetType("aspire::mito_blast6"), parents={study})
chits     = model.AddRequirement(lib.GetType("aspire::contaminant_blast6"), parents={study})
cont_set  = model.AddRequirement(lib.GetType("aspire::contaminant_reference_set"))
params    = model.AddRequirement(lib.GetType("aspire::params"), parents={study})
clean     = model.AddProduct(lib.GetType("aspire::counts_clean"))
removed   = model.AddProduct(lib.GetType("aspire::counts_removed"))
summaries = model.AddProduct(lib.GetType("aspire::mito_summary_tables"))
plots     = model.AddProduct(lib.GetType("aspire::mito_plots"))


def protocol(context: ExecutionContext):
    iclean, iremoved = context.Output(clean), context.Output(removed)
    isummaries, iplots = context.Output(summaries), context.Output(plots)
    iscripts, itax = context.Input(scripts), context.Input(tax)
    cfg = yaml.safe_load(context.Input(params).local.read_text())["curate"]
    excluded = " ".join(f'--exclude-taxon "{t}"' for t in cfg.get("exclude_taxa") or [])

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        mkdir -p {isummaries.container} {iplots.container}
        python {iscripts.container}/mito_checker.py \
            --mitomaster-file {context.Input(master).container} \
            --mito-blast {context.Input(mhits).container} \
            --silva-tax {itax.container} \
            --biof-file {context.Input(chits).container} \
            --output-dir {isummaries.container} --prefix nontarget --formats svg,pdf \
            --min-pident {cfg['min_pident']} --min-percov {cfg['min_percov']} --overwrite
        find {isummaries.container} -maxdepth 1 \\( -name '*.svg' -o -name '*.pdf' \\) -exec mv {{}} {iplots.container}/ \\;
        python {iscripts.container}/curate.py \
            --counts {context.Input(fcounts).container} --taxonomy {itax.container} \
            --master {isummaries.container}/nontarget.master.tsv \
            --abundance-threshold {cfg['abundance_threshold']} --min-consensus {cfg['min_consensus']} \
            {excluded} --clean {iclean.container} --removed {iremoved.container}
        python {iscripts.container}/contaminant_hits.py \
            --removed {iremoved.container} --master {isummaries.container}/nontarget.master.tsv \
            --set {context.Input(cont_set).container} --out {isummaries.container}/contaminant_hits.tsv
    """)

    return ExecutionResult(
        manifest=[{clean: iclean.local, removed: iremoved.local,
                   summaries: isummaries.local, plots: iplots.local}],
        success=iclean.local.exists() and iremoved.local.exists()
                and (isummaries.local / "nontarget.master.tsv").exists()
                and (isummaries.local / "contaminant_hits.tsv").exists(),
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
