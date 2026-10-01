# asv_mag_link -- ASV_MAG_LINK (asv_pipeline.nf:5821) at upstream's config defaults: BLAST the
# filtered ASVs against every barrnap 16S of the collection, pair at 97% identity over 90% of
# the ASV, and plot the top 20.
#
# The product is upstream's link directory, which the MAG network and the master summary read
# by path. The pairing table is also emitted alone for the consumers that read only it.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
policy  = model.AddRequirement(lib.GetType("aspire::asv_mag_link_on"), parents={study})
fseqs   = model.AddRequirement(lib.GetType("aspire::asv_filtered_seqs"), parents={study})
mags    = model.AddRequirement(lib.GetType("aspire::mag_collection"))
pairing = model.AddProduct(lib.GetType("aspire::asv_mag_pairing"))
out     = model.AddProduct(lib.GetType("aspire::asv_mag_outputs"))


def protocol(context: ExecutionContext):
    iout, ipair = context.Output(out), context.Output(pairing)
    s = context.Input(scripts).container
    threads = context.params.get("cpus") or 1

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        export MPLCONFIGDIR=$PWD/.cache/mpl
        python {s}/asv_mag_barrnap_linker.py --asv-fasta {context.Input(fseqs).container} \
            --genome-qc-dir {context.Input(mags).container} --outdir {iout.container} \
            --threads {threads} --min-pident 97 --min-qcov 90 --top-n 5
        python {s}/plot_asv_mag_link.py --input-dir {iout.container} --top-n 20
        cp {iout.container}/tables/asv2mag_pairing.tsv {ipair.container}
        rm -rf .cache
    """)

    return ExecutionResult(
        manifest=[{pairing: ipair.local, out: iout.local}],
        success=ipair.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=8),
    ),
)
