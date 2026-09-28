# sina_trim -- SINA_TRIM (asv_pipeline.nf:3322), with upstream's log parser and trimmer.
#
# SINA writes its search index beside the reference it is given, and the bundle may sit on
# a read-only filesystem. So the bundle's files are linked into the work directory first: an
# index the bundle already carries is reused, and a missing one is built here.

import yaml
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
sina    = model.AddRequirement(lib.GetType("env::sina.env"))
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
fseqs   = model.AddRequirement(lib.GetType("aspire::asv_filtered_seqs"), parents={study})
silva   = model.AddRequirement(lib.GetType("amplicon::silva_db"))
params  = model.AddRequirement(lib.GetType("aspire::params"), parents={study})
trimmed = model.AddProduct(lib.GetType("aspire::sina_trimmed_seqs"))
aligned = model.AddProduct(lib.GetType("aspire::sina_aligned_seqs"))
log     = model.AddProduct(lib.GetType("aspire::sina_log"))
vreg    = model.AddProduct(lib.GetType("aspire::sina_v_regions"))


def protocol(context: ExecutionContext):
    itrimmed, ialigned = context.Output(trimmed), context.Output(aligned)
    ilog, ivreg = context.Output(log), context.Output(vreg)
    isilva, iscripts = context.Input(silva), context.Input(scripts)
    cfg = yaml.safe_load(context.Input(params).local.read_text())["sina"]
    regions = ",".join(cfg["regions"])
    trim_to = cfg.get("trim_to") or cfg["regions"][0]
    threads = context.params.get("cpus", 16)

    context.ExecWithEnv(env=sina, cmd=f"""\
        set -euo pipefail
        for f in {isilva.container}/silva.arb*; do ln -sf "$f" .; done
        gzip -cd {context.Input(fseqs).container} > asvs.fasta
        sina -i asvs.fasta -o aligned.fasta -r silva.arb -v -p {threads} --log-file {ilog.container}
    """)
    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        python {iscripts.container}/parse_sina_log.py --log {ilog.container} --output {ivreg.container}
        python {iscripts.container}/trim_v_sina.py -m {ivreg.container} -f aligned.fasta \
            -r "{regions}" -t "{trim_to}" -o trimmed.fasta --id-column ASV_ID \
            --threads {threads} --batch-size 1000000
        gzip -n -c aligned.fasta > {ialigned.container}
        gzip -n -c trimmed.fasta > {itrimmed.container}
        rm asvs.fasta aligned.fasta trimmed.fasta
    """)

    made = (itrimmed, ialigned, ilog, ivreg)
    return ExecutionResult(
        manifest=[{trimmed: itrimmed.local, aligned: ialigned.local, log: ilog.local, vreg: ivreg.local}],
        success=all(p.local.exists() for p in made),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(
        cpus=16,
        memory=Size.GB(32),
        duration=Duration(hours=8),
    ),
)
