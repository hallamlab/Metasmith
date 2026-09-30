# merge_and_filter_reads -- MERGE_READS (asv_pipeline.nf:3179) and FILTER_READS (3221)
#
# The relabel is RELABEL_FILTERED's (3248): every read becomes `<sample>.<n>;sample=<sample>`,
# which is what lets `vsearch --otutabout` in denoise assign it to a sample column.

import json

import yaml
from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::vsearch.env"))
study    = model.AddRequirement(lib.GetType("aspire::study_metadata"))
meta     = model.AddRequirement(lib.GetType("sequences::read_metadata"), parents={study})
qc       = model.AddRequirement(lib.GetType("aspire::qc_reads"), parents={meta})
params   = model.AddRequirement(lib.GetType("aspire::params"), parents={study})
filtered = model.AddProduct(lib.GetType("aspire::filtered_fasta"))
counts   = model.AddProduct(lib.GetType("aspire::read_counts"))


def protocol(context: ExecutionContext):
    iqc, ifiltered, icounts = context.Input(qc), context.Output(filtered), context.Output(counts)
    info = json.loads(context.Input(meta).local.read_text())
    sample, parity = info["sample"], info["parity"]
    assert parity in {"single", "paired"}, f"unknown parity: [{parity}]"
    cfg = yaml.safe_load(context.Input(params).local.read_text())
    merge, filt = cfg["merge"], cfg["filter"]
    threads = context.params.get("cpus", 4)

    if parity == "paired":
        stagger = "--fastq_allowmergestagger" if merge["allow_stagger"] else ""
        merged = f"""\
            gzip -cd {iqc.container} | awk '{{ print > (int((NR - 1) / 4) % 2 ? "r2.fq" : "r1.fq") }}'
            vsearch --fastq_mergepairs r1.fq --reverse r2.fq --fastqout merged.fq \
                --fastq_maxdiffs {merge['max_diffs']} --fastq_minovlen {merge['min_overlap']} \
                --fastq_truncqual {merge['trunc_quality']} {stagger} --threads {threads}
            rm r1.fq r2.fq
        """
    else:
        merged = f"gzip -cd {iqc.container} > merged.fq"
    maxlen = f"--fastq_maxlen {filt['max_len']}" if filt["max_len"] else ""
    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        {merged}
        vsearch --fastx_filter merged.fq --fastq_maxee {filt['max_ee']} \
            --fastq_minlen {filt['min_len']} {maxlen} \
            --relabel {sample}. --sample {sample} --fastaout filtered.fasta
        printf 'sample\\tparity\\tmerged\\tfiltered\\n{sample}\\t{parity}\\t%s\\t%s\\n' \
            $(( $(wc -l < merged.fq) / 4 )) $(grep -c '^>' filtered.fasta || true) > {icounts.container}
        gzip -n -c filtered.fasta > {ifiltered.container}
        rm merged.fq filtered.fasta
    """)

    return ExecutionResult(
        manifest=[{filtered: ifiltered.local, counts: icounts.local}],
        success=ifiltered.local.exists() and icounts.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(
        cpus=4,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)
