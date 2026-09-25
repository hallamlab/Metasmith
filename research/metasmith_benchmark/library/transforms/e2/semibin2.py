import glob
import gzip
import json
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::semibin2.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
asm     = model.AddRequirement(lib.GetType("e2::assembly"), parents={meta})
bam     = model.AddRequirement(lib.GetType("e2::binning_bam"), parents={asm})
bins    = model.AddProduct(lib.GetType("e2::semibin2_bin"))
table   = model.AddProduct(lib.GetType("e2::semibin2_contig_to_bin"))


def protocol(context: ExecutionContext):
    imeta = context.Input(meta)
    iasm = context.Input(asm)
    ibam = context.Input(bam)
    read_set = json.loads(Path(imeta.local).read_text())
    cpus = context.params.get("cpus") or 1
    prefix = f"SemiBin2-{read_set['sample']}"
    seq_type = "short_reads" if read_set["platform"] == "ILLUMINA" else "long_reads"

    # SEMIBIN_SINGLEEASYBIN for one sample: semibin_rng_seed 1, min_contig_size 1500, environment global.
    context.ExecWithEnv(env=image, cmd=f"""
        SemiBin2 single_easy_bin --input-fasta {iasm.container} --input-bam {ibam.container} \
            --tag-output {prefix} --output {prefix} -t {cpus} \
            --random-seed 1 --min-len 1500 --compression gz --engine cpu \
            --sequencing-type {seq_type} --environment global
    """)

    otable = context.Output(table)
    manifest = []
    with open(otable.local, "w") as t:
        for i, path in enumerate(sorted(glob.glob(f"{prefix}/output_bins/*.fa.gz"))):
            obin = context.Output(bins, i=i)
            with gzip.open(path, "rt") as f:
                text = f.read()
            obin.local.write_text(text)
            name = Path(path).name.removesuffix(".fa.gz")
            for line in text.splitlines():
                if line.startswith(">"):
                    t.write(f"{line[1:].split()[0]}\t{name}\n")
            manifest.append({bins: obin.local})

    return ExecutionResult(manifest=manifest + [{table: otable.local}], success=len(manifest) > 0)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=8)),
)
