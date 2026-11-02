# Pratama's binning (reproduction_map A5), MetaBAT2 alone. One binner per transform, so a failure costs one
# binner on one sample rather than the whole sample, and each wall is sized for one tool.
#
# `metawrap binning --universal --metabat2`. The bins are an e3 sibling type, so only this library's
# refinement stage consumes them and no standard binner, refiner or dereplicator binds to them.
import glob
import json
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::metawrap.env"))
meta  = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={meta})
asm   = model.AddRequirement(lib.GetType("sequences::spades_assembly"), parents={reads})

bins = model.AddProduct(lib.GetType("e3::metawrap_metabat2_bins"))

BINNER = "metabat2"


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    iasm = context.Input(asm)
    with open(context.Input(meta).local) as j:
        assert json.load(j)["parity"] == "paired", "MetaWRAP's binning module takes a read pair"

    threads = context.params.get("cpus", 8)
    mem_gb = context.params.get("memory")
    mem = max(int(mem_gb * 0.85), 4) if mem_gb else 16

    # MetaWRAP accepts only uncompressed `*_1.fastq` and `*_2.fastq`.
    # CAUTION MetaWRAP aligns with bwa into `<out>/work_files/<sample>.bam` and skips the alignment when
    # that file already exists (binning.sh:211, 232, 242). A shared alignment transform is therefore
    # possible, but the skip keys on a filename MetaWRAP derives itself, so a mismatch re-aligns SILENTLY.
    # Each binner re-aligns here until that filename derivation is verified against the image.
    context.ExecWithEnv(env=image, cmd=f"""
        zcat -f {ireads.container} \
            | awk '{{ if (int((NR-1)/4) % 2 == 0) print > "reads_1.fastq"; else print > "reads_2.fastq" }}'
        test -s reads_1.fastq && test -s reads_2.fastq

        metawrap binning -o binning -t {threads} -m {mem} --universal -a {iasm.container} \
            --{BINNER} reads_1.fastq reads_2.fastq
        rm -f reads_1.fastq reads_2.fastq
    """)

    outdir = Path(f"binning/{BINNER}_bins")
    assert outdir.is_dir(), f"metawrap binning wrote no {outdir}"
    bin_files = sorted(glob.glob(f"{outdir}/*.fa"))
    Log.Info(f"{BINNER} produced {len(bin_files)} bins")

    outputs = []
    for i, bin_path in enumerate(bin_files):
        out_bin = context.Output(bins, i=i)
        out_bin.local.write_bytes(Path(bin_path).read_bytes())
        outputs.append({bins: out_bin.local})

    return ExecutionResult(manifest=outputs, success=len(outputs) > 0)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    # CAUTION duration must satisfy base x 2^(tries-1) <= 168 h: fir refuses ANY job over 7.0 days at
    # submit time, and an ignored SUBMISSION failure corrupts nextflow's running counter until the run
    # wedges. The 48 h monolith this replaces asked 192 h and 384 h on rungs 3 and 4, both illegal.
    # 20 h is legal on every rung and above the whole monolith's worst case of 13:38 over 54 samples.
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=20)),
)
