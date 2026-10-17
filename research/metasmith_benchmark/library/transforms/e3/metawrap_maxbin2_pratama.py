# Pratama's binning (reproduction_map A5), MaxBin2 alone. See metawrap_metabat2_pratama.py for why each
# binner is its own transform and why each re-aligns rather than sharing one BAM.
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

bins = model.AddProduct(lib.GetType("e3::metawrap_maxbin2_bins"))

BINNER = "maxbin2"


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    iasm = context.Input(asm)
    with open(context.Input(meta).local) as j:
        assert json.load(j)["parity"] == "paired", "MetaWRAP's binning module takes a read pair"

    threads = context.params.get("cpus", 8)
    mem_gb = context.params.get("memory")
    mem = max(int(mem_gb * 0.85), 4) if mem_gb else 16

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
    # MaxBin2 builds its own per-sample abundance files from the BAM, then runs single-threaded marker
    # searches, so it is the slower of the two light binners. 20 h keeps every retry rung legal.
    resources=Resources(cpus=16, memory=Size.GB(96), duration=Duration(hours=20)),
)
