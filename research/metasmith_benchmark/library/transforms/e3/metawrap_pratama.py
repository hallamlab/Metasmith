# Pratama's binning (reproduction_map A5) and refinement (A10), on the metaSPAdes assembly only.
#
# `metawrap binning --universal --maxbin2 --metabat2 --concoct`, then `bin_refinement -c 50 -x 10`. Pratama refines
# twice: round 1 over abawaca and BinSanity, round 2 over metabat2, concoct and round 1's output. Both
# binners are gapfills, so until they land this runs one round over the three MetaWRAP binners.
#
# spades_assembly rather than the bare assembly type, so the MEGAHIT assembly (viral lane only) is never binned.
import glob
import json
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

# bin_refinement runs CheckM 1, whose database ships inside this image.
image = model.AddRequirement(lib.GetType("env::metawrap.env"))
meta  = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={meta})
asm   = model.AddRequirement(lib.GetType("sequences::spades_assembly"), parents={reads})

bin_fasta = model.AddProduct(lib.GetType("sequences::metawrap_bin_fasta"))
table     = model.AddProduct(lib.GetType("binning::metawrap_contig_to_bin_table"))
stats     = model.AddProduct(lib.GetType("binning::metawrap_bin_stats"))

MIN_COMPLETION = 50
MAX_CONTAMINATION = 10
CHECKM_FULL_TREE_GB = 40


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    iasm = context.Input(asm)
    with open(context.Input(meta).local) as j:
        assert json.load(j)["parity"] == "paired", "MetaWRAP's binning module takes a read pair"

    threads = context.params.get("cpus", 8)
    mem_gb = context.params.get("memory")
    mem = max(int(mem_gb * 0.85), 4) if mem_gb else 16
    # CAUTION bin_refinement's -m only sizes pplacer, as mem // 40 threads. Below 40 that is zero threads,
    # so floor it and fall back to CheckM's reduced tree when the task cannot afford the full one.
    quick = mem < CHECKM_FULL_TREE_GB
    if quick:
        Log.Warn(f"only {mem} GB for bin_refinement: using CheckM's reduced tree (--quick)")
    refine_mem = max(mem, CHECKM_FULL_TREE_GB)

    # MetaWRAP accepts only uncompressed `*_1.fastq` and `*_2.fastq`.
    context.ExecWithEnv(env=image, cmd=f"""
        zcat -f {ireads.container} \
            | awk '{{ if (int((NR-1)/4) % 2 == 0) print > "reads_1.fastq"; else print > "reads_2.fastq" }}'
        test -s reads_1.fastq && test -s reads_2.fastq

        metawrap binning -o binning -t {threads} -m {mem} --universal -a {iasm.container} \
            --metabat2 --maxbin2 --concoct reads_1.fastq reads_2.fastq
        rm reads_1.fastq reads_2.fastq

        metawrap bin_refinement -o refinement -t {threads} -m {refine_mem} {"--quick" if quick else ""} \
            -A binning/metabat2_bins -B binning/maxbin2_bins -C binning/concoct_bins \
            -c {MIN_COMPLETION} -x {MAX_CONTAMINATION}
    """)

    refined = Path(f"refinement/metawrap_{MIN_COMPLETION}_{MAX_CONTAMINATION}_bins")
    stats_file = Path(f"{refined}.stats")
    assert refined.is_dir(), f"bin_refinement wrote no {refined}"
    assert stats_file.exists(), f"bin_refinement wrote no {stats_file.name}"

    bin_files = sorted(glob.glob(f"{refined}/*.fa"))
    Log.Info(f"bin_refinement kept {len(bin_files)} bins")
    outputs = []
    for i, bin_path in enumerate(bin_files):
        out_bin = context.Output(bin_fasta, i=i)
        out_bin.local.write_bytes(Path(bin_path).read_bytes())
        outputs.append({bin_fasta: out_bin.local})

    otable = context.Output(table)
    with open(otable.local, "w") as f:
        f.write("contig\tbin\n")
        for bin_path in bin_files:
            with open(bin_path) as bf:
                for line in bf:
                    if line.startswith(">"):
                        f.write(f"{line[1:].strip().split()[0]}\t{Path(bin_path).stem}\n")
    ostats = context.Output(stats)
    ostats.local.write_bytes(stats_file.read_bytes())

    return ExecutionResult(
        manifest=outputs + [{table: otable.local, stats: ostats.local}],
        success=len(outputs) > 0 and otable.local.exists() and ostats.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=32, memory=Size.GB(240), duration=Duration(hours=48)),
)
