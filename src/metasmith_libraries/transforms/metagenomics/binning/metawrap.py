# One transform for MetaWRAP's binning AND its bin_refinement, deliberately.
#
# The three raw bin sets are not independently interesting here -- the
# refinement's consolidation is the thing being used -- and each MetaWRAP module
# expects its own on-disk layout, so every boundary between them would be a copy
# of every bin. Splitting it would buy nothing and cost the layout.
#
# The groundwater paper additionally runs two more binners and a dereplication
# step, and reassembles each bin. None of that is here; it is the next increment.
import glob
import json
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

# One image and no reference. bin_refinement shells out to CheckM **1**, and this
# image carries CheckM1's 1.4 GB database at /usr/local/etc/checkm, which is
# already the dataRoot in the package's own DATA_CONFIG -- checked by running
# `checkm taxon_list` inside it as a non-root user. Note that CheckM 1.x runs on
# python2.7 here and does NOT honour the CHECKM_DATA_PATH variable, so moving it
# would mean binding over that path rather than exporting anything.
image   = model.AddRequirement(lib.GetType("env::metawrap.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads   = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={meta})
asm     = model.AddRequirement(lib.GetType("sequences::assembly"), parents={reads})

bin_fasta = model.AddProduct(lib.GetType("sequences::metawrap_bin_fasta"))
table     = model.AddProduct(lib.GetType("binning::metawrap_contig_to_bin_table"))
stats     = model.AddProduct(lib.GetType("binning::metawrap_bin_stats"))

# bin_refinement interpolates both thresholds into its output names
# (`metawrap_50_10_bins`), so they are pinned rather than exposed: a transform
# that let them vary would have to glob for its own products.
MIN_COMPLETION = 50
MAX_CONTAMINATION = 10

# CheckM's own floor for its full reference tree, and the number bin_refinement
# divides -m by to size pplacer.
CHECKM_FULL_TREE_GB = 40


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    imeta = context.Input(meta)
    iasm = context.Input(asm)

    with open(imeta.local) as j:
        parity = json.load(j)["parity"]
    assert parity == "paired", (
        f"MetaWRAP's binning module takes a read pair; this sample is [{parity}]"
    )

    threads = context.params.get("cpus", 8)
    # `params["memory"]` is a count of GIGABYTES, which is also what MetaWRAP's
    # -m wants, so this scales rather than converts. megahit.py multiplies the
    # same value by 1024**3 because its own flag takes bytes. A direct run passes
    # memory=1, and MetaWRAP reads -m 0 as "no memory" and dies inside MaxBin2,
    # so the floor is not decoration.
    mem_gb = context.params.get("memory")
    mem = max(int(mem_gb * 0.85), 4) if mem_gb else 16

    # bin_refinement turns -m into a placement thread count by integer division:
    # `ram_max=$((mem / 40))`, then pplacer gets min(ram_max, threads). So every
    # value below 40 asks CheckM for ZERO placement threads, which is not a slow
    # run, it is a broken argument -- and the floor above guarantees it on any
    # direct run that does not say otherwise.
    #
    # Below CheckM's own stated 40 GB, the supported answer is its reduced
    # reference tree, which is what --quick passes. That is a real fork rather
    # than a tuning knob: the reduced tree places against a subset of the
    # reference, so completeness and contamination shift slightly and bins near
    # the thresholds can cross them. Whichever side this lands on is worth
    # knowing, so it is logged rather than inferred.
    quick = mem < CHECKM_FULL_TREE_GB
    if quick:
        Log.Warn(
            f"only {mem} GB for bin_refinement, below CheckM's {CHECKM_FULL_TREE_GB} GB"
            " floor: using --quick (reduced reference tree), so completeness and"
            " contamination will differ slightly from a full-tree run"
        )
    # bin_refinement's -m is NOT a memory cap. The only thing its source does with the
    # number is `ram_max=$((mem / 40))`, and pplacer then gets min(ram_max, threads) -- so
    # an honest small value asks CheckM for ZERO placement threads, and --quick does not
    # fix that, it only adds --reduced_tree. Passing the floor buys exactly one thread,
    # and the reduced tree costs around 16 GB per thread rather than 40, so one thread
    # fits on the machine that could not afford the full tree. The binning module's -m
    # below IS a real cap and stays honest.
    refine_mem = max(mem, CHECKM_FULL_TREE_GB)

    # MetaWRAP refuses anything but two uncompressed files named `*_1.fastq` and
    # `*_2.fastq`, and the library's clean reads are one gzipped interleaved
    # file, so the split happens here. It is written into the work directory
    # rather than into the reference: at real depth this is tens of gigabytes
    # that exist only for the length of the task.
    _cmd = f"""
        zcat -f {ireads.container} \
            | awk '{{ if (int((NR-1)/4) % 2 == 0) print > "reads_1.fastq"; else print > "reads_2.fastq" }}'
        test -s reads_1.fastq && test -s reads_2.fastq

        metawrap binning -o binning -t {threads} -m {mem} -a {iasm.container} \
            --metabat2 --maxbin2 --concoct reads_1.fastq reads_2.fastq

        metawrap bin_refinement -o refinement -t {threads} -m {refine_mem} {"--quick" if quick else ""} \
            -A binning/metabat2_bins -B binning/maxbin2_bins -C binning/concoct_bins \
            -c {MIN_COMPLETION} -x {MAX_CONTAMINATION}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    refined = Path(f"refinement/metawrap_{MIN_COMPLETION}_{MAX_CONTAMINATION}_bins")
    stats_file = Path(f"{refined}.stats")
    assert refined.is_dir(), (
        f"bin_refinement wrote no {refined}; refinement/ holds "
        f"{sorted(p.name for p in Path('refinement').iterdir()) if Path('refinement').is_dir() else 'nothing'}"
    )
    assert stats_file.exists(), f"bin_refinement wrote no {stats_file.name}"

    bin_files = sorted(glob.glob(f"{refined}/*.fa"))
    Log.Info(f"bin_refinement consolidated {len(bin_files)} bins")

    outputs = []
    for i, bin_path in enumerate(bin_files):
        out_bin = context.Output(bin_fasta, i=i)
        out_bin.local.write_bytes(Path(bin_path).read_bytes())
        outputs.append({bin_fasta: out_bin.local})

    otable = context.Output(table)
    with open(otable.local, "w") as f:
        f.write("contig\tbin\n")
        for bin_path in bin_files:
            bin_name = Path(bin_path).stem
            with open(bin_path) as bf:
                for line in bf:
                    if line.startswith(">"):
                        f.write(f"{line[1:].strip().split()[0]}\t{bin_name}\n")

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
    resources=Resources(
        cpus=32,
        memory=Size.GB(240),
        duration=Duration(hours=48),
    ),
)
