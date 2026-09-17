# Pratama's refinement (reproduction_map A10): `metawrap bin_refinement -c 50 -x 10` over the three
# MetaWRAP binners' bins. Split out of the binning monolith so refinement retries without re-binning.
#
# Pratama refines twice: round 1 over abawaca and BinSanity, round 2 over metabat2, concoct and round 1's
# output. Both binners are gapfills, so until they land this runs one round over the three MetaWRAP binners.
#
# The products are unchanged from the monolith this replaces, so every downstream consumer -- DRAM on the
# MAGs, GTDB-Tk, the recovery table -- binds exactly as before.
import glob
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

# bin_refinement runs CheckM 1, whose database ships inside this image.
image = model.AddRequirement(lib.GetType("env::metawrap.env"))
asm   = model.AddRequirement(lib.GetType("sequences::spades_assembly"))
mb2   = model.AddRequirement(lib.GetType("e3::metawrap_metabat2_bins"), parents={asm})
mx2   = model.AddRequirement(lib.GetType("e3::metawrap_maxbin2_bins"), parents={asm})
cct   = model.AddRequirement(lib.GetType("e3::metawrap_concoct_bins"), parents={asm})

bin_fasta = model.AddProduct(lib.GetType("sequences::metawrap_bin_fasta"))
table     = model.AddProduct(lib.GetType("binning::metawrap_contig_to_bin_table"))
stats     = model.AddProduct(lib.GetType("binning::metawrap_bin_stats"))

MIN_COMPLETION = 50
MAX_CONTAMINATION = 10
CHECKM_FULL_TREE_GB = 40


def _stage(context, slot, name: str) -> str:
    """Write one binner's bins into the directory layout bin_refinement expects."""
    d = Path(name)
    d.mkdir(parents=True, exist_ok=True)
    n = 0
    for i, handle in enumerate(context.InputGroup(slot)):
        (d / f"bin_{i:04d}.fa").write_bytes(handle.local.read_bytes())
        n += 1
    Log.Info(f"staged {n} bins into {name}")
    assert n > 0, f"no bins staged into {name}"
    return name


def protocol(context: ExecutionContext):
    a = _stage(context, mb2, "bins_metabat2")
    b = _stage(context, mx2, "bins_maxbin2")
    c = _stage(context, cct, "bins_concoct")

    threads = context.params.get("cpus", 8)
    mem_gb = context.params.get("memory")
    mem = max(int(mem_gb * 0.85), 4) if mem_gb else 16
    # CAUTION bin_refinement's -m only sizes pplacer, as mem // 40 threads. Below 40 that is zero threads,
    # so floor it and fall back to CheckM's reduced tree when the task cannot afford the full one.
    quick = mem < CHECKM_FULL_TREE_GB
    if quick:
        Log.Warn(f"only {mem} GB for bin_refinement: using CheckM's reduced tree (--quick)")
    refine_mem = max(mem, CHECKM_FULL_TREE_GB)

    context.ExecWithEnv(env=image, cmd=f"""
        metawrap bin_refinement -o refinement -t {threads} -m {refine_mem} {"--quick" if quick else ""} \
            -A {a} -B {b} -C {c} \
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
    # CheckM 1's full tree needs at least 40 GB, and pplacer takes mem // 40 threads. 20 h keeps every
    # retry rung legal under fir's 7.0-day submit cap.
    resources=Resources(cpus=16, memory=Size.GB(128), duration=Duration(hours=20)),
)
