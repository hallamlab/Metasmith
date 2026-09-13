import glob
import json
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
depth   = model.AddRequirement(lib.GetType("e2::jgi_depth.env"))
image   = model.AddRequirement(lib.GetType("e2::metabat2.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
asm     = model.AddRequirement(lib.GetType("e2::assembly"), parents={meta})
bam     = model.AddRequirement(lib.GetType("e2::binning_bam"), parents={asm})
bins    = model.AddProduct(lib.GetType("e2::metabat2_bin"))
table   = model.AddProduct(lib.GetType("e2::metabat2_contig_to_bin"))

# nf-core/mag 5.5.0 METABAT2_METABAT2: min_contig_size 1500, metabat_rng_seed 1. Its depth step runs
# the metabat2 2.15 image, and binning runs 2.17.
ARGS = "-m 1500 --unbinned --seed 1"
# jgi depth counts a read only at --percentIdentity 97 by default, which nanopore reads (mean 85.8-88.0%
# to their contigs) never reach, so every contig lands in lowDepth. E1 long sets
# longread_percentidentity to the same 80; findings/R1_WAVES.md B19 has the measurement.
LONG_READ_IDENTITY = 80


def protocol(context: ExecutionContext):
    imeta = context.Input(meta)
    iasm = context.Input(asm)
    ibam = context.Input(bam)
    read_set = json.loads(Path(imeta.local).read_text())
    sample = read_set["sample"]
    identity = "" if read_set["platform"] == "ILLUMINA" else f"--percentIdentity {LONG_READ_IDENTITY}"
    cpus = context.params.get("cpus") or 1
    prefix = f"MetaBAT2-{sample}"

    context.ExecWithEnv(env=depth, cmd=f"""
        export OMP_NUM_THREADS={cpus}
        jgi_summarize_bam_contig_depths {identity} --outputDepth depth.txt {ibam.container}
    """)
    context.ExecWithEnv(env=image, cmd=f"""
        metabat2 {ARGS} -i {iasm.container} -a depth.txt -t {cpus} --saveCls -o {prefix}
    """)

    bin_files = sorted(p for p in glob.glob(f"{prefix}.*.fa") if Path(p).stem.split(".")[-1].isdigit())
    manifest = write_bins(context, bin_files, bins, table)
    return ExecutionResult(manifest=manifest, success=len(bin_files) > 0)


def write_bins(context, bin_files, bin_type, table_type):
    otable = context.Output(table_type)
    manifest = []
    with open(otable.local, "w") as t:
        for i, path in enumerate(bin_files):
            obin = context.Output(bin_type, i=i)
            text = Path(path).read_text()
            obin.local.write_text(text)
            name = Path(path).stem
            for line in text.splitlines():
                if line.startswith(">"):
                    t.write(f"{line[1:].split()[0]}\t{name}\n")
            manifest.append({bin_type: obin.local})
    return manifest + [{table_type: otable.local}]


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=4)),
)
