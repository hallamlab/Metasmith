from pathlib import Path
import json
import pandas as pd
import numpy as np
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

img_mm2 = model.AddRequirement(lib.GetType("env::minimap2.env"))
img_mam = model.AddRequirement(lib.GetType("env::miniasm.env"))
rmeta   = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads   = model.AddRequirement(lib.GetType("sequences::long_reads"), parents={rmeta})
rstats  = model.AddRequirement(lib.GetType("sequences::read_qc_stats"), parents={rmeta})
out_asm = model.AddProduct(lib.GetType("sequences::miniasm_gfa"))

def protocol(context: ExecutionContext):
    irmeta = context.Input(rmeta)
    ireads = context.Input(reads)
    irstats = context.Input(rstats)
    iout_asm = context.Output(out_asm)

    # https://lh3.github.io/minimap2/minimap2.html
    # minimap2 options:
    # -x sr                 short read preset
    # -x map-hifi           pacbio hifi (type of read that is more accurate) long read preset
    # -x map-ont            oxford nanopore long read preset
    # --sr                  Enable short-read alignment heuristics, more sensitivity
    # -2                    use two io threads, more peak memory
    # -a                    SAM format
    # --secondary=no        Whether to output secondary alignments [no]
    # --sam-hit-only        In SAM, don’t output unmapped reads. !this results in report saying 100% reads mapped!
    # --heap-sort=no|yes    Heap merge is faster for short reads, but slower for long reads. [no]
    #   Preset:
    #     -x STR       preset (always applied before other options; see minimap2.1 for details) []
    #                 - map-pb/map-ont: PacBio/Nanopore vs reference mapping
    #                 - ava-pb/ava-ont: PacBio/Nanopore read overlap
    #                 - asm5/asm10/asm20: asm-to-ref mapping, for ~0.1/1/5% sequence divergence
    #                 - splice: long-read spliced alignment
    #                 - sr: genomic short-read mapping
    # https://www.nature.com/articles/s41587-023-01983-6
    # minimap2 -x asm20     this is for hifi

    with open(irmeta.local) as j:
        read_meta = json.load(j)
    length_class = read_meta["length_class"]
    assert length_class in {"long"}, f"unknown length_class: [{length_class}]"

    with open(irstats.local) as j:
        read_stats = json.load(j)
    q = read_stats["mean_quality"]

    if q>=20:
        preset = "-x ava-pb"
    else:
        preset = "-x ava-ont"

    Log.Info("start minimap align")
    cpus = context.params.get("cpus")
    cpus_string = "" if cpus is None else f"-t {cpus}"
    temp_mapping_path = Path("./temp.paf.gz")
    context.ExecWithEnv().ifContainerDo(
        env = img_mm2,
        cmd = f"""
            minimap2 {preset} {cpus_string} \
                {ireads.container} {ireads.container} | gzip -1 >{temp_mapping_path}
        """
    )

    Log.Info("miniasm")
    context.ExecWithEnv().ifContainerDo(
        env = img_mam,
        cmd = f"""
            miniasm \
                -f {ireads.container} {temp_mapping_path} \
                >{iout_asm.container}
        """
    )

    return ExecutionResult(
        manifest=[{
            out_asm: iout_asm.local,
        }],
        success=iout_asm.local.exists(),
    )

TransformInstance(
    protocol = protocol,
    group_by=rmeta,
    model = model,
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=12),
    ),
)
