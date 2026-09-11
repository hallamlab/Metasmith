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
    _cmd = f"""
            minimap2 {preset} {cpus_string} \
                {ireads.container} {ireads.container} | gzip -1 >{temp_mapping_path}
        """
    context.ExecWithEnv(env=img_mm2, cmd=_cmd)

    Log.Info("miniasm")
    _cmd = f"""
            miniasm \
                -f {ireads.container} {temp_mapping_path} \
                >{iout_asm.container}
        """
    context.ExecWithEnv(env=img_mam, cmd=_cmd)

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
