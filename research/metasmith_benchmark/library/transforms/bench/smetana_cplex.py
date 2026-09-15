# SMETANA over one metaGEM sample's community: every CarveMe CPLEX model built from that sample's
# published MAGs. metaGEM's own call and solver (config.yaml: smetanaSolver CPLEX, smetanaMedia the 15
# below), against metaGEM's media_db.tsv. CPLEX arrives the way carveme_from_orfs_cplex.py gets it: the
# driver registers the runtime directory and PYTHONPATH points at it, so no image carries a solver licence.
import shutil
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("bench::smetana.env"))
media   = model.AddRequirement(lib.GetType("bench::smetana_media_db"))
cplex   = model.AddRequirement(lib.GetType("modelling::cplex_installation"))
sample  = model.AddRequirement(lib.GetType("bench::gem_community"))
mags    = model.AddRequirement(lib.GetType("sequences::bin_fasta"), parents={sample})
orfs    = model.AddRequirement(lib.GetType("sequences::bin_orfs"), parents={mags})
gems    = model.AddRequirement(lib.GetType("modelling::carveme_model_cplex"), parents={orfs})
out     = model.AddProduct(lib.GetType("bench::smetana_detailed_cplex"))

MEDIA = "M1,M2,M3,M4,M5,M7,M8,M9,M10,M11,M13,M14,M15A,M15B,M16"
HEADER = "community\tmedium\treceiver\tdonor\tcompound\tscs\tmus\tmps\tsmetana\n"


def protocol(context: ExecutionContext):
    Path("models").mkdir()
    for k, gem in enumerate(context.InputGroup(gems)):
        shutil.copy(gem.local, Path("models") / f"mag{k:04d}_{gem.local.stem}.xml")
    ounit = context.Output(out)
    manifest = [{out: ounit.local}]
    if len(list(Path("models").iterdir())) < 2:
        Log.Warn("fewer than two models; a community needs two, writing an empty table")
        ounit.local.write_text(HEADER)
        return ExecutionResult(manifest=manifest, success=True)

    context.ExecWithEnv(env=image, cmd=f"""
        export PYTHONPATH={context.Input(cplex).container}:$PYTHONPATH
        smetana -o community --flavor fbc2 --mediadb {context.Input(media).container} -m {MEDIA} \\
            --detailed --solver cplex -v models/*.xml
    """)
    shutil.move("community_detailed.tsv", ounit.local)
    return ExecutionResult(manifest=manifest, success=ounit.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sample,
    resources=Resources(cpus=12, memory=Size.GB(32), duration=Duration(hours=48)),
)
