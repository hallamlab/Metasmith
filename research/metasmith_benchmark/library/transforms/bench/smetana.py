# SMETANA over one assembly's community: every CarveMe model built from its DAS Tool MAGs. The call
# is metaGEM's (`--flavor fbc2 --mediadb media_db.tsv -m <15 media> --detailed`) on SCIP, not CPLEX.
# The media table is the one CarveMe ships, the table metaGEM copies next to its models.
import shutil
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("bench::smetana.env"))
carveme = model.AddRequirement(lib.GetType("bench::carveme_166.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
mags    = model.AddRequirement(lib.GetType("sequences::das_tool_bin_fasta"), parents={asm})
orfs    = model.AddRequirement(lib.GetType("sequences::bin_orfs"), parents={mags})
gems    = model.AddRequirement(lib.GetType("modelling::carveme_model"), parents={orfs})
out     = model.AddProduct(lib.GetType("bench::smetana_detailed"))

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

    context.ExecWithEnv(env=carveme, cmd="""
        cp "$(python -c 'import carveme, os; print(os.path.join(os.path.dirname(carveme.__file__), "data", "input", "media_db.tsv"))')" media_db.tsv
    """)
    context.ExecWithEnv(env=image, cmd=f"""
        smetana -o community --flavor fbc2 --mediadb media_db.tsv -m {MEDIA} --detailed --solver scip -v models/*.xml
    """)
    shutil.move("community_detailed.tsv", ounit.local)
    return ExecutionResult(manifest=manifest, success=ounit.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=2, memory=Size.GB(32), duration=Duration(hours=48)),
)
