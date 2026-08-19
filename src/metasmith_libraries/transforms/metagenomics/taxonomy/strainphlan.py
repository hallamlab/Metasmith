import glob
from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::metaphlan.env"))
reads = model.AddRequirement(lib.GetType("sequences::clean_short_reads"))
db = model.AddRequirement(lib.GetType("annotation::metaphlan_db"))
out_markers = model.AddProduct(lib.GetType("annotation::strainphlan_consensus_markers"))

MPA_INDEX = "mpa_vJan25_CHOCOPhlAnSGB_202503"
MPA_PKL = f"{MPA_INDEX}.pkl"


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    idb = context.Input(db)
    iout = context.Output(out_markers)

    threads = context.params.get("cpus", 8)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idb.external, "/mpa_db")],
        cmd=f"""
            export HOME="$(pwd)/home"; mkdir -p "$HOME" markers work
            S=$(basename {ireads.container}); S=${{S%.gz}}; S=${{S%.fq}}; S=${{S%.fastq}}
            metaphlan {ireads.container} \
                --input_type fastq --offline \
                --db_dir /mpa_db --index {MPA_INDEX} \
                --nproc {threads} \
                --mapout work/$S.mapout \
                -s work/$S.sam.bz2 \
                -o work/$S.profile.tsv
            sample2markers.py -i work/$S.sam.bz2 -f bz2 \
                -d /mpa_db/{MPA_PKL} -o markers -n {threads}
        """,
    )

    hits = sorted(glob.glob("markers/*.json.bz2") + glob.glob("markers/*.pkl"))
    if hits:
        context.LocalShell(f"cp {hits[0]} {iout.local}")

    return ExecutionResult(
        manifest=[{out_markers: iout.local}],
        success=bool(hits) and iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(48),
        duration=Duration(hours=3),
    ),
)
