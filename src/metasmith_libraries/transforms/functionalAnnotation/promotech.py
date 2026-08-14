import shutil
from pathlib import Path

from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("env::promotech.env"))
assembly   = model.AddRequirement(lib.GetType("sequences::assembly"))
gff        = model.AddRequirement(lib.GetType("sequences::gff"))

out_pred = model.AddProduct(lib.GetType("annotation::promotech_predictions"))

# Helpers next to this transform file, copied into /ws at runtime
# (same pattern as bakta_noncoding's _piler_cr_to_gff3.py).
EXTRACT = Path(__file__).parent / "_extract_noncoding_chunks.py"
MERGE   = Path(__file__).parent / "_merge_promotech_results.py"


def protocol(context: ExecutionContext):
    iasm = context.Input(assembly)
    igff = context.Input(gff)
    opred = context.Output(out_pred)

    shutil.copy(EXTRACT, Path.cwd() / "_extract_noncoding_chunks.py")
    shutil.copy(MERGE,   Path.cwd() / "_merge_promotech_results.py")

    chunks_dir  = "/ws/pt_chunks"
    results_dir = "/ws/pt_results"

    # Step 1: Extract non-coding intervals into <=1Mbp chunk FASTAs
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            python /ws/_extract_noncoding_chunks.py \
                --fasta {iasm.container} \
                --gff {igff.container} \
                --outdir {chunks_dir} \
                --max-chunk-bp 1000000 &&
            cd /ws
        """,
    )

    # Step 2: Run PromoTech parse + predict on each chunk in parallel.
    cpus = context.params.get("cpus", 4)
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            cd /opt/promotech &&
            mkdir -p {results_dir} &&
            run_chunk() {{
                chunk=$1
                name=$(basename "$chunk" .fna)
                mkdir -p {results_dir}/$name
                python promotech.py -pg -f "$chunk" \
                    -o {results_dir}/$name -m RF-HOT 2>&1 &&
                python promotech.py -g -i {results_dir}/$name \
                    -o {results_dir}/$name -m RF-HOT -t 0.5 2>&1
            }} &&
            export -f run_chunk &&
            ls {chunks_dir}/*.fna | xargs -I{{}} -P {cpus} \
                bash -c 'run_chunk "$@"' _ {{}} &&
            cd /ws
        """,
    )

    # Step 3: Merge results and remap coordinates
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            python /ws/_merge_promotech_results.py \
                --manifest {chunks_dir}/manifest.json \
                --results-dir {results_dir} \
                --output /ws/pt_merged.tsv &&
            cd /ws
        """,
    )

    context.LocalShell(f"cp /ws/pt_merged.tsv {opred.local}")

    return ExecutionResult(
        manifest=[{out_pred: opred.local}],
        success=opred.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=assembly,
    resources=Resources(
        cpus=4,
        memory=Size.GB(64),
        duration=Duration(hours=4),
    ),
)
