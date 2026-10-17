from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("env::promotech.env"))
assembly   = model.AddRequirement(lib.GetType("sequences::assembly"))
gff        = model.AddRequirement(lib.GetType("sequences::gff"))
extract    = model.AddRequirement(lib.GetType("lib::extract_noncoding_chunks.py"))
merge      = model.AddRequirement(lib.GetType("lib::merge_promotech_results.py"))

out_pred = model.AddProduct(lib.GetType("annotation::promotech_predictions"))


def protocol(context: ExecutionContext):
    iasm = context.Input(assembly)
    igff = context.Input(gff)
    iextract = context.Input(extract)
    imerge = context.Input(merge)
    opred = context.Output(out_pred)

    chunks_dir  = "pt_chunks"
    results_dir = "pt_results"

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            python {iextract.container} \
                --fasta {iasm.container} \
                --gff {igff.container} \
                --outdir {chunks_dir} \
                --max-chunk-bp 1000000
        """,
    )

    cpus = context.params.get("cpus", 4)
    context.ExecWithEnv(
        env=image,
        cmd=f"""
            export ws="$PWD"
            cd /opt/promotech &&
            mkdir -p "$ws/{results_dir}" &&
            run_chunk() {{
                chunk=$1
                name=$(basename "$chunk" .fna)
                mkdir -p "$ws/{results_dir}/$name"
                python promotech.py -pg -f "$chunk" \
                    -o "$ws/{results_dir}/$name" -m RF-HOT 2>&1 &&
                python promotech.py -g -i "$ws/{results_dir}/$name" \
                    -o "$ws/{results_dir}/$name" -m RF-HOT -t 0.5 2>&1
            }} &&
            export -f run_chunk &&
            ls "$ws/{chunks_dir}"/*.fna | xargs -I{{}} -P {cpus} \
                bash -c 'run_chunk "$@"' _ {{}}
        """,
    )

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            python {imerge.container} \
                --manifest {chunks_dir}/manifest.json \
                --results-dir {results_dir} \
                --output pt_merged.tsv
        """,
    )

    context.LocalShell(f"cp pt_merged.tsv {opred.local}")

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
