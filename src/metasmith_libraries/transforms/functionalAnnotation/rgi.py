"""rgi — RGI/CARD main ARG detection on Prodigal proteins.

Runs on a per-sample protein CHUNK (sequences::orf_chunk, sample-prefixed by
w4_rebatch.py) so RGI, like every other AMR tool, consumes batched ORFs and
never a whole-sample file. RGI's ORF_ID column carries the input protein header
(SG<id>~k141_XXXXXX_N), so the sample + original contig ID are preserved for the
downstream merge_rgi + w4_recompile de-prefix. CARD is provided pre-loaded as the
./localDB directory (annotation::card_db, built by downloadCardDB.py).
"""
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::rgi.env"))
chunk = model.AddRequirement(lib.GetType("sequences::orf_batch"))
card = model.AddRequirement(lib.GetType("annotation::card_db"))
out_results = model.AddProduct(lib.GetType("annotation::rgi_results_chunk"))


def protocol(context: ExecutionContext):
    iorfs = context.Input(chunk)
    icard = context.Input(card)
    iout = context.Output(out_results)

    threads = context.params.get("cpus", 8)

    # rgi main --local consumes ./localDB in the work dir; stage the prepared
    # CARD localDB there. RGI appends .txt to --output_file.
    context.LocalShell(f"cp -r {icard.external} ./localDB")
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            rgi main \
                --input_sequence {iorfs.container} \
                --output_file rgi_out \
                --input_type protein \
                --local \
                -n {threads} \
                --include_loose \
                --clean
        """,
    )

    context.LocalShell(f"cp rgi_out.txt {iout.local}")

    return ExecutionResult(
        manifest=[{out_results: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=chunk,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
