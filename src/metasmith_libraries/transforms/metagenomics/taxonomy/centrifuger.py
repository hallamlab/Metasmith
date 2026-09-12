from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::centrifuger.env"))
img_bb  = model.AddRequirement(lib.GetType("env::bbtools.env"))
img_pq  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
db      = model.AddRequirement(lib.GetType("ref::centrifuger_db"))
reads   = model.AddRequirement(lib.GetType("sequences::short_reads"))

classif = model.AddProduct(lib.GetType("taxonomy::centrifuger_classifications"))
kreport = model.AddProduct(lib.GetType("taxonomy::centrifuger_kreport"))
summary = model.AddProduct(lib.GetType("taxonomy::centrifuger_summary"))

def protocol(context: ExecutionContext):
    idb     = context.Input(db)
    ireads  = context.Input(reads)
    iclass  = context.Output(classif)
    ikrep   = context.Output(kreport)
    isumm   = context.Output(summary)

    threads = context.params.get('cpus')
    threads_arg = "" if threads is None else f"-t {threads}"

    context.ExecWithEnv(
        env=img_bb,
        cmd=f"""
            reformat.sh in={ireads.container} \
                unbgzip=f out1=split_r1.fq.gz out2=split_r2.fq.gz
        """
    )

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            pfx=$(ls {idb.container}/*.1.cfr 2>/dev/null | head -1)
            pfx=${{pfx%.1.cfr}}
            test -n "$pfx" -a -f "$pfx.1.cfr" || {{ echo "centrifuger prefix not discovered in {idb.container}"; exit 1; }}

            centrifuger -x "$pfx" \
                -1 split_r1.fq.gz -2 split_r2.fq.gz \
                {threads_arg} > classifications.tsv

            centrifuger-kreport -x "$pfx" classifications.tsv > {ikrep.container}

            centrifuger-quant -x "$pfx" -c classifications.tsv 2>/dev/null > {isumm.container}
        """
    )

    context.ExecWithEnv(
        env=img_pq,
        cmd=f"""
            python <<'PY'
            import polars as pl
            pl.read_csv(
                "classifications.tsv",
                separator="\\t",
                has_header=True,
                schema_overrides={{
                    "readID":       pl.String,
                    "seqID":        pl.Categorical,
                    "taxID":        pl.UInt32,
                    "score":        pl.UInt32,
                    "2ndBestScore": pl.UInt32,
                    "hitLength":    pl.UInt16,
                    "queryLength":  pl.UInt16,
                    "numMatches":   pl.UInt16,
                }},
                low_memory=True,
            ).write_parquet(
                "{iclass.container}",
                compression="zstd",
                compression_level=10,
                statistics=True,
            )
            PY
        """
    )

    return ExecutionResult(
        manifest=[{
            classif: iclass.local,
            kreport: ikrep.local,
            summary: isumm.local,
        }],
        success=ikrep.local.exists() and iclass.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(288),
        duration=Duration(hours=3),
    )
)
