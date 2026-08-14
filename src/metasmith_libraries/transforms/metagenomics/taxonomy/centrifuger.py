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

    context.ExecWithEnv().ifContainerDo(
        env=img_bb,
        cmd=f"""
            reformat.sh in={ireads.container} \
                out1=split_r1.fq.gz out2=split_r2.fq.gz
        """
    )

    # centrifuger writes per-read classifications as a TSV (one row per read);
    # kreport + quant downstream tools both consume the TSV. Stage it locally
    # then convert to parquet for the {iclass} product at the end.
    context.ExecWithEnv().ifContainerDo(
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

    # TSV -> parquet (zstd, polars). Schema-explicit so we get dictionary-encoded
    # categoricals on seqID + narrow integer widths — ~7-8x smaller than the TSV
    # for typical short-read metagenomes (benchmarked SG10E12: 3.42 GiB -> 0.45 GiB).
    # NOTE on indentation: metasmith's RemoveLeadingIndent (coms/ipc.py)
    # strips chars equal to the FIRST non-empty line's leading indent from
    # EVERY line. So the heredoc body must share that same 12-space indent;
    # otherwise the body and the `PY` terminator get chopped, leaving an
    # unterminated heredoc and a syntax-corrupted Python script.
    context.ExecWithEnv().ifContainerDo(
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
        memory=Size.GB(288),  # r226 SLURM peak RSS 172-194 GB (last 20 array steps
                              # retained from Wp5jjOW2 sweep). r232 hvfpc hybrid
                              # documents ~230 GB classifier RAM in README;
                              # 288 GB leaves ~50 GB headroom. Parquet conversion
                              # step needs ~15 GB peak (single sample) — fits.
        duration=Duration(hours=3),  # r226 max elapsed 75 min; r232 projected ~90 min;
                                     # parquet step adds ~10s (negligible).
    )
)
