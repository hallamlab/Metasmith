from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
img_k2  = model.AddRequirement(lib.GetType("env::kraken2.env"))
img_brk = model.AddRequirement(lib.GetType("env::bracken.env"))
img_bb  = model.AddRequirement(lib.GetType("env::bbtools.env"))
img_pq  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
db      = model.AddRequirement(lib.GetType("ref::kraken2_db"))
reads   = model.AddRequirement(lib.GetType("sequences::short_reads"))

classif = model.AddProduct(lib.GetType("taxonomy::kraken2_classifications"))
kreport = model.AddProduct(lib.GetType("taxonomy::kraken2_report"))
bspec   = model.AddProduct(lib.GetType("taxonomy::bracken_species"))
breport = model.AddProduct(lib.GetType("taxonomy::bracken_kreport"))

def protocol(context: ExecutionContext):
    idb     = context.Input(db)
    ireads  = context.Input(reads)
    iclass  = context.Output(classif)
    ikrep   = context.Output(kreport)
    ibspec  = context.Output(bspec)
    ibrep   = context.Output(breport)

    threads = context.params.get('cpus')
    threads_arg = "" if threads is None else f"--threads {threads}"

    context.ExecWithEnv().ifContainerDo(
        env=img_bb,
        cmd=f"""
            reformat.sh in={ireads.container} \
                unbgzip=f out1=split_r1.fq.gz out2=split_r2.fq.gz
        """
    )

    context.ExecWithEnv().ifContainerDo(
        env=img_k2,
        cmd=f"""
            kraken2 --paired --db {idb.container} {threads_arg} \
                --report {ikrep.container} \
                --output classifications.tsv \
                split_r1.fq.gz split_r2.fq.gz
        """
    )

    context.ExecWithEnv().ifContainerDo(
        env=img_brk,
        cmd=f"""
            bracken -d {idb.container} \
                -i {ikrep.container} \
                -o {ibspec.container} \
                -w {ibrep.container} \
                -r 150 -l S
        """
    )

    context.ExecWithEnv().ifContainerDo(
        env=img_pq,
        cmd=f"""
            python <<'PY'
            import polars as pl
            df = pl.read_csv(
                "classifications.tsv",
                separator="\\t",
                has_header=False,
                new_columns=["status", "readID", "taxID", "length", "kmers"],
                schema_overrides={{
                    "status": pl.Categorical,
                    "readID": pl.String,
                    "taxID":  pl.UInt32,
                    "length": pl.String,
                    "kmers":  pl.String,
                }},
                low_memory=True,
            )
            df = df.with_columns(pl.col("length").cast(pl.Categorical))
            df.write_parquet(
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
            bspec:   ibspec.local,
            breport: ibrep.local,
        }],
        success=ikrep.local.exists() and ibspec.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(128),
        duration=Duration(hours=2),
    )
)
