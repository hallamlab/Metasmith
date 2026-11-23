from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
img_k2  = model.AddRequirement(lib.GetType("env::kraken2.env"))
img_pq  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
db      = model.AddRequirement(lib.GetType("ref::kraken2_db"))
# Required and never read: this is how the sample's name enters the lineage of the
# two reports, so a later step that pools them can ask which sample each came from.
# `logistics/getNcbiAssembly.py` does the same with `ncbi::genome_name`.
name    = model.AddRequirement(lib.GetType("sequences::sample_name"))
reads   = model.AddRequirement(lib.GetType("sequences::long_reads"), parents={name})

classif = model.AddProduct(lib.GetType("taxonomy::kraken2_classifications"))
kreport = model.AddProduct(lib.GetType("taxonomy::kraken2_report"))

def protocol(context: ExecutionContext):
    idb     = context.Input(db)
    ireads  = context.Input(reads)
    iclass  = context.Output(classif)
    ikrep   = context.Output(kreport)

    threads = context.params.get('cpus')
    threads_arg = "" if threads is None else f"--threads {threads}"

    context.ExecWithEnv(
        env=img_k2,
        cmd=f"""
            kraken2 --db {idb.container} {threads_arg} \
                --report {ikrep.container} \
                --output classifications.tsv \
                {ireads.container}
        """
    )

    context.ExecWithEnv(
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
        }],
        success=ikrep.local.exists(),
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
