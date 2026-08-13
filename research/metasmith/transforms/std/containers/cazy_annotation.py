import polars as pl
from polars import col, lit, Expr
from helpers import parse_args

input_file, output_file = parse_args(
    (("cazy", "Raw CAZy output TSV"), ("out", "Cleaned and annotated CAZy output"))
)
cazy_raw = pl.read_csv(input_file, has_header=False, separator="\t")

cazy_clean = (
    cazy_raw.lazy()
    .with_columns(col("column_2").str.split(lit("|")).alias("cazy_sseqid"))
    .with_columns(
        col("cazy_sseqid").list.eval(
            col("").filter(
                col("")
                .str.contains(
                    r"^(GH|GT|CBM|AA|PL|CE)\d+", literal=False, strict=True
                )
            )
        )
        .alias("cazy_families")
    )
    .with_columns(
        col("cazy_families")
        .list.eval(
            pl.when(col("").str.contains(lit(r"^GH\d+")))
            .then(lit("glycoside_hydrolase"))
            .when(col("").str.contains(lit(r"^GT\d+")))
            .then(lit("glycosyltransferase"))
            .when(col("").str.contains(lit(r"^CBM\d+")))
            .then(lit("carbohydrate_binding_module"))
            .when(col("").str.contains(lit(r"^AA\d+")))
            .then(lit("auxiliary_activity"))
            .when(col("").str.contains(lit(r"^PL\d+")))
            .then(lit("polysaccharide_lyase"))
            .when(col("").str.contains(lit(r"^CE\d+")))
            .then(lit("carbohydrate_esterase"))
            .otherwise(lit("NA"))
        )
        .alias("cazy_family_names"),
    )
)


def flattened(column: Expr) -> Expr:
    return column.list.join(lit(","));


cazy_final = (
    cazy_clean.lazy()
    .select(
        [
            col("column_1").alias("qseqid"),
            flattened(col("cazy_families")),
            flattened(col("cazy_family_names")),
            flattened(col("cazy_sseqid")),
            col("column_3").alias("cazy_pident"),
            col("column_4").alias("cazy_length"),
            col("column_11").alias("cazy_evalue"),
            col("column_12").alias("cazy_scord"),
        ]
    )
    .collect()
)

cazy_final.write_csv(output_file, separator="\t", include_header=True)
