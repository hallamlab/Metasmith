import polars as pl
from polars import col, lit
from helpers import parse_args

busco_raw, busco_map, output_file = parse_args(
    (
        ("busco", "Raw BUSCO output TSV"),
        ("busco_map", "BUSCO mapping file of species id -> taxonomy string"),
        ("out", "Cleaned and annotated BUSCO output"),
    )
)

busco_df = pl.read_csv(busco_raw, separator="\t", has_header=False)
busco_clean = busco_df.lazy().select(
    [
        col("column_1").alias("qseqid"),
        col("column_2").alias("busco_sseqid"),
        col("column_3").alias("busco_pident"),
        col("column_11").alias("busco_evalue"),
        col("column_2")
        .str.split(lit("_"))
        .list.get(lit(1), null_on_oob=True)
        .cast(pl.Int64)
        .alias("species_id"),
    ]
)

map_df = pl.read_csv(busco_map, separator=" ", has_header=False).select(
    [col("column_1").alias("species_id"), col("column_2").alias("taxonomy")]
)
final_table = (
    busco_clean.join(map_df.lazy(), on="species_id", how="left")
    .with_columns(col("taxonomy").fill_null(lit("NA")))
    .collect()
)

final_table.write_csv(output_file, separator="\t", include_header=True)
