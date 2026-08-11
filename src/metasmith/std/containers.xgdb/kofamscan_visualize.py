from io import StringIO
from plotly.graph_objects import Figure, Bar
import polars as pl
from helpers import parse_args
import re

kofamscan, output_file = parse_args((
    ("kofamscan", "Annotated kofamscan output TSV"),
    ("out", "HTML visualization of kofamscan KO frequencies"),
))

with open(kofamscan) as f:
    kofamscan_str = f.read()

kofamscan_str = "\n".join(
    re.sub(r" +", ",", line.strip())
    for line in kofamscan_str.splitlines()
    if line.strip() and not line.startswith("#")
)

df = pl.read_csv(StringIO(kofamscan_str), has_header=False, infer_schema_length=0, truncate_ragged_lines=True).select(pl.col("column_2").alias("KO"))
function_counts = df.filter(pl.col("KO").str.starts_with("K")).group_by("KO").agg(pl.len().alias("count"))

taxonomies = function_counts.get_column("KO").to_list()
counts = function_counts.get_column("count").to_list()

fig = Figure(data=[Bar(
    x=taxonomies,
    y=counts,
    text=counts,
    textposition="outside",
)])

fig.update_layout(
    title="KO Frequency",
    xaxis_title="KO",
    yaxis_title="Count",
    template="seaborn",
    height=1080,
    width=1920,
    xaxis=dict(tickangle=-45),
)

fig.write_html(output_file)
