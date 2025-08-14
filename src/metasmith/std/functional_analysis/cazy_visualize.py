from plotly.graph_objects import Figure, Pie
import polars as pl
from polars import col, lit
from helpers import parse_args

cazy_clean, output_file = parse_args((
    ("cazy", "Annotated CAZy output TSV"),
    ("out", "HTML visualization of CAZy enzyme class frequencies"),
))

df = pl.read_csv(cazy_clean, separator='\t').with_columns(col("cazy_class_names").str.split(lit(',')).alias("class_list"))
function_counts = df.explode("class_list").group_by("class_list").agg(pl.len().alias("count"))

taxonomies = function_counts.get_column("class_list").to_list()
counts = function_counts.get_column("count").to_list()

fig = Figure(data=[Pie(
    labels=taxonomies,
    values=counts,
    textposition="inside",
    textinfo="percent",
    hole=0.9,
    title={
        "text": "Enzyme Class Distribution",
    },
    textfont={
        "family": "JetBrainsMono Nerd Font, monospace"
    },
    hoverlabel={
        "font": {
            "family": "JetBrainsMono Nerd Font, monospace"
        }
    },
    showlegend=True,
)])

fig.update_layout(
    template="seaborn",
    height=1080,
    width=1920,
)

fig.write_html(output_file)
