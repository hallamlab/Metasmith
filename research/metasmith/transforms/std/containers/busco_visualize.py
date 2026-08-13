from plotly.graph_objects import Figure, Pie
import polars as pl
from polars import col
from helpers import parse_args

busco_clean, output_file = parse_args((
    ("busco", "Annotated BUSCO output TSV"),
    ("out", "HTML visualization of BUSCO enzyme class frequencies"),
))

df = pl.read_csv(busco_clean, separator='\t').lazy().select(col("taxonomy").str.replace_all("_", " "))
tax_counts = df.group_by("taxonomy").agg(pl.len().alias("count")).collect()

taxonomies = tax_counts.get_column("taxonomy").to_list()
counts     = tax_counts.get_column("count").to_list()

fig = Figure(data=[Pie(
    labels=taxonomies,
    values=counts,
    textposition="inside",
    textinfo="percent",
    hole=0.9,
    title={
        "text": "Taxonomy Distribution",
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
