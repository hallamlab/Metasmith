"""Paths, palette and the save contract shared by the six insert figures.

Every data path a figure reads resolves under `./data`. That is the rule this
module exists to hold in one place: no figure here reaches into a scratch
directory, a sibling project, or another host, and one that needs something not
yet in `./data` is expected to fail rather than to substitute.

Outputs go to `cache/`, which is gitignored repo-wide -- the script is the
artifact under version control, not its render. Every figure writes a `.png` to
look at and a `.svg` vector master beside it, from one stem.
"""
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
CACHE = HERE / "cache"

RUN = REPO / "data" / "fabfos" / "scadc_fosmids"
SEQUENCES = RUN / "sequences"
INSERTS = SEQUENCES / "inserts"
INSERT_META = INSERTS / "insert_metadata"
COVERAGE = SEQUENCES / "insert_coverage"
ASSEMBLY = RUN / "assembly"
POOLS = RUN / "pools"
PLASMIDSAURUS = RUN / "plasmidsaurus"
VECTOR = REPO / "data" / "originals" / "vector" / "pcc1.fna"

# The coverage matrix's reference is inserts PLUS the backbone, so that a read off
# the vector has somewhere to map and the mapped fraction means something. That
# row is a seventh to a fifth of every pool by aligned bases and is not an insert,
# so nothing that reads as composition may include it.
VECTOR_ID = "pCC1fos"


def read_coverage_matrix():
    """The insert x pool coverage matrix, without the vector row. -> DataFrame."""
    import pandas as pd
    m = pd.read_csv(COVERAGE / "insert_coverage_matrix.tsv", sep="\t")
    return m[m["insert_id"] != VECTOR_ID].reset_index(drop=True)

# Plotly's qualitative colorway, as the scadc figures used it, so the two sets
# read as one family.
PLOTLY = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A",
          "#19D3F3", "#FF6692", "#B6E880", "#FF97FF", "#FECB52"]
C_A, C_B = PLOTLY[0], PLOTLY[1]
INK = "#212121"          # near-black: outlines, rules, the unemphasised series
FAINT = "#9E9E9E"

ASSEMBLERS = ["spades", "megahit"]
ASM_COLOR = {"spades": C_A, "megahit": C_B}
ASM_LABEL = {"spades": "SPAdes (meta)", "megahit": "MEGAHIT"}


def save(fig, stem, dpi=300):
    """Write `<stem>.png` + `<stem>.svg` under cache/. -> the png path."""
    CACHE.mkdir(parents=True, exist_ok=True)
    png = CACHE / f"{stem}.png"
    fig.savefig(png, dpi=dpi, bbox_inches="tight")
    fig.savefig(CACHE / f"{stem}.svg", bbox_inches="tight")
    print(f"wrote {png.relative_to(REPO)} (+ .svg)", flush=True)
    return png


def pool_sort_key(pool):
    """`pool07_GAGGACTT` -> (7, 'GAGGACTT'), so pool 2 sorts before pool 10."""
    num, _, bc = pool.partition("_")
    return (int(num.removeprefix("pool")), bc)
