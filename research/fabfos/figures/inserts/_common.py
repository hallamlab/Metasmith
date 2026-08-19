from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
CACHE = HERE / "cache"

RUN = REPO / "data" / "fabfos" / "runs" / "scadc_fosmids"
SEQUENCES = RUN / "sequences"
INSERTS = SEQUENCES / "inserts"
INSERT_META = INSERTS / "insert_metadata"
COVERAGE = SEQUENCES / "insert_coverage"
ASSEMBLY = RUN / "assembly"
POOLS = RUN / "pools"
PLASMIDSAURUS = RUN / "plasmidsaurus"
VECTOR = REPO / "data" / "fabfos" / "originals" / "vector" / "pcc1.fna"

VECTOR_ID = "pCC1fos"


def read_coverage_matrix():
    import pandas as pd
    m = pd.read_csv(COVERAGE / "insert_coverage_matrix.tsv", sep="\t")
    return m[m["insert_id"] != VECTOR_ID].reset_index(drop=True)

PLOTLY = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A",
          "#19D3F3", "#FF6692", "#B6E880", "#FF97FF", "#FECB52"]
C_A, C_B = PLOTLY[0], PLOTLY[1]
INK = "#212121"
FAINT = "#9E9E9E"

ASSEMBLERS = ["spades", "megahit"]
ASM_COLOR = {"spades": C_A, "megahit": C_B}
ASM_LABEL = {"spades": "SPAdes (meta)", "megahit": "MEGAHIT"}


def save(fig, stem, dpi=300):
    CACHE.mkdir(parents=True, exist_ok=True)
    png = CACHE / f"{stem}.png"
    fig.savefig(png, dpi=dpi, bbox_inches="tight")
    fig.savefig(CACHE / f"{stem}.svg", bbox_inches="tight")
    print(f"wrote {png.relative_to(REPO)} (+ .svg)", flush=True)
    return png


def pool_sort_key(pool):
    num, _, bc = pool.partition("_")
    return (int(num.removeprefix("pool")), bc)
