import json

JS4038 = "GCF_052446975.1"

# PPanGGOLiN tile-plot label -> (NCBI accession, figure label).
GENOMES = {
    "CB13b1a": ("GCF_002310375.3", "C. vibrioides CB13b1a"),
    "CB2": ("GCA_002310295.2", "C. vibrioides CB2 — tap water, California"),
    "DSM_9893": ("GCF_002858865.1", "C. vibrioides DSM 9893 (draft)"),
    "CB15": ("GCF_000006905.1", "C. vibrioides CB15"),
    "CB2A": ("GCA_003856435.1", "C. vibrioides CB2A reference"),
    "CB1": ("GCF_002310455.2", "C. vibrioides CB1"),
    "NA1000": ("GCF_000022005.1", "C. vibrioides NA1000"),
    "CB_S2B_lysogen": ("GCF_019356235.1", "C. vibrioides CB S2B lysogen"),
    "OR37": ("GCF_000372645.1", "OR37 — sediment, Tennessee"),
    "32-67-7": ("GCA_002280275.1", "32-67-7 — MAG, mine wastewater, Ontario"),
    "T5M6": ("GCF_001449105.1", "T5M6 — mine tailing, Hubei"),
    "UBA2596": ("GCA_002340065.1", "UBA2596 — MAG, water, South Africa"),
    "c1db9d16-026a-43e8-9d2b-e4c5db0a04e5": ("GCA_951805235.1", "sponge MAG — freshwater sponge, Canada"),
}

OTHER_CAULOBACTER = [
    ("GCF_044056025.1", "Caulobacter sp. UC70_42 — protist culture, USA"),
    ("GCF_005502685.1", "Caulobacter sp. 3R27C2-B — alpine spring, Italy"),
    ("GCF_002742635.1", "Caulobacter sp. X — freshwater, New York"),
    ("GCF_001556585.1", "Caulobacter sp. CCH5-E12 — shower-hose biofilm, Ohio"),
]


# Reads a PPanGGOLiN `draw --tile_plot` HTML: leaf order, dendrogram links, families per genome.
def load(html):
    s = open(html).read()
    body = s[s.rfind("Plotly.newPlot(") + len("Plotly.newPlot("):]
    dec = json.JSONDecoder()
    i = body.index(",") + 1
    while body[i] in " \n":
        i += 1
    data, n = dec.raw_decode(body[i:])
    i += n
    while body[i] in " ,\n":
        i += 1
    layout, _ = dec.raw_decode(body[i:])
    leaves = layout["xaxis2"]["ticktext"]
    links = [(t["x"], t["y"]) for t in data if t.get("type") == "scatter"]
    z = next(t["z"] for t in data if t.get("type") == "heatmap")
    families = {leaf: sum(1 for row in z if row[j]) for j, leaf in enumerate(leaves)}
    return leaves, links, families


def draw(ax, links, first_leaf_y):
    # Plotly's dendrogram puts leaf k at x = 5 + 10k; rows run downward from first_leaf_y.
    y = lambda x: first_leaf_y - (x - 5) / 10
    for xs, hs in links:
        ax.plot(hs, [y(x) for x in xs], color="#52514e", lw=0.8, solid_capstyle="butt")
    ax.invert_xaxis()
    ax.axis("off")
