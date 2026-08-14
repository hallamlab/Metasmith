# Resolving fosmids — the manuscript figure set

Six figures on what this pipeline recovers from pooled fosmid DNA, drawn on its
own output: the 170 inserts of the SCADC run under
`data/fabfos/scadc_fosmids/`. They replace an earlier set drawn in the `scadc`
project on a 199-contig set produced before this pipeline existed, under a
different identity metric — so every number on them has moved. These are not a
reproduction of those.

## The rule this directory keeps

**Every path a figure reads resolves under `./data`.** No scratch directory, no
sibling project, no other host, no temp path. Two inputs were missing when the
set was built and were brought in rather than worked around, as their own pinned
chunks: `scadc_fosmids/pools/` (the enrichment lineage) and
`scadc_fosmids/plasmidsaurus/` (the two long-read clone assemblies).

Outputs go to `cache/`, gitignored repo-wide. The script is the artifact under
version control; from a cold cache the whole set redraws in about a minute.

## The figures

| script | what it claims | reads |
|---|---|---|
| `readqc_vs_assembly.py` | read quality and depth do not predict assembly outcome; the two assemblers trade contiguity against completeness | `assembly/assembly_stats/` |
| `cluster_quality.py` | where the pipeline's 0.99 cut sits against the silhouette optimum, and what identity each granularity means | the shared layer |
| `identity_matrix.py` | the 170 inserts read as clean diagonal blocks, and most of them land in the λ packaging window | the shared layer + `insert_metadata/` |
| `insert_lengths.py` | length distribution, how many vector junctions each insert closed on, and how far each one took over a pool | `insert_metadata/`, `insert_coverage/`, `pool_pie_tree.py` |
| `pool_pie_tree.py` | the enrichment collapses: a diverse founder narrows to one insert being most of a pool, with a black wedge for the 0.9–3.2% of reads that mapped to nothing | `pools/`, `insert_coverage/` |
| `clone_alignment.py` | a recovered insert is the molecule it came from, end to end, against long-read assemblies of two picked clones | `plasmidsaurus/`, `inserts.fna`, `originals/vector/` |

Beside them, `junction_table.py` writes the counts behind the length figure's
barcode — how many inserts closed on 2, 1 and 0 vector junctions, with each
group's median length — as `cache/junction_table.{png,svg,tsv}`. It reads
`inserts.csv` and nothing else, so it needs neither the pieces rebuild nor
`blastn`.

Each script's docstring is its spec. Run any of them with no arguments:

    mamba run -n figure-net python main/figures/inserts/<script>.py

`figure-net` carries numpy, scipy, scikit-learn, umap and matplotlib. Three of
the six also need `blastn`, taken from `fabfos-bio` (override with
`FABFOS_BLAST_BIN`); nothing else on this machine has it.

## The shared layer

`pieces.py` and `identity.py` draw nothing. They exist because two figures are
about the CLUSTERING, so their input is the 669 pieces the clustering ran on —
and the run ships the inserts, not the pieces.

`pieces.py` puts the pieces back by re-running the pipeline's own `rectify` over
the shipped junction blast and the assembly graphs. `--verify` re-derives the 170
centroids and diffs them against `inserts.fna`; it reports 170 exact, 0 mismatch,
and that check is the module's whole licence.

It re-runs rather than slicing the coordinates out of the ids because **edit
status is a property of the cut, not of the coordinates**. Inferring "trimmed"
from whether a piece spans its whole contig is wrong on 10 of the 669, and wrong
in the direction that matters: it calls untouched graph-circular contigs trimmed,
because their stated join overlap makes the piece shorter than the raw record.
The length figure reports the action rather than drawing it — its barcode now
keys on junction count — so the 10 are a count that would be wrong, not a
colour.

`identity.py` runs one all-vs-all at the dedup's own permissive settings and
reduces it with `fabfos_recovery._similarity`. Both are imported, never restated:
a second definition of identity living in the figure of the pipeline would show
up as a slightly different curve and as nothing else. Its output lands in the
piece work directory under the pipeline's own name, so that directory *is* a
dedup work directory — which is what `main/clustering_sweep/` now reads.

Three cross-checks that the layer is measuring what the run measured: the 0.99
cut lands at 272 clusters, the silhouette optimum at 199 clusters and identity
0.579, and the day-6 recall sweep reproduces `../clustering_sweep/RECALL.md` line
for line.

## Three things to say when reporting these

- **The coverage matrix's reference is the inserts PLUS the pCC1fos backbone**, so
  that a read off the vector has somewhere to map and the mapped fraction means
  something — 96–99% here against 79–85% without it. That row is a seventh to a
  fifth of every pool by aligned bases and is not an insert, so
  `_common.read_coverage_matrix()` drops it and everything reading composition
  goes through that.

- **The reads are host-depleted, and that is the only read QC this run recorded.**
  There is no raw-versus-filtered pair and no trimming report, so figure 1 says
  what the assembler was given, not what QC removed.
- **The clones are identified by alignment, not by name.** The archived figure
  named its two inserts in an id space this run retired. Each clone's counterpart
  is now the insert that covers most of it — which is the honest form of the
  claim, and here it is 100% of both.
- **The pool figure is much starker than it was, and that is a bug fix.** One
  picked clone used to appear as six inserts, so its pools drew as six wedges of
  the same molecule. With the dedup summing HSPs and using closure, they are one:
  28 of 33 pools are now more than half a single insert and most of those are
  above 99%. The enrichment really does collapse to one clone — in fact to two,
which between them dominate 25 of those 28.

- **`pool_pie_tree_legend.svg` is the key for two figures.** The length figure
  imports that palette and the share behind it, so an insert is one colour
  wherever it appears. The share is per POOL, not per library: the founder's
  three barcodes are three samplings of one pool, 91 of its 166 inserts are seen
  in only one of them, and dividing by one library's depth inflates exactly
  those rare clones threefold.
