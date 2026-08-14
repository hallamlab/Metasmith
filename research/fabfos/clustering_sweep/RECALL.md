# What the silhouette-selected clustering threshold costs in clones

The shipped insert set is cut where the silhouette is highest. Nothing had ever asked
what that costs. On the day-6 pool it costs **16 of 158 clones** — 10.1% — and buys a
silhouette 0.099 higher than the library's own `identity 0.99` default, which loses
none.

## The three cuts

| cut | N | identity | silhouette | clone recall | inserts |
|---|---|---|---|---|---|
| silhouette peak — what `--select-k silhouette` ships | 176 | 0.579 | **0.821** | 0.899 | 142 |
| coarsest cut holding clone recall ≥ 0.99 | 220 | 0.954 | 0.756 | **0.994** | 158 |
| library default `--select-k identity 0.99` | 231 | 0.990 | 0.722 | **1.000** | 158 |

Measured after similarity moved to summing non-overlapping HSPs and the absorb pass
became closure-aware; the previous metric gave 162 clones and the same shape. **The read
recall column is dropped rather than restated** — it was measured per partition on fir and
the partitions moved, so the numbers below are the old ones and the column would be a
mismatch. Re-fetch it with `tests/recall_map_on_fir.sh` before quoting a figure.

**Recommendation: take identity 0.99 — the library default, unchanged.** The ≥99%
frontier at identity 0.954 is eleven clusters coarser and still drops a clone, so
the 0.034 of silhouette it recovers is not worth the one clone it costs. The honest
headline is not that a new threshold was found: it is that the *swept* threshold is the
outlier, and `--select-k silhouette` should not be the setting a shipped insert set is
cut at.

The trade is smooth and there is no interior compromise to find. Between N=176 and
N=231 the silhouette falls monotonically while clone recall rises monotonically —
every clone recovered is paid for in silhouette at a near-constant rate. Nothing in the
curve marks a natural stopping point between them, which is why the choice has to be
made on what recall is worth rather than on the shape of the curve.

`cache/recall_sweep.png` draws it; `cache/recall_sweep.tsv` is the full 180-partition
curve.

## Why clone recall and not bases

Base-level retention is the wrong instrument and says so by staying above 99% almost
everywhere. The loss is not bases trimmed off the ends of an insert — it is a whole
fosmid merged into a centroid it shares 58% identity with, and the centroid keeps its
own full length either way. Counting bases scores that merge as free.

Clone recall counts it the way it happens. Fix a reference cut at identity R,
post-absorb, and call each of its surviving representatives a clone; for a candidate
partition, recall is the number of *distinct* surviving centroids those clones resolve
to, over the number of clones. Two clones landing on one centroid is one clone lost.
The measured curve tracks the insert count exactly — each merge is one clone gone —
which is the sanity check that the definition is doing what it claims.

**The reference identity R does not matter.** R = 0.98, 0.99 and 0.995 all yield 158
clones and identical recall at all 180 partitions. They are not the same *set* — the
representative piece differs — but the tree has no merge anywhere in [0.98, 1.0] that
changes how many post-absorb representatives survive, so the free parameter in the
definition is inert over its whole plausible range. That was the main thing this
measurement was at risk of quietly depending on, and it does not.

## Read recall does not constrain, and is worth knowing why

**Measured under the previous similarity metric**, so the partition-indexed numbers here
are approximate now; the pieces it maps against did not change, and neither did the shape.

Measured on fir: 24.4M read pairs across the three barcodes mapped against the same
409 pieces with secondaries retained, reduced to per-pair hit-sets. A pair is recalled
if any piece it hit survived as a centroid.

Read recall is **≥ 0.99 across the entire range anyone would consider**, including the
silhouette peak (0.994). It only breaks below identity ≈ 0.10, where a single very
abundant clone is finally merged away and it falls off a cliff to 0.69. So read recall
and clone recall disagree completely about the silhouette peak: by reads it is fine, by
clones it drops one insert in ten.

That disagreement is the finding, not a discrepancy to be resolved. The clones lost at
the peak carry roughly a twentieth of the read support of an average clone — losing
10.1% of clones costs ~0.6% of reads. They are the low-abundance end of the library.
Whether that matters is a question about what the insert set is *for*: a
coverage-weighted use barely notices, a per-clone catalogue loses one in ten.

Denominator: the 19,922,140 pairs that hit any piece at all, of 24,389,304 total.
The other 18.3% hit nothing and are outside the question. Those per-barcode hit
fractions — 0.823 / 0.822 / 0.803 — sit just above the 0.815 / 0.813 / 0.791
`fraction_reads_mapped` the last 35-pool run measured, which is the expected direction:
a pair counts if either mate hits, and the reference here is 409 pieces rather than 157
inserts.

## Caveats

- **Scope.** Day-6 only: the three `pool01_*` barcodes, one library sequenced three
  times, 409 of the 669 pieces the full dedup clusters. It earns its standing as a
  stand-in by reproducing the full run's silhouette optimum exactly — identity 0.5791
  on both — but it is one pool, and the day-6 library is the one with the lowest
  mapping fraction of the 35.
- **Pairs, not reads.** Interleaved mates share a name and PAF carries no flag, so the
  pair is the smallest unit that can be counted honestly. The `-N 50` secondary cap was
  reached by 4 pairs in 24.4M and the largest hit-set seen was 52, so the cap never
  bound.
- **The reference is a cut of the same tree it judges.** That is why R is swept and why
  read recall was measured independently at all. Both checks came back clean.

## Reproducing

    mamba run -n figure-net python main/clustering_sweep/gen_recall_sweep.py   # the curve
    ./tests/recall_map_on_fir.sh submit                                        # ~10 min on fir
    ./tests/recall_map_on_fir.sh fetch
    mamba run -n figure-net python main/clustering_sweep/gen_recall_sweep.py   # merges read recall
    mamba run -n figure-net python main/clustering_sweep/gen_recall_figure.py

The sweep builds its work directory from `./data` through
`main/figures/inserts/{pieces,identity}.py`, which re-run the pipeline's own cut over
the shipped junction blast and its own all-vs-all over the result. The all-vs-all covers
all 669 pieces, so the day-6 submatrix is already in it. Representatives come from the
pipeline's `_representatives`, so the curve scores the insert sets the pipeline would
actually ship.

If that scratch tree has been deleted, `--rebuild` re-derives it from
`v4/pool01_*/split_contigs.fna` (needs blastn; `FABFOS_BLAST_BIN` overrides where it is
looked up). It was checked against the cached-work path and reproduces the curve
byte-for-byte, read-recall column included.
