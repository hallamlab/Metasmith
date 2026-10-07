# ab48_r1 — run journal

## Purpose & Contents

Dated entries for runs of this campaign's studies: what ran, what broke, what the
results say. Newest last. How to run the campaign is in `README.md`, and what each step does
is in its transform.

---

## 2026-10-01 — purify: measurement association, key BfQLUCYC

The first purify study held 8 samples: the seed, two lab samples and the 5 bioreactor
samples. Each measurement is a 7-day mean of one sensor column from the bioreactor logs. The
run was green, but `measurement_association.py` fills a missing measurement with the column
median (line 380). The seed and lab samples therefore entered the ordination with bioreactor
readings they never had. Those results were deleted unpinned.

The study is now the 5 bioreactor samples alone, with `Round` its only label of two or more
levels. The rerun ends with 24 tasks succeeded and is pinned at `data/aspire/purify_r1.dvc`.

- CCA, RDA and db-RDA each constrain 96-97% of the inertia, which is the rank limit at five
  samples. Each reaches p = 0.1, the smallest p five samples allow.
- None of 154 ASV-sensor Spearman correlations survives FDR.
- A05 and A06 share one averaging window, so their readings are identical.

The row runs on real measurements. The study is too small to say anything with them.

## 2026-10-01 — ab48_e5: two empty libraries, key JkQdnUeM killed

Samples B11 and F07 (`3229_23…`, `3229_67…`) are staged as 20-byte gzips. fastp failed on
both, and the run continued with the errors ignored. The run was killed. The driver now
skips any sample whose reads are under 100 bytes, which leaves Enrichment5 at 94 samples and
the full study at 198.

## 2026-10-01 — ab48_e5: every check but three steps, key Y5QSU6Hf

306 tasks succeeded and 3 failed. Every check over the steps that ran passed. Each failure
was a path cyano_r1's one complete label never exercised.

- `diversity_analysis` and `diversity_mito` passed the next label as the secondary column.
  Some samples have no value for it, and the faceted plot has no colour for a blank. Each
  run now leaves out the samples its label leaves empty, and takes a secondary label only
  when it is complete.
- `master_summary` names a table's columns by its path below a directory named for its
  source. The port passed the clustermap product's own path, so every label's
  `clustermap_ASV_ID_plot.tsv` produced the same column names. The clustermaps are now staged
  under `clustermaps/`.

Both fixes were proven on this run's own tables in local docker before the rerun.

The July contig map named 7 ASVs that hit a binned contig at 100% but did not pair. None of
those hits lies inside a 16S gene: ASV366, for one, lands at 4.07 Mb on bin 1-15's contig,
whose 16S genes sit at 2.08 Mb and 4.71 Mb. Several are the wrong length for the V4-V5
product. They are off-target amplicons of genomic DNA, and the linker rightly pairs only
through 16S genes. The checker now counts a July hit only inside a catalogued 16S locus. On
Enrichment5, 14 July ASVs hit the assembly outside every 16S locus.

## 2026-10-01 — ab48_e5: every check passes, key 0yme26PJ

The rerun carries both fixes and vsearch 2.32.0 (see `../cyano_r1/JOURNAL.md`). 309 tasks
succeeded and none failed. The results are pinned at `data/aspire/ab48_e5_r1.dvc`.

- 88 of the 94 samples pass the 5000-read floor. 272 ASVs pass table filtering, and 29
  survive curation. Under vsearch 2.28.1 the first pass kept 334 filtered ASVs.
- The dominant cyanobacterial ASV holds 17% of the reads. It pairs only with bin 1-15, the
  *Sodalinema* MAG, at 100% identity over the whole ASV.
- 87 ASVs have a candidate MAG. A blastn run outside the pipeline agrees with every pairing.
- All 25 July ASVs that lie inside a binned 16S gene pair with that bin here.
- All 10 most abundant ASVs match a July ASV by sequence. Four of the five dominant genera
  agree: *Halomonas*, *Geitlerinema*, an uncultured genus and *Porphyrobacter*.
- SpiecEasi keeps 28 nodes, 10 with an edge, in 5 modules of more than one node.
- The MAG network accepts 18 ASV-MAG mappings. 24 module ASVs carry a MAG pair, and so do 24
  ASVs in the master summary.

## 2026-10-02 — ab48: indicspecies outruns its task, key MVVDNwB8 killed

The full study reached the analyses with 613 tasks succeeded and none failed. Then
`indicspecies` ran for 2.5 hours on one label. `multipatt` with `duleg=FALSE` tests every
combination of a label's levels, and `Condition` has 15 levels held by three or more
samples: 32,767 combinations. Enrichment5's 13-level `Condition` (8,191 combinations, 29 ASVs,
88 samples) took 28 minutes. Scaled to 123 ASVs and 189 samples, this one needed about 15
hours, past the 12-hour task limit. The run was killed.

A label with more than 8 levels is now tested in combinations of at most 3 groups. Smaller
labels keep upstream's exhaustive test, so cyano_r1 and purify are unchanged. Enrichment5's
`Condition` is not, so Enrichment5 reruns with the full study.

## 2026-10-02 — ab48_e5: re-pinned under the cap, key 0yme26PJ

The rerun recomputed all 309 tasks and none failed. `indicspecies` took 2 m 47 s where the
exhaustive test took 28 minutes, and every check in `check_results.py` passes with the
numbers of the entry above. The pin at `data/aspire/ab48_e5_r1.dvc` now holds these results.

## 2026-10-02 — ab48: graph_network fails on a mixed label, key MVVDNwB8

Under the cap, `indicspecies` took 13 m 40 s on the full study, and 618 tasks succeeded.
`graph_network` failed twice on the same error. Upstream's `natural_sort_key` returns an int
for a digit run and a string otherwise, so it compares an int with a string when one level
of a label starts with a number and another with a word. Enrichment5's labels never mix the
two. The port's copy now tags each part, so numbers sort before words. Any label the old key
could sort keeps its order, so the Enrichment5 pin stays valid.

The fix was proven on this run's own tables in local docker: `graph_network` drew every
label's overlay, and `module_mag_anchors` and `master_summary` ran after it. 29 module ASVs
carry a MAG pair, and 30 of the summary's 123 ASVs are paired.

## 2026-10-02 — ab48: every check passes, key ccynVmee

The full study carries both fixes. 621 tasks succeeded and none failed. The results are
pinned at `data/aspire/ab48_r1.dvc`, and `check_results.py --name ab48_r1` checks them
against all eight July cohorts.

- 189 of the 198 samples pass the 5000-read floor. 1274 ASVs pass table filtering, and 123
  survive curation.
- The dominant cyanobacterial ASV holds 24% of the reads. It pairs only with bin 1-15 at
  100% identity over the whole ASV.
- 133 ASVs have a candidate MAG: 113 pair uniquely and 41 ambiguously. A blastn run outside
  the pipeline agrees with every pairing.
- All 25 July ASVs that lie inside a binned 16S gene pair with that bin here. The count
  equals Enrichment5's because every July ASV that hits the assembly at 100% over its whole
  length occurs in Enrichment5. The other seven cohorts add none.
- All 10 most abundant ASVs match a July ASV by sequence. Four of the five dominant genera
  agree with the July lane's.
- SpiecEasi keeps 104 nodes, 50 with an edge, in 13 modules of more than one node. The
  largest holds 7.
- The MAG network accepts 18 ASV-MAG mappings. 29 module ASVs carry a MAG pair, and 30 ASVs
  in the master summary.

## 2026-10-02 — indicspecies: single groups past 8 levels

The combination cap still grows as k^3. A label with more than 8 levels now gets only the
single-group test, `duleg=TRUE`, which is linear in its levels and which upstream already
runs beside the combination test. On the full study the 8-level `cohort` label took 103 s
for the combination test and 17 s for the single-group test. The pins above came from the
cap. The analyses rerun under the new rule.

## 2026-10-03 — lab: one ASV table over every V4–V5 sample, key eKhZC4ql

The `lab` study joins the eight AB48 cohorts, the 2025 purify runs and the Nostoc and
Anabaena cultures. The 21 Nostoc libraries of 2024-07-04 came from Globus. Each library was
sequenced twice, and the two runs are pooled per library. 720 tasks succeeded, and
`check_results.py --name lab_r1` passes. The results are pinned at `data/aspire/lab_r1.dvc`.

- 220 of 231 samples pass the read floor. 1775 ASVs pass table filtering, and 240 survive
  curation.
- Over the 189 AB48 samples, all 10 most abundant ASVs match a July ASV. Lab-wide, Nostoc and
  Arthrospira ASVs from the cultures the July lane never saw fill half the top 10.
- 144 ASVs have a candidate MAG, and blastn agrees with every one. SpiecEasi keeps 194 nodes
  in 32 modules of more than one node.
- Round, Glycerol and DMSO hold one level among the samples that pass curation.
  indicspecies records them as skipped.
- clustermaps ran out of 16 GB at this size and gets 64 GB.

The first attempt held 238 samples. The 2026 purify run's ASVs were 253 bp against 372 for
every other run, and 81% of its reads fell in ASVs shorter than 300 bp. Its primers were
515F/806R, a V4 product. It is now the `purify_v4` study, trimmed to V4 by SINA, pinned at
`data/aspire/purify_v4_r1.dvc` (key zEBjRQO0, 7 samples, 19 curated ASVs). Arthrospira
holds 61% of its reads.

The reactor log now runs to 2026-10-01: 696,528 rows, rebuilt with `merge_reactor_logs.py`
from the exported ReactorLogs folder. The rebuild reproduces the earlier merge row for row.

The timeline page, built by `report_data.py`, is a private claude.ai artifact.

## 2026-10-06 — lab2 and lab_v4: the 2026 runs join, key BefOu3ah and XdxlvU1T

Two Biofactorial runs arrived from the Cyanogroup2 SharePoint: July 2026, six Purify Spirulina
libraries and a blank, and September 2026, one Spirulina library and seven of Patrik's
cultures with an elution-buffer blank. Both carry 515F/926R primers. Their libraries have
names but no sampling dates, and the sheets mark their source as not recorded.

- `lab_v4` (XdxlvU1T): 757 tasks succeeded. 240 of 250 samples pass the read floor, and 284
  ASVs survive curation. The March 2026 V4 samples share 87–100% of their reads with ASVs that
  other purify samples hold. Arthrospira ASV3 dominates both. The cut keeps more reads than
  `lab_r1` did in every run, because shorter reads pass the expected-error filter.
- `lab2` (BefOu3ah): 756 tasks succeeded and `check_results.py --name lab_r2` passes. 232 of 243
  samples pass the floor, and 300 ASVs survive curation.
- In `lab2` the new runs hold 30 ASVs above 1% that occur in no earlier run, each one base from
  another ASV. The mismatch sits 13 or about 40 nt from the 3' end, nearly always G to A, and
  the twin-to-original ratio is fixed per run (2.1–2.2 across the July run). It is a read-2
  miscall, not a strain. The page takes its V4–V5 figures from `lab_r1` for that reason.
- The page now draws from `lab_v4_r1`. It marks each purify sample as a UBC lab culture, a PBR
  sample or unrecorded, and splits each sample's reads by whether the other source holds the
  ASV. Across the V4 table, 80% of purify reads sit in ASVs both sources hold, against 11 of
  42 ASVs by count.
