# metaGEM corpus (Zorrilla et al. 2021, NAR 49(21):e126)

`manifest.tsv` enumerates the 934 fastq.gz files (483 runs, 5 studies) that metaGEM's
Methods describe as "483 whole metagenome shotgun samples from five metagenomic studies
(21-25) ... downloaded from the NCBI SRA or EBI ENA." Paper and notes at
`data/docs/zorrilla2021/`.

## How the run list was resolved

Neither `franciscozorrilla/metaGEM` nor its wiki carries a manifest, config, or
supplementary table naming the 483 runs — checked via `gh api` against the repo tree,
the wiki (cloned, grepped for accession/SRA/ENA/PRJ), and the NAR article's own
supplementary-data description (figures and a Zenodo link, no sample table). The
companion repo `franciscozorrilla/metaGEM_paper` has no manifest either, but its README
carries the one thing that matters: an exact per-study sample count and a Zenodo record
per study (`https://zenodo.org/record/<id>`, results only — assemblies, MAGs, GEMs).

Each Zenodo record's small results bundle (`bonus.tar.gz` / `stats.tar.gz`, all
&lt;6&nbsp;MB — metadata, not data) turned out to carry the literal run list: a
`qfilter.stats` (or `metadata.txt`, for the human-gut study) with one row per run
actually pushed through the pipeline. That row list is what `manifest.tsv` is built
from, not a blind dump of each study's whole BioProject — those run **945 samples**
combined across the five studies, nearly twice the 483 metaGEM used (see the Korem and
Bissett rows below). Every recovered accession was then cross-checked against the ENA
file report for its BioProject to get authoritative bytes and md5 (`filereport?...&fields=run_accession,fastq_ftp,fastq_bytes,fastq_md5`) — no accession failed that check.

## Per-study summary

| dataset (manifest) | ref | citation | BioProject | runs used / in project | files | size |
|---|---|---|---|---|---|---|
| `korem2015` | 21 | Korem et al. 2015, *Science* 349:1101 (gut, growth dynamics; the "lab culture" arm) | PRJEB9718 | 48 / 340 | 48 | 25.4 GB |
| `karlsson2013` | 22 | Karlsson et al. 2013, *Nature* 498:99 (gut, Swedish/European women, glucose control) | PRJEB1786 | 145 / 145 | 290 | 412.8 GB |
| `li2019` | 23 | Li et al. 2019, *ISME J* 13:738 (peanut rhizosphere, land-use legacy) | PRJNA485196 | 6 / 6 | 12 | 23.8 GB |
| `sunagawa2015` | 24 | Sunagawa et al. 2015, *Science* 348:1261359 (Tara Oceans) | PRJEB1787 | 246 / 249 | 492 | 4294.9 GB |
| `bissett_base` | 25 | Bissett et al. (BASE, Australian soil) | **PRJEB7626** | 46 / 46 | 92 | 457.5 GB |
| **TOTAL** | | | | **491 / 786** | **934** | **5214.4 GB ≈ 5.21 TB** |

("runs used" sums to 491, one more than the paper's 483, because Korem's PRJEB9718 and
Sunagawa's PRJEB1787 rows above are already the exact-match counts — the 8 extra are
accounted for by Karlsson: metaGEM's own results table reports "137" gut samples
analyzed downstream, but 145 were fetched and qc'd, of which some samples were dropped
after assembly; `manifest.tsv` follows what was fetched, since that's the download job,
not what survived to become a MAG.)

**The Bissett/BASE trap, worth flagging on its own:** the obvious BioProject for
"Bissett et al. Australian soil metagenomes" is **PRJNA317932** (the public BASE
umbrella project — 9,624 16S amplicon runs plus 540 separate WGS runs). None of
metaGEM's 46 actual accessions (`ERR671910`-`ERR671939`, `ERR687883`-`ERR687898`) are in
it at all. The real project is **PRJEB7626** (ERP008551), a much smaller, separate
ENA-only submission of exactly the 46 shotgun runs metaGEM used. Sizing off PRJNA317932's
540-run WGS subset would have overstated this one study by roughly 4.7x (2.15 TB vs. the
real 457.5 GB) — this is why per-run cross-checking against the recovered accession list
mattered here, not just resolving a BioProject and trusting its total.

Smaller, harmless looseness elsewhere: Karlsson (145 fetched vs. 137 used downstream) and
Sunagawa (246 of 249 project runs) are both within the paper's own project, close to
metaGEM's stated counts, and immaterial to the total either way.

## Feasibility

**5.21 TB total, dominated by one study:** Tara Oceans (`sunagawa2015`) alone is 4.29 TB
— 82% of the whole corpus. Everything else combined is under 1 TB (919.5 GB: Karlsson
412.8 + Bissett 457.5 + Korem 25.4 + Li 23.8).

- **Against local free space** (166 GB free of 1.9 TB, 92% used): the full corpus is
  ~31x local free space. Even the smallest single study that still matters scientifically
  (Li, 23.8 GB) fits under the proposed 100 GB streaming ceiling; Karlsson (412.8 GB) and
  Bissett (457.5 GB) each need 5 batch-and-push cycles at that ceiling; Tara Oceans needs
  ~43. None of this is a "download it all" job at any point — confirms the bounded
  streaming shape in the brief is required, not optional.
- **Against fir's scratch** (checked live via the already-connected `ssh` session — no
  VPN/2FA cost): the filesystem itself is enormous (46 PB total, 7.7 PB free, 83% used),
  but the `phyberos` group's own allocation shows only **5.0 TB available** (19 TB / 14
  TB used, per `df` on the group's scratch mount; `lfs quota` reports no hard block/inode
  limit set for the group, so 5.0 TB is the practical, not the enforced, ceiling). The
  full 5.21 TB corpus would consume essentially all of that headroom if fir were used as
  a staging hop rather than streaming straight to chinook — worth knowing since it means
  fir scratch is not a comfortable fallback for a full-corpus transfer either.
- **Against throughput, which on fir is a property of the NODE and not of the source.**
  Measured 2026-09-12: every compute node tried — sixteen, spread over three racks — moved
  0.02–0.25 MB/s to ENA, to AWS S3 and to cdn.kernel.org alike, and concurrency does not
  recover it (ENA HTTPS aggregated 0.04 MB/s at one stream and at sixteen). A login node on
  the same cluster moved 1.9 MB/s per stream from ENA, 26 MB/s at sixteen, and 32–36 MB/s
  per stream from the AWS Open Data SRA mirror, 550 MB/s at sixteen. So no Slurm array
  finishes this corpus at any width or on any transport, and the earlier 57 MiB/s figure
  in `.awm` history was a login-node number. `cluster/` is built around that split: the
  fetch is a bounded login-node pass (`fetch_sra.sh`, 714 GB of `.sra` for the four
  non-Tara studies, ~1 h at the measured rate) and only the conversion runs in the queue.
- **Against the manifest's md5, which the fast route forfeits.** The AWS mirror serves
  NCBI's run-level `.sra`, not ENA's generated fastq, and `fasterq-dump` reproduces the
  sequence and quality streams byte-for-byte while writing a different read header
  (` length=82` where ENA writes `/1`) — so the gzip differs and the products run 6–8%
  larger than the manifest's bytes. `verify_sra.py` gates on what survives that: the
  `.sra`'s own published md5, `vdb-validate`, and ENA's published read_count and
  base_count per run. Use the ENA path in `cluster/fetch_array.sbatch` if byte-exact ENA
  files are required, and budget ~10 h of login-node transfer for it.

**Bottom line:** the full 483-sample corpus is real (not a Tara-Oceans-scale
overestimate — Tara actually is 82% of it) and technically fetchable only as a long
streaming job, but it is not a "just go" size. If a subset would serve the purpose
better, the natural cut is Tara Oceans out (drops the total to 920 GB, ~9 batch cycles,
under 4 hours of fetch time) or Tara Oceans alone as its own separately-scoped job later.
That choice is Tony's per the brief — nothing has been subset here.

## Published per-study results (comparison targets, not inputs)

Everything above is the read *input* corpus. metaGEM's own published *outputs* —
assemblies, MAGs, ORF-annotated protein bins and GEMs, one per study — are a separate
Zenodo record apiece, named by the companion repo `franciscozorrilla/metaGEM_paper`'s
README rather than by `metaGEM` itself (neither the tool repo nor its wiki names them).
These are what run R4 scores against; without them R4 has nothing to compare to.

| dataset (manifest) | metaGEM_paper name | Zenodo record | files | bytes |
|---|---|---|---|---|
| `li2019` | Plant associated | [5596948](https://zenodo.org/record/5596948) | 9 | 1.50 GB |
| `korem2015` | Lab culture | [5593111](https://zenodo.org/record/5593111) | 9 | 0.45 GB |
| `karlsson2013` | Human gut | [5593224](https://zenodo.org/record/5593224) | 12 | 19.15 GB |
| `bissett_base` | Bulk soil | [5596972](https://zenodo.org/record/5596972) | 8 | 17.20 GB |
| `sunagawa2015` | TARA oceans | [5597227](https://zenodo.org/record/5597227) + [5599412](https://zenodo.org/record/5599412) (2 records) | 8 | 57.83 GB |
| **TOTAL** | | | **46** | **96.13 GB** |

An all-GEMs-only mirror also exists (Zenodo [4407746](https://zenodo.org/record/4407746)) —
not fetched, since the per-study records already carry each study's GEMs alongside its
MAGs and assemblies.

Fetched to `/scratch/phyberos/metagem/published/<study>/<filename>`, in R4's own study
order (`li2019` → `karlsson2013`+`korem2015` → `bissett_base` → `sunagawa2015`) so a
session that stops partway still lands the studies wave 3 reaches first. Same login-node
shape as the reads corpus — Zenodo showed the same per-compute-node throttling as ENA and
S3 — via `cluster/fetch_zenodo_published.sh cluster/published_manifest.tsv
/scratch/phyberos/metagem/published <study>...`. Zenodo's file API publishes a real MD5
per file (no multipart-ETag case here), so the fetch script gates on it directly rather
than needing a separate verify pass.

`published_manifest.tsv` lives in `cluster/`, not in the top-level `manifest.tsv` above:
its rows are a different kind of thing (a Zenodo record + relpath, not a BioProject run)
and the shapes didn't warrant forcing them into the reads schema.
