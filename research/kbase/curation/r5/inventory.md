# The container inventory, and what could be merged

Rebuild with `python inventory.py`. Sizes come from the registry MANIFEST -- the sum
of the compressed layer sizes -- and not from `docker images`, which reports the
uncompressed on-disk size and runs two to three times larger. Comparing one against
the other makes a merge look cheaper or dearer than it is.

**90 environments. 80 resolve to an image an anonymous pull can see,
totalling 80 GB compressed.**

## What the sweep found before any merge is considered

- **One environment declares no container at all: `cobra.env`.** That is this round's
  doing and is deliberate -- `docker/cobra/` exists and is built locally, and the
  `container:` line goes in after the push. Before this round it had no image at all,
  anywhere, and the four modelling transforms could not run under DOCKER or APPTAINER.
- **Nine environments name an image an anonymous pull cannot see.** All nine are lab-
  private (`quay.io/hallamlab/external_*`, `ecspr_bake`, `metabolomics-python`, and one
  `quay.io/txyliu/pathologic`). A host with quay credentials pulls them; a fresh host,
  or anyone outside the lab, gets a 401 at stage time with no earlier warning.
- **28 environments carry no `conda:` arm**, so no `ifVirtualEnvDo` is possible for them
  and "can this run without a container?" is answerable as no.
- **Four environments are required by no transform**: `dgbyg.env`, `equilibrator.env`,
  `rdkit.env` and `vsearch.env`. The first three are ecspr_bake tags.

## The two constraints that decide every merge

**numpy.** cobra's optlang stack and scikit-sparse/CHOLMOD need numpy<2; polars needs
numpy>=2. That single line is why `cobra.env` cannot fold into
`python_for_data_science.env`, why `docker/fabfos` carries two conda envs in one image,
and why `docker/ecspr_bake` splits `:aam` from `:direction`.

**Reference binds.** An environment whose tool takes a large staged database -- bakta,
gtdbtk, kraken2, interproscan, eggnog -- passes `binds=`, and a chain that passes binds
cannot have a mamba arm at all. Merging two such images saves the base layer and
nothing else, because the reference is the bulk and it is not in the image.

**And the one this round learned the hard way: `provides` is a matched property set,
not documentation.** Listing a tool an image genuinely contains makes that env a
property-superset of any env offering a subset, and every requirement for the smaller
one becomes satisfiable by the larger. Adding `py/polars` to
`python_for_data_science.env` -- true, polars is in the 1.4.0 image -- moved five of the
eleven shipped template plans. A merge that widens a `provides` list is a solver change,
not a packaging change.

## What the KBase parity set alone costs

The twenty-one transforms under `transforms/kbase/` reach seventeen environments, and
they come to **3.85 GB compressed** -- under 5% of the library's total. Twelve of the
seventeen are under 300 MB. `cobra:local` adds 506 MB when it is pushed.

| env | compressed |
|---|---:|
| `python_for_data_science.env` | 910 MB |
| `nanoplot.env` | 747 MB |
| `bakta.env` | 438 MB |
| `polars.env` | 360 MB |
| `fastqc.env` | 337 MB |
| `blast.env` | 255 MB |
| `gtdbtk.env` | 170 MB |
| `samtools.env` | 160 MB |
| `bowtie2.env` | 141 MB |
| `bcftools.env` | 93 MB |
| `stringtie.env` | 87 MB |
| `seqkit` 40, `fastp` 35, `bwa` 30, `mafft` 28, `polypolish` 17 | 150 MB |
| `cobra.env` | *unpushed; 506 MB locally* |

The two large ones are not the tools. `python_for_data_science` is a scientific python
stack four transforms share, and `nanoplot` is a plotting stack for one. Neither is a
merge candidate: the first is already shared, and the second has no sibling.

## Where the mass is

| env | compressed | image |
|---|---:|---|
| `clean.env` | 10,203 MB | `quay.io/hallamlab/external_clean:2026.06.14` |
| `antismash.env` | 4,896 MB | `antismash/standalone:7.1.0` |
| `ecspr.env` | 4,433 MB | `quay.io/hallamlab/ecspr:2026.07.14` |
| `esmc.env` | 4,170 MB | `quay.io/hallamlab/external_esmc:2026.05.19` |
| `ezpred.env` | 4,170 MB | `quay.io/hallamlab/external_esmc:2026.05.19` |
| `checkm.env` | 4,098 MB | `quay.io/hallamlab/external_checkm2:1.1.0` |
| `ankh.env` | 4,052 MB | `quay.io/hallamlab/external_ankh:2026.05.19` |
| `esmfold.env` | 4,047 MB | `quay.io/hallamlab/external_esmfold:2026.05.19` |
| `prott5.env` | 4,041 MB | `quay.io/hallamlab/external_prott5:2026.05.19` |
| `saprot.env` | 4,041 MB | `quay.io/hallamlab/external_saprot:2026.05.19` |
| `comebin.env` | 3,910 MB | `quay.io/hallamlab/external_comebin:gpu-1.0.4` |
| `qiime2.env` | 3,809 MB | `quay.io/qiime2/metagenome:2024.10` |
| `braker3.env` | 3,154 MB | `teambraker/braker3:v3.0.7.4` |
| `promotech.env` | 2,266 MB | `quay.io/hallamlab/external_promotech:1.0` |
| `metaphlan.env` | 1,364 MB | `quay.io/biocontainers/metaphlan:4.2.4--pyhdfd78af_0` |

The top fifteen are 62.7 GB of the 80.0 GB -- 78% of the whole -- and thirteen of them
are a deep-learning weights image or a bespoke external tool: one env, one image,
nothing to share.

## Images already shared by more than one env

- `quay.io/hallamlab/external_esmc:2026.05.19` -> `esmc.env`, `ezpred.env`

`esmc.env` and `ezpred.env` are the pattern that works: two envs, one image, distinct
`provides` lists. Nothing is duplicated on disk and neither env claims the other's tools.

## The merge candidates, and the verdict on each

| candidate | compressed now | verdict |
|---|---:|---|
| the small biocontainers (`samtools`, `bcftools`, `seqkit`, `bwa`, `mafft`, `bowtie2`, `stringtie`, `blast`, `fastp`, `polypolish`) | 886 MB across ten | **No.** A mulled biocontainer pins a hash nobody can audit, and the whole set is 1.1% of the total. The saving is a rounding error against the cost of an unauditable pin. |
| `cobra.env` into `python_for_data_science.env` | 506 MB local + 910 MB | **No.** numpy<2 against numpy>=2. |
| `polars.env` into `python_for_data_science.env` | 360 MB + 910 MB | **No**, and this is the one that looks most tempting: polars IS already in the 1.4.0 image. Doing it means widening `provides`, which moves plans. |
| the six ESM/ProtT5/Ankh/SaProt/ESMFold/CLEAN weights images | 30.6 GB | **Not here.** They are reference data wearing a container, the same bucket as uniref and the kofam profiles, and each is one model's weights. |
| `dgbyg.env`, `equilibrator.env`, `rdkit.env` | 3 tags of `ecspr_bake` | Already one repository, three tags. They are also required by no transform -- the question for them is deletion, not merging. |

## The full table

| env | compressed | image | conda | required by |
|---|---:|---|---|---:|
| `amrfinderplus.env` | 313 MB | `quay.io/biocontainers/ncbi-amrfinderplus:4.2.7--hf69ffd2_0` | — | 2 |
| `ankh.env` | 4,052 MB | `quay.io/hallamlab/external_ankh:2026.05.19` | — | 1 |
| `antismash.env` | 4,896 MB | `antismash/standalone:7.1.0` | `antismash` | 2 |
| `bakta.env` | 438 MB | `quay.io/biocontainers/bakta:1.11.0--pyhdfd78af_0` | `bakta` | 3 |
| `bam2fastx.env` | 18 MB | `quay.io/biocontainers/pbtk:3.1.1--h9ee0642_0` | `bam2fastx` | 1 |
| `bbtools.env` | 141 MB | `staphb/bbtools:39.49` | `bbtools` | 12 |
| `bcftools.env` | 93 MB | `quay.io/biocontainers/bcftools:1.24--h118bc1c_2` | `bcftools` | 1 |
| `bedtools.env` | 53 MB | `biocontainers/bedtools:v2.27.1dfsg-4-deb_cv1` | `bedtools` | 1 |
| `blast.env` | 255 MB | `quay.io/biocontainers/blast:2.16.0--hc155240_2` | `blast` | 4 |
| `bowtie2.env` | 141 MB | `quay.io/biocontainers/bowtie2:2.5.5--ha27dd3b_0` | `bowtie2` | 1 |
| `bracken.env` | 307 MB | `quay.io/biocontainers/bracken:3.0--h9948957_2` | `bracken` | 1 |
| `braker3.env` | 3,154 MB | `teambraker/braker3:v3.0.7.4` | — | 1 |
| `busco.env` | 1,114 MB | `quay.io/biocontainers/busco:5.8.3--pyhdfd78af_0` | `busco` | 3 |
| `bwa.env` | 30 MB | `staphb/bwa:0.7.19` | `bwa` | 1 |
| `centrifuger.env` | 133 MB | `quay.io/biocontainers/centrifuger:1.1.1--h3be2455_0` | `centrifuger` | 1 |
| `checkm.env` | 4,098 MB | `quay.io/hallamlab/external_checkm2:1.1.0` | `checkm` | 2 |
| `clean.env` | 10,203 MB | `quay.io/hallamlab/external_clean:2026.06.14` | — | 1 |
| `cobra.env` | *none* | *no container declared* | `build-refs-cobra` | 4 |
| `comebin.env` | 3,910 MB | `quay.io/hallamlab/external_comebin:gpu-1.0.4` | `comebin` | 1 |
| `deepec.env` | 615 MB | `quay.io/hallamlab/external_deepec:0.4.1` | — | 1 |
| `deeptfactor.env` | 683 MB | `quay.io/hallamlab/external_deeptfactor:1.0` | — | 1 |
| `dgbyg.env` | — | `quay.io/hallamlab/ecspr_bake:dgbyg` | `build-refs-dgbyg` | 0 |
| `diamond.env` | 225 MB | `quay.io/biocontainers/diamond:2.1.8--h43eeafb_0` | `diamond` | 17 |
| `dram.env` | 318 MB | `quay.io/biocontainers/dram:1.5.0--pyhdfd78af_0` | `dram` | 3 |
| `ecspr.env` | 4,433 MB | `quay.io/hallamlab/ecspr:2026.07.14` | `ecspr` | 1 |
| `eggnog-mapper.env` | 407 MB | `quay.io/biocontainers/eggnog-mapper:2.1.12--pyhdfd78af_0` | `eggnog-mapper` | 2 |
| `equilibrator.env` | — | `quay.io/hallamlab/ecspr_bake:direction` | `build-refs-equilibrator` | 0 |
| `esmc.env` | 4,170 MB | `quay.io/hallamlab/external_esmc:2026.05.19` | — | 1 |
| `esmfold.env` | 4,047 MB | `quay.io/hallamlab/external_esmfold:2026.05.19` | — | 1 |
| `ezpred.env` | 4,170 MB | `quay.io/hallamlab/external_esmc:2026.05.19` | — | 1 |
| `fastani.env` | 30 MB | `staphb/fastani:1.34` | `fastani` | 1 |
| `fastp.env` | 35 MB | `staphb/fastp:1.0.1` | `fastp` | 1 |
| `fastqc.env` | 337 MB | `biocontainers/fastqc:v0.11.9_cv8` | `fastqc` | 1 |
| `feast.env` | — | `quay.io/hallamlab/external_feast:1.0` | — | 1 |
| `filtlong.env` | 37 MB | `staphb/filtlong:0.3.1` | `filtlong` | 2 |
| `flye.env` | 80 MB | `staphb/flye:2.9.6` | `flye` | 2 |
| `foldseek.env` | — | `quay.io/hallamlab/external_foldseek:2026.05.18` | — | 1 |
| `ganon.env` | 205 MB | `quay.io/biocontainers/ganon:2.4.1--py312h88d62d1_0` | `ganon` | 2 |
| `genomad.env` | 980 MB | `antoniopcamargo/genomad:1.11.0` | `genomad` | 2 |
| `gfatools.env` | 153 MB | `dnalinux/gfatools:final-gt` | — | 2 |
| `gffread.env` | 12 MB | `quay.io/biocontainers/gffread:0.12.7--hdcf5f25_4` | `gffread` | 1 |
| `gtdbtk.env` | 170 MB | `quay.io/biocontainers/gtdbtk:2.6.1--pyh1f0d9b5_2` | `gtdbtk` | 3 |
| `hifiasm-meta.env` | 14 MB | `quay.io/biocontainers/hifiasm_meta:hamtv0.3.5--h5ca1c30_0` | `hifiasm-meta` | 1 |
| `hifiasm.env` | 18 MB | `quay.io/biocontainers/hifiasm:0.25.0--h5ca1c30_0` | `hifiasm` | 1 |
| `instrain.env` | 179 MB | `quay.io/biocontainers/instrain:1.10.0--pyhdfd78af_0` | — | 2 |
| `integronfinder.env` | 216 MB | `quay.io/biocontainers/integron_finder:2.0.6--pyhdfd78af_0` | — | 1 |
| `interproscan.env` | 327 MB | `interpro/interproscan:5.67-99.0` | — | 2 |
| `kofamscan.env` | 67 MB | `quay.io/biocontainers/kofamscan:1.3.0--hdfd78af_2` | `kofamscan` | 1 |
| `kraken2.env` | 315 MB | `quay.io/biocontainers/kraken2:2.1.6--pl5321h077b44d_0` | `kraken2` | 1 |
| `mafft.env` | 28 MB | `quay.io/biocontainers/mafft:7.525--h031d066_1` | `mafft` | 1 |
| `megahit.env` | 295 MB | `biocontainers/megahit:1.2.9_cv1` | `megahit` | 1 |
| `metabat2.env` | 86 MB | `quay.io/biocontainers/metabat2:2.17--h6f16272_1` | `metabat2` | 1 |
| `metabolomics-python.env` | — | `quay.io/hallamlab/metabolomics-python:0.1.0` | — | 4 |
| `metabuli.env` | 83 MB | `quay.io/biocontainers/metabuli:1.2.0--pl5321h0bb26bb_0` | `metabuli` | 2 |
| `metaphlan.env` | 1,364 MB | `quay.io/biocontainers/metaphlan:4.2.4--pyhdfd78af_0` | `metaphlan` | 3 |
| `miniasm.env` | 10 MB | `quay.io/biocontainers/miniasm:0.2_r168--ha92aebf_3` | `miniasm` | 1 |
| `minimap2.env` | 52 MB | `biocontainers/minimap2:v2.15dfsg-1-deb_cv1` | `minimap2` | 6 |
| `mobileelementfinder.env` | — | `quay.io/hallamlab/external_mobileelementfinder:1.1.2` | — | 1 |
| `nanoplot.env` | 747 MB | `nanozoo/nanoplot:1.42.0--547049c` | `nanoplot` | 1 |
| `ncbi-datasets.env` | 50 MB | `staphb/ncbi-datasets:18.9.0` | `ncbi-datasets` | 1 |
| `orad.env` | 734 MB | `quay.io/hallamlab/illumina-dragen-orad:2.7.0` | — | 1 |
| `pathofact.env` | — | `quay.io/hallamlab/external_pathofact:2.0` | — | 1 |
| `pathologic.env` | — | `quay.io/txyliu/pathologic:latest` | — | 1 |
| `phyloflash.env` | 750 MB | `quay.io/biocontainers/phyloflash:3.4.2--hdfd78af_0` | `phyloflash` | 2 |
| `polars.env` | 360 MB | `quay.io/hallamlab/polars:1.38.1` | `polars` | 7 |
| `polypolish.env` | 17 MB | `quay.io/biocontainers/polypolish:0.7.1--hec9b1f2_0` | `polypolish` | 1 |
| `ppanggolin.env` | 290 MB | `staphb/ppanggolin:2.2.5` | `ppanggolin` | 1 |
| `pprodigal.env` | 62 MB | `quay.io/biocontainers/pprodigal:1.0.1--pyhdfd78af_0` | `pprodigal` | 1 |
| `predictf.env` | 856 MB | `quay.io/hallamlab/external_predictf:1.0` | — | 2 |
| `promotech.env` | 2,266 MB | `quay.io/hallamlab/external_promotech:1.0` | — | 1 |
| `proteinbert.env` | 1,010 MB | `quay.io/hallamlab/external_proteinbert:2024.03.28` | — | 1 |
| `prott5.env` | 4,041 MB | `quay.io/hallamlab/external_prott5:2026.05.19` | — | 1 |
| `pydeseq2.env` | 308 MB | `quay.io/biocontainers/pydeseq2:0.5.4--pyhdfd78af_0` | `pydeseq2` | 2 |
| `python_for_data_science.env` | 910 MB | `quay.io/hallamlab/python_for_data_science:1.4.0` | `python_for_data_science` | 27 |
| `qiime2.env` | 3,809 MB | `quay.io/qiime2/metagenome:2024.10` | — | 1 |
| `rdkit.env` | — | `quay.io/hallamlab/ecspr_bake:aam` | `build-refs-rdkit` | 0 |
| `rgi.env` | 591 MB | `quay.io/biocontainers/rgi:6.0.8--pyh05cac1d_0` | — | 2 |
| `salmon.env` | 79 MB | `quay.io/biocontainers/salmon:1.10.3--h45fbf2d_5` | `salmon` | 2 |
| `samtools.env` | 160 MB | `staphb/samtools:1.23` | `samtools` | 8 |
| `saprot.env` | 4,041 MB | `quay.io/hallamlab/external_saprot:2026.05.19` | — | 1 |
| `semibin.env` | 772 MB | `quay.io/biocontainers/semibin:2.1.0--pyhdfd78af_0` | `semibin` | 1 |
| `seqkit.env` | 40 MB | `staphb/seqkit:2.13.0` | `seqkit` | 5 |
| `skani.env` | 30 MB | `staphb/skani:0.2.2` | `skani` | 1 |
| `spades.env` | 165 MB | `quay.io/biocontainers/spades:3.15.5--h95f258a_1` | `spades` | 1 |
| `sra-tools.env` | 75 MB | `quay.io/hallamlab/sra-tools` | — | 2 |
| `star.env` | 38 MB | `quay.io/biocontainers/star:2.7.11b--h5ca1c30_8` | `star` | 2 |
| `stringtie.env` | 87 MB | `quay.io/biocontainers/stringtie:2.2.3--h29c0135_1` | `stringtie` | 4 |
| `sylph.env` | 12 MB | `quay.io/biocontainers/sylph:0.8.0--ha6fb395_0` | `sylph` | 1 |
| `virsorter2.env` | 199 MB | `quay.io/biocontainers/virsorter:2.2.4--pyhdfd78af_2` | `virsorter2` | 2 |
| `vsearch.env` | 13 MB | `quay.io/biocontainers/vsearch:2.28.1--h6a68c12_1` | `vsearch` | 0 |

A dash in the size column is an image that resolved to a 401: private to the lab, not
missing. `provides` lists and the transforms behind each count are in `inventory.json`.
