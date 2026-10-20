# OMD MetaPathways 4 rebuild on fir

## Purpose & Contents

This directory holds the fir scripts that regenerate MetaPathways and Pathway Tools output for the 34,815 OMD genomes in the EngCyc manifest. It records the host facts that the scripts depend on and that the scripts cannot show. Results per run go to the journal, not here.

## Layout on fir

- `/scratch/phyberos/mp4/metapathways_4.0.1.sif` is `quay.io/hallamlab/metapathways:4.0.1`.
- `/scratch/phyberos/mpdb/<db>/` holds one MPDB per functional reference.
- `/scratch/phyberos/mp4/private/` is mode 700 and holds the licensed Pathway Tools installer.
- `/scratch/phyberos/bench/e4_mm/ptools/pathologic.sif` is the Pathway Tools 27.0 image that builds the PGDBs.

**WARNING** The Pathway Tools installer and every image built from it are licensed. Keep them in private paths. Never push or share them.

## Build an MPDB

1. Run `fetch_mpdb.sh` on a login node. Compute nodes have no internet.
2. Submit `build_mpdb.sbatch`. MetaPathways adopts the downloaded files and only indexes.

**CAUTION** `ftp.ebi.ac.uk` serves fir at about 1 MB/s. `ftp.uniprot.org` serves it at about 48 MB/s, so the fetch script uses it.

## PathoLogic route

MetaPathways 4 annotates each genome (`metapathways run`), and `pathologic.sif` turns its `ptools/0.pf` into a PGDB. `t2_one_genome.sbatch` is the reference implementation.

**CAUTION** The image's wrapper moves `<NAME>cyc` in upper case, but PathoLogic writes the PGDB in lower case. Collect the PGDB from the `pgdbs` bind (`pgdbs/user/<name>cyc/1.0`), not from `-o`.

The 27.0 image ignores the `TAXONOMIC-ANNOT` slot that MetaPathways 4 writes, so this route runs without MetaPathways 4's taxonomic pruning.

MetaPathways 4's own route (`build_pt`, then `metapathways ptools`) does not work with the 27.0 installer. On fir the nested fakeroot build and the install succeed. Then `conf-ptools` waits on stdin for an answer the build does not supply. Ryan validated `build_pt` with 29.5, and the lab has no 29.5 installer.
