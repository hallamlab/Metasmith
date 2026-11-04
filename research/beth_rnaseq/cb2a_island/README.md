# CB2A island: protein families and origin

Answers Beth's question about the grey genes in the CB2A island and top-30 figures (`../cb2a_figures.py`), and where the island came from. The inputs, outputs and delivered zip are pinned at `data/beth_cb2a_island`.

## Run order

All scripts take their working folder as the current directory unless they take it as an argument.

1. `targets.py` picks the 83 proteins: the island RS07640–RS07925 and the top 30 by TPM at 10 ppm.
2. `pfam.py` runs EBI HMMER hmmscan against Pfam.
3. `afdb.py` maps each RefSeq WP accession through UniParc to an AlphaFold DB model.
4. `esmfold_sockeye.sbatch` folds the proteins that have no AlphaFold DB model. It runs on a Sockeye GPU, under `/scratch/st-shallam-1/txyliu/cb2a_island`.
5. `foldseek.py` searches each structure on the Foldseek web server. It resumes and skips finished files.
6. `consolidate.py` joins all of the above into `annotations.tsv`.
7. `short_labels.tsv` is written by hand from `annotations.tsv`. Its `refines` column marks the labels that add to the RefSeq product name.
8. `../cb2a_figures.py --labels short_labels.tsv` and `element_figure.py` draw the figures.

The origin analysis uses `uniparc_orgs.py` for organisms that carry an identical sequence, `gc.py` for GC content, and `cov.py` for merged-interval minimap2 coverage.

## Traps

- Sum minimap2 blocks only after merging them. Overlapping blocks over-count coverage.
- ESMFold writes pLDDT to the B-factor on a 0–1 scale. AlphaFold DB uses 0–100.
- Foldseek's top PDB hit can carry a misleading name, such as a microtubule complex for the CopC-like RS07740. Check the SwissProt and AFDB50 hits before you write a label.
