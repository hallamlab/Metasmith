#!/usr/bin/env python3
import sys

import _authoring as A
import engcyc_metagenomics as M
from metasmith.python_api import Template

NAME = "engcyc_metagenomics_lite"
DESCRIPTION = """
engcyc_metagenomics with the lite lanes: the same three cases (short reads, hybrid,
long reads) and the references case, about 220 GB of reference downloads in place of
full's 1,070 GB. Funnel each sample into the case for its reads, or give an SRA run
accession. Every lane is its own target; deselect a target to drop its lane, and drop
its database from the references case too. engcyc_metagenomics' description gives
each lane's size.

Assembly: as full. Binning: MetaBAT2, SemiBin2, MetaDecoder -> DAS Tool -> CheckM2
-> quality MAGs.
Taxonomy: GTDB-Tk r232 on the quality MAGs; rRNA scan against GTDB r232 SSU.
Read taxonomy: MetaPhlAn, sylph and Kraken2 with Bracken, each in its long-read mode
on long reads; inStrain on short and hybrid.
Gene function: DIAMOND UniRef50 and KOfam; CLEAN, ProteinBERT and BiGG lanes; the
4-lane GPR table.
Genome features: Bakta non-coding, ARAGORN tRNA, mobileOG.
Resistome: RGI/CARD, BacMet, VFDB. RefSeq viral lane.
GEM per quality MAG: CarveMe from the BiGG lane, then MEMOTE. CarveMe needs a growth
medium: give modelling::media and modelling::medium_name.
Viral: geNomad, VirSorter2, VIBRANT -> CheckV -> vOTUs.
"""


def build_template(rebuild: bool = False) -> Template:
    return M.template_for(NAME, M.DEFAULT_SAMPLES, rebuild=rebuild, description=DESCRIPTION,
                          case_targets=M.LITE_CASE_TARGETS, references=M.LITE_REFERENCES,
                          binners=M.LITE_BINNERS)


if __name__ == "__main__":
    A.cli(sys.modules[__name__])
