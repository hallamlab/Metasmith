#!/usr/bin/env python3
"""Author the `pangenome_heatmap_from_assembly` template.

One NCBI assembly accession per sample -- add more rows via the GUI's sample
table to grow the pangenome:

  name --> accession --> getNcbiAssembly --> gbk
  gbk (sharing a pangenome ancestor) --> ppanggolin --> ppanggolin_matrix
  ppanggolin_matrix --> heatmap --> pangenome::heatmap

The name is what each genome is called in the matrix and on the heatmap's axes.
It is declared above the accession so everything fetched from that accession
inherits it, which is what lets ppanggolin ask a genome which name it descends
from. Scraping the name out of the GenBank header instead cannot work: no field
there is both unique and readable, and two assemblies of one species collide.

`pangenome::pangenome` has no producing transform -- it exists purely so
`ppanggolin` can require "all genomes sharing this ancestor." Modeled as one
literal value every deferred accession is minted under, and named in
`shared_input_paths` so it stays a single shared ancestor across however many
accession rows the user adds, rather than each sample getting its own.

    python main/pangenome_heatmap_from_assembly.py [--rebuild] [--dag]
"""
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "pangenome_heatmap_from_assembly"
DESCRIPTION = """
Build a pangenome from NCBI assembly accessions and render it as a heatmap.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "ncbi.yml")
        lib.AddTypeLibrary(A.TYPES / "pangenome.yml")
        lib.AddTypeLibrary(A.TYPES / "sequences.yml")
        pan = lib.AddValue("pangenome.json", {"logistics": "pangenome"},
                            "pangenome::pangenome")
        # pangenome -> name -> accession. The name is what labels the genome
        # everywhere downstream, so it is a declared input beside the accession
        # rather than something scraped back out of whatever gets fetched.
        nm = lib.AddItem(DEFERRED, "ncbi::genome_name", parents={pan})
        lib.AddItem(DEFERRED, "ncbi::assembly_accession", parents={nm})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="ncbi::assembly_accession",
        shared_input_paths=["pangenome.json"],
        target_types=["pangenome::heatmap"],
        transform_libraries=A.transforms("logistics", "pangenome"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])
