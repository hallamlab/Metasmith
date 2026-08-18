#!/usr/bin/env python3
"""Author the `viromics_survey_from_assembly` template.

The viral legs the library can reach from an assembly and its clean reads:

  assembly --> splitContigsForAmr --> contig_batch --> virsorter2  (viral contigs,
                                                    |               scores, affi)
                                                    +-> genomad    (virus +
                                                                    plasmid summaries)
  assembly + virsorter2 seqs/affi   --> dramv       (AMG distillation)
  clean_short_reads                 --> crassphage  (human faecal marker coverage)

plus the four reference-DB legs, each of which has a producing transform in
`logistics/` and so is planned rather than staged: VirSorter2 DB, geNomad DB,
DRAM DB, crAssphage reference.

Reads are seeded as `sequences::clean_short_reads` rather than raw reads, and
the assembly is an input rather than a target: both keep the plan inside the
viromics boundary. Seeding raw reads instead pulls bbduk and the whole assembly
leg back in, which is a different template.

Two legs of the collaborator's manual pipeline (research/metasmith_libraries/
viromics/reference/) have no counterpart here and are deliberately absent
rather than approximated: CheckV quality/host-trimming and VIBRANT lifestyle
have no env or transform in the library, and vOTU dereplication needs an mmseqs
clustering transform that does not exist either. geNomad also publishes only
its two summary tables, not the virus FASTA, so nothing downstream of it can be
wired until it does.

    python src/metasmith_libraries/viromics_survey_from_assembly.py [--rebuild] [--dag]
"""
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "viromics_survey_from_assembly"
DESCRIPTION = """
Call viral contigs from an assembly with VirSorter2 and geNomad, distill their
auxiliary metabolic genes with DRAM-v, and quantify crAssphage coverage in the
clean reads as a faecal-source marker.
"""

# geNomad's two summaries are named separately even though one step produces
# both: the plasmid leg is a result in its own right, and a target list that
# omitted it would read as though the template only looked for viruses.
TARGETS = [
    "annotation::virsorter2_viral_sequences",
    "taxonomy::genomad_virus_summary",
    "taxonomy::genomad_plasmid_summary",
    "annotation::dramv_distill",
    "annotation::crassphage_coverage",
]


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        for tl in ("sequences.yml", "annotation.yml", "taxonomy.yml", "ref.yml"):
            lib.AddTypeLibrary(A.TYPES / tl)
        # The assembly is the sample root and the reads hang off it: the reads
        # are here for crassphage only, and this is what puts them in the same
        # lineage as the contigs they were assembled from.
        asm = lib.AddItem(DEFERRED, "sequences::assembly")
        lib.AddItem(DEFERRED, "sequences::clean_short_reads", parents={asm})

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="sequences::assembly",
        target_types=TARGETS,
        transform_libraries=A.transforms(
            "logistics", "metagenomics", "functionalAnnotation"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])
