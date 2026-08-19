#!/usr/bin/env python3
"""Author the `gpr_table_from_assembly` template.

One ORF set through the four canonical GPR lanes and the mapper:

  assembly --> prodigal --> orfs --> kofamscan  --> merge_kofamscan
                                  --> diamond_uniref50 --> merge_diamond_uniref50
                                  --> proteinbert --> merge_proteinbert
                                  --> clean (unchunked)
  orfs + all four lanes + bridge + pool --> gpr_4lane --> annotation::gpr_table

`gpr_4lane` is the CANONICAL mapper (see its own docstring): it produces
`annotation::gpr_table` directly rather than a subtype, so this template needs
no tiebreak between mappers. `gpr_7lane` stays available in the library for
whoever wants the extra three lanes explicitly, but is not what this template
builds.

No `sample_type`. `gpr_4lane` groups by its own `orfs` requirement -- one ORF
set in, one GPR table out (see `research/fabfos/examples/clone_gpr_on_hpc.py`,
which runs the identical mapper this way on a cluster) -- so there is no
cross-sample aggregation for a sample mask to protect, and masking would only
force `ref::mnxr_lookup` / `ref::reference_label_pool` into `shared_input_paths`
for no benefit: those two are unpinned already (see below) and belong wherever
the plan can already see them, which is everywhere.

`ref::mnxr_lookup` (the id-space bridge) and `ref::reference_label_pool` (the
kNN reference embeddings) have no producing transform anywhere in this
library -- they are the frozen MetaNetX-derived / Swiss-Prot-derived reference
basis `gpr_4lane`'s own docstring calls out as staged rather than built, so
they are deferred leaf inputs here exactly as `fabfos::vector_backbone` and
`sequences::background_genome` are in `fosmid_inserts_from_pooled_reads`.
`lib::fabfos_evidence.py` needs no such entry: it ships as a resource under
`resources/lib/`, not an input row.

    python src/metasmith_libraries/gpr_table_from_assembly.py [--rebuild] [--dag]
"""
import sys

import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "gpr_table_from_assembly"
DESCRIPTION = """
Build the canonical chosen-4 GPR evidence table for an assembly: KOFAMSCAN,
CLEAN, DIAMOND UniRef50 and ProteinBERT folded into one gene-attributed
annotation::gpr_table.
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "sequences.yml")
        lib.AddTypeLibrary(A.TYPES / "ref.yml")
        lib.AddTypeLibrary(A.TYPES / "annotation.yml")
        lib.AddItem(DEFERRED, "sequences::assembly")
        # Network-agnostic reference basis, shared rather than per-assembly --
        # see the module docstring.
        lib.AddItem(DEFERRED, "ref::mnxr_lookup")
        lib.AddItem(DEFERRED, "ref::reference_label_pool")

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type=None,
        target_types=["annotation::gpr_table"],
        transform_libraries=A.transforms(
            "metagenomics", "logistics", "functionalAnnotation", "fabfos"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])
