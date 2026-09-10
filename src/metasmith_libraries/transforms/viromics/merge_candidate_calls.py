# MOCK -- model only. The requirements, products and grouping are real and the
# lineage lookups below are exercised for real; the protocol writes empty outputs
# and runs no tool.
#
# This is where the pipeline stops writing sequence. Every caller's calls, for
# every sample, land in one task: identical calls are deduplicated, overlapping
# ones are unioned earliest-start to latest-end, the merged intervals are cut out
# of the sample's own assembly, headers are prefixed with the sample label, and
# the result is pooled across samples. Nothing downstream writes a FASTA again.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

# The pangenome::pangenome shape: a contentless root the driver declares as a
# shared input, so every sample's calls have one common ancestor to group under.
study = model.AddRequirement(lib.GetType("viromics::contig_study"))
pair  = model.AddRequirement(lib.GetType("sequences::read_pair"), parents={study})

# One assembly, then batches of THAT assembly. Both lines are load-bearing.
#
# `asm` exists to pin the assembler. This is a collecting transform, so its batch
# slot holds every batch in the group -- and with the constraint reaching only as
# far as the read pair, batches from every assembler in the library qualify, so
# the planner runs both of them and every per-sample step downstream twice.
# Binding one `sequences::assembly` endpoint and hanging the batches off it is
# what makes "one assembler" a property of the model instead of something each
# driver has to re-pin target by target (and pinning targets does not fix it:
# the duplication is upstream of them).
#
# `batch` rather than `asm` for the calls, because the batch is the contig set
# the coordinates are on, so it is also what the merged intervals are cut from.
asm   = model.AddRequirement(lib.GetType("sequences::assembly"), parents={pair})
batch = model.AddRequirement(lib.GetType("sequences::contig_batch"), parents={asm})

# Three slots, not one, because a Dependency hashes over its properties and
# parents: three requirements built from one type would be one key in
# `Application.used` and collapse to a single slot. Keeping them distinct is also
# what puts all three callers in the DAG by data dependency.
gn = model.AddRequirement(lib.GetType("viromics::genomad_candidate_virus"), parents={batch})
vs = model.AddRequirement(lib.GetType("viromics::virsorter2_candidate_virus"), parents={batch})
vb = model.AddRequirement(lib.GetType("viromics::vibrant_candidate_virus"), parents={batch})

out_frozen = model.AddProduct(lib.GetType("viromics::dereplicated_candidate_virus"))
out_prov   = model.AddProduct(lib.GetType("viromics::candidate_call_provenance"))


def protocol(context: ExecutionContext):
    # Grouped slots are related by lineage, never by index -- two slots of the
    # same group are in arbitrary order, and pairing them positionally is this
    # library's documented quiet failure. Exercised here even though the mock
    # writes nothing, because it is the part of the model most likely to be wrong.
    for slot in (gn, vs, vb):
        for call_table in context.InputGroup(slot):
            sample = context.SourceOf(call_table, pair)
            assert sample is not None, (
                f"no read_pair in the lineage of [{call_table.local.name}] -- the "
                "sample label is what namespaces contig ids across samples, and "
                "without it the pooled set collides on bare k141_N"
            )
            contigs = context.SourceOf(call_table, batch)
            assert contigs is not None, (
                f"no contig batch in the lineage of [{call_table.local.name}] -- "
                "the call coordinates are on its contigs and cannot be extracted "
                "without it"
            )

    ofrozen = context.Output(out_frozen)
    oprov = context.Output(out_prov)
    for o in (ofrozen, oprov):
        context.external_shell.Exec(f"touch {o.external}")
    return ExecutionResult(
        manifest=[{out_frozen: ofrozen.local, out_prov: oprov.local}],
        success=ofrozen.local.exists() and oprov.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    output_signature={
        out_frozen: "dereplicated_candidate_virus.fna",
        out_prov: "candidate_call_provenance.tsv",
    },
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
