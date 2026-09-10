# Antonio's step 23, and the second place the two lanes meet: every CRISPR spacer
# the survey's own MAGs carry, searched against the frozen viral set. One task,
# because a spacer database is only useful pooled.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::blast.env"))
study   = model.AddRequirement(lib.GetType("viromics::contig_study"))
spacers = model.AddRequirement(lib.GetType("viromics::crispr_spacers"), parents={study})
frozen  = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"), parents={study})

out = model.AddProduct(lib.GetType("viromics::spacer_host_links"))

HEADER = "\t".join([
    "spacer_id", "contig_id", "percent_identity", "alignment_length",
    "mismatches", "spacer_length", "spacer_start", "spacer_end",
    "contig_start", "contig_end", "evalue", "bitscore", "spacer_coverage",
]) + "\n"


def protocol(context: ExecutionContext):
    ifrozen = context.Input(frozen)
    threads = context.params.get("cpus", 8)

    # Every bin's spacer file, concatenated. cctyper already tagged each header
    # with its bin and array, so provenance survives the merge without a side
    # table.
    pooled = Path("all_spacers.fna")
    n = 0
    with open(pooled, "w") as dst:
        for handle in context.InputGroup(spacers):
            text = handle.local.read_text()
            n += text.count(">")
            dst.write(text)
            if text and not text.endswith("\n"):
                dst.write("\n")
    Log.Info(f"pooled {n} spacers")

    o = context.Output(out)
    if n == 0:
        # No spacer anywhere in the survey. A header-only table is the honest
        # product: the join downstream then yields nothing, rather than the step
        # failing and taking every other target with it.
        o.local.write_text(HEADER)
        return ExecutionResult(manifest=[{out: o.local}], success=True)

    # -task blastn-short is the whole point. Spacers are 30-40 bp and the default
    # task's word size of 11 misses them outright. -subject rather than a made
    # database because the frozen set is the subject and building one buys
    # nothing at this scale.
    #
    # Every hit is emitted with its identity, alignment length and mismatch count.
    # The paper's acceptance rule -- at most one mismatch over at least 95% of the
    # spacer -- is deliberately NOT applied here: it belongs to the join, where it
    # can be varied without re-running an alignment.
    _cmd = f"""
        blastn -task blastn-short -query {pooled} -subject {ifrozen.container} \
            -evalue 1e-5 -word_size 7 -reward 1 -penalty -1 -ungapped -dust no \
            -num_threads {threads} \
            -outfmt "6 qseqid sseqid pident length mismatch qlen qstart qend sstart send evalue bitscore qcovs" \
            -out hits.raw.tsv
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    raw = Path("hits.raw.tsv")
    rows = raw.read_text() if raw.exists() else ""
    with open(o.local, "w") as dst:
        dst.write(HEADER)
        dst.write(rows)
    Log.Info(f"{rows.count(chr(10))} spacer-to-contig hits")

    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    output_signature={out: "spacer_hits.tsv"},
    resources=Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=4)),
)
