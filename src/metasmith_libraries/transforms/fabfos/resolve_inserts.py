"""Pooled assemblies -> the putative fosmid insert set, in one step.

    inserts.fna         the insert set
    insert_metadata/    inserts.csv + membership.csv + junctions.tsv

Two products, and the method behind them is `algorithm::fabfos_recovery.py`,
which this drives rather than reimplements. That module is runnable on a
directory of assemblies with no planner, no staging and no container, because
that is how a cut or a threshold gets inspected before it is trusted; a copy of
its logic inlined here would be a second thing to keep true.

ONE TRANSFORM, NOT TWO
It was `junction_split` (per pool) then `cluster_contigs` (all pools), and the
boundary between them was not a staging boundary. Splitting at the vector is a
pure function of the map plus the contigs the map was called against, so a step
boundary there re-stages both assemblies to re-derive an in-memory interval list,
and hands the second half no guarantee that the contigs it cuts are the ones the
junctions were called on. What the two halves really differ in is GRAIN, not
lifetime -- and grain is what the batch is for (below).

BOTH GRAINS, IN ONE JOB
`group_by=exp` puts every pool in one job. Inside it, `context.AsBatch()` walks
one lineage per pool, and the per-pool half -- map, then rectify against that
pool's own graphs -- runs there, in that pool's own assembly coordinates. The
dedup then runs once, after the loop, over every pool's pieces. Both products are
written at batch 0, because there is one insert set for the run and not one per
pool.

Per-pool is not a convenience here. blast's e-value scales with database size, so
mapping all 35 pools as one subject returns ~3.6k terminal-proximal backbone hits
where per-pool batching returns ~13k. Flattening the loop would silently retune
`--min-hsp` by a factor of three.

THE ASSEMBLY GRAPHS ARE REQUIRED, NOT OPTIONAL
Circularity is the assembler's own claim -- a spades walk whose last node links
back to its first, or a megahit fastg self-loop -- and never a terminal repeat,
which is common in linear sequence. Wrapping a linear contig fuses the two ends
of one real insert, so a pool whose graphs are absent does not plan. spades joins
to its contigs exactly via `contigs.paths`, which is why that ships as its own
requirement: the GFA's P lines name SCAFFOLDS and must not be used for it.

WHY THE STEP IS FIVE EXEC CALLS AND NOT ONE
The method needs blastn and pandas/sklearn, and no env in the library has both,
so every blast is its own dispatch into `blast.env` with the python either side
of it in `python_for_data_science.env`. The module exposes exactly the seams that
requires -- `map-prep` / `map-write`, `rectify-prep` / `rectify --graph-hits`,
`dedup-prep` / `dedup-cluster` -- and the intermediate files they agree on live
in a per-pool work directory that no product is declared over.

HOST-FILTER COERCION LIVES HERE
This is the first consumer of the assemblies, so pinning the consumed assembly's
lineage to `host_filtered_short_reads` is what forces `reads ->
background_filter -> {megahit, spades}` upstream. Move it and host filtering
silently drops out of the plan.
"""
from pathlib import Path

from metasmith.python_api import *

lib       = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model     = Transform()
# The experiment node is the root of the run. Both per-run references below hang
# off it, so a plan cannot silently reach for some other run's backbone or host.
exp       = model.AddRequirement(lib.GetType("fabfos::experiment"))
meta      = model.AddRequirement(lib.GetType("sequences::read_metadata"), parents={exp})
# Lineage constraint only -- the protocol never reads it. See the note above.
hf        = model.AddRequirement(lib.GetType("sequences::host_filtered_short_reads"), parents={meta})
# Both assemblers explicitly: a generic `sequences::assembly` requirement lets the
# planner satisfy the DAG with one of them, but the intended pipeline runs megahit
# AND spades per pool and carries both through to the dedup. Each comes with the
# graph it is judged against, and spades additionally with its contig->walk map.
asm_mh    = model.AddRequirement(lib.GetType("sequences::megahit_assembly"), parents={meta, hf})
gr_mh     = model.AddRequirement(lib.GetType("sequences::megahit_assembly_graph"), parents={meta, hf})
asm_sp    = model.AddRequirement(lib.GetType("sequences::spades_assembly"), parents={meta, hf})
gr_sp     = model.AddRequirement(lib.GetType("sequences::spades_assembly_graph"), parents={meta, hf})
pa_sp     = model.AddRequirement(lib.GetType("sequences::spades_contig_paths"), parents={meta, hf})
backbone  = model.AddRequirement(lib.GetType("fabfos::vector_backbone"), parents={exp})
recovery  = model.AddRequirement(lib.GetType("algorithm::fabfos_recovery.py"))
img_blast = model.AddRequirement(lib.GetType("env::blast.env"))
img_pyds  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out_ins   = model.AddProduct(lib.GetType("fabfos::putative_inserts"))
out_meta  = model.AddProduct(lib.GetType("fabfos::insert_metadata"))

# blast's own tabular columns, in blast's own order. `junctions.tsv` IS this
# table, so this string is a product contract: widening it changes a shipped file.
OUTFMT = "6 qseqid sseqid sstart send pident length nident qlen slen"
# The all-vs-all needs the QUERY coordinates on top, because its similarity sums
# non-overlapping HSPs. Must stay identical to `fabfos_recovery.AVA_HSP_FMT`: the
# reader there raises rather than returning an empty matrix if they disagree.
AVA_OUTFMT = "6 qseqid sseqid qstart qend sstart send pident length nident qlen slen"
# The backbone-vs-graph-nodes blast is strict on purpose: a node qualifies as
# backbone only when the vector covers ~all of it, so the e-value here is not the
# permissive one the contig map uses.
GRAPH_EVALUE = "1e-10"
# The all-vs-all is deliberately permissive (scadc settings). The similarity that
# matters is one long contiguous match, and a strict e-value drops the HSPs that
# make it up.
AVA_EVALUE = "1000"
AVA_PERC_IDENTITY = "50"
# A representative >= this much contained in a longer one is absorbed into it.
CONTAINMENT = "0.99"
# ...and one closed at NEITHER end needs only this much to be absorbed into one
# closed at both, since its boundaries are where the assembly stopped rather than
# where the vector was, so it cannot be a clone in its own right.
FRAGMENT_CONTAINMENT = "0.90"


def protocol(context: ExecutionContext):
    threads = context.params.get("cpus")
    nt = "" if threads is None else f"-num_threads {threads}"
    tflag = "" if threads is None else f"--threads {threads}"

    def py(cmd):
        context.ExecWithEnv() \
            .ifContainerDo(env=img_pyds, cmd=cmd) \
            .ifVirtualEnvDo(env=img_pyds, cmd=cmd)

    def blast(cmd):
        context.ExecWithEnv() \
            .ifContainerDo(env=img_blast, cmd=cmd) \
            .ifVirtualEnvDo(env=img_blast, cmd=cmd)

    # `python <module>.py` rather than `python -m fabfos.algorithm...`: the file is
    # staged as a resource, so the package it normally lives in is not importable
    # here. Its CLI is the whole interface, which is why it has no imports of its
    # own beyond the standard library and pandas/sklearn.
    R = f"python {context.Input(recovery).container}"
    bb = context.Input(backbone).container

    # ---------------------------------------------------------------
    # per pool: map the backbone, then cut against that pool's own graphs.
    # One lineage per pool under group_by=exp -- see the docstring.
    splits = []
    for i, pool in enumerate(context.AsBatch()):
        w = Path(f"pool_{i:03d}")
        mh, sp = pool.Input(asm_mh).container, pool.Input(asm_sp).container
        # The assembler and the pool are STATED, never parsed off a filename:
        # metasmith's staged names are content hashes, and `k141_9` names a
        # different contig in every pool. `pool_{i}` is the run-local pool label
        # that ends up inside every qualified id this step writes.
        asm_args = f"--assembly {mh}:megahit:{w.name} --assembly {sp}:spades:{w.name}"
        graph_args = (f"--graph megahit:{pool.Input(gr_mh).container} "
                      f"--graph spades:{pool.Input(gr_sp).container}:{pool.Input(pa_sp).container}")

        py(f"""
            mkdir -p {w}/work
            {R} map-prep {asm_args} --backbone {bb} --work {w}/work
        """)
        blast(f"""
            makeblastdb -dbtype nucl -in {w}/work/pooled.fna -out {w}/work/db >/dev/null
            blastn -query {bb} -db {w}/work/db -evalue 1e-5 {nt} \
                -outfmt "{OUTFMT}" -out {w}/work/backbone_hits.tsv
        """)
        py(f"""
            {R} map-write --work {w}/work --out-junctions {w}/junctions.tsv
            {R} rectify-prep {graph_args} --work {w}/work
        """)
        blast(f"""
            for a in megahit spades; do
                makeblastdb -dbtype nucl -in {w}/work/graph_$a/graph_nodes.fna \
                    -out {w}/work/graph_$a/db >/dev/null
                blastn -query {bb} -db {w}/work/graph_$a/db -evalue {GRAPH_EVALUE} {nt} \
                    -outfmt "{OUTFMT}" -out {w}/work/graph_$a/bb_hits.tsv
            done
        """)
        py(f"""
            {R} rectify {asm_args} {graph_args} \
                --graph-hits megahit:{w}/work/graph_megahit/bb_hits.tsv \
                --graph-hits spades:{w}/work/graph_spades/bb_hits.tsv \
                --junctions {w}/junctions.tsv --backbone {bb} \
                --out-split {w}/split_contigs.fna --work {w}/work {tflag}
        """)
        splits.append(w)

    # ---------------------------------------------------------------
    # once, over every pool's pieces. Both products are written here: there is one
    # insert set for the run, not one per pool.
    iins = context.Output(out_ins)
    imeta = context.Output(out_meta)
    py(f"""
        mkdir -p {imeta.container} dedup/work
        cat pool_*/junctions.tsv     > {imeta.container}/junctions.tsv
        cat pool_*/split_contigs.fna > dedup/all_split.fna
        {R} dedup-prep --split dedup/all_split.fna --work dedup/work
    """)
    blast(f"""
        makeblastdb -dbtype nucl -in dedup/work/pooled.fna -out dedup/work/db >/dev/null
        blastn -query dedup/work/pooled.fna -db dedup/work/db \
            -evalue {AVA_EVALUE} -perc_identity {AVA_PERC_IDENTITY} {nt} \
            -outfmt "{AVA_OUTFMT}" -out dedup/work/ava_hits.tsv
    """)
    # Both absorb thresholds are stated rather than inherited: they decide how
    # many inserts the run reports, so a default moving underneath this transform
    # would change the product with nothing in the plan to show for it.
    py(f"""
        {R} dedup-cluster --work dedup/work \
            --containment {CONTAINMENT} --fragment-containment {FRAGMENT_CONTAINMENT} \
            --out-inserts {iins.container} --out-metadata {imeta.container}
    """)

    produced = [imeta.local / n for n in ("inserts.csv", "membership.csv", "junctions.tsv")]
    Log.Info(f"resolve_inserts: {len(splits)} pools -> {iins.local}")
    return ExecutionResult(
        manifest=[{out_ins: iins.local, out_meta: imeta.local}],
        success=iins.local.exists() and all(p.exists() for p in produced),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    # One job for the whole run. The per-pool grain is the batch inside it, which
    # is what lets the cross-pool dedup see every pool's pieces at once.
    group_by=exp,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    )
)
