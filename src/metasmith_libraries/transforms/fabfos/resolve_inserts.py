from pathlib import Path

from metasmith.python_api import *

lib       = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model     = Transform()
exp       = model.AddRequirement(lib.GetType("fabfos::experiment"))
meta      = model.AddRequirement(lib.GetType("sequences::read_metadata"), parents={exp})
hf        = model.AddRequirement(lib.GetType("sequences::host_filtered_short_reads"), parents={meta})
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

OUTFMT = "6 qseqid sseqid sstart send pident length nident qlen slen"
AVA_OUTFMT = "6 qseqid sseqid qstart qend sstart send pident length nident qlen slen"
GRAPH_EVALUE = "1e-10"
AVA_EVALUE = "1000"
AVA_PERC_IDENTITY = "50"
CONTAINMENT = "0.99"
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

    R = f"python {context.Input(recovery).container}"
    bb = context.Input(backbone).container

    splits = []
    for i, pool in enumerate(context.AsBatch()):
        w = Path(f"pool_{i:03d}")
        mh, sp = pool.Input(asm_mh).container, pool.Input(asm_sp).container
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
    group_by=exp,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    )
)
