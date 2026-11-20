from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
barrnap  = model.AddRequirement(lib.GetType("env::barrnap.env"))
blast    = model.AddRequirement(lib.GetType("env::blast.env"))
python   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
asm      = model.AddRequirement(lib.GetType("sequences::assembly"))
ssu_ref  = model.AddRequirement(lib.GetType("ref::gtdb_rrna"))
rna_lib  = model.AddRequirement(lib.GetType("lib::rna_scan"))
lca_lib  = model.AddRequirement(lib.GetType("lib::lcastar"))
out_gff  = model.AddProduct(lib.GetType("annotation::rrna_gff"))
out_tax  = model.AddProduct(lib.GetType("taxonomy::rrna_gtdb"))

KINGDOMS = ("bac", "arc", "euk")
# MetaPathways 4's rRNA thresholds.
EVALUE, MIN_IDENTITY, MIN_BITSCORE = 1e-6, 20, 50


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    iref = context.Input(ssu_ref)
    irna = context.Input(rna_lib)
    ilca = context.Input(lca_lib)
    igff = context.Output(out_gff)
    itax = context.Output(out_tax)

    threads = context.params.get("cpus", 4)
    scans = "\n".join(
        f"barrnap --kingdom {k} --threads {threads} --evalue {EVALUE} {iasm.container} > barrnap_{k}.gff"
        for k in KINGDOMS)
    context.ExecWithEnv(env=barrnap, cmd=scans)

    py = f"PYTHONPATH={irna.container}:{ilca.container} python3 {irna.container}/rrna.py"
    context.ExecWithEnv(env=python, cmd=f"""
        {py} merge --gff {" ".join(f"barrnap_{k}.gff" for k in KINGDOMS)} \
            --assembly {iasm.container} --out-gff {igff.container} --out-ssu ssu_genes.fna
    """)
    context.ExecWithEnv(env=blast, cmd=f"""
        touch ssu_hits.tsv
        if [ -s ssu_genes.fna ]; then
            blastn -query ssu_genes.fna -db {iref.container}/ssu \
                -evalue {EVALUE} -max_target_seqs 50 -num_threads {threads} \
                -outfmt "6 qseqid sseqid pident length evalue bitscore stitle" \
                -out ssu_hits.tsv
        fi
    """)
    context.ExecWithEnv(env=python, cmd=f"""
        {py} classify --gff {igff.container} --blast ssu_hits.tsv --out {itax.container} \
            --evalue {EVALUE} --min-identity {MIN_IDENTITY} --min-bitscore {MIN_BITSCORE}
    """)

    return ExecutionResult(
        manifest=[{out_gff: igff.local, out_tax: itax.local}],
        success=igff.local.exists() and itax.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=6),
    ),
)
