from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::dram.env"))
asm         = model.AddRequirement(lib.GetType("sequences::assembly"))
vs2_seqs    = model.AddRequirement(lib.GetType("annotation::virsorter2_viral_sequences"), parents={asm})
vs2_affi    = model.AddRequirement(lib.GetType("annotation::virsorter2_affi_contigs"), parents={asm})
db          = model.AddRequirement(lib.GetType("annotation::dram_db"))
out_annot   = model.AddProduct(lib.GetType("annotation::dramv_annotations"))
out_distill = model.AddProduct(lib.GetType("annotation::dramv_distill"))

def protocol(context: ExecutionContext):
    ivs2_seqs = context.Input(vs2_seqs)
    ivs2_affi = context.Input(vs2_affi)
    idb       = context.Input(db)
    iannot    = context.Output(out_annot)
    idistill  = context.Output(out_distill)

    threads = context.params.get("cpus", 8)
    annot_dir = "dramv_annot"
    distill_dir = "dramv_distill"

    # Use --config_loc to point at the exported DRAM config in the DB directory
    # This avoids needing to write to the read-only container filesystem
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idb.external, "/db")],
        cmd=f"""
            export HOME=/tmp

            DRAM-v.py annotate \
                -i {ivs2_seqs.container} \
                -v {ivs2_affi.container} \
                -o {annot_dir} \
                --threads {threads} \
                --min_contig_size 1000 \
                --config_loc /db/DRAM.config

            DRAM-v.py distill \
                -i {annot_dir}/annotations.tsv \
                -o {distill_dir} \
                --config_loc /db/DRAM.config
        """,
    )

    # Copy annotations TSV
    context.LocalShell(f"cp {annot_dir}/annotations.tsv {iannot.local}")

    # Copy distill directory
    context.LocalShell(f"cp -r {distill_dir} {idistill.local}")

    return ExecutionResult(
        manifest=[
            {
                out_annot: iannot.local,
                out_distill: idistill.local,
            },
        ],
        success=iannot.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=12),
    ),
)
