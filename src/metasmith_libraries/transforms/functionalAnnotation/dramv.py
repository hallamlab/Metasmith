from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::dram.env"))
# The contig set VirSorter2 was run on, not the assembly behind it. DRAM-v must
# read the affi table of ONE VirSorter2 run, and once VirSorter2 runs both per
# sample and on the frozen viral set, "descends from the assembly" is true of
# both -- naming the contig set is what keeps the two runs' outputs from being
# paired across.
asm         = model.AddRequirement(lib.GetType("sequences::contig_batch"))
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

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            export HOME=/tmp
            sed "s|\\"/db/|\\"{idb.container}/|g" {idb.container}/DRAM.config > DRAM.local.config
            # DRAM only warns about a database it cannot open, and annotates without it.
            grep -o '"{idb.container}/[^"]*"' DRAM.local.config | tr -d '"' | while read -r f; do
                [ -e "$f" ] || {{ echo "DRAM.config names a missing file [$f]" >&2; exit 1; }}
            done

            DRAM-v.py annotate \
                -i {ivs2_seqs.container} \
                -v {ivs2_affi.container} \
                -o {annot_dir} \
                --threads {threads} \
                --min_contig_size 1000 \
                --config_loc DRAM.local.config

            DRAM-v.py distill \
                -i {annot_dir}/annotations.tsv \
                -o {distill_dir} \
                --config_loc DRAM.local.config
        """,
    )

    context.LocalShell(f"cp {annot_dir}/annotations.tsv {iannot.local}")

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
