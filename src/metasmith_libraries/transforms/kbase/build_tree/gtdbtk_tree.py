from metasmith.python_api import *
from pathlib import Path
import shutil

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::gtdbtk.env"))
ref     = model.AddRequirement(lib.GetType("ref::gtdb"))
asm     = model.AddRequirement(lib.GetType("sequences::putative_genome"))
msa     = model.AddProduct(lib.GetType("taxonomy::gtdbtk_msa"))
tree    = model.AddProduct(lib.GetType("taxonomy::gtdbtk_tree"))

def protocol(context: ExecutionContext):
    iref = context.Input(ref)

    # NOT RUN. The GTDB reference is ~110 GB and is not on any host this was written on,
    # so this body was checked by reading `metagenomics/taxonomy/gtdbtk.py` -- same image,
    # same bind, same GTDBTK_DATA_PATH -- and not by execution. See
    # research/kbase/curation/r5/README.md.
    #
    # Same tool as that transform, which runs `classify_wf` -- identify and align
    # included -- and then globs only `classify/*summary.tsv`, so the concatenated
    # marker MSA it wrote is discarded. This declares that MSA and adds the one
    # command classify_wf does not run.
    #
    # A tree is one product over many genomes, and this library has no grouping type
    # above an assembly. The batch is what stands in for one: every member's genome is
    # staged into the same directory and the single tree is emitted from member 0,
    # which is the same arrangement `gtdbtk.py` uses to reach 200 genomes per task.
    genome_dir = Path("./assemblies")
    genome_dir.mkdir(exist_ok=True)
    ext = None
    for item in context.AsBatch():
        iasm = item.Input(asm)
        ext = iasm.local.suffix.replace(".", "")
        dest = genome_dir/iasm.local.name
        Log.Info(f"registering genome [{iasm.local}] -> [{dest}]")
        shutil.copy(iasm.local, dest, follow_symlinks=True)
    assert ext is not None, "no genomes in the batch"

    imsa = context.Output(msa)
    itree = context.Output(tree)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--cpus {threads}"
    temp_ws = Path("temp.ws")
    align_ws = Path("./gtdb_align")
    infer_ws = Path("./gtdb_infer")
    context.ExecWithEnv(
        env=image,
        binds=[(iref.external, "/ref")],
        cmd=f"""\
            mkdir -p {temp_ws}
            export GTDBTK_DATA_PATH=/ref
            gtdbtk identify --genome_dir {genome_dir} -x {ext} \
                --out_dir {align_ws} {threads}
            gtdbtk align --identify_dir {align_ws} \
                --out_dir {align_ws} {threads}
            # align writes the MSA gzipped and taxonomy::gtdbtk_msa is plain FASTA
            gzip -dc {align_ws}/align/*.user_msa.fasta.gz > {imsa.container}
            gtdbtk infer --msa_file {imsa.container} \
                --out_dir {infer_ws} {threads}
            cp {infer_ws}/*.unrooted.tree {itree.container}
        """,
    )

    return ExecutionResult(
        manifest=[{msa: imsa.local, tree: itree.local}],
        success=imsa.local.exists() and itree.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    batch_size=200,
    resources=Resources(
        cpus=8,
        memory=Size.GB(240),
        duration=Duration(hours=24),
    ),
)
