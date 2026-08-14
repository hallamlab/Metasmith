import shutil
from pathlib import Path
from metasmith.python_api import *

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::checkm.env")) # database included at /checkm_database
asm         = model.AddRequirement(lib.GetType("sequences::putative_genome"))
out         = model.AddProduct(lib.GetType("taxonomy::checkm_stats"))

def protocol(context: ExecutionContext):
    input_dir = Path("input")
    input_dir.mkdir()

    in2out = {}  # asm_stem -> iout
    for item in context.AsBatch():
        iasm = item.Input(asm)
        iout = item.Output(out)
        in2out[iasm.local.stem] = iout
        src = iasm.local
        dest = input_dir / iasm.local.name
        Log.Info(f"registering genome [{src}] -> [{dest}]")
        shutil.copy(src, dest, follow_symlinks=True)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-t {threads}"
    ext = iasm.container.suffix.replace(".", "")
    temp_ws = "checkm_ws"
    qa_file = "checkm_qa.tsv"
    context.ExecWithEnv().ifContainerDo(
        env = image,
        cmd = f"""
            export PATH=/opt/conda/bin:/opt/conda/condabin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
            checkm lineage_wf {threads} -x {ext} ./input ./{temp_ws}
            checkm qa ./{temp_ws}/lineage.ms ./{temp_ws} --out_format 2 --tab_table --file {qa_file}
        """
    )

    # Parse the combined QA table and split per-genome
    with open(qa_file) as f:
        header = f.readline()
        rows = {}
        for line in f:
            if line.strip():
                genome_name = line.split("\t")[0].strip()
                rows[genome_name] = line

    manifest = []
    for asm_stem, iout in in2out.items():
        # Write per-genome checkm stats
        with open(iout.local, "w") as f:
            f.write(header)
            if asm_stem in rows:
                f.write(rows[asm_stem])
        manifest.append({out: iout.local})

    return ExecutionResult(
        manifest=manifest,
        success=Path(qa_file).exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    batch_size=25,
    resources=Resources(
        cpus=2,
        memory=Size.GB(44), # checkm needs 40
        duration=Duration(hours=1),
    )
)
