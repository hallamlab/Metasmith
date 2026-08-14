from metasmith.python_api import *
from pathlib import Path
import shutil

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::gtdbtk.env"))
ref         = model.AddRequirement(lib.GetType("ref::gtdb"))
asm         = model.AddRequirement(lib.GetType("sequences::putative_genome"))
tax         = model.AddProduct(lib.GetType("taxonomy::gtdbtk"))
# raw         = model.AddProduct(lib.GetType("taxonomy::gtdbtk_raw"))

def protocol(context: ExecutionContext):
    iref    = context.Input(ref)
    # iraw    = context.Output(raw)

    genome_dir = Path("./assemblies")
    genome_dir.mkdir()
    in2out = {}  # asm_stem -> (tax_output_name, tax_output_handle)
    for item in context.AsBatch():
        iasm    = item.Input(asm)
        itax    = item.Output(tax)
        in2out[iasm.local.stem] = (itax.local.name, itax)  # gtdb removes the file extension, so .stem
        src = iasm.local
        dest = genome_dir/iasm.local.name
        Log.Info(f"registering genome [{src}] -> [{dest}]")
        shutil.copy(src, dest, follow_symlinks=True)
    
    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--cpus {threads}"
    mem = context.params.get('memory')
    if mem:
        _mem_gb = int(float(mem))
        # pplacer keeps a FULL copy of the reference tree (~128 GB for r232's
        # scaled bac120 tree) IN MEMORY PER THREAD, so peak RAM ~= pplacer_cpus
        # * ~130 GB. The old (mem-8)//40 heuristic assumed ~40 GB/thread and
        # yielded 5 threads at 240 GB -> ~640 GB peak -> SIGKILL right at
        # "Step 8: Placing genomes" (silently swallowed by errorStrategy=ignore,
        # producing empty taxonomy). Budget ~140 GB/thread over a ~40 GB base so
        # 240 GB -> 1 thread (peak ~128 GB, fits). --skip_ani_screen is set, so
        # the sketch DB is not loaded and pplacer is the sole memory driver.
        pplacer_cpus = f"--pplacer_cpus {max(1, (_mem_gb-40)//140)}"
    else:
        pplacer_cpus = ""

    ext = iasm.container.suffix.replace(".", "")
    TEMP_PREFIX = "temp"
    # temp_scratch = Path(f"{TEMP_PREFIX}.scratch") # replaces RAM
    temp_ws = Path(f"{TEMP_PREFIX}.ws")
    # reduce pplacer memory usage by writing to disk (slower).
    # --scratch_dir {temp_scratch} \
    # Skip the skani ANI screening step to classify genomes.
    # --skip_ani_screen
    # todo:
    # - use prodigal genes
    # - separate the skani screen? is this needed for small runs? 
    # - batchify
    out_raw = Path("./gtdb_raw")
    context.ExecWithEnv().ifContainerDo(
        binds=[
            (iref.external, "/ref"),
        ],
        env = image,
        cmd = f"""\
            mkdir -p {temp_ws}
            export GTDBTK_DATA_PATH=/ref
            gtdbtk classify_wf -x {ext} \
                {threads} {pplacer_cpus} \
                --force \
                --skip_ani_screen \
                --tmpdir {temp_ws} \
                --genome_dir {genome_dir} \
                --out_dir {out_raw}
        """
    )
    
    # collect rows from summary tables (per-domain classification results)
    file_candidates = [p for p in out_raw.glob("classify/*summary.tsv")]
    rows = {}
    last_header = None
    for table in file_candidates:
        with open(table) as tsv:
            header = tsv.readline()
            last_header = header
            for l in tsv:
                toks = l.strip().split("\t")
                k = toks[0]
                rows[k] = l, header

    # genomes that gtdbtk placed but couldn't classify end up in
    # tree.unclassified.tsv / failed_genomes.tsv rather than *summary.tsv —
    # accept them with empty taxonomy so downstream consumers still see the bin
    aux_candidates = (
        list(out_raw.glob("classify/*.tree.unclassified.tsv"))
        + list(out_raw.glob("classify/*failed_genomes*.tsv"))
        + list(out_raw.glob("identify/*failed_genomes*.tsv"))
        + list(out_raw.glob("*failed_genomes*.tsv"))
    )
    aux_keys = set()
    for table in aux_candidates:
        with open(table) as tsv:
            _hdr = tsv.readline()
            for l in tsv:
                toks = l.strip().split("\t")
                if toks:
                    aux_keys.add(toks[0])

    fallback_header = last_header or "user_genome\tclassification\n"
    n_cols = len(fallback_header.rstrip("\n").split("\t"))

    # have output file creation order match input order, since paranoid
    manifest = []
    for _asm, (_tax_name, _tax_handle) in in2out.items():
        if _asm in rows:
            row, header = rows[_asm]
        else:
            # genome missing from summary.tsv — emit a row with N/A taxonomy
            Log.Warn(f"genome [{_asm}] absent from summary.tsv (aux={_asm in aux_keys}); emitting N/A row")
            empty_fields = [_asm] + ["N/A"] * max(0, n_cols - 1)
            row = "\t".join(empty_fields) + "\n"
            header = fallback_header
        if row[:-1] != "\n": row+="\n"
        if header[:-1] != "\n": header+="\n"
        with open(_tax_name, "w") as out:
            out.write(header)
            out.write(row)
        manifest.append({tax: _tax_handle.local})

    return ExecutionResult(
        manifest=manifest,
        success=len(manifest) > 0,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    batch_size=200,
    resources=Resources(
        cpus=8,
        memory=Size.GB(240), # r232 sketches.db needs >120GB; r226 used 107 GB
        duration=Duration(hours=24),
    )
)
