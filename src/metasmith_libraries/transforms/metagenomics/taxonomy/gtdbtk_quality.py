# gtdbtk.py on the aggregator's pool of quality MAGs, which no binner's own bin type reaches.
#
# CAUTION a copy of gtdbtk.py with one requirement changed: keep the protocol and the
# resources in step with it. A retype of the pool into a putative_genome cannot replace
# this copy. Lineage matching is ancestral and the pool descends from every binner's
# bins, so CheckM2 on a retyped pool satisfies the aggregator's own per-binner CheckM2
# inputs, and the solve cycles.
# Run as the Cyanoverse AB48 classifications were (GTDB-Tk 2.7.2 on r232, the
# arbutus-infra GTDB-Tk service): skani ANI pre-screen on, pplacer on one thread
# and memory-mapped to --scratch_dir, everything else on every core. That run peaked
# at 110.7 GB with --scratch_dir set and was OOM-killed at 116 GB without it.
# 2.7 removed --skip_ani_screen and --mash_db, and either is an argument error.
# Never --full_tree: its own help asks for more than 320 GB.
from metasmith.python_api import *
from pathlib import Path
import shutil

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::gtdbtk.env"))
ref         = model.AddRequirement(lib.GetType("ref::gtdb"))
asm         = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"))
tax         = model.AddProduct(lib.GetType("taxonomy::gtdbtk"))


def _first_column(tables: list[Path]) -> set[str]:
    keys = set()
    for table in tables:
        with open(table) as tsv:
            tsv.readline()
            for l in tsv:
                toks = l.strip().split("\t")
                if toks and toks[0]:
                    keys.add(toks[0])
    return keys


def protocol(context: ExecutionContext):
    iref    = context.Input(ref)

    genome_dir = Path("./assemblies")
    genome_dir.mkdir()
    in2out = {}
    for item in context.AsBatch():
        iasm    = item.Input(asm)
        itax    = item.Output(tax)
        in2out[iasm.local.stem] = (itax.local.name, itax)
        src = iasm.local
        dest = genome_dir/iasm.local.name
        Log.Info(f"registering genome [{src}] -> [{dest}]")
        shutil.copy(src, dest, follow_symlinks=True)

    threads = context.params.get('cpus') or 1
    pplacer_cpus = context.params.get('gtdbtk_pplacer_cpus', 1)

    ext = iasm.container.suffix.replace(".", "")
    out_raw = Path("./gtdb_raw")
    context.ExecWithEnv(
        env = image,
        exports = {"GTDBTK_DATA_PATH": iref.container},
        cmd = f"""\
            mkdir -p tmp pplacer
            gtdbtk classify_wf --genome_dir {genome_dir} --extension {ext} \
                --out_dir {out_raw} \
                --cpus {threads} --pplacer_cpus {pplacer_cpus} \
                --scratch_dir "$PWD/pplacer" --tmpdir "$PWD/tmp"
        """
    )

    summaries = sorted(out_raw.glob("classify/*summary.tsv"))
    rows = {}
    last_header = None
    for table in summaries:
        with open(table) as tsv:
            header = tsv.readline()
            last_header = header
            for l in tsv:
                rows[l.strip().split("\t")[0]] = l, header

    failed = _first_column(
        list(out_raw.glob("identify/*failed_genomes*.tsv"))
        + list(out_raw.glob("*failed_genomes*.tsv"))
    )
    unclassified = _first_column(list(out_raw.glob("classify/*.tree.unclassified.tsv")))
    missing = set(in2out) - set(rows) - failed - unclassified
    if missing:
        Log.Error(f"classify_wf accounted for none of {sorted(missing)}; summaries: {summaries}")
        return ExecutionResult(manifest=[], success=False)

    fallback_header = last_header or "user_genome\tclassification\n"
    n_cols = len(fallback_header.rstrip("\n").split("\t"))

    manifest = []
    for _asm, (_tax_name, _tax_handle) in in2out.items():
        if _asm in rows:
            row, header = rows[_asm]
        else:
            Log.Warn(f"genome [{_asm}] has no classification (failed={_asm in failed}); emitting an N/A row")
            row = "\t".join([_asm] + ["N/A"] * max(0, n_cols - 1)) + "\n"
            header = fallback_header
        if not row.endswith("\n"): row += "\n"
        if not header.endswith("\n"): header += "\n"
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
        cpus=32,
        memory=Size.GB(128),
        duration=Duration(hours=24),
    )
)
