from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::genomad.env"))
assembly = model.AddRequirement(lib.GetType("sequences::contig_batch"))
ref  = model.AddRequirement(lib.GetType("ref::genomad"))

virus_summary_out   = model.AddProduct(lib.GetType("taxonomy::genomad_virus_summary"))
plasmid_summary_out = model.AddProduct(lib.GetType("taxonomy::genomad_plasmid_summary"))
virus_genes_out     = model.AddProduct(lib.GetType("taxonomy::genomad_virus_genes"))
taxonomy_out        = model.AddProduct(lib.GetType("taxonomy::genomad_taxonomy"))
calls_out           = model.AddProduct(lib.GetType("viromics::genomad_candidate_virus"))


def _write_calls(summary_tsv: Path, out: Path):
    # geNomad states a provirus twice: as the suffix of `seq_name`
    # (`<contig>|provirus_<start>_<end>`) and in `coordinates` as 1-based
    # inclusive `start-end`. A whole-contig call has coordinates NA, and its
    # interval is the contig, so `length` supplies the end.
    with open(summary_tsv) as f:
        header = f.readline().rstrip("\n").split("\t")
        col = {name: i for i, name in enumerate(header)}
        missing = [c for c in ("seq_name", "length", "coordinates", "virus_score")
                   if c not in col]
        assert not missing, (
            f"geNomad's virus_summary.tsv has no {missing}; header was {header}. "
            "The candidate_virus adapter reads these four columns by name -- if "
            "geNomad renamed one, fix it here rather than by index."
        )
        with open(out, "w") as o:
            o.write("contig_id\tstart\tend\tcaller\tscore\n")
            for line in f:
                if not line.strip(): continue
                row = line.rstrip("\n").split("\t")
                name = row[col["seq_name"]].split("|")[0]
                coords = row[col["coordinates"]]
                if coords in ("NA", "", "."):
                    start, end = 1, int(row[col["length"]])
                else:
                    start, end = (int(x) for x in coords.split("-"))
                o.write(f"{name}\t{start}\t{end}\tgenomad\t{row[col['virus_score']]}\n")


def protocol(context: ExecutionContext):
    iasm    = context.Input(assembly)
    idb     = context.Input(ref)
    ovirus  = context.Output(virus_summary_out)
    oplasmid = context.Output(plasmid_summary_out)
    ogenes  = context.Output(virus_genes_out)
    otax    = context.Output(taxonomy_out)
    ocalls  = context.Output(calls_out)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-t {threads}"

    _cmd = f"/usr/local/bin/_entrypoint.sh genomad end-to-end {iasm.container} genomad_output {idb.container} {threads} --cleanup"
    context.ExecWithEnv(env=image, cmd=_cmd)

    prefix = Path(iasm.local).stem
    summary_dir = Path(f"genomad_output/{prefix}_summary")

    import shutil
    shutil.copy2(summary_dir / f"{prefix}_virus_summary.tsv", ovirus.local)
    shutil.copy2(summary_dir / f"{prefix}_plasmid_summary.tsv", oplasmid.local)
    shutil.copy2(summary_dir / f"{prefix}_virus_genes.tsv", ogenes.local)
    # Not in the summary directory. `--cleanup` removes only `_mmseqs2`, so the
    # annotate directory and this file both survive it.
    shutil.copy2(Path(f"genomad_output/{prefix}_annotate/{prefix}_taxonomy.tsv"), otax.local)
    _write_calls(ovirus.local, ocalls.local)

    return ExecutionResult(
        manifest=[
            {
                virus_summary_out: ovirus.local,
                plasmid_summary_out: oplasmid.local,
                virus_genes_out: ogenes.local,
                taxonomy_out: otax.local,
                calls_out: ocalls.local,
            },
        ],
        success=all(p.local.exists() for p in (ovirus, oplasmid, ogenes, otax, ocalls)),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=assembly,
    resources=Resources(
        cpus=16,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    )
)
