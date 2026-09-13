# Pratama's VIBRANT (reproduction_map B2): `VIBRANT_run.py -f nucl -virome -d DB`.
# -virome is the only difference from the standard vibrant.py.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::vibrant.env"))
ref     = model.AddRequirement(lib.GetType("ref::vibrant_db"))
contigs = model.AddRequirement(lib.GetType("sequences::contig_batch"))

out_quality   = model.AddProduct(lib.GetType("viromics::vibrant_genome_quality"))
out_lifestyle = model.AddProduct(lib.GetType("viromics::vibrant_lifestyle_table"))
out_amgs      = model.AddProduct(lib.GetType("viromics::vibrant_amgs"))
out_calls     = model.AddProduct(lib.GetType("viromics::vibrant_candidate_virus"))


def _read_fasta_lengths(path: Path) -> dict[str, int]:
    lengths, name, n = {}, None, 0
    with open(path) as f:
        for line in f:
            if line.startswith(">"):
                if name is not None:
                    lengths[name] = n
                name, n = line[1:].strip(), 0
            else:
                n += len(line.strip())
    if name is not None:
        lengths[name] = n
    return lengths


def _read_table(path: Path):
    with open(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        rows = [l.rstrip("\n").split("\t") for l in f if l.strip()]
    return {n: i for i, n in enumerate(header)}, rows


def _write_lifestyle(quality_tsv: Path, out: Path):
    col, rows = _read_table(quality_tsv)
    missing = [c for c in ("scaffold", "type", "Quality") if c not in col]
    assert not missing, f"VIBRANT's genome_quality has no {missing}; header was {sorted(col)}"
    with open(out, "w") as o:
        o.write("call_id\tcontig_id\tlifestyle\tquality\n")
        for r in rows:
            call = r[col["scaffold"]]
            o.write(f"{call}\t{call.split()[0]}\t{r[col['type']]}\t{r[col['Quality']]}\n")


def _write_calls(combined_fna: Path, coords_tsv: Path, out: Path):
    """Calls as intervals. A whole-contig call spans its record; a prophage `_fragment_N` takes the coordinates table's interval."""
    fragments = {}
    if coords_tsv.exists():
        col, rows = _read_table(coords_tsv)
        missing = [c for c in ("scaffold", "fragment", "nucleotide start", "nucleotide stop") if c not in col]
        assert not missing, f"VIBRANT's prophage coordinates table has no {missing}; header was {sorted(col)}"
        for r in rows:
            fragments[r[col["fragment"]]] = (r[col["scaffold"]], int(r[col["nucleotide start"]]),
                                             int(r[col["nucleotide stop"]]))
    with open(out, "w") as o:
        o.write("contig_id\tstart\tend\tcaller\tscore\n")
        for name, length in _read_fasta_lengths(combined_fna).items():
            scaffold, start, end = fragments.get(name, (name, 1, length))
            # VIBRANT publishes no numeric confidence.
            o.write(f"{scaffold.split()[0]}\t{start}\t{end}\tvibrant\tNA\n")


def protocol(context: ExecutionContext):
    ictg = context.Input(contigs)
    iref = context.Input(ref)
    threads = context.params.get("cpus", 8)

    context.ExecWithEnv(env=image, cmd=f"""
        VIBRANT_run.py -i {ictg.container} -f nucl -virome -folder ./vibrant_out -t {threads} -no_plot \
            -d {iref.container}/databases -m {iref.container}/files
    """)

    stem = Path(ictg.local).stem
    root = Path("vibrant_out") / f"VIBRANT_{stem}"
    results = root / f"VIBRANT_results_{stem}"
    assert results.is_dir(), f"VIBRANT wrote no {results}"
    quality_tsv = results / f"VIBRANT_genome_quality_{stem}.tsv"
    amg_tsv = results / f"VIBRANT_AMG_individuals_{stem}.tsv"
    for p in (quality_tsv, amg_tsv):
        assert p.exists(), f"VIBRANT wrote no {p.name}"

    outs = {p: context.Output(p) for p in (out_quality, out_lifestyle, out_amgs, out_calls)}
    outs[out_quality].local.write_bytes(quality_tsv.read_bytes())
    outs[out_amgs].local.write_bytes(amg_tsv.read_bytes())
    _write_lifestyle(quality_tsv, outs[out_lifestyle].local)
    _write_calls(root / f"VIBRANT_phages_{stem}" / f"{stem}.phages_combined.fna",
                 results / f"VIBRANT_integrated_prophage_coordinates_{stem}.tsv", outs[out_calls].local)
    return ExecutionResult(manifest=[{p: o.local for p, o in outs.items()}],
                           success=all(o.local.exists() for o in outs.values()))


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=contigs,
    output_signature={
        out_quality: "genome_quality.tsv",
        out_lifestyle: "lifestyle.tsv",
        out_amgs: "amgs.tsv",
        out_calls: "vibrant_calls.tsv",
    },
    resources=Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=12)),
)
