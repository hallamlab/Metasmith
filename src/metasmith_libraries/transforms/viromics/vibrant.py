# Antonio's step 15, and one of the three callers feeding the merge.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::vibrant.env"))
ref   = model.AddRequirement(lib.GetType("ref::vibrant_db"))
# contig_batch rather than assembly, so VIBRANT sits on the same input as the
# other two callers per sample. It does NOT also run on the frozen set: see
# research/viromics/implementation_handoff.md on why a caller reachable there
# makes the merge eligible to consume its own output.
contigs = model.AddRequirement(lib.GetType("sequences::contig_batch"))

out_quality   = model.AddProduct(lib.GetType("viromics::vibrant_genome_quality"))
out_lifestyle = model.AddProduct(lib.GetType("viromics::vibrant_lifestyle_table"))
out_amgs      = model.AddProduct(lib.GetType("viromics::vibrant_amgs"))
out_calls     = model.AddProduct(lib.GetType("viromics::vibrant_candidate_virus"))

LIFESTYLE_HEADER = "call_id\tcontig_id\tlifestyle\tquality\n"


def _read_fasta_lengths(path: Path) -> dict[str, int]:
    """Record name (the header minus '>', description included) to length."""
    lengths: dict[str, int] = {}
    name = None
    n = 0
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


def _read_table(path: Path) -> tuple[dict[str, int], list[list[str]]]:
    with open(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        rows = [l.rstrip("\n").split("\t") for l in f if l.strip()]
    return {n: i for i, n in enumerate(header)}, rows


def _write_lifestyle(quality_tsv: Path, out: Path):
    # VIBRANT states the lifestyle outright, in the `type` column of
    # genome_quality. Antonio derived it instead from which of
    # `<stem>.phages_lytic.fna` and `…_lysogenic.ffn` a record landed in, which
    # is the same call read off a different artifact -- and reading it off the
    # table also carries the quality tier for free.
    col, rows = _read_table(quality_tsv)
    missing = [c for c in ("scaffold", "type", "Quality") if c not in col]
    assert not missing, (
        f"VIBRANT's genome_quality has no {missing}; header was {sorted(col)}."
    )
    with open(out, "w") as o:
        o.write(LIFESTYLE_HEADER)
        for r in rows:
            call = r[col["scaffold"]]
            o.write(f"{call}\t{call.split()[0]}\t{r[col['type']]}\t{r[col['Quality']]}\n")


def _write_calls(combined_fna: Path, coords_tsv: Path, out: Path):
    """VIBRANT's called sequences as intervals on the assembly's contigs.

    Two shapes arrive together. A whole-contig call is named for the contig and
    spans it, so its extracted length is the interval. An integrated prophage is
    named `<scaffold>_fragment_N` and its interval is only stated in
    `VIBRANT_integrated_prophage_coordinates_*.tsv` -- the extracted record's
    length would give the span but not where it starts.

    Every VIBRANT table keys on the FULL fasta header, description included, so
    the join is on that string; `contig_id` is its first whitespace token, which
    is the identifier the assembly and every other caller use.
    """
    fragments: dict[str, tuple[str, int, int]] = {}
    if coords_tsv.exists():
        col, rows = _read_table(coords_tsv)
        missing = [c for c in ("scaffold", "fragment", "nucleotide start", "nucleotide stop")
                   if c not in col]
        assert not missing, (
            f"VIBRANT's prophage coordinates table has no {missing}; header was "
            f"{sorted(col)}. Without it a provirus call would be written as its "
            "whole contig."
        )
        for r in rows:
            fragments[r[col["fragment"]]] = (
                r[col["scaffold"]],
                int(r[col["nucleotide start"]]),
                int(r[col["nucleotide stop"]]),
            )

    with open(out, "w") as o:
        o.write("contig_id\tstart\tend\tcaller\tscore\n")
        for name, length in _read_fasta_lengths(combined_fna).items():
            if name in fragments:
                scaffold, start, end = fragments[name]
            else:
                scaffold, start, end = name, 1, length
            # VIBRANT publishes no numeric confidence -- `VIBRANT_machine_*.tsv`
            # carries a bare virus/non-virus call -- so the score column is NA
            # rather than a quality tier dressed up as a number. The tier is in
            # the genome_quality product, keyed on the same string.
            o.write(f"{scaffold.split()[0]}\t{start}\t{end}\tvibrant\tNA\n")


def protocol(context: ExecutionContext):
    ictg = context.Input(contigs)
    iref = context.Input(ref)
    threads = context.params.get("cpus", 8)

    # -d and -m are two separate paths into one reference tree: the HMM
    # `databases/` half that download-db.sh fetches, and the `files/` half that
    # ships inside the image and the script copies out.
    _cmd = f"""
        VIBRANT_run.py -i {ictg.container} -f nucl -folder ./vibrant_out \
            -t {threads} -no_plot \
            -d {iref.container}/databases -m {iref.container}/files
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    stem = Path(ictg.local).stem
    root = Path("vibrant_out")/f"VIBRANT_{stem}"
    results = root/f"VIBRANT_results_{stem}"
    phages = root/f"VIBRANT_phages_{stem}"
    assert results.is_dir(), (
        f"VIBRANT wrote no {results}; {root} holds "
        f"{sorted(p.name for p in root.iterdir()) if root.is_dir() else 'nothing'}"
    )

    oquality = context.Output(out_quality)
    olifestyle = context.Output(out_lifestyle)
    oamgs = context.Output(out_amgs)
    ocalls = context.Output(out_calls)

    quality_tsv = results/f"VIBRANT_genome_quality_{stem}.tsv"
    amg_tsv = results/f"VIBRANT_AMG_individuals_{stem}.tsv"
    for p in (quality_tsv, amg_tsv):
        assert p.exists(), f"VIBRANT wrote no {p.name}"
    oquality.local.write_bytes(quality_tsv.read_bytes())
    oamgs.local.write_bytes(amg_tsv.read_bytes())
    _write_lifestyle(quality_tsv, olifestyle.local)
    _write_calls(
        phages/f"{stem}.phages_combined.fna",
        results/f"VIBRANT_integrated_prophage_coordinates_{stem}.tsv",
        ocalls.local,
    )

    outs = {
        out_quality: oquality, out_lifestyle: olifestyle,
        out_amgs: oamgs, out_calls: ocalls,
    }
    return ExecutionResult(
        manifest=[{p: o.local for p, o in outs.items()}],
        success=all(o.local.exists() for o in outs.values()),
    )


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
    resources=Resources(
        cpus=16,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    ),
)
