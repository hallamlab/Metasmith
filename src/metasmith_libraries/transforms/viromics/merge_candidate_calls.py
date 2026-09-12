#
# This is where the pipeline stops writing sequence. Every caller's calls, for
# every sample, land in one task: identical calls are deduplicated, overlapping
# ones are unioned earliest-start to latest-end, the merged intervals are cut out
# of the sample's own assembly, headers are prefixed with the sample label, and
# the result is pooled across samples. Nothing downstream writes a FASTA again.
from collections import defaultdict
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::seqkit.env"))

# The pangenome::pangenome shape: a contentless root the driver declares as a
# shared input, so every sample's calls have one common ancestor to group under.
study = model.AddRequirement(lib.GetType("viromics::contig_study"))
pair  = model.AddRequirement(lib.GetType("sequences::read_pair"), parents={study})

# One assembly, then batches of THAT assembly. Both lines are load-bearing.
#
# `asm` exists to pin the assembler. This is a collecting transform, so its batch
# slot holds every batch in the group -- and with the constraint reaching only as
# far as the read pair, batches from every assembler in the library qualify, so
# the planner runs both of them and every per-sample step downstream twice.
# Binding one `sequences::assembly` endpoint and hanging the batches off it is
# what makes "one assembler" a property of the model instead of something each
# driver has to re-pin target by target (and pinning targets does not fix it:
# the duplication is upstream of them).
#
# `batch` rather than `asm` for the calls, because the batch is the contig set
# the coordinates are on, so it is also what the merged intervals are cut from.
asm   = model.AddRequirement(lib.GetType("sequences::assembly"), parents={pair})
batch = model.AddRequirement(lib.GetType("sequences::contig_batch"), parents={asm})

# Three slots, not one, because a Dependency hashes over its properties and
# parents: three requirements built from one type would be one key in
# `Application.used` and collapse to a single slot. Keeping them distinct is also
# what puts all three callers in the DAG by data dependency.
gn = model.AddRequirement(lib.GetType("viromics::genomad_candidate_virus"), parents={batch})
vs = model.AddRequirement(lib.GetType("viromics::virsorter2_candidate_virus"), parents={batch})
vb = model.AddRequirement(lib.GetType("viromics::vibrant_candidate_virus"), parents={batch})

out_frozen = model.AddProduct(lib.GetType("viromics::dereplicated_candidate_virus"))
out_prov   = model.AddProduct(lib.GetType("viromics::candidate_call_provenance"))


PROV_HEADER = "\t".join([
    "frozen_id", "sample", "source_contig", "start", "end", "length", "callers",
]) + "\n"


def _read_calls(path: Path):
    """Rows of (contig_id, start, end, caller) from one normalised call table."""
    rows = []
    with open(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        col = {n: i for i, n in enumerate(header)}
        missing = [c for c in ("contig_id", "start", "end", "caller") if c not in col]
        assert not missing, (
            f"{path.name} has no {missing}; header was {header}. Every caller's "
            "adapter writes this shape, so a rename is a defect in that adapter."
        )
        for line in f:
            if not line.strip():
                continue
            r = line.rstrip("\n").split("\t")
            rows.append((
                r[col["contig_id"]], int(r[col["start"]]), int(r[col["end"]]),
                r[col["caller"]],
            ))
    return rows


def _to_bed(start: int, end: int) -> tuple[int, int]:
    """A 1-based inclusive interval as BED's 0-based half-open pair.

    The start converts and the end does not. Getting this wrong shortens every
    frozen contig by one base at the 5' end, which no downstream tool would ever
    complain about -- which is why it is a named function with a test rather than
    an expression inside a loop. Verified against seqkit: bed 4..10 on a known
    sequence returns the same six bases as `-r 5:10`.
    """
    return start - 1, end


def _union(intervals):
    """Merge overlapping 1-based inclusive intervals, keeping every caller.

    Two calls touching end-to-end (`end + 1 == start`) are one interval: they
    describe adjacent bases of one provirus, and leaving a gap of zero between
    them would emit the same region twice.
    """
    merged = []
    for start, end, caller in sorted(intervals):
        if merged and start <= merged[-1][1] + 1:
            prev_start, prev_end, callers = merged[-1]
            merged[-1] = (prev_start, max(prev_end, end), callers | {caller})
        else:
            merged.append((start, end, {caller}))
    return merged


def protocol(context: ExecutionContext):
    # Grouped slots are related by lineage, never by index -- two slots of the
    # same group arrive in arbitrary order, and pairing them positionally is this
    # library's documented quiet failure.
    #
    # by_contigs keys on the contig batch each call was made against, because that
    # batch is both what the coordinates mean and what the sequence is cut from.
    by_contigs = defaultdict(lambda: defaultdict(list))
    sample_of = {}
    for slot in (gn, vs, vb):
        for call_table in context.InputGroup(slot):
            sample = context.SourceOf(call_table, pair)
            assert sample is not None, (
                f"no read_pair in the lineage of [{call_table.local.name}] -- the "
                "sample label is what namespaces contig ids across samples, and "
                "without it the pooled set collides on bare k141_N"
            )
            contigs = context.SourceOf(call_table, batch)
            assert contigs is not None, (
                f"no contig batch in the lineage of [{call_table.local.name}] -- "
                "the call coordinates are on its contigs and cannot be extracted "
                "without it"
            )
            key = contigs.local
            sample_of[key] = Path(sample.local).stem
            for contig_id, start, end, caller in _read_calls(call_table.local):
                by_contigs[key][contig_id].append((start, end, caller))

    ofrozen = context.Output(out_frozen)
    oprov = context.Output(out_prov)

    # One regions file per contig batch, then one seqkit call per batch. Doing it
    # per call would be one container round trip per provirus.
    n_calls = 0
    n_frozen = 0
    with open(ofrozen.local, "w") as fasta, open(oprov.local, "w") as prov:
        prov.write(PROV_HEADER)
        for k, (contigs_path, per_contig) in enumerate(sorted(by_contigs.items())):
            sample = sample_of[contigs_path]
            regions = Path(f"regions_{k}.bed")
            wanted = []
            with open(regions, "w") as rf:
                for contig_id, intervals in sorted(per_contig.items()):
                    for start, end, callers in _union(intervals):
                        n_calls += 1
                        bed_start, bed_end = _to_bed(start, end)
                        rf.write(f"{contig_id}\t{bed_start}\t{bed_end}\n")
                        wanted.append((contig_id, start, end, sorted(callers)))

            if not wanted:
                continue

            got = _extract(context, contigs_path, regions, k)
            for contig_id, start, end, callers in wanted:
                seq = got.get((contig_id, start, end))
                if seq is None:
                    Log.Warn(
                        f"[{sample}] {contig_id}:{start}-{end} was called but not "
                        "extracted; the interval is off the end of its contig"
                    )
                    continue
                # The frozen id carries its whole provenance, because the pooled
                # set is the only thing most downstream tools ever see and a bare
                # contig name collides across samples.
                frozen_id = f"{sample}|{contig_id}|{start}_{end}"
                fasta.write(f">{frozen_id}\n")
                for i in range(0, len(seq), 70):
                    fasta.write(seq[i:i + 70] + "\n")
                prov.write("\t".join([
                    frozen_id, sample, contig_id, str(start), str(end),
                    str(end - start + 1), ",".join(callers),
                ]) + "\n")
                n_frozen += 1

    Log.Info(f"{n_calls} merged calls over {len(by_contigs)} contig batches -> {n_frozen} frozen contigs")
    return ExecutionResult(
        manifest=[{out_frozen: ofrozen.local, out_prov: oprov.local}],
        success=ofrozen.local.exists() and oprov.local.exists(),
    )


def _extract(context, contigs_path: Path, regions: Path, k: int) -> dict:
    """Cut every requested interval out of one contig batch, keyed by interval."""
    out_fa = Path(f"subseq_{k}.fna")
    _cmd = f"""
        seqkit subseq --bed {regions} {contigs_path} > {out_fa}
    """
    context.ExecWithEnv(env=image, cmd=_cmd)

    # seqkit names each cut `<contig>_<start>-<end>:<strand>` with the interval
    # already converted BACK to 1-based inclusive, so the header is the key and
    # nothing has to be paired by position -- which matters, because seqkit drops
    # a region whose contig is absent and any positional pairing would then be
    # silently shifted for every region after it.
    #
    # rsplit on the last underscore, because a contig id legitimately contains
    # both `|` and `_` (the pipeline mints `SAMPLE|assembler|k141_37`).
    got = {}
    header = None
    cur = []

    def _flush():
        if header is None:
            return
        name = header.split(":")[0]
        contig_id, _, span = name.rpartition("_")
        start_s, _, end_s = span.partition("-")
        got[(contig_id, int(start_s), int(end_s))] = "".join(cur)

    for line in open(out_fa):
        if line.startswith(">"):
            _flush()
            header = line[1:].strip()
            cur = []
        elif line.strip():
            cur.append(line.strip())
    _flush()
    return got


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    output_signature={
        out_frozen: "dereplicated_candidate_virus.fna",
        out_prov: "candidate_call_provenance.tsv",
    },
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
