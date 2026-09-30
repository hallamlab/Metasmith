# The frozen viral set from BOTH of a sample's assemblies, as Pratama pooled metaSPAdes and MEGAHIT
# calls before clustering. The standard merge binds one assembly per sample.
#
# Calls are unioned per contig batch, so the two assemblies' overlapping contigs stay separate records.
# The vOTU clustering downstream is what collapses them.
import json
from collections import defaultdict
from pathlib import Path
from metasmith.python_api import *


def _label(read_pair: Path) -> str:
    """The run accession a read pair holds. A pool given's file is named for its content hash, not its value."""
    try:
        value = json.loads(read_pair.read_text())
    except (json.JSONDecodeError, UnicodeDecodeError):
        return read_pair.stem
    return value if isinstance(value, str) else read_pair.stem

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::seqkit.env"))
study = model.AddRequirement(lib.GetType("viromics::contig_study"))
pair  = model.AddRequirement(lib.GetType("sequences::read_pair"), parents={study})

# One (batch slot, three caller slots) per assembler. The batch slots share a type and differ by parent,
# which is what keeps them two slots.
LANES = []
for assembler in ("spades_assembly", "megahit_assembly"):
    asm = model.AddRequirement(lib.GetType(f"sequences::{assembler}"), parents={pair})
    batch = model.AddRequirement(lib.GetType("sequences::contig_batch"), parents={asm})
    callers = tuple(model.AddRequirement(lib.GetType(f"viromics::{c}_candidate_virus"), parents={batch})
                    for c in ("genomad", "virsorter2", "vibrant"))
    LANES.append((batch, callers))

out_frozen = model.AddProduct(lib.GetType("viromics::dereplicated_candidate_virus"))
out_prov   = model.AddProduct(lib.GetType("viromics::candidate_call_provenance"))

PROV_HEADER = "\t".join(["frozen_id", "sample", "source_contig", "start", "end", "length", "callers"]) + "\n"


def _read_calls(path: Path):
    rows = []
    with open(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        col = {n: i for i, n in enumerate(header)}
        missing = [c for c in ("contig_id", "start", "end", "caller") if c not in col]
        assert not missing, f"{path.name} has no {missing}; header was {header}"
        for line in f:
            if line.strip():
                r = line.rstrip("\n").split("\t")
                rows.append((r[col["contig_id"]], int(r[col["start"]]), int(r[col["end"]]), r[col["caller"]]))
    return rows


def _union(intervals):
    """Merge overlapping or abutting 1-based inclusive intervals, keeping every caller."""
    merged = []
    for start, end, caller in sorted(intervals):
        if merged and start <= merged[-1][1] + 1:
            prev_start, prev_end, callers = merged[-1]
            merged[-1] = (prev_start, max(prev_end, end), callers | {caller})
        else:
            merged.append((start, end, {caller}))
    return merged


def _extract(context, contigs_path: Path, regions: Path, k: int) -> dict:
    """Every requested interval of one contig batch, keyed by (contig, start, end) from seqkit's own header."""
    out_fa = Path(f"subseq_{k}.fna")
    context.ExecWithEnv(env=image, cmd=f"seqkit subseq --bed {regions} {contigs_path} > {out_fa}")
    got, header, cur = {}, None, []

    def _flush():
        if header is not None:
            contig_id, _, span = header.split(":")[0].rpartition("_")
            start_s, _, end_s = span.partition("-")
            got[(contig_id, int(start_s), int(end_s))] = "".join(cur)

    for line in open(out_fa):
        if line.startswith(">"):
            _flush()
            header, cur = line[1:].strip(), []
        elif line.strip():
            cur.append(line.strip())
    _flush()
    return got


def protocol(context: ExecutionContext):
    # Grouped slots arrive in arbitrary order, so pair them by lineage, never by index.
    by_contigs = defaultdict(lambda: defaultdict(list))
    sample_of = {}
    for batch, callers in LANES:
        for slot in callers:
            for call_table in context.InputGroup(slot):
                sample = context.SourceOf(call_table, pair)
                contigs = context.SourceOf(call_table, batch)
                assert sample is not None and contigs is not None, (
                    f"[{call_table.local.name}] lacks a read_pair or contig batch in its lineage")
                sample_of[contigs.local] = _label(Path(sample.local))
                for contig_id, start, end, caller in _read_calls(call_table.local):
                    by_contigs[contigs.local][contig_id].append((start, end, caller))

    ofrozen = context.Output(out_frozen)
    oprov = context.Output(out_prov)
    n_calls = n_frozen = 0
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
                        # BED is 0-based half-open: the start converts and the end does not.
                        rf.write(f"{contig_id}\t{start - 1}\t{end}\n")
                        wanted.append((contig_id, start, end, sorted(callers)))
            if not wanted:
                continue
            got = _extract(context, contigs_path, regions, k)
            for contig_id, start, end, callers in wanted:
                seq = got.get((contig_id, start, end))
                if seq is None:
                    Log.Warn(f"[{sample}] {contig_id}:{start}-{end} was called but not extracted")
                    continue
                frozen_id = f"{sample}|{contig_id}|{start}_{end}"
                fasta.write(f">{frozen_id}\n")
                for i in range(0, len(seq), 70):
                    fasta.write(seq[i:i + 70] + "\n")
                prov.write("\t".join([frozen_id, sample, contig_id, str(start), str(end),
                                      str(end - start + 1), ",".join(callers)]) + "\n")
                n_frozen += 1

    Log.Info(f"{n_calls} merged calls over {len(by_contigs)} contig batches -> {n_frozen} frozen contigs")
    return ExecutionResult(manifest=[{out_frozen: ofrozen.local, out_prov: oprov.local}],
                           success=ofrozen.local.exists() and oprov.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    output_signature={
        out_frozen: "dereplicated_candidate_virus.fna",
        out_prov: "candidate_call_provenance.tsv",
    },
    resources=Resources(cpus=4, memory=Size.GB(16), duration=Duration(hours=4)),
)
