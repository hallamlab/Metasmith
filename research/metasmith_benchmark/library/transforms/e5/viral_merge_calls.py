# The frozen viral set from the four callers on each sample's assembly.
#
# Each caller's call is its own record, as in Pratama's pool: two callers on one contig give two
# records with their own boundaries, and only the vOTU clustering downstream collapses them.
import json
from collections import defaultdict
from pathlib import Path
from metasmith.python_api import *


def _label(read_metadata: Path) -> str:
    """The sample's name, which its read_metadata carries. A pool given's file is named for its content hash."""
    return json.loads(read_metadata.read_text())["sample"]

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::seqkit.env"))
study = model.AddRequirement(lib.GetType("sequences::study"))
meta  = model.AddRequirement(lib.GetType("sequences::read_metadata"), parents={study})

# One lane: the sample's own assembly, MEGAHIT for short reads and OPERA-MS for hybrid samples.
LANES = {"assembly": "sequences::assembly"}
CALLERS = (
    "viromics::genomad_candidate_virus",
    "viromics::virsorter2_candidate_virus",
    "viromics::vibrant_candidate_virus",
    "e3::deepvirfinder_candidate_virus",
)

# One (batch slot, caller slots) per lane. The batch slots share a type and differ by parent, which is
# what keeps them separate slots.
SLOTS = []
for lane, assembly_type in LANES.items():
    asm = model.AddRequirement(lib.GetType(assembly_type), parents={meta})
    batch = model.AddRequirement(lib.GetType("sequences::contig_batch"), parents={asm})
    callers = tuple(model.AddRequirement(lib.GetType(c), parents={batch}) for c in CALLERS)
    SLOTS.append((lane, batch, callers))

out_frozen = model.AddProduct(lib.GetType("viromics::dereplicated_candidate_virus"))
out_prov   = model.AddProduct(lib.GetType("viromics::candidate_call_provenance"))

PROV_HEADER = "\t".join(["frozen_id", "sample", "lane", "caller", "source_contig", "start", "end", "length"]) + "\n"


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


def _plan_extraction(per_contig):
    regions = sorted({(c, s, e) for c, calls in per_contig.items() for s, e, _ in calls})
    records = sorted({(c, s, e, caller) for c, calls in per_contig.items() for s, e, caller in calls})
    return regions, records


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
    sample_of, lane_of = {}, {}
    for lane, batch, callers in SLOTS:
        for slot in callers:
            for call_table in context.InputGroup(slot):
                sample = context.SourceOf(call_table, meta)
                contigs = context.SourceOf(call_table, batch)
                assert sample is not None and contigs is not None, (
                    f"[{call_table.local.name}] lacks a read_metadata or contig batch in its lineage")
                sample_of[contigs.local] = _label(Path(sample.local))
                lane_of[contigs.local] = lane
                for contig_id, start, end, caller in _read_calls(call_table.local):
                    by_contigs[contigs.local][contig_id].append((start, end, caller))

    ofrozen = context.Output(out_frozen)
    oprov = context.Output(out_prov)
    n_calls = n_frozen = 0
    with open(ofrozen.local, "w") as fasta, open(oprov.local, "w") as prov:
        prov.write(PROV_HEADER)
        for k, (contigs_path, per_contig) in enumerate(sorted(by_contigs.items())):
            sample, lane = sample_of[contigs_path], lane_of[contigs_path]
            regions = Path(f"regions_{k}.bed")
            spans, wanted = _plan_extraction(per_contig)
            n_calls += len(wanted)
            with open(regions, "w") as rf:
                for contig_id, start, end in spans:
                    # BED is 0-based half-open: the start converts and the end does not.
                    rf.write(f"{contig_id}\t{start - 1}\t{end}\n")
            if not wanted:
                continue
            got = _extract(context, contigs_path, regions, k)
            for contig_id, start, end, caller in wanted:
                seq = got.get((contig_id, start, end))
                if seq is None:
                    Log.Warn(f"[{sample}] {contig_id}:{start}-{end} was called but not extracted")
                    continue
                frozen_id = f"{sample}|{lane}|{caller}|{contig_id}|{start}_{end}"
                fasta.write(f">{frozen_id}\n")
                for i in range(0, len(seq), 70):
                    fasta.write(seq[i:i + 70] + "\n")
                prov.write("\t".join([frozen_id, sample, lane, caller, contig_id, str(start), str(end),
                                      str(end - start + 1)]) + "\n")
                n_frozen += 1

    Log.Info(f"{n_calls} calls over {len(by_contigs)} contig batches -> {n_frozen} frozen records")
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
