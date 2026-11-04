from pathlib import Path
import shutil
import tarfile
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::antismash.env"))
assembly = model.AddRequirement(lib.GetType("sequences::assembly"))
db       = model.AddRequirement(lib.GetType("ref::antismash"))

out_json = model.AddProduct(lib.GetType("annotation::antismash_json"))
out_gbk  = model.AddProduct(lib.GetType("annotation::antismash_regions_gbk"))


MIN_CONTIG_LENGTH = 1000
# One antiSMASH process uses ~3 cores whatever --cpus says, and a 1 Gbp assembly ran ~20 h that way,
# against the 24 h task ceiling. Past SHARD_BP the contigs are split into shards run side by side.
SHARD_BP = 120_000_000


def _has_long_enough_contigs(fasta_path: Path, min_len: int = MIN_CONTIG_LENGTH) -> bool:
    length = 0
    with open(fasta_path) as f:
        for line in f:
            if line.startswith(">"):
                if length >= min_len:
                    return True
                length = 0
            else:
                length += len(line.strip())
    return length >= min_len


def _write_mock_outputs(ojson_path: Path, ogbk_path: Path, assembly_name: str):
    import json as _json
    mock = {
        "records": [],
        "version": "mock",
        "input_file": assembly_name,
        "note": f"Skipped: all contigs < {MIN_CONTIG_LENGTH} bp",
    }
    with open(ojson_path, "w") as f:
        _json.dump(mock, f, indent=2)
    with tarfile.open(ogbk_path, "w:gz") as tar:
        pass


def _iter_fasta(path: Path):
    head, buf = None, []
    with open(path) as f:
        for line in f:
            if line.startswith(">"):
                if head is not None:
                    yield head, buf
                head, buf = line, []
            else:
                buf.append(line)
    if head is not None:
        yield head, buf


def _shard_fasta(path: Path, n: int, outdir: Path) -> list:
    outdir.mkdir(parents=True, exist_ok=True)
    shards = [outdir / f"shard_{i}.fasta" for i in range(n)]
    fhs = [open(s, "w") for s in shards]
    filled = [0] * n
    for head, buf in _iter_fasta(path):
        i = filled.index(min(filled))
        fhs[i].write(head)
        fhs[i].writelines(buf)
        filled[i] += sum(len(l.strip()) for l in buf)
    for fh in fhs:
        fh.close()
    return [s for s, bp in zip(shards, filled) if bp > 0]


# Records are written one shard at a time, so peak memory is one shard's JSON, not the assembly's.
def _merge_json(parts: list, out: Path, input_file: str):
    import json as _json
    tail, timings = None, {}
    with open(out, "w") as fo:
        first = True
        for part in parts:
            with open(part) as f:
                doc = _json.load(f)
            if tail is None:
                fo.write(f'{{"version": {_json.dumps(doc["version"])}, '
                         f'"input_file": {_json.dumps(input_file)}, "records": [')
                tail = {k: v for k, v in doc.items() if k not in ("version", "input_file", "records")}
            for rec in doc["records"]:
                fo.write(("" if first else ", ") + _json.dumps(rec))
                first = False
            timings.update(doc.get("timings") or {})
            del doc
        tail["timings"] = timings
        fo.write("]" + "".join(f", {_json.dumps(k)}: {_json.dumps(v)}" for k, v in tail.items()) + "}")


def protocol(context: ExecutionContext):
    iasm = context.Input(assembly)
    idb  = context.Input(db)
    ojson = context.Output(out_json)
    ogbk  = context.Output(out_gbk)

    if not _has_long_enough_contigs(iasm.local):
        print(f"NOTICE: All contigs in {iasm.local.name} are < {MIN_CONTIG_LENGTH} bp. "
              f"Producing mock output (antiSMASH requires >= {MIN_CONTIG_LENGTH} bp contigs).")
        _write_mock_outputs(ojson.local, ogbk.local, iasm.local.name)
        return ExecutionResult(
            manifest=[{out_json: ojson.local, out_gbk: ogbk.local}],
            success=True,
        )

    cpus = context.params.get("cpus")
    cpus = 8 if cpus is None else cpus

    total_bp = sum(sum(len(l.strip()) for l in buf) for _, buf in _iter_fasta(iasm.local))
    n = max(1, min(-(-total_bp // SHARD_BP), cpus // 2))
    inputs = [Path(iasm.container)] if n == 1 else _shard_fasta(Path(iasm.local), n, Path("antismash_shards"))
    per = max(1, cpus // len(inputs))
    outdirs = [Path("antismash_out")] if len(inputs) == 1 else [Path(f"antismash_out_{i}") for i in range(len(inputs))]
    runs = " ".join(
        f"( antismash --databases /antismash_db --genefinding-tool prodigal-m --cb-general --cb-knownclusters"
        f" --cb-subclusters --cpus {per} --output-dir {o} {i} > {o}.log 2>&1 || {{ tail -50 {o}.log; exit 1; }} ) &"
        for i, o in zip(inputs, outdirs))
    print(f"antiSMASH on {total_bp} bp: {len(inputs)} shard(s) at {per} cpus each", flush=True)

    context.ExecWithEnv(
        env=image,
        binds=[(idb.external, "/antismash_db")],
        cmd=f"""
            rc=0
            {runs}
            for p in $(jobs -p); do wait $p || rc=1; done
            cat {" ".join(f"{o}.log" for o in outdirs)} | tail -100
            exit $rc
        """,
    )

    json_files = [j for o in outdirs for j in sorted(o.glob("*.json"))[:1]]
    if len(outdirs) == 1:
        if json_files:
            shutil.copy2(json_files[0], ojson.local)
    elif len(json_files) == len(outdirs):
        _merge_json(json_files, ojson.local, iasm.local.name)

    gbk_files = [g for o in outdirs for g in o.glob("*.region*.gbk")]
    with tarfile.open(ogbk.local, "w:gz") as tar:
        for gbk in gbk_files:
            tar.add(gbk, arcname=gbk.name)

    return ExecutionResult(
        manifest=[
            {
                out_json: ojson.local,
                out_gbk: ogbk.local,
            },
        ],
        success=ojson.local.exists() and ogbk.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=assembly,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=12),
    ),
)
