"""Promote one step's finished products that a stale shard kept out of the cache.

    repromote_step.py <run dir> <cache root> <step name> [--exclude FILE] [--bam-min-align PCT] [--apply]

A task qualifies when it exited 0, its .command.cache record says `exists`, the shard at its key is
not servable (tombstoned, no manifest, or a file missing), and the task dir still holds every
produced file at the recorded size. --exclude names task dirs (`ab/cdef01`, one per line) to skip.
--bam-min-align also requires each produced .bam to end in a BGZF EOF block and the task's
bowtie2.log to report at least PCT% overall alignment with no error line.
--apply writes each shard with the engine's write_shard, from the host path of the task dir, so the
products hard-link, then copies the task logs into it. Records stay as they are.
"""
import argparse
import re
import sys
from pathlib import Path

from metasmith.caching.admission import PoolFile, shard_for, write_shard, _servable
from metasmith.caching.promote import CACHE_RECORD_FILE, StepCacheMeta, _copy_task_logs
from metasmith.models.lineage import LinPayload

BGZF_EOF = bytes.fromhex("1f8b08040000000000ff0600424302001b0003000000000000000000")


def read_meta(task: Path, order: int) -> StepCacheMeta | None:
    raw = {}
    for line in (task / ".command.metadata").read_text().splitlines():
        head, _, rest = line.partition(" ")
        if head:
            raw[head] = rest
    return StepCacheMeta.from_raw(order, raw) if "transform_key" in raw else None


def bam_ok(task: Path, bam: Path, min_align: float) -> str | None:
    with open(bam, "rb") as f:
        f.seek(-len(BGZF_EOF), 2)
        if f.read() != BGZF_EOF:
            return "bam has no BGZF EOF"
    log = task / "bowtie2.log"
    text = log.read_text() if log.exists() else ""
    if re.search(r"Error|exception", text):
        return "bowtie2 error line"
    m = re.search(r"([0-9.]+)% overall alignment rate", text)
    if m is None or float(m.group(1)) < min_align:
        return "alignment rate low or missing"
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run"); ap.add_argument("cache_root"); ap.add_argument("step")
    ap.add_argument("--exclude", type=Path)
    ap.add_argument("--bam-min-align", type=float)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    run = Path(a.run).resolve(); root = Path(a.cache_root).resolve()
    excluded = set(a.exclude.read_text().split()) if a.exclude else set()

    ok, skipped = [], {}
    def skip(why):
        skipped[why] = skipped.get(why, 0) + 1
    for rec_file in sorted((run / "nxf_work").glob(f"??/*/{CACHE_RECORD_FILE}")):
        task = rec_file.parent
        short = f"{task.parent.name}/{task.name[:6]}"
        ec = task / ".exitcode"
        if not ec.exists() or ec.read_text().strip() != "0":
            continue
        import json
        for line in rec_file.read_text().splitlines():
            r = json.loads(line)
            if r.get("step_name") != a.step:
                continue
            if short in excluded:
                skip("excluded"); continue
            if r.get("status") != "exists":
                skip(f"status {r.get('status')}"); continue
            key = r["key"]
            final = shard_for(root, key, "lineage")
            if final.exists() and _servable(final):
                skip("shard servable"); continue
            meta = read_meta(task, int(r["step"]))
            if meta is None:
                skip("no step meta"); continue
            files, why = [], None
            for p in r.get("produces", []):
                name = Path(p["relpath"]).name
                srcs = [f for f in task.iterdir()
                        if not f.is_symlink() and f.is_file()
                        and LinPayload.canonical_output_name(f.name) == name]
                if len(srcs) != 1 or srcs[0].stat().st_size != int(p["size"]):
                    why = "task copy missing or differs"; break
                if a.bam_min_align is not None and name.endswith(".bam"):
                    why = bam_ok(task, srcs[0], a.bam_min_align)
                    if why:
                        break
                files.append(PoolFile(
                    dtype_name=p.get("dtype_name", ""), dtype_key=p.get("dtype_key", ""),
                    relpath=p["relpath"], slot_id=p.get("slot_id", ""),
                    branch_idx=int(p.get("branch_idx", 0)), parents=list(p.get("parents") or []),
                    size=int(p["size"]), src=str(srcs[0]),
                ))
            if why or not files:
                skip(why or "no products"); continue
            ok.append((task, short, key, meta, files, r))

    print(f"run {run.name} step {a.step}: {len(ok)} qualify, "
          f"{sum(f.size for *_, fs, _ in ok for f in fs)} B; skipped {skipped}")
    if not a.apply:
        return 0
    done, failed = 0, []
    for task, short, key, meta, files, r in ok:
        w = write_shard(
            cache_root=root, key=bytes.fromhex(key), origin="lineage", files=files,
            transform_key=meta.transform_key, signature=meta.signature, step_name=meta.step_name,
            consumes=r.get("consumes") or {}, lineage=r.get("lineage") or {},
        )
        linked = all((w.shard / f.relpath).stat().st_nlink > 1 for f in files) if w.status == "promoted" else False
        if w.status != "promoted" or not linked:
            failed.append((short, w.status, linked)); continue
        _copy_task_logs(task, w.shard)
        done += 1
    print(f"applied: {done} promoted and linked; failed {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
