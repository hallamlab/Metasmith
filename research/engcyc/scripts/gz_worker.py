"""Compress staged read files on a Slurm node: one result file per unit, standard library and pigz only.

usage: gz_worker.py <scratch> <units.tsv> <task_index> <n_tasks>

A unit line is: unit, kind, output name, input names. Inputs sit in <scratch>/in/<unit>/, the output goes to
<scratch>/out/<unit>/, and <scratch>/results/<unit>.tsv records status, bytes, md5, read count, and the 454 run
date from the first read header (or the error, on failure).
  gzip     one file, compressed as is, checked by decompressing and comparing byte for byte
  fnaqual  a 454 .fna and its .qual, written as one fastq (Phred+33), checked by record count
"""
import hashlib
import os
import re
import subprocess
import sys
from itertools import zip_longest
from pathlib import Path

THREADS = os.environ.get("SLURM_CPUS_PER_TASK", "4")


def records(path):
    header, chunks = None, []
    with open(path) as fh:
        for line in fh:
            line = line.rstrip("\n\r")
            if line.startswith(">"):
                if header is not None:
                    yield header, chunks
                header, chunks = line[1:], []
            elif line:
                chunks.append(line)
    if header is not None:
        yield header, chunks


def fnaqual(fna, qual, out):
    n = 0
    with open(out, "wb") as fh:
        pigz = subprocess.Popen(["pigz", "-p", THREADS, "-c"], stdin=subprocess.PIPE, stdout=fh)
        for a, b in zip_longest(records(fna), records(qual)):
            if a is None or b is None:
                raise ValueError(f"fna and qual hold different record counts, after {n}")
            (h_seq, seq), (h_q, q) = a, b
            if h_seq.split()[0] != h_q.split()[0]:
                raise ValueError(f"fna and qual disagree at record {n}: {h_seq} / {h_q}")
            seq = "".join(seq)
            scores = " ".join(q).split()
            if len(seq) != len(scores):
                raise ValueError(f"{h_seq}: {len(seq)} bases, {len(scores)} scores")
            pigz.stdin.write(f"@{h_seq}\n{seq}\n+\n{''.join(chr(int(s) + 33) for s in scores)}\n".encode())
            n += 1
        pigz.stdin.close()
        if pigz.wait():
            raise RuntimeError("pigz failed")
    lines = int(subprocess.run(f"pigz -dc '{out}' | wc -l", shell=True, check=True, capture_output=True,
                               text=True).stdout)
    if lines != 4 * n:
        raise ValueError(f"wrote {n} records, read back {lines} lines")
    return n


def gzip(src, out):
    with open(out, "wb") as fh:
        subprocess.run(["pigz", "-p", THREADS, "-c", src], stdout=fh, check=True)
    subprocess.run(f"pigz -dc '{out}' | cmp - '{src}'", shell=True, check=True)
    if src.endswith((".fastq", ".fq")):
        return int(subprocess.run(["wc", "-l", src], capture_output=True, text=True, check=True).stdout.split()[0]) // 4
    return ""


def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 24), b""):
            h.update(block)
    return h.hexdigest()


def main():
    scratch, units, index, n_tasks = Path(sys.argv[1]), sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
    lines = [l.rstrip("\n").split("\t") for l in open(units) if l.strip()]
    (scratch / "results").mkdir(exist_ok=True)
    for unit, kind, name, *inputs in lines[index::n_tasks]:
        result = scratch / "results" / f"{unit}.tsv"
        if result.exists() and result.read_text().split("\t")[1] == "ok":
            continue
        src = [str(scratch / "in" / unit / i) for i in inputs]
        out = scratch / "out" / unit / name
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_name(out.name + ".part")
        try:
            reads = fnaqual(*src, tmp) if kind == "fnaqual" else gzip(src[0], tmp)
            tmp.rename(out)
            run = re.search(r" run=(R_\d{4}_\d\d_\d\d)", open(src[0], errors="replace").readline()) if kind == "fnaqual" else None
            row = [unit, "ok", str(out.stat().st_size), md5(out), str(reads), run.group(1) if run else ""]
        except Exception as err:
            tmp.unlink(missing_ok=True)
            row = [unit, "failed", "", "", "", str(err).replace("\t", " ")[:300]]
        result.write_text("\t".join(row) + "\n")
        print("\t".join(row), flush=True)


if __name__ == "__main__":
    main()
