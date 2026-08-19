#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import os
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, "/opt/conda/envs/metasmith_env/lib/python3.12/site-packages")
for _p in ("/msm_home/lib", os.environ.get("FABFOS_LIB", "")):
    if _p and Path(_p).is_dir():
        sys.path.insert(0, _p)

import fabfos_evidence as fe  # noqa: E402

DELIM = "::"
LANE_SET = "chosen_4"
TASK_I = int(os.environ.get("SLURM_ARRAY_TASK_ID", "0"))
TASK_N = int(os.environ.get("SHARD_N", "1"))

ORFS_DIR = Path(os.environ.get(
    "CYANOVERSE_ORFS",
    "/project/rpp-shallam/phyberos/cyanoverse/data/orfs_and_metabuli"
    "/sequences-open_reading_frames"))

EXPECT_LIB_SHA = os.environ.get("FABFOS_LIB_SHA256", "")


def lib_digest() -> str:
    return hashlib.sha256(Path(fe.__file__).read_bytes()).hexdigest()


def check_lib() -> str:
    got = lib_digest()
    if EXPECT_LIB_SHA and got != EXPECT_LIB_SHA:
        raise SystemExit(
            f"REFUSING: {fe.__file__} digests {got[:16]}, expected "
            f"{EXPECT_LIB_SHA[:16]}. The delivered tables would be re-validated "
            f"against a different contract than the one they were written "
            f"under, and a widened score range or an added lane set would pass "
            f"in silence.")
    return got


def read_parts(gpr: Path) -> dict[str, list[str]]:
    out: dict[str, set[str]] = defaultdict(set)
    with (gpr / "parts.tsv").open() as fh:
        next(fh)
        for line in fh:
            shard, _order, sample, _start, _n = line.rstrip("\n").split("\t")
            out[sample].add(shard)
    return {k: sorted(v) for k, v in out.items()}


def all_shards(gpr: Path) -> list[str]:
    seen = set()
    with (gpr / "parts.tsv").open() as fh:
        next(fh)
        for line in fh:
            seen.add(line.split("\t", 1)[0])
    return sorted(seen)


def shard_tables(roots: list[Path]) -> dict[str, Path]:
    found: dict[str, Path] = {}
    for root in roots:
        if not root.is_dir():
            continue
        for p in sorted(root.rglob("*.parquet")):
            rp = p.resolve()
            try:
                names = set(pq.ParquetFile(rp).schema_arrow.names)
            except Exception:
                continue
            if not set(fe.SCHEMA_COLS) <= names:
                continue
            batch = next(pq.ParquetFile(rp).iter_batches(
                batch_size=1, columns=["source"]), None)
            if batch is None or batch.num_rows == 0:
                continue
            stem = batch.column("source")[0].as_py()
            if stem in found and found[stem] != rp:
                raise SystemExit(
                    f"REFUSING: two products both claim source={stem!r}:\n"
                    f"  {found[stem]}\n  {rp}\n"
                    f"They may come from different library commits, so taking "
                    f"either would make the delivered corpus a mixture. This "
                    f"happens when a pilot run and a campaign batch both cover "
                    f"the shard. Pass only the run directories that belong to "
                    f"the campaign, or delete the superseded run.")
            found[stem] = rp
    return found


def _marker(parts_dir: Path, shard: str) -> Path:
    return parts_dir / f"_{shard}.done"


def _read_marker(parts_dir: Path, shard: str) -> tuple[int, set[str]] | None:
    m = _marker(parts_dir, shard)
    if not m.exists():
        return None
    lines = m.read_text().splitlines()
    rows = int(lines[0].split("\t")[0])
    return rows, set(lines[1:])


def partition(gpr: Path, roots: list[Path]) -> int:
    check_lib()
    parts_dir = gpr / "split" / "parts"
    tables = shard_tables(roots)
    if not tables:
        raise SystemExit(
            f"REFUSING: no gpr_table parquet found under {[str(r) for r in roots]}. "
            f"An unmatched shell glob reaches here as a literal pattern and "
            f"would otherwise make every array task exit 0 having done nothing, "
            f"turning up one phase later as a missing part.")

    known = set(all_shards(gpr))
    for stem in sorted(set(tables) - known):
        print(f"[{TASK_I}] ignoring {stem}: collected, but not in parts.tsv "
              f"(expected for the control shard; anything else means the "
              f"resharder's record and the run results disagree)")

    mine = all_shards(gpr)[TASK_I::TASK_N]
    skipped = 0
    for shard in mine:
        if _read_marker(parts_dir, shard) is not None:
            continue
        if shard not in tables:
            skipped += 1
            continue
        df = pq.read_table(tables[shard]).to_pandas()
        if list(df.columns) != fe.SCHEMA_COLS:
            raise SystemExit(f"REFUSING {shard}: columns are not the schema")
        sample = df["orf"].str.split(DELIM, n=1).str[0]
        if sample.isna().any() or (sample == df["orf"]).any():
            bad = df.loc[sample == df["orf"], "orf"].unique()[:5]
            raise SystemExit(
                f"REFUSING {shard}: {len(bad)} ORF id(s) carry no {DELIM!r} "
                f"prefix, e.g. {list(bad)}. The sample is recoverable ONLY from "
                f"that prefix, so an unprefixed id has no assembly to go to.")
        emitted = []
        for smp, grp in df.groupby(sample, sort=False):
            d = parts_dir / smp
            d.mkdir(parents=True, exist_ok=True)
            tmp = d / f"{shard}.parquet.part.{os.getpid()}"
            grp.to_parquet(tmp, index=False)
            tmp.rename(d / f"{shard}.parquet")
            emitted.append(smp)
        m = _marker(parts_dir, shard)
        m.parent.mkdir(parents=True, exist_ok=True)
        tmp = m.with_suffix(f".done.{os.getpid()}")
        tmp.write_text(f"{len(df)}\t{len(emitted)}\n" + "\n".join(sorted(emitted)) + "\n")
        tmp.rename(m)
        print(f"[{TASK_I}] {shard}: {len(df):,} rows -> {len(emitted)} assemblies",
              flush=True)
    print(f"[{TASK_I}] partition done: {len(mine)} shard(s), "
          f"{skipped} not yet collected", flush=True)
    return 0


def compact(gpr: Path) -> int:
    check_lib()
    parts_dir = gpr / "split" / "parts"
    out_dir = gpr / "split" / "tables"
    expected = read_parts(gpr)
    mine = sorted(expected)[TASK_I::TASK_N]
    out_dir.mkdir(parents=True, exist_ok=True)
    notes_dir = gpr / "split" / "notes"
    notes_dir.mkdir(parents=True, exist_ok=True)
    empty = notes_dir / f"no_evidence.{TASK_I}.tsv"
    empty.write_text("sample\torfs\n")

    for sample in mine:
        out = out_dir / f"{sample}.gpr.parquet"
        if out.exists():
            continue
        want = expected[sample]
        have = []
        for shard in want:
            p = parts_dir / sample / f"{shard}.parquet"
            if p.exists():
                have.append(p)
                continue
            mk = _read_marker(parts_dir, shard)
            if mk is None:
                raise SystemExit(
                    f"REFUSING {sample}: its slice in {shard} is absent and "
                    f"{shard} carries no completion marker, so the shard was "
                    f"never partitioned. Run `partition` to completion first; "
                    f"compacting now would ship a table missing the ORFs that "
                    f"shard holds.")
            if sample in mk[1]:
                raise SystemExit(
                    f"REFUSING {sample}: {shard} reports having emitted this "
                    f"sample, but {p} is absent. The part was lost after the "
                    f"marker was written -- delete {_marker(parts_dir, shard)} "
                    f"and re-partition that shard.")

        ids = _fasta_ids(ORFS_DIR / f"{sample}.faa")
        if not have:
            df = pd.DataFrame({c: pd.Series(dtype="object")
                               for c in fe.SCHEMA_COLS})
            with empty.open("a") as fh:
                fh.write(f"{sample}\t{len(ids)}\n")
            print(f"[{TASK_I}] {sample}: {len(ids):,} ORF(s), NO evidence in any "
                  f"lane -- empty table delivered", flush=True)
        else:
            df = pd.concat([pd.read_parquet(p) for p in have], ignore_index=True)
            pre = f"{sample}{DELIM}"
            df["orf"] = df["orf"].str.slice(len(pre))
            df["source"] = sample
            df = df.sort_values(["orf", "channel", "intermediate_id", "mnxr"],
                                kind="stable").reset_index(drop=True)
            _validate(df, ids, sample)

        tmp = out.with_suffix(f".parquet.part.{os.getpid()}")
        df.to_parquet(tmp, index=False)
        tmp.rename(out)
        if have:
            print(f"[{TASK_I}] {sample}: {len(df):,} rows, "
                  f"{df['orf'].nunique():,}/{len(ids):,} ORFs, "
                  f"{len(have)} of {len(want)} shard(s)", flush=True)
    print(f"[{TASK_I}] compact done: {len(mine)} sample(s)", flush=True)
    return 0


def _fasta_ids(faa: Path) -> set[str]:
    if not faa.exists():
        raise SystemExit(f"REFUSING: {faa} absent; nothing to validate against")
    ids = set()
    with faa.open("rb") as fh:
        for line in fh:
            if line[:1] == b">":
                ids.add(line[1:].split()[0].decode())
    return ids


def _validate(df, ids: set[str], sample: str) -> None:
    fe.validate_gpr(df, LANE_SET, ids, sample)


def finish(gpr: Path) -> int:
    digest = check_lib()
    expected = set(read_parts(gpr))
    parts_dir = gpr / "split" / "parts"
    out_dir = gpr / "split" / "tables"
    got = {p.name[: -len(".gpr.parquet")] for p in out_dir.glob("*.gpr.parquet")}

    unpartitioned = [s for s in all_shards(gpr)
                     if _read_marker(parts_dir, s) is None]
    if unpartitioned:
        raise SystemExit(
            f"REFUSING: {len(unpartitioned):,} shard(s) were never partitioned, "
            f"e.g. {unpartitioned[:5]}. Their assemblies' tables would be "
            f"missing exactly the ORFs those shards hold, and every other check "
            f"here would still pass.")

    if got != expected:
        raise SystemExit(
            f"REFUSING to call the split complete: {len(got):,} tables for "
            f"{len(expected):,} manifest samples. missing="
            f"{sorted(expected - got)[:5]} extra={sorted(got - expected)[:5]}")

    rows_in = sum(_read_marker(parts_dir, s)[0] for s in all_shards(gpr))

    rows = 0
    per_channel: dict[str, int] = defaultdict(int)
    per_sample = []
    for p in sorted(out_dir.glob("*.gpr.parquet")):
        t = pq.read_table(p, columns=["channel"])
        n = len(t)
        rows += n
        for pair in t.column("channel").value_counts():
            per_channel[pair["values"].as_py()] += pair["counts"].as_py()
        per_sample.append((p.name[: -len(".gpr.parquet")], n))

    if rows != rows_in:
        raise SystemExit(
            f"REFUSING: {rows_in:,} rows across the shard tables but {rows:,} "
            f"across the delivered tables ({rows_in - rows:+,}). Every row of "
            f"every shard belongs to exactly one assembly, so these must be "
            f"equal; a difference means the transpose lost or duplicated rows.")

    empty_tables = [s for s, n in per_sample if n == 0]
    no_ev = _collect_notes(gpr, "no_evidence")

    missing_ch = [c for c in fe.LANE_SETS[LANE_SET] if per_channel.get(c, 0) == 0]
    if missing_ch:
        raise SystemExit(
            f"REFUSING: channel(s) {missing_ch} carry zero rows across all "
            f"{len(got):,} assemblies. A lane that is empty corpus-wide is an "
            f"unstaged reference or a broken join, not a biological result.")

    summary = gpr / "split" / "per_assembly_rows.tsv"
    with summary.open("w") as fh:
        fh.write("sample\trows\n")
        for s, n in per_sample:
            fh.write(f"{s}\t{n}\n")
    print(f"{len(got):,} per-assembly tables, {rows:,} evidence rows "
          f"(== {rows_in:,} in the shard tables)")
    for c in fe.LANE_SETS[LANE_SET]:
        print(f"  {c:9s} {per_channel[c]:>14,} rows "
              f"({100.0 * per_channel[c] / rows:5.1f}%)")
    print(f"  {len(empty_tables):,} assemblies with no evidence in any lane "
          f"(empty tables delivered)")
    print(f"  contract digest {digest[:16]}")
    print(f"per-assembly row counts -> {summary}")
    if len(no_ev) != len(empty_tables):
        print(f"  NOTE: {len(no_ev):,} no-evidence notes for "
              f"{len(empty_tables):,} empty tables (a resumed run re-counts)")
    return 0


def _collect_notes(gpr: Path, stem: str) -> list[str]:
    out = []
    for p in sorted((gpr / "split" / "notes").glob(f"{stem}.*.tsv")):
        out += [ln for ln in p.read_text().splitlines()[1:] if ln]
    (gpr / "split" / f"{stem}.tsv").write_text("\n".join(out) + "\n" if out else "")
    return out


def main() -> int:
    mode = sys.argv[1]
    gpr = Path(sys.argv[2])
    if mode == "partition":
        return partition(gpr, [Path(p) for p in sys.argv[3:]])
    if mode == "compact":
        return compact(gpr)
    if mode == "finish":
        return finish(gpr)
    if mode == "digest":
        print(lib_digest())
        return 0
    raise SystemExit(f"unknown mode {mode!r}; want partition|compact|finish|digest")


if __name__ == "__main__":
    sys.exit(main())
