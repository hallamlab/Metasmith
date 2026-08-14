#!/usr/bin/env python3
"""Turn the 994 shard GPR tables into the 2,844 per-assembly tables asked for.

    SLURM_ARRAY_TASK_ID=<i> SHARD_N=<n> python split_by_assembly.py partition <gpr> <results...>
    SLURM_ARRAY_TASK_ID=<i> SHARD_N=<n> python split_by_assembly.py compact   <gpr>
    python split_by_assembly.py finish <gpr>

Two phases, because the two directions of the transpose want different memory.
`partition` is per shard and reads one shard table; `compact` is per sample and
reads that sample's parts. Both are arrayable and both skip completed work, so a
wall-clock kill costs one unit rather than the pass.

WHY A SPLITTER AND NOT PARTITIONED OUTPUT FROM THE JOINER. A shard holds ~35
assemblies, so hive-partitioning inside `gpr_4lane` would have changed what the
transform declares as its product and broken `validate_gpr`'s single-source
assumption -- a run-identity change, mid-campaign, for a saving a post-hoc pass
gets for free. The sample is already in the `{sample}::{orf}` id prefix, so no
join is needed to recover it.

WHAT GETS REWRITTEN, AND WHY IT IS NOT COSMETIC. `source` NAMES THE ORF SET
(fabfos_evidence.py:34), and the joiner sets it to the shard's stem. Shipping
that would give 2,844 tables whose own `source` column names ~35 assemblies the
reader did not ask for. So `source` becomes the sample and the `{sample}::`
prefix comes off `orf`, restoring the ids the sample's own fasta carries -- which
is also what makes the re-validation below a real check rather than a tautology.

TWO POPULATIONS THIS PASS MUST NOT CONFUSE, because they look identical on disk
and only one is a defect:

  * An assembly with NO EVIDENCE AT ALL. The corpus minimum is 1 ORF; one ORF
    that no lane calls is biology, and it has no group in any shard table, hence
    no part file. Refusing that as a lost partition would block delivery on the
    smallest assemblies -- so `partition` records which samples it emitted, and
    `compact` treats an absent part as legitimate ONLY when that shard's marker
    proves the shard was processed and did not emit the sample.
  * A LOST PART -- a partition task killed mid-shard, a truncated write. This
    must still refuse, and does: the marker is written last, so an absent part
    with no marker, or with a marker that lists the sample, is a real failure.
"""
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

# The corpus as it was BEFORE sharding. Compaction validates each delivered
# table against the assembly's own fasta, which is what makes the stray-id check
# a real test of the split rather than a restatement of it.
ORFS_DIR = Path(os.environ.get(
    "CYANOVERSE_ORFS",
    "/project/rpp-shallam/phyberos/cyanoverse/data/orfs_and_metabuli"
    "/sequences-open_reading_frames"))

# Above this many ORFs, an assembly missing a whole evidence channel is not
# biology. Every lane calls a large fraction of ORFs -- CLEAN predicts an EC for
# every sequence, DIAMOND hit ~96.6% of a measured 100,000 -- so a 1,000-ORF
# assembly with zero rows in some channel means that channel's evidence was lost
# upstream, and shipping it would be a table whose own `lane_set` column claims
# four lanes it does not have.
RELAX_MAX_ORFS = int(os.environ.get("CYANOVERSE_RELAX_MAX_ORFS", "1000"))

# The contract this pass re-validates against is a COPY on the cluster, not the
# `src/metasmith_libraries` file `gpr_4lane` ran under. A drift that reorders
# SCHEMA_COLS refuses loudly; a drift that WIDENS a score range or adds a lane
# set would pass silently and make the re-validation a decoration. Pinned by
# digest, recorded next to the delivered tables.
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
    """sample -> [shard, ...]; the resharder's own record of where it put things."""
    out: dict[str, set[str]] = defaultdict(set)
    with (gpr / "parts.tsv").open() as fh:
        next(fh)
        for line in fh:
            shard, _order, sample, _start, _n = line.rstrip("\n").split("\t")
            out[sample].add(shard)
    return {k: sorted(v) for k, v in out.items()}


def all_shards(gpr: Path) -> list[str]:
    """Every shard the resharder wrote, from `parts.tsv`.

    The stride is taken over THIS list and never over what happens to be on
    disk. An array task that computed its slice from the live results would get
    a different slice depending on when it started -- a batch landing mid-array
    changes the list -- and two tasks would then process one shard concurrently
    onto the same paths. `reshard.py` paid for that lesson once already.
    """
    seen = set()
    with (gpr / "parts.tsv").open() as fh:
        next(fh)
        for line in fh:
            seen.add(line.split("\t", 1)[0])
    return sorted(seen)


def shard_tables(roots: list[Path]) -> dict[str, Path]:
    """shard stem -> its gpr_table parquet, keyed by the table's OWN `source`.

    Not by filename: products land under content-addressed names, so the file a
    shard produced is identified by what it says it is, never by where it sits.
    Only ONE row is read to get there -- `validate_gpr` has already proved the
    column single-valued, and reading it whole 994 times is a corpus-sized scan
    to learn 994 strings.

    Two files claiming one shard is a REFUSAL and not a merge -- but not because
    it would double-count. It cannot: parts are written keyed by shard stem, so
    a second collection overwrites. It refuses because the two runs may have
    been produced under different library commits, and picking either silently
    would make the delivered corpus a mixture. Pass only the run directories
    that belong to the campaign.
    """
    found: dict[str, Path] = {}
    for root in roots:
        if not root.is_dir():
            continue
        for p in sorted(root.rglob("*.parquet")):
            rp = p.resolve()      # a symlinked run dir is not two collections
            # Decide what this file IS from its schema, never from whether
            # reading it happened to raise. `iter_batches(columns=["source"])`
            # does NOT fail on a parquet without that column -- it returns a
            # batch that simply lacks it, and the KeyError then lands on the
            # `.column()` call OUTSIDE any guard. The results directory holds
            # the ProteinBERT embeddings parquet, which is exactly that case,
            # so an exception-shaped guard passes the unit tests and dies on
            # the first real run.
            try:
                names = set(pq.ParquetFile(rp).schema_arrow.names)
            except Exception:
                continue                       # not readable as parquet at all
            if not set(fe.SCHEMA_COLS) <= names:
                continue                       # not a gpr_table
            batch = next(pq.ParquetFile(rp).iter_batches(
                batch_size=1, columns=["source"]))
            if batch.num_rows == 0:
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


# ---------------------------------------------------------------------------


def _marker(parts_dir: Path, shard: str) -> Path:
    return parts_dir / f"_{shard}.done"


def _read_marker(parts_dir: Path, shard: str) -> tuple[int, set[str]] | None:
    """(rows, samples emitted) for a completed shard, or None if not done."""
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

    # A collected table whose shard is not in `parts.tsv` is dropped -- correctly,
    # since the control shard (`shard_9000`) lives in the same agent home and its
    # kofam lane was computed fresh rather than reused, so it must NEVER reach a
    # delivered table. But dropping it in silence is how a REAL shard missing
    # from `parts.tsv` would also disappear, so say which ones and why.
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
            skipped += 1        # not collected yet; `finish` refuses on the gap
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
            # PID-unique, so two tasks that somehow collide cannot rename over
            # each other's half-written file and leave a truncated part behind
            # a marker that says the shard is done.
            tmp = d / f"{shard}.parquet.part.{os.getpid()}"
            grp.to_parquet(tmp, index=False)
            tmp.rename(d / f"{shard}.parquet")
            emitted.append(smp)
        m = _marker(parts_dir, shard)
        m.parent.mkdir(parents=True, exist_ok=True)
        # Written LAST and listing what was emitted: that is what lets `compact`
        # tell "this sample had no evidence in this shard" from "this part was
        # lost". Without the list the two are indistinguishable.
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
    relaxed = notes_dir / f"relaxed_channels.{TASK_I}.tsv"
    empty = notes_dir / f"no_evidence.{TASK_I}.tsv"
    relaxed.write_text("sample\torfs\tmissing\n")
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
            # else: the shard was processed and this sample contributed no
            # evidence row there. Legitimate; nothing to collect.

        ids = _fasta_ids(ORFS_DIR / f"{sample}.faa")
        if not have:
            # No evidence anywhere in the corpus for this assembly. Deliver an
            # empty table with the right schema rather than no table: the
            # delivery contract is one file per assembly, and a silently absent
            # file is indistinguishable from a lost one. `validate_gpr` refuses
            # a zero-row table by design, so it is not called; `finish` counts
            # these and reports them.
            df = pd.DataFrame({c: pd.Series(dtype="object")
                               for c in fe.SCHEMA_COLS})
            with empty.open("a") as fh:
                fh.write(f"{sample}\t{len(ids)}\n")
            print(f"[{TASK_I}] {sample}: {len(ids):,} ORF(s), NO evidence in any "
                  f"lane -- empty table delivered", flush=True)
        else:
            df = pd.concat([pd.read_parquet(p) for p in have], ignore_index=True)
            # The rewrite. `orf` loses the prefix this splitter used to route
            # it, and `source` stops naming the shard and starts naming the file
            # it is in.
            pre = f"{sample}{DELIM}"
            df["orf"] = df["orf"].str.slice(len(pre))
            df["source"] = sample
            df = df.sort_values(["orf", "channel", "intermediate_id", "mnxr"],
                                kind="stable").reset_index(drop=True)
            _validate(df, ids, sample, relaxed)

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


def _validate(df, ids: set[str], sample: str, relaxed: Path) -> None:
    """`validate_gpr` unchanged, with EXACTLY ONE check relaxed, and BOUNDED.

    Its completeness check refuses a table missing any of the four channels.
    That is right for a 100,000-ORF shard and wrong for an assembly of one ORF,
    and the corpus has those (per-sample minimum is 1, median 6,081). A single
    ORF with no KOfam hit is a biological fact, not a broken join.

    But the relaxation must not extend to a LARGE assembly, where a whole
    missing channel means the evidence was lost upstream -- a header-only legacy
    kofam file, say, which the shard-level checks cannot see because the other
    ~34 samples in that shard keep the shard's own count non-zero. So above
    RELAX_MAX_ORFS ORFs the original refusal stands, and every relaxed sample is
    written to a file `finish` reads, rather than to a print in one of 32 logs.

    Everything else -- schema, nulls, score kinds, ranges, MNXR pattern,
    evidence quality, the grain key, and the stray-id check against the sample's
    OWN fasta -- runs exactly as it does inside the transform. The `unknown
    channel` check is NOT weakened: `observed` is a subset of the declared
    tuple, so a channel outside the vocabulary still lands in `unknown`.
    """
    saved = fe.LANE_SETS[LANE_SET]
    observed = tuple(c for c in saved if (df["channel"] == c).any())
    missing = sorted(set(saved) - set(observed))
    if missing and len(ids) > RELAX_MAX_ORFS:
        raise SystemExit(
            f"REFUSING {sample}: {len(ids):,} ORFs but channel(s) {missing} "
            f"contributed zero rows. At this size that is lost evidence, not "
            f"biology -- most likely this assembly's legacy lane input was "
            f"empty or header-only, which no shard-level check can see because "
            f"the shard's other member samples keep its own count non-zero. "
            f"Shipping it would give a table whose `lane_set` column claims "
            f"four lanes it does not have.")
    try:
        fe.LANE_SETS[LANE_SET] = observed
        fe.validate_gpr(df, LANE_SET, ids, sample)
    finally:
        fe.LANE_SETS[LANE_SET] = saved
    if missing:
        with relaxed.open("a") as fh:
            fh.write(f"{sample}\t{len(ids)}\t{','.join(missing)}\n")


def finish(gpr: Path) -> int:
    """The delivery gate: the recovered sample set IS the manifest's, no row was
    lost between the shard tables and the delivered ones, and all four channels
    are non-zero across the corpus."""
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

    # The ledger. Rows in == rows out, summed over the whole corpus. Without it
    # a lane silently dropped for hundreds of assemblies still delivers green.
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
    relaxed = _collect_notes(gpr, "relaxed_channels")
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
    print(f"  {len(relaxed):,} small assemblies missing >=1 channel "
          f"(<= {RELAX_MAX_ORFS} ORFs, allowed)")
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
