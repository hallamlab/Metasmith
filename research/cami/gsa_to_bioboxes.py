#!/usr/bin/env python3
"""Convert a CAMI contig-to-genome mapping into CAMI Bioboxes binning format.

Four of the six CAMI II datasets ship the bin gold standard only as
`gsa_mapping.tsv.gz` -- a plain "#anonymous_contig_id genome_id tax_id
contig_id number_reads start_position end_position" table -- rather than the
Bioboxes spelling (`binning_gs.tsv`) that
`metasmith_libraries/transforms/metagenomics/binning/amber.py` requires
(version line, `@SampleID:` line, `@@SEQUENCEID BINID TAXID _LENGTH` table).
Marine and strain already ship both files for the same samples, which is what
`self_check()` below uses as a round-trip proof: convert marine's own
`gsa_mapping.tsv.gz` and diff the result against its shipped `binning_gs.tsv`.

CAMI III's per-sample `gsa_mapping.tsv.gz` (packed inside `sample_N_contigs/`)
already carries an `@SampleID:` line and an `@@SEQUENCEID BINID TAXID LENGTH
...` header -- CAMISIM emits it closer to Bioboxes shape than CAMI II's does.
Both shapes are handled by locating columns by name off whichever header line
is present, rather than by fixed position, and `_LENGTH` is always recomputed
from start/end rather than trusted from an upstream LENGTH column, so the two
inputs produce byte-identical output shape.

CAUTION: the CAMI II toy mouse gut set ships a second mapping,
`gsa_mapping_new.tsv.gz`, beside the first, with different TAXID values for
the same contigs (genome_id agrees, so genome binning is unaffected). This
script always takes the file literally named `gsa_mapping.tsv.gz` -- the
older of the two -- and never `*_new.tsv.gz`.
"""

import argparse
import gzip
import sys
from pathlib import Path

VERSION = "0.9.1"

# Header line -> candidate source column names, tried in order.
_COLUMNS = {
    "SEQUENCEID": ("SEQUENCEID", "anonymous_contig_id", "#anonymous_contig_id"),
    "BINID": ("BINID", "genome_id"),
    "TAXID": ("TAXID", "tax_id"),
    "start_position": ("start_position",),
    "end_position": ("end_position",),
}


def _open(path: Path):
    path = Path(path)
    return gzip.open(path, "rt") if path.suffix == ".gz" else path.open("rt")


def _parse_header(line: str) -> tuple[dict[str, int], str | None]:
    """Column-name -> index off a source header line, plus an inline @SampleID if present."""
    sample_id = None
    if line.startswith("@SampleID:"):
        sample_id = line.strip().split(":", 1)[1]
        return {}, sample_id
    stripped = line.lstrip("#@").strip()
    names = stripped.split("\t")
    index = {name: i for i, name in enumerate(names)}
    return index, sample_id


def convert(gsa_path: Path, out_path: Path, sample_id: str | None = None) -> int:
    """Read a CAMI gsa_mapping table (either shape) and write Bioboxes binning format.

    Returns the number of data rows written. Raises if the source has no
    recognizable header, or if no sample_id is available from either the
    source file's own `@SampleID:` line or the `sample_id` argument.
    """
    column_index: dict[str, int] = {}
    inline_sample_id: str | None = None

    with _open(gsa_path) as fin:
        for raw in fin:
            line = raw.rstrip("\n")
            if not line:
                continue
            if line.startswith("@SampleID:"):
                inline_sample_id = line.split(":", 1)[1]
                continue
            if line.startswith("#") or line.startswith("@@"):
                column_index, _ = _parse_header(line)
                break
            raise AssertionError(f"[{gsa_path}] has no recognizable header before data: {line!r}")
        else:
            raise AssertionError(f"[{gsa_path}] has no header line at all")

        resolved = {}
        for field, candidates in _COLUMNS.items():
            for c in candidates:
                if c in column_index:
                    resolved[field] = column_index[c]
                    break
            else:
                raise AssertionError(f"[{gsa_path}] header has no column for {field}: {sorted(column_index)}")

        label = sample_id or inline_sample_id
        assert label, f"[{gsa_path}] has no @SampleID and none was given"

        out_path.parent.mkdir(parents=True, exist_ok=True)
        n = 0
        with out_path.open("w") as fout:
            fout.write(f"@Version:{VERSION}\n\n@SampleID:{label}\n\n@@SEQUENCEID\tBINID\tTAXID\t_LENGTH\n")
            for raw in fin:
                line = raw.rstrip("\n")
                if not line:
                    continue
                fields = line.split("\t")
                seq = fields[resolved["SEQUENCEID"]]
                bin_id = fields[resolved["BINID"]]
                tax_id = fields[resolved["TAXID"]]
                start = int(fields[resolved["start_position"]])
                end = int(fields[resolved["end_position"]])
                length = end - start + 1
                fout.write(f"{seq}\t{bin_id}\t{tax_id}\t{length}\n")
                n += 1
        return n


def self_check(marine_gsa: Path, marine_binning_gs: Path) -> None:
    """Round-trip proof: convert marine's own gsa_mapping.tsv.gz and diff against
    its shipped binning_gs.tsv. Raises AssertionError on any mismatch."""
    import tempfile

    want = Path(marine_binning_gs).read_text().splitlines()
    want_sample_id = next(
        l.split(":", 1)[1] for l in want if l.startswith("@SampleID:")
    )

    with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        # gsa_mapping.tsv.gz has no @SampleID of its own for this (CAMI II)
        # shape -- take the shipped file's own value, which is what a real
        # conversion run would be told via --sample-id.
        n = convert(marine_gsa, tmp_path, sample_id=want_sample_id)
        got = tmp_path.read_text().splitlines()
        assert got == want, (
            f"round-trip mismatch: {n} rows converted, "
            f"{len(got)} lines produced vs {len(want)} lines shipped "
            f"(first diff at line {next((i for i, (a, b) in enumerate(zip(got, want)) if a != b), min(len(got), len(want)))})"
        )
        print(f"self-check OK: {n} rows, {len(got)} lines, byte-identical to {marine_binning_gs}")
    finally:
        tmp_path.unlink(missing_ok=True)


def fill_corpus(root: Path) -> None:
    """Write contigs/binning_gs.tsv for every sample under root/work that lacks one.

    Reuses build_samples.py's dataset-directory -> samples.tsv label mapping so
    the @SampleID written here matches what samples.tsv calls the same dataset.
    Always takes contigs/gsa_mapping.tsv.gz -- never a `*_new` sibling (see
    module docstring) -- and skips a sample outright if neither exists.
    """
    from build_samples import DATASET_LABELS

    converted = skipped_already = skipped_missing = 0
    for sample_dir in sorted((root / "work").glob("**/reads/anonymous_reads.fq.gz")):
        sample_dir = sample_dir.parent.parent
        top = sample_dir
        while top.parent != root / "work":
            top = top.parent
        dataset = DATASET_LABELS.get(top.name)
        if dataset is None:
            continue

        contigs = sample_dir / "contigs"
        out = contigs / "binning_gs.tsv"
        if out.exists():
            skipped_already += 1
            continue
        gsa = contigs / "gsa_mapping.tsv.gz"
        if not gsa.exists():
            print(f"MISSING gsa_mapping.tsv.gz: {contigs}", file=sys.stderr)
            skipped_missing += 1
            continue

        n = convert(gsa, out, sample_id=dataset)
        print(f"{dataset}/{sample_dir.name}: {n} rows -> {out}")
        converted += 1

    print(f"\n{converted} converted, {skipped_already} already had binning_gs.tsv, "
          f"{skipped_missing} had neither file", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("convert", help="convert one gsa_mapping table to Bioboxes binning format")
    c.add_argument("--gsa", type=Path, required=True)
    c.add_argument("--out", type=Path, required=True)
    c.add_argument("--sample-id", default=None, help="override; else taken from the source's own @SampleID: line")

    s = sub.add_parser("self-check", help="round-trip a marine sample against its shipped binning_gs.tsv")
    s.add_argument("--gsa", type=Path, required=True)
    s.add_argument("--binning-gs", type=Path, required=True)

    f = sub.add_parser("fill-corpus", help="convert every sample under --root/work missing contigs/binning_gs.tsv")
    f.add_argument("--root", type=Path, default=Path("/scratch/phyberos/cami"))

    args = ap.parse_args()
    if args.cmd == "convert":
        n = convert(args.gsa, args.out, args.sample_id)
        print(f"{n} rows -> {args.out}")
    elif args.cmd == "self-check":
        self_check(args.gsa, args.binning_gs)
    elif args.cmd == "fill-corpus":
        fill_corpus(args.root)
    return 0


if __name__ == "__main__":
    sys.exit(main())
