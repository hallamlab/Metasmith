#!/usr/bin/env python3
"""Move the SCADC annotation and GPR tables from the old insert set to the new one.

    mamba run -n figure-net python tests/retarget_annotations.py --out <dir>
    mamba run -n figure-net python tests/retarget_annotations.py --out <dir> --merge-from <published root>

WHY THIS EXISTS INSTEAD OF A RE-RUN
-----------------------------------
`sequences/inserts/` was rebuilt and went 183 -> 170 inserts, while
`annotations/`, `annotation_alts/` and `gpr/` beside it are still keyed on the
183. Re-running the seven lanes would spend a GPU day reproducing numbers that
are already correct: 166 of the 170 are byte-identical to an insert that was
already annotated, so their 5,254 ORFs, their embeddings and their evidence rows
carry over untouched. This driver carries them over and deletes the rest.

Three of the four remaining inserts are the REVERSE COMPLEMENT of an insert that
was dropped -- the same molecule re-elected under another pool's name, drawn on
the other strand. Their ORFs are the same ORFs, so they are rekeyed rather than
recomputed: prodigal numbers genes by ascending start, so reversing the sequence
reverses the numbering exactly, and every coordinate mirrors against the insert
length with the strand and the partial-end flags swapped. The protein sequence
and every score are untouched, because reverse complement does not change the
gene -- only which end of the record it is measured from.

The fourth is genuinely new and cannot be served from these tables at all. It is
annotated by its own one-insert run (`examples/scadc_gpr.py --inserts ...`) and
folded in here with `--merge-from`.

IT DERIVES ITS OWN MAPPING
--------------------------
Nothing about the old->new correspondence is taken on faith or read out of a
provenance note. Both insert fastas are hashed record by record and each new id
is classified as survivor, reverse complement of a dropped id, or new. The run
refuses unless that comes out 166 / 3 / 1, because an unexamined classification
is the one way this produces a plausible table that is wrong.

IT NEVER WRITES INTO A CHUNK
----------------------------
Materialised DVC files are read-only hardlinks into a cache shared by every
worktree, so editing one in place corrupts that object everywhere. Everything is
written to `--out` and the swap is a separate, deliberate step.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUN = REPO / "data/fabfos/runs/scadc_fosmids"
NEW_INSERTS = RUN / "sequences/inserts/inserts.fna"
DVC_CACHE = Path("/home/tony/agentic_workspace/data/.dvc_cache/files/md5")

EXPECTED = (166, 3, 1)

COMPLEMENT = str.maketrans("ACGTNacgtnRYKMBVDHrykmbvdh", "TGCANtgcanYRMKVBHDyrmkvbhd")

FLAT_TABLES = [
    ("annotations/uniref50/fosmids.uniref50.blast6.tsv", "\t", None),
    ("annotations/kofam/fosmids.kofam.csv", ",", ""),
    ("annotations/clean/fosmids.clean.tsv", "\t", ""),
    ("annotation_alts/ezpred/fosmids.ezpred.csv", ",", ""),
    ("annotation_alts/deepec/fosmids.deepec.tsv", "\t", "Query ID"),
]

SEPARATED = "_SEPARATED_SEQUENCE_"

GPR_TABLES = ["gpr/gpr_4lane.parquet", "gpr/gpr_7lane.parquet"]

EMBEDDINGS = [
    ("annotations/proteinbert/fosmids.pbert.index.csv",
     ["annotations/proteinbert/fosmids.pbert.parquet"]),
    ("annotation_alts/esmc/fosmids.esmc.index.csv",
     ["annotation_alts/esmc/fosmids.esmc.parquet",
      "annotation_alts/esmc/fosmids.esmc.layer_means.npy"]),
]


def read_fasta(path: Path) -> dict[str, tuple[str, str]]:
    out, ident, desc, seq = {}, None, "", []
    for line in Path(path).read_text().splitlines():
        if line.startswith(">"):
            if ident is not None:
                out[ident] = (desc, "".join(seq))
            head = line[1:]
            ident, _, desc = head.partition(" ")
            seq = []
        else:
            seq.append(line.strip())
    if ident is not None:
        out[ident] = (desc, "".join(seq))
    return out


def revcomp(seq: str) -> str:
    return seq.translate(COMPLEMENT)[::-1]


def peel(field: str) -> tuple[str, str]:
    i = field.find(SEPARATED)
    return (field, "") if i < 0 else (field[:i], field[i:])


def split_orf(orf: str) -> tuple[str, int]:
    i = orf.rindex("_")
    return orf[:i], int(orf[i + 1:])


def resolve_old_inserts() -> Path:
    pin = subprocess.run(
        ["git", "-C", str(REPO), "show", "HEAD:data/fabfos/runs/scadc_fosmids/sequences.dvc"],
        capture_output=True, text=True, check=True).stdout
    md5 = next(l.split("md5:")[1].strip() for l in pin.splitlines() if "md5:" in l)
    manifest = json.loads((DVC_CACHE / md5[:2] / md5[2:]).read_text())
    entry = next(e for e in manifest if e["relpath"] == "inserts/inserts.fna")
    return DVC_CACHE / entry["md5"][:2] / entry["md5"][2:]


class Mapping:
    def __init__(self, old_fna: Path, new_fna: Path, orf_counts: dict[str, int],
                 expected: tuple[int, int, int]):
        old, new = read_fasta(old_fna), read_fasta(new_fna)
        self.new_desc = {i: d for i, (d, _) in new.items()}
        self.new_seq = {i: s.upper() for i, (_, s) in new.items()}
        self.new_len = {i: len(s) for i, (_, s) in new.items()}

        survivors = [i for i in new if i in old and old[i][1] == new[i][1]]
        dropped = [i for i in old if i not in set(survivors)]
        by_seq: dict[str, list[str]] = {}
        for i in dropped:
            by_seq.setdefault(hashlib.md5(old[i][1].upper().encode()).hexdigest(), []).append(i)

        self.revcomp: dict[str, str] = {}
        novel = []
        for i in new:
            if i in set(survivors):
                continue
            key = hashlib.md5(revcomp(new[i][1]).upper().encode()).hexdigest()
            hits = by_seq.get(key, [])
            if len(hits) == 1:
                self.revcomp[i] = hits[0]
            elif not hits:
                novel.append(i)
            else:
                sys.exit(f"[x] {i} is the reverse complement of {len(hits)} dropped inserts; ambiguous")

        self.survivors, self.dropped, self.novel = sorted(survivors), sorted(dropped), sorted(novel)
        got = (len(self.survivors), len(self.revcomp), len(self.novel))
        if got != expected:
            sys.exit(f"[x] classification is {got[0]} survivors / {got[1]} reverse-complement / "
                     f"{got[2]} new, expected {expected}. This driver has not been checked "
                     f"against that set; look at it before retargeting onto it.")

        self.insert: dict[str, str] = {i: i for i in self.survivors}
        self.insert.update({old: new for new, old in self.revcomp.items()})
        self.orf: dict[str, str] = {}
        for ins in self.survivors:
            for k in range(1, orf_counts.get(ins, 0) + 1):
                self.orf[f"{ins}_{k}"] = f"{ins}_{k}"
        for new_id, old_id in self.revcomp.items():
            n = orf_counts.get(old_id, 0)
            for k in range(1, n + 1):
                self.orf[f"{old_id}_{k}"] = f"{new_id}_{n + 1 - k}"

    def describe(self):
        print(f"[i] {len(self.survivors)} inserts survive byte-identical")
        print(f"[i] {len(self.revcomp)} inserts are the reverse complement of a dropped insert:")
        for new_id, old_id in sorted(self.revcomp.items()):
            print(f"      {new_id}\n        <- {old_id}  ({self.new_len[new_id]} bp)")
        print(f"[i] {len(self.novel)} insert(s) are new and cannot be served from these tables:")
        for i in self.novel:
            print(f"      {i}  ({self.new_len[i]} bp)")
        print(f"[i] {len(self.dropped)} old inserts dropped")


def mirror(a: int, b: int, length: int) -> tuple[int, int]:
    return length - b + 1, length - a + 1


def flip_attrs(attrs: str, ordinal: int) -> str:
    out = []
    for field in attrs.split(";"):
        if field.startswith("ID="):
            seqnum = field[3:].split("_")[0]
            field = f"ID={seqnum}_{ordinal}"
        elif field.startswith("partial=") and len(field) == 10:
            field = f"partial={field[9]}{field[8]}"
        out.append(field)
    return ";".join(out)


def _faa_records(path: Path, join: bool = False) -> dict:
    recs, ident, head, seq = {}, None, None, []
    for line in Path(path).read_text().splitlines():
        if line.startswith(">"):
            if ident is not None:
                recs[ident] = (head, "".join(seq) if join else seq)
            head = line[1:]
            ident = head.split(" ", 1)[0]
            seq = []
        else:
            seq.append(line)
    if ident is not None:
        recs[ident] = (head, "".join(seq) if join else seq)
    return recs


def rewrite_faa(src: Path, dst: Path, m: Mapping, rank: dict[str, int]) -> list[str]:
    recs = _faa_records(src)
    out = {}
    for old_orf, (head, seq) in recs.items():
        new_orf = m.orf.get(old_orf)
        if new_orf is None:
            continue
        if new_orf != old_orf:
            _, start, end, strand, attrs = [p.strip() for p in head.split("#")]
            ins, ordinal = split_orf(new_orf)
            s, e = mirror(int(start), int(end), m.new_len[ins])
            head = f"{new_orf} # {s} # {e} # {-int(strand)} # {flip_attrs(attrs, ordinal)}"
        out[new_orf] = (head, seq)

    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("w") as fh:
        for orf in sorted(out, key=rank.__getitem__):
            head, seq = out[orf]
            fh.write(f">{head}\n")
            fh.write("\n".join(seq) + "\n")
    return sorted(out, key=rank.__getitem__)


def rewrite_gff(src: Path, dst: Path, m: Mapping, rank: dict[str, int]):
    version, blocks, cur = None, {}, None
    for line in src.read_text().splitlines():
        if line.startswith("##"):
            version = line
        elif line.startswith("# Sequence Data:"):
            cur = {"seqdata": line, "model": None, "cds": []}
            blocks[line.split('seqhdr="')[1].split(" ", 1)[0]] = cur
        elif line.startswith("# Model Data:"):
            cur["model"] = line
        elif line.strip():
            cur["cds"].append(line.split("\t"))

    out = {}
    for old_ins, blk in blocks.items():
        new_ins = m.insert.get(old_ins)
        if new_ins is None:
            continue
        seqdata = blk["seqdata"]
        pre, _, rest = seqdata.partition('seqhdr="')
        seqdata = f'{pre}seqhdr="{new_ins} {m.new_desc[new_ins]}"' + rest[rest.index('"') + 1:]
        cds = []
        for cols in blk["cds"]:
            cols = list(cols)
            if new_ins != old_ins:
                ordinal = split_orf(m.orf[f"{old_ins}_{_gff_ordinal(cols)}"])[1]
                s, e = mirror(int(cols[3]), int(cols[4]), m.new_len[new_ins])
                cols[0], cols[3], cols[4] = new_ins, str(s), str(e)
                cols[6] = "-" if cols[6] == "+" else "+"
                cols[8] = flip_attrs(cols[8].rstrip(";"), ordinal) + ";"
            cds.append(cols)
        cds.sort(key=lambda c: int(c[3]))
        out[new_ins] = (seqdata, blk["model"], cds)

    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("w") as fh:
        fh.write(version + "\n")
        for ins in sorted(out):
            seqdata, model, cds = out[ins]
            fh.write(seqdata + "\n" + model + "\n")
            for cols in cds:
                fh.write("\t".join(cols) + "\n")


def _gff_ordinal(cols: list[str]) -> int:
    for field in cols[8].split(";"):
        if field.startswith("ID="):
            return int(field.split("_")[1])
    raise SystemExit(f"[x] GFF line has no ID= attribute: {cols[:5]}")


def split_blocks(lines: list[str], header: str | None) -> list[tuple[str | None, list[str]]]:
    if header is None:
        return [(None, [l for l in lines if l])]
    if header == "":
        return [(lines[0], [l for l in lines[1:] if l])]
    blocks, cur = [], None
    for line in lines:
        if line.startswith(header):
            cur = (line, [])
            blocks.append(cur)
        elif line and cur is not None:
            cur[1].append(line)
        elif line:
            sys.exit(f"[x] a data line precedes the first '{header}' header")
    return blocks


def rewrite_flat(src: Path, dst: Path, m: Mapping,
                 delim: str, header: str | None, known: set[str]) -> tuple[int, int]:
    out, before, after = [], 0, 0
    for head, rows in split_blocks(src.read_text().splitlines(), header):
        kept = []
        for line in rows:
            before += 1
            field, sep, rest = line.partition(delim)
            orf, suffix = peel(field)
            if orf not in known:
                sys.exit(f"[x] {src.name}: '{field}' is not an ORF of this annotation. "
                         f"Dropping it would be a silent loss; teach the driver its shape.")
            new_orf = m.orf.get(orf)
            if new_orf is None:
                continue
            kept.append(new_orf + suffix + sep + rest)
        after += len(kept)
        out += ([head] if head is not None else []) + kept
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text("\n".join(out) + "\n")
    return before, after


def rewrite_embedding(src_root: Path, dst_root: Path, index_rel: str, matrix_rels: list[str],
                      m: Mapping, rank: dict[str, int], known: set[str]):
    import numpy as np
    import pandas as pd

    idx = pd.read_csv(src_root / index_rel)
    ids = idx["sequence_id"].tolist()
    stray = set(ids) - known
    if stray:
        sys.exit(f"[x] {index_rel}: {len(stray)} ids are not ORFs of this annotation, e.g. "
                 f"{sorted(stray)[:2]}")
    if "index" in idx.columns and idx["index"].tolist() != list(range(len(idx))):
        sys.exit(f"[x] {index_rel} 'index' column is not the row position; "
                 f"the matrices cannot be permuted by it")
    order = [(i, m.orf[o]) for i, o in enumerate(ids) if o in m.orf]
    order.sort(key=lambda t: rank[t[1]])
    rows = [i for i, _ in order]

    out = pd.DataFrame({"sequence_id": [o for _, o in order]})
    if "index" in idx.columns:
        out["index"] = range(len(order))
    (dst_root / index_rel).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(dst_root / index_rel, index=False)

    for rel in matrix_rels:
        if rel.endswith(".npy"):
            a = np.load(src_root / rel, mmap_mode="r")
            np.save(dst_root / rel, np.asarray(a[rows]))
        else:
            df = pd.read_parquet(src_root / rel)
            df.iloc[rows].reset_index(drop=True).to_parquet(dst_root / rel, index=False)
    return len(ids), len(rows)


def rewrite_gpr(src: Path, dst: Path, m: Mapping, known: set[str]) -> tuple[int, int]:
    import pandas as pd
    df = pd.read_parquet(src)
    before = len(df)
    stray = set(df["orf"]) - known
    if stray:
        sys.exit(f"[x] {src.name}: {len(stray)} orf values are not ORFs of this "
                 f"annotation, e.g. {sorted(stray)[:2]}")
    df = df[df["orf"].isin(m.orf)].copy()
    df["orf"] = df["orf"].map(m.orf)
    dst.parent.mkdir(parents=True, exist_ok=True)
    df.reset_index(drop=True).to_parquet(dst, index=False)
    return before, len(df)


def merge_from(out: Path, extra: Path, source_key: str | None):
    import numpy as np
    import pandas as pd

    base_orfs = _faa_records(out / "annotations/fosmids.faa", join=True)
    inc_orfs = _faa_records(extra / "annotations/fosmids.faa", join=True)
    base_ins = {split_orf(o)[0] for o in base_orfs}
    fresh = {split_orf(o)[0] for o in inc_orfs} - base_ins
    controls = {split_orf(o)[0] for o in inc_orfs} & base_ins
    keep = {o for o in inc_orfs if split_orf(o)[0] in fresh}
    print(f"[i] top-up run: {len(inc_orfs)} ORFs over {len(fresh) + len(controls)} inserts")
    print(f"[i]   {len(fresh)} new insert(s), {len(keep)} ORFs -> merged")
    print(f"[i]   {len(controls)} already-annotated insert(s) -> held back as controls")
    if not fresh:
        sys.exit("[x] the top-up run carries no insert this tree lacks")

    compare_controls(out, extra, controls, base_orfs, inc_orfs)

    rank = {o: i for i, o in enumerate(
        sorted(list(base_orfs) + sorted(keep), key=lambda o: (split_orf(o)[0], split_orf(o)[1])))}

    seqmap = plan_seqnums(out / "annotations/fosmids.gff",
                          extra / "annotations/fosmids.gff", fresh)
    _merge_fasta(out / "annotations/fosmids.faa",
                 extra / "annotations/fosmids.faa", rank, keep, seqmap)
    _merge_gff(out / "annotations/fosmids.gff",
               extra / "annotations/fosmids.gff", fresh, seqmap)

    for rel, delim, header in FLAT_TABLES:
        a, b = out / rel, extra / rel
        if not b.exists():
            print(f"[!] {rel}: nothing from the new run")
            continue
        base = split_blocks(a.read_text().splitlines(), header)
        inc = split_blocks(b.read_text().splitlines(), header)
        if [h for h, _ in base] != [h for h, _ in inc]:
            sys.exit(f"[x] {rel}: the two runs' tables are not the same shape "
                     f"({len(base)} vs {len(inc)} sections). Merge it by hand.")
        lines, added = [], 0
        for (head, rows), (_, more) in zip(base, inc):
            more = [l for l in more if peel(l.partition(delim)[0])[0] in keep]
            added += len(more)
            lines += ([head] if head is not None else []) + rows + more
        a.write_text("\n".join(lines) + "\n")
        print(f"[i] {rel}: +{added} rows")

    for index_rel, matrix_rels in EMBEDDINGS:
        bi, xi = pd.read_csv(out / index_rel), pd.read_csv(extra / index_rel)
        take = xi["sequence_id"].isin(keep).to_numpy()
        idx = pd.concat([bi, xi[take]], ignore_index=True)
        order = np.argsort([rank[o] for o in idx["sequence_id"]], kind="stable")
        idx = idx.iloc[order].reset_index(drop=True)
        if "index" in idx.columns:
            idx["index"] = range(len(idx))
        idx.to_csv(out / index_rel, index=False)
        for rel in matrix_rels:
            if rel.endswith(".npy"):
                a = np.concatenate([np.load(out / rel), np.load(extra / rel)[take]], axis=0)
                np.save(out / rel, a[order])
            else:
                d = pd.concat([pd.read_parquet(out / rel),
                               pd.read_parquet(extra / rel)[take]], ignore_index=True)
                d.iloc[order].reset_index(drop=True).to_parquet(out / rel, index=False)
        print(f"[i] {index_rel}: +{int(take.sum())} -> {len(idx)} rows")

    for rel in GPR_TABLES:
        a = pd.read_parquet(out / rel)
        if not (extra / rel).exists():
            print(f"[!] {rel}: THE NEW RUN PRODUCED NO SUCH TABLE. This table still "
                  f"covers one insert fewer than fosmids.faa does.")
            continue
        b = pd.read_parquet(extra / rel)
        b = b[b["orf"].isin(keep)]
        if source_key:
            b = b.assign(source=source_key)
        d = pd.concat([a, b], ignore_index=True)
        d.to_parquet(out / rel, index=False)
        print(f"[i] {rel}: {len(a)} +{len(b)} -> {len(d)}")


def compare_controls(out: Path, extra: Path, controls: set[str],
                     base_orfs: dict, inc_orfs: dict) -> None:
    import pandas as pd
    if not controls:
        print("[!] the top-up run carries no control insert; nothing to compare")
        return

    shared = [o for o in inc_orfs if split_orf(o)[0] in controls]
    missing = [o for o in shared if o not in base_orfs]
    differing = [o for o in shared if o in base_orfs and inc_orfs[o][1] != base_orfs[o][1]]
    if missing or differing:
        sys.exit(f"[x] prodigal disagrees between the two runs on the control inserts: "
                 f"{len(missing)} ORFs absent from the published set, {len(differing)} "
                 f"with a different protein. The merge would be joining two ORF spaces.")
    print(f"[i] controls: all {len(shared)} ORFs identical in id and protein "
          f"across the two runs")

    key = ["orf", "channel", "intermediate_id", "mnxr"]
    for rel in GPR_TABLES:
        if not (extra / rel).exists():
            continue
        a = pd.read_parquet(out / rel)
        b = pd.read_parquet(extra / rel)
        a = a[a["orf"].isin(set(shared))]
        b = b[b["orf"].isin(set(shared))]
        print(f"[i] controls in {rel}:")
        for ch in sorted(set(a["channel"]) | set(b["channel"])):
            ka = set(map(tuple, a[a.channel == ch][key].values))
            kb = set(map(tuple, b[b.channel == ch][key].values))
            both = ka & kb
            same = "-"
            if both:
                ja = a[a.channel == ch].set_index(key)["raw_score"]
                jb = b[b.channel == ch].set_index(key)["raw_score"]
                same = f"{100.0 * sum(1 for k in both if ja[k] == jb[k]) / len(both):.1f}%"
            print(f"[i]     {ch:9s} published {len(ka):>6}  rerun {len(kb):>6}  "
                  f"shared {len(both):>6}  identical score {same}")


def _gff_blocks(path: Path):
    version, out, cur = None, {}, None
    for line in Path(path).read_text().splitlines():
        if line.startswith("##"):
            version = line
        elif line.startswith("# Sequence Data:"):
            cur = {"seqdata": line, "model": None, "cds": []}
            out[line.split('seqhdr="')[1].split(" ", 1)[0]] = cur
        elif line.startswith("# Model Data:"):
            cur["model"] = line
        elif line.strip():
            cur["cds"].append(line.split("\t"))
    return version, out


def _seqnum(blk) -> int:
    return int(blk["seqdata"].split("seqnum=")[1].split(";")[0])


def plan_seqnums(base_gff: Path, extra_gff: Path, fresh: set[str]) -> dict[str, int]:
    _, base = _gff_blocks(base_gff)
    _, inc = _gff_blocks(extra_gff)
    nxt = max(_seqnum(b) for b in base.values()) + 1
    plan = {}
    for ins in sorted(inc):
        if ins in fresh:
            plan[ins] = nxt
            nxt += 1
    return plan


def _merge_fasta(base: Path, extra: Path, rank: dict[str, int], keep: set[str],
                 seqmap: dict[str, int]):
    recs = dict(_faa_records(base))
    for o, (head, seq) in _faa_records(extra).items():
        if o not in keep:
            continue
        n = seqmap.get(split_orf(o)[0])
        if n is not None:
            parts = [p.strip() for p in head.split("#")]
            parts[4] = _renum_id(parts[4], n)
            head = " # ".join(parts)
        recs[o] = (head, seq)
    with base.open("w") as fh:
        for orf in sorted(recs, key=rank.__getitem__):
            head, seq = recs[orf]
            fh.write(f">{head}\n" + "\n".join(seq) + "\n")


def _merge_gff(base: Path, extra: Path, fresh: set[str], seqmap: dict[str, int]):
    version, blocks = _gff_blocks(base)
    _, incoming = _gff_blocks(extra)

    for ins, blk in incoming.items():
        if ins not in fresh:
            continue
        n = seqmap[ins]
        blk["seqdata"] = blk["seqdata"].replace(f"seqnum={_seqnum(blk)};", f"seqnum={n};")
        blk["cds"] = [c[:8] + [_renum_id(c[8], n)] for c in blk["cds"]]
        blocks[ins] = blk

    with base.open("w") as fh:
        fh.write(version + "\n")
        for ins in sorted(blocks):
            blk = blocks[ins]
            fh.write(blk["seqdata"] + "\n" + blk["model"] + "\n")
            for cols in blk["cds"]:
                fh.write("\t".join(cols) + "\n")


def _renum_id(attrs: str, seqnum: int) -> str:
    out = []
    for field in attrs.split(";"):
        if field.startswith("ID="):
            field = f"ID={seqnum}_{field.split('_')[1]}"
        out.append(field)
    return ";".join(out)


_BASES = "TCAG"
_AAS = "FFLLSSSSYY**CC*WLLLLPPPPHHQQRRRRIIIMTTTTNNKKSSRRVVVVAAAADDEEGGGG"
CODONS = {_BASES[n // 16] + _BASES[n // 4 % 4] + _BASES[n % 4]: a
          for n, a in enumerate(_AAS)}


def check_rekey(out: Path, m: Mapping) -> None:
    targets = set(m.revcomp)
    bad, n = [], 0
    for head, prot in _faa_records(out / "annotations/fosmids.faa", join=True).values():
        orf = head.split(" ", 1)[0]
        ins = split_orf(orf)[0]
        if ins not in targets:
            continue
        n += 1
        _, s, e, strand, _ = [p.strip() for p in head.split("#")]
        dna = m.new_seq[ins][int(s) - 1:int(e)]
        if strand == "-1":
            dna = revcomp(dna)
        got = "".join(CODONS.get(dna[i:i + 3], "?") for i in range(0, len(dna) - 2, 3))
        if got[1:] != prot[1:] or (prot[0] != "M" and got[0] != prot[0]):
            bad.append(orf)
    if bad:
        sys.exit(f"[x] {len(bad)} of {n} rekeyed ORFs do not translate to their "
                 f"published protein at the mirrored coordinates, e.g. {bad[:3]}")
    print(f"[i] all {n} rekeyed ORFs translate out of the new insert at their "
          f"mirrored coordinates and match the published protein")


def validate(out: Path):
    import importlib.util
    import pandas as pd

    path = REPO / "src/metasmith_libraries/resources/lib/fabfos_evidence.py"
    spec = importlib.util.spec_from_file_location("fabfos_evidence", path)
    fe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fe)

    orf_ids = set(read_fasta(out / "annotations/fosmids.faa"))
    print(f"[i] validating against {len(orf_ids)} ORFs in fosmids.faa")
    for rel in GPR_TABLES:
        df = pd.read_parquet(out / rel)
        fe.validate_gpr(df, df["lane_set"].iat[0], orf_ids, df["source"].iat[0])
    print("[+] both tables pass validate_gpr")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=REPO / "data/fabfos/scratch/retarget",
                    help="tree to write; never a materialised chunk")
    ap.add_argument("--inserts", type=Path, default=NEW_INSERTS,
                    help="the insert set to retarget ONTO")
    ap.add_argument("--old-inserts", type=Path, default=None,
                    help="the superseded set (default: HEAD's sequences.dvc via the cache)")
    ap.add_argument("--src", type=Path, default=RUN,
                    help="run root holding annotations/, annotation_alts/, gpr/")
    ap.add_argument("--expect", default=",".join(str(n) for n in EXPECTED),
                    help="survivors,reverse-complement,new -- refuses on anything else")
    ap.add_argument("--merge-from", type=Path, default=None,
                    help="a published one-insert tree to fold into --out (skips the retarget)")
    ap.add_argument("--source-key", default=None,
                    help="rewrite the merged rows' gpr 'source' to this task key")
    ap.add_argument("--validate", action="store_true",
                    help="run validate_gpr over --out and stop (also run after a merge)")
    args = ap.parse_args()

    if args.validate and not args.merge_from:
        validate(args.out)
        return

    if args.merge_from:
        merge_from(args.out, args.merge_from, args.source_key)
        validate(args.out)
        return

    old_fna = args.old_inserts or resolve_old_inserts()
    print(f"[i] old inserts: {old_fna}")
    print(f"[i] new inserts: {args.inserts}")

    counts: dict[str, int] = {}
    for line in (args.src / "annotations/fosmids.faa").read_text().splitlines():
        if line.startswith(">"):
            ins, k = split_orf(line[1:].split(" ", 1)[0])
            counts[ins] = max(counts.get(ins, 0), k)

    m = Mapping(old_fna, args.inserts, counts, tuple(int(n) for n in args.expect.split(",")))
    m.describe()

    new_orfs = sorted(m.orf.values(), key=lambda o: (split_orf(o)[0], split_orf(o)[1]))
    rank = {o: i for i, o in enumerate(new_orfs)}
    print(f"[i] {sum(counts.values())} ORFs -> {len(new_orfs)} "
          f"({sum(counts.values()) - len(new_orfs)} dropped with their inserts)")

    if args.out.exists():
        shutil.rmtree(args.out)
    args.out.mkdir(parents=True)

    kept = rewrite_faa(args.src / "annotations/fosmids.faa",
                       args.out / "annotations/fosmids.faa", m, rank)
    assert kept == new_orfs, "fosmids.faa does not carry the mapped ORF set"
    rewrite_gff(args.src / "annotations/fosmids.gff",
                args.out / "annotations/fosmids.gff", m, rank)
    print(f"[i] annotations/fosmids.{{faa,gff}}: {len(kept)} ORFs")
    check_rekey(args.out, m)

    known = {f"{ins}_{k}" for ins, n in counts.items() for k in range(1, n + 1)}
    for rel, delim, header in FLAT_TABLES:
        before, after = rewrite_flat(args.src / rel, args.out / rel, m,
                                     delim, header, known)
        print(f"[i] {rel}: {before} -> {after}")

    for index_rel, matrix_rels in EMBEDDINGS:
        before, after = rewrite_embedding(args.src, args.out, index_rel, matrix_rels,
                                          m, rank, known)
        print(f"[i] {index_rel}: {before} -> {after} rows")

    for rel in GPR_TABLES:
        before, after = rewrite_gpr(args.src / rel, args.out / rel, m, known)
        print(f"[i] {rel}: {before} -> {after}")

    print(f"[+] wrote {args.out}")
    if m.novel:
        print(f"[!] {len(m.novel)} insert(s) still have no ORFs; annotate them and "
              f"fold the result in with --merge-from")


if __name__ == "__main__":
    main()
