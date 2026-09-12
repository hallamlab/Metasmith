import argparse as _argparse
import ast as _ast

_p = _argparse.ArgumentParser()
_p.add_argument("--ev-lib", required=True)
_p.add_argument("--extensions", required=True)
_p.add_argument("--genomes", required=True)
_p.add_argument("--gpr-paths", required=True)
_p.add_argument("--lane-set", required=True)
_p.add_argument("--out", required=True)
A = _p.parse_args()
_LIT_extensions = _ast.literal_eval(A.extensions)
_LIT_gpr_paths = _ast.literal_eval(A.gpr_paths)

import json, os, sys
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(A.ev_lib))
import fabfos_evidence as fe

GENOMES = Path(A.genomes)
OUT     = Path(A.out)
LANE_SET = A.lane_set
EXTENSIONS = tuple(_LIT_extensions)

# staged mapper table -> the host it describes, BY THE ORF IDS IN IT.
#
# The file name cannot carry this. `host_proteomes` copies each proteome to the path the
# engine assigns -- `{batch}-{i}-{branch}.{hash}-{key}.faa`, which the generated
# nextflow process collects by glob -- so the accession does not survive into the staged
# stem, and the mapper's `source` column is that stem. Attributing on it read the
# accession for exactly as long as nobody ran this step.
#
# The ids do carry it. Every record in a host's proteome is unique to that assembly, so
# the ORFs a table describes name their proteome whatever the file is called. That is the
# same rule check_epi300_identity.py settled on for a different join: go by content.
ACCESSION_FOR_HOST = {}
HOST_FOR_ORF = {}
for d in sorted(p for p in GENOMES.glob("*") if p.is_dir()):
    faa = sorted((d / "genome").glob("*.faa"))
    if len(faa) != 1:
        raise SystemExit(f"[denovo_gpr] expected one proteome under {d}/genome, "
                         f"found {[p.name for p in faa]}")
    ACCESSION_FOR_HOST[d.name] = faa[0].stem
    for line in faa[0].open():
        if line.startswith(">"):
            orf = line[1:].split()[0]
            # A record id shared by two hosts would make the attribution ambiguous, and
            # silence about it would attach one host's table to another. RefSeq protein
            # ids are per-assembly here because the id carries the contig accession.
            if HOST_FOR_ORF.setdefault(orf, d.name) != d.name:
                raise SystemExit(f"[denovo_gpr] ORF id {orf} appears in two host "
                                 f"proteomes -- attribution by id is not possible")

tables = [Path(p) for p in _LIT_gpr_paths]
print(f"[denovo_gpr] {len(tables)} mapper table(s) for {len(ACCESSION_FOR_HOST)} hosts",
      flush=True)

summary = []
seen = set()
for path in tables:
    g = pd.read_parquet(path)
    srcs = sorted(g["source"].unique())
    if len(srcs) != 1:
        raise SystemExit(f"[denovo_gpr] {path.name} carries {len(srcs)} ORF sets "
                         f"{srcs} -- one table must describe one proteome")
    attributed = {HOST_FOR_ORF.get(o) for o in g["orf"].unique()}
    known = sorted(h for h in attributed if h)
    if len(known) != 1 or None in attributed:
        raise SystemExit(
            f"[denovo_gpr] mapper table {path.name} (source {srcs[0]!r}) has ORFs "
            f"from {known or 'no'} host proteome(s)"
            + (", and some belong to none of them" if None in attributed else "")
            + f". The host set is declared in acquire/genomes.py; a table whose ORFs "
              f"are not one host's has no host to attribute it to.")
    host = known[0]
    if host in seen:
        raise SystemExit(f"[denovo_gpr] two mapper tables both name {host}")
    seen.add(host)

    lanes = sorted(g["channel"].unique())
    print(f"[denovo_gpr] {host}: {len(g):,} mapper rows, {g['orf'].nunique():,} ORFs, "
          f"{g['mnxr'].nunique():,} MNXR, lanes {lanes}", flush=True)

    expected = sorted(fe.LANE_SETS[LANE_SET])
    if lanes != expected:
        raise SystemExit(
            f"[denovo_gpr] {host}: lane set is {lanes}, not {expected}. Missing "
            f"{sorted(set(expected) - set(lanes))}; unexpected "
            f"{sorted(set(lanes) - set(expected))}. A lane that contributed no rows is a "
            f"broken join or an unstaged reference, and a table short of the declared set "
            f"rescales every belief weight downstream.")

    # The mapper's own columns ARE the core -- channel keeps the frozen spelling, and
    # `lane_set` is what says these rows are de-novo evidence rather than a curated
    # assertion. This step adds attribution and nothing else.
    df = g.copy()
    df["build_id"] = "denovo_" + host
    df["host"] = host
    # The unit is the proteome, not a model: this table's claim is "this host's own
    # annotation lanes infer these reactions", and naming a GEM here would imply a
    # curated model was consulted, which is the whole thing the de-novo line is not.
    df["unit_id"] = df["source"]
    df["feature_kind"] = "orf"
    df["feature_name"] = df["intermediate_name"]
    # There is no boolean rule: a de-novo call is per ORF, and inventing "orf" as a
    # one-gene rule would make the two tables look like the same kind of claim.
    df["gpr_rule"] = None
    # Not computable here without the bake, and NOT defaulted to True: a row wrongly
    # marked in-universe claims an edge can exist for a reaction that has no atom
    # pairs. Null means "not asserted", which a consumer can see.
    df["in_atom_universe"] = pd.Series([None] * len(df), dtype="object")
    df = df[fe.schema_for(EXTENSIONS)]
    df = df.sort_values(fe.grain_key(EXTENSIONS), kind="mergesort",
                        na_position="last").reset_index(drop=True)
    fe.validate_gpr(df, LANE_SET, None, df["source"].iat[0], EXTENSIONS)
    d = OUT / "hosts" / host
    d.mkdir(parents=True, exist_ok=True)
    df.to_parquet(d / "gpr_denovo.parquet", index=False, compression="zstd")
    print(f"[denovo_gpr] wrote {len(df):,} rows for {host}", flush=True)
    summary.append(dict(host=host, orf_set=srcs[0], lanes=lanes, n_lanes=len(lanes),
                        rows=len(df), orfs=int(df["orf"].nunique()),
                        mnxr=int(df["mnxr"].nunique())))

missing = sorted(set(ACCESSION_FOR_HOST) - seen)
if missing:
    raise SystemExit(f"[denovo_gpr] no mapper table for {missing} -- the de-novo half "
                     f"of the host benchmark is not comparable across a missing host")

# The lane set, by name, per host -- a record of something checked rather than merely
# observed. Every host passed the same gate above, so cross-host comparison is
# like-for-like by construction and needs no warning here.
(OUT / "BUILD.json").write_text(json.dumps(
    dict(lane_set=LANE_SET, extensions=_LIT_extensions, hosts=summary), indent=2))
print(f"[denovo_gpr] {len(summary)} hosts -> {OUT}/hosts/<host>/gpr_denovo.parquet",
      flush=True)
