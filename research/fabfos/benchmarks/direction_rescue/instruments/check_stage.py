# Gate a staged bake chunk BEFORE `dvc add`, per the r8 precedent at `ed9331d`.
#
# A suffixed chunk is read by nothing, so every property that matters about it has to be
# asserted here rather than inferred from a green test suite later. Five checks:
#
#   1. ZERO INODES SHARED with the deployed tree. The deployed files are read-only hardlinks
#      into the shared DVC cache pinned by ~12 sibling worktrees and by every historical
#      commit; writing through a shared inode would corrupt r8 for all of them. This is the
#      one check whose failure is destructive rather than merely wrong.
#   2. INTERNAL HARDLINK PAIRS PRESERVED. `cp -a` is used precisely because the chunk has its
#      own internal pairs (the ledger and its aam_ledger seam; each aam_cache/*.attempted and
#      its logs/ twin). Discovered from the deployed tree rather than hardcoded, so a new pair
#      is covered without editing this file.
#   3. EXACTLY the four intended files differ, and no others.
#   4. logs/ and aam_cache/ BYTE-IDENTICAL -- they are inputs the next full bake stages from,
#      and a retrieval once rewrote them from an empty run.
#   5. Criterion 17: sha256(seams/direction_annotation.parquet) == the `src_direction_sha256`
#      that direction.parquet declares in its `ecspr_bake_file` metadata block. Verified to
#      hold on r8 at 96cc532c, so the check is known to be live rather than vacuously true.
import hashlib, json, os, sys
from collections import defaultdict
from pathlib import Path
import pyarrow.parquet as pq

SWAP = {"direction.parquet",
        "seams/direction_annotation.parquet",
        "seams/direction_member_eq.parquet",
        "seams/direction_member_dgbyg.parquet"}

def walk(root):
    out = {}
    for dirpath, _, names in os.walk(root):
        for n in names:
            p = Path(dirpath) / n
            out[str(p.relative_to(root))] = p
    return out

def sha(p, buf=1 << 20):
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for b in iter(lambda: fh.read(buf), b""):
            h.update(b)
    return h.hexdigest()

dep, stg = Path(sys.argv[1]), Path(sys.argv[2])
D, S = walk(dep), walk(stg)
bad = []
print(f"deployed {dep}  ({len(D):,} files)")
print(f"staged   {stg}  ({len(S):,} files)\n")

if set(D) != set(S):
    bad.append(f"file set differs: only-deployed {sorted(set(D)-set(S))[:5]}, "
               f"only-staged {sorted(set(S)-set(D))[:5]}")

shared = [r for r in set(D) & set(S) if D[r].stat().st_ino == S[r].stat().st_ino]
print(f"[1] {'ok' if not shared else 'FAIL'} shared inodes with deployed: {len(shared)}")
if shared:
    bad.append(f"{len(shared)} files share an inode with the deployed tree: {shared[:5]}")

def pairs(tbl):
    by = defaultdict(list)
    for r, p in tbl.items():
        st = p.stat()
        if st.st_nlink > 1:
            by[st.st_ino].append(r)
    return {frozenset(v) for v in by.values() if len(v) > 1}
pd_, ps_ = pairs(D), pairs(S)
print(f"[2] {'ok' if pd_ == ps_ else 'FAIL'} internal hardlink groups: "
      f"deployed {len(pd_)}, staged {len(ps_)}")
if pd_ != ps_:
    bad.append(f"internal hardlink groups differ; missing in staged: "
               f"{[sorted(g)[:2] for g in list(pd_ - ps_)[:3]]}")

diff = sorted(r for r in set(D) & set(S)
              if D[r].stat().st_size != S[r].stat().st_size or sha(D[r]) != sha(S[r]))
extra, missing = set(diff) - SWAP, SWAP - set(diff)
print(f"[3] {'ok' if not extra and not missing else 'FAIL'} files differing: {len(diff)}")
for r in diff:
    print(f"      {'(intended)' if r in SWAP else '(UNEXPECTED)'} {r}")
if extra:
    bad.append(f"files changed that should not have: {sorted(extra)}")
if missing:
    bad.append(f"intended swaps that did NOT change: {sorted(missing)}")

for sub in ("logs", "aam_cache"):
    moved = [r for r in diff if r.startswith(sub + "/")]
    n = sum(1 for r in S if r.startswith(sub + "/"))
    print(f"[4] {'ok' if not moved else 'FAIL'} {sub}/ byte-identical ({n} files)")
    if moved:
        bad.append(f"{sub}/ changed: {moved[:5]}")

ann = stg / "seams/direction_annotation.parquet"
md = (pq.ParquetFile(stg / "direction.parquet").schema_arrow.metadata or {})
declared = json.loads(md[b"ecspr_bake_file"].decode())["src_direction_sha256"]
actual = sha(ann)
print(f"[5] {'ok' if declared == actual else 'FAIL'} criterion 17 "
      f"src_direction_sha256\n      declared {declared}\n      annotation {actual}")
if declared != actual:
    bad.append("criterion 17: direction.parquet points at an annotation it does not carry")

print("\n" + "=" * 70)
if bad:
    print("STAGE IS NOT SOUND -- do not dvc add:")
    for x in bad:
        print("  -", x)
    sys.exit(1)
print("stage is sound: zero shared inodes, pairs preserved, exactly four files swapped")
