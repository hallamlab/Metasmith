"""The other direction: one MNXR carrying EC numbers from unrelated classes.
The EC lane joins ec == ec, so every EC on a row makes that whole reaction reachable."""
import re
from collections import Counter, defaultdict
import pandas as pd

RP = "data/fabfos/originals/metanetx/4.5/reac_prop.tsv"
E4 = re.compile(r"^\d+\.\d+\.\d+\.\d+$")

ecs_of, ref_of = {}, {}
for line in open(RP):
    if line.startswith("#"): continue
    p = line.rstrip("\n").split("\t")
    if len(p) < 5 or not p[0].startswith("MNXR"): continue
    e = [x.strip() for x in p[3].split(";") if x.strip() and E4.match(x.strip())]
    if e:
        ecs_of[p[0]] = e
        ref_of[p[0]] = p[2]

n = len(ecs_of)
cnt = Counter(len(v) for v in ecs_of.values())
print(f"MNXR carrying >=1 level-4 EC: {n:,}")
print("  #ECs per reaction:", {k: cnt[k] for k in sorted(cnt)[:8]},
      f"... max={max(cnt)}")

def cls(e): return e.split(".")[0]
def sub(e): return ".".join(e.split(".")[:2])

multi_cls = {r: v for r, v in ecs_of.items() if len({cls(e) for e in v}) >= 2}
multi_sub = {r: v for r, v in ecs_of.items() if len({sub(e) for e in v}) >= 2}
print(f"  spanning >=2 EC subclasses (x.y): {len(multi_sub):,} ({100*len(multi_sub)/n:.1f}%)")
print(f"  spanning >=2 EC top classes (x):  {len(multi_cls):,} ({100*len(multi_cls)/n:.1f}%)")
worst = sorted(multi_cls.items(), key=lambda kv: -len({cls(e) for e in kv[1]}))[:6]
for r, v in worst:
    print(f"    {r}  {len({cls(e) for e in v})} classes, {len(v)} ECs: {';'.join(sorted(v))[:110]}")
    print(f"        ref: {ref_of[r][:110]}")

df = pd.read_parquet("data/fabfos/runs/scadc_metagenome/gpr/gpr_4lane.parquet",
                     columns=["projection_via", "intermediate_id", "mnxr", "orf"])
df = df[df.projection_via == "ec"]
for label, s in [("subclass", set(multi_sub)), ("top class", set(multi_cls))]:
    hit = df.mnxr.isin(s)
    print(f"\n  metagenome EC rows landing on a reaction whose ECs span >=2 {label}s: "
          f"{int(hit.sum()):,} / {len(df):,} ({100*hit.mean():.1f}%) | "
          f"{df[hit].mnxr.nunique():,} reactions | {df[hit].orf.nunique():,} ORFs")

# the sharpest statement: ORF -> reaction credited through an EC that disagrees
# with every other EC on that same reaction
rows = []
for r, v in multi_cls.items():
    for e in v:
        rows.append((e, r))
bridge = pd.DataFrame(rows, columns=["intermediate_id", "mnxr"])
m = df.merge(bridge.drop_duplicates(), on=["intermediate_id", "mnxr"], how="inner")
print(f"\n  ... of which the EMITTED EC is itself one of the disagreeing set: "
      f"{len(m):,} rows / {m.orf.nunique():,} ORFs / {m.mnxr.nunique():,} reactions")
