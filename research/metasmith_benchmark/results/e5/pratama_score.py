"""Score E5's Pratama catalogue against Pratama's 257,252 vOTUs as the E3 ablation scored its rungs (abl_score.py).

    pratama_score.py <final vOTU fna> <skani table> > pratama_score.json

The skani table comes from the ablation's final_votu_recovery_pratama settings. A vOTU's lane is the assembler of the
assembly its contig came from, looked up by file name in assemblies.tsv.
"""
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "e3"))
from votu_lib import pair  # noqa: E402

ANI_MIN, AF_MIN = 95, 85
fna, skani = sys.argv[1:3]

pub = pd.read_csv(HERE.parent / "e3" / "published_votu_tally.tsv.gz", sep="\t")[["name", "lane"]]
assert len(pub) == 257252, len(pub)
pub_idx = pd.Series(np.arange(len(pub)), index=pub.name)

asm = pd.read_csv(HERE / "assemblies.tsv", sep="\t")
asm = asm[asm.study.str.startswith("pratama_")]
lane_of = dict(zip(asm.path.map(lambda p: Path(p).stem), asm.assembler))

heads = subprocess.run(["grep", "-F", ">", fna], capture_output=True, text=True, check=True).stdout
ours = pd.DataFrame({"name": [h[1:].split()[0] for h in heads.splitlines()]})
ours["lane"] = ours.name.str.split("|").str[3].str.split("~").str[0].map(lane_of)
assert ours.lane.notna().all(), ours[ours.lane.isna()].head()
ours_idx = pd.Series(np.arange(len(ours)), index=ours.name)

sk = pd.read_csv(skani, sep="\t", usecols=["ANI", "Align_fraction_ref", "Align_fraction_query", "Ref_name", "Query_name"])
sk.columns = ["ani", "af_ours", "af_pub", "ours", "pub"]
sk["ours"] = sk.ours.str.split().str[0]
sk["pub"] = sk.pub.str.split().str[0]
assert sk.pub.isin(pub_idx.index).all() and sk.ours.isin(ours_idx.index).all()
sk["i"] = pub_idx[sk.pub].to_numpy()
sk["j"] = ours_idx[sk.ours].to_numpy()
sk["identity"] = sk.ani * sk.af_pub / 100

hit = (sk.ani >= ANI_MIN) & (sk.af_pub >= AF_MIN)
recovered = np.zeros(len(pub), bool)
recovered[sk.i[hit]] = True
matched = np.zeros(len(ours), bool)
matched[sk.j[(sk.ani >= ANI_MIN) & (sk.af_ours >= AF_MIN)]] = True
paired = pair(sk, len(pub), len(ours))

out = {
    "votus": len(ours), "matched": int(matched.sum()),
    "recovered": int(recovered.sum()), "recovery": round(recovered.mean(), 4),
    "recovery_by_published_lane": {k: round(v, 4) for k, v in pd.Series(recovered).groupby(pub.lane.to_numpy()).mean().items()},
    "lanes": ours.lane.value_counts().to_dict(),
    "matched_lanes": ours.lane[matched].value_counts().to_dict(),
    "paired_median_identity": round(float(np.median(paired.identity)), 2),
    "paired": len(paired),
}
json.dump(out, sys.stdout, indent=1)
