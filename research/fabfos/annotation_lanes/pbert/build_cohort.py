"""THE QUESTION -- how many DH10B ORFs can be adjudicated against curated EC truth,
and does that cohort match the 1,288 the scadc replication reports?

SCOPE   DH10B only. The cohort is rebuilt by the source study's own method; see
        _common.load_cohort. EPI300 is not built here -- the seven-lane table's
        N = 1,280 is EPI300 and is a different cohort.
INPUT   data/fabfos/benchmarks/lane_dh10b/{annotations/dh10b.faa,generators/sprot_k12.tsv}
        data/fabfos/originals/metanetx/4.5/reac_prop.tsv
METHOD  md5 of each Swiss-Prot K-12 sequence carrying a level-4 EC, matched against
        the md5 of each DH10B ORF; EC fanned out to MNXR through reac_prop classifs.
ENV     PYTHONPATH="$PWD/src" mamba run -n msm python \
            research/fabfos/annotation_lanes/pbert/build_cohort.py
OUT     research/fabfos/annotation_lanes/pbert/cohort_dh10b.tsv   (committed; small)
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import HERE, load_cohort, iter_fasta, ANN  # noqa: E402


def main():
    n_orf = sum(1 for _ in iter_fasta(ANN / "dh10b.faa"))
    df = load_cohort()
    df = df.assign(
        ec=df["ec"].map(lambda s: ";".join(sorted(s))),
        n_ec=df["ec"].map(len),
        mnxr=df["mnxr"].map(lambda s: ";".join(sorted(s))),
        n_mnxr=df["mnxr"].map(len),
    )
    out = HERE / "cohort_dh10b.tsv"
    df.to_csv(out, sep="\t", index=False)
    n_no_mnxr = int((df["n_mnxr"] == 0).sum())
    print(f"dh10b ORFs            {n_orf}")
    print(f"adjudicable (EC truth) {len(df)}   (scadc replication reports 1288)")
    print(f"  with no MNXR fanout  {n_no_mnxr}  ({n_no_mnxr/len(df):.1%})")
    print(f"  mean EC per ORF      {df['n_ec'].mean():.2f}")
    print(f"  mean MNXR per ORF    {df['n_mnxr'].mean():.2f}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
