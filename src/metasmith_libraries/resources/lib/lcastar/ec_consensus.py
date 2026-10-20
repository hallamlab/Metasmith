import argparse
from collections import defaultdict

import fabfos_evidence as fe
from lcastar import ec_rank, lcastar, parse_ec, to_text


def mnxr_to_ecs(bridge):
    ec_rows = bridge[bridge["id_source"] == "ec"]
    out = defaultdict(set)
    for ec, mnxr in zip(ec_rows["id"], ec_rows["mnxr"]):
        out[mnxr].add(ec)
    return out


def lane_ecs(gpr, mnxr_ec):
    # CLEAN names its EC directly. Every other lane reaches an EC only through
    # the reactions it nominated, so its ECs are those MetaNetX gives each MNXR.
    lanes = defaultdict(set)
    for orf, channel, mnxr, inter in zip(gpr["orf"], gpr["channel"], gpr["mnxr"], gpr["intermediate_id"]):
        if channel == "clean":
            lanes[(orf, channel)].add(inter)
        else:
            lanes[(orf, channel)].update(mnxr_ec.get(mnxr, ()))
    return lanes


def consensus(lanes, alpha):
    # One vote per lane, split evenly over the ECs that lane names.
    by_orf = defaultdict(list)
    for (orf, channel), ecs in lanes.items():
        paths = [p for p in (parse_ec(ec) for ec in ecs) if p]
        for path in paths:
            by_orf[orf].append((channel, path, 1.0 / len(paths)))
    rows = []
    for orf, votes in sorted(by_orf.items()):
        r = lcastar([(path, w) for _, path, w in votes], alpha)
        if r is None or not r.lineage:
            continue
        agreeing = sorted({c for c, path, _ in votes if path[:len(r.lineage)] == r.lineage})
        rows.append((orf, r.lineage[-1], ec_rank(r.lineage), r.p_value, r.fraction, r.votes,
                     len({c for c, _, _ in votes}), ",".join(agreeing)))
    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--gpr", required=True)
    p.add_argument("--bridge", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--alpha", type=float, default=0.51)
    a = p.parse_args()

    gpr = fe.read_parquet(a.gpr, columns=["orf", "channel", "mnxr", "intermediate_id"])
    bridge = fe.read_parquet(a.bridge, columns=["id", "id_source", "mnxr"])
    rows = consensus(lane_ecs(gpr, mnxr_to_ecs(bridge)), a.alpha)
    with open(a.out, "w") as fh:
        fh.write("orf\tec\trank\tp_value\tfraction\tvotes\tlanes\tagreeing_lanes\n")
        for orf, ec, rank, pv, frac, votes, lanes, agreeing in rows:
            fh.write(f"{orf}\t{ec}\t{rank}\t{pv:.4g}\t{frac:.4f}\t{votes:.4g}\t{lanes}\t{agreeing}\n")
    print(f"[ec_consensus] {len(rows):,} ORFs with a consensus EC", flush=True)


if __name__ == "__main__":
    main()
