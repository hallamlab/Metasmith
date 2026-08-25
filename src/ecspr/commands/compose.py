from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ..model import compose as compose_mod
from ..model import compositions as comp_mod
from ..model import conditions as cond_mod
from ..model.probes import load_corpus

#: What one composed network directory holds. The four are meaningless apart -- a pair
#: table carries member-prefixed ids that only this network's GPR table and direction table
#: agree with -- so they are one product rather than four.
FILES = ("atom_pairs.parquet", "direction.parquet", "gpr.parquet", "bridges.parquet")


def compose(args, log):
    corpus = load_corpus(args.gpr)
    conditions = {c.condition_id: c for c in cond_mod.read(args.conditions)}
    comps = comp_mod.read(args.compositions)
    pairs = pd.read_parquet(args.atom_pairs)
    direction = pd.read_parquet(args.direction) if args.direction else None
    names = pd.read_parquet(args.names) if args.names else None
    blacklist = (set(pd.read_parquet(args.blacklist).mnxm) if args.blacklist
                 else frozenset())

    # THE SUBSET DIRECTION, AND ONLY THAT DIRECTION. The name table is cut from the bake's
    # own universe, so a name the bake does not pair cannot have come from here -- it is
    # the tell that the table was built against a different bake, which is the silent
    # staleness a repin leaves behind. The converse is NOT a signal: thousands of
    # metabolites the bake pairs carry no parseable formula and so appear in no name row.
    if names is not None:
        strays = set(names.mnxm) - (set(pairs.substrate) | set(pairs["product"]))
        if strays:
            raise SystemExit(
                f"REFUSING: {len(strays):,} metabolites in {args.names} are absent from "
                f"the atom-pair table (e.g. {', '.join(sorted(strays)[:4])}). The name "
                f"table is cut from the bake's own universe, so it was built against a "
                f"different one.")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    elements = tuple(args.elements)
    log(f"[compose] {len(comps)} composition(s) | {len(pairs):,} pair rows | "
        f"{len(blacklist):,} blacklisted carriers | g0={args.g0} | "
        f"elements={'.'.join(elements)}")

    reports = []
    for c in comps:
        d = out / c.composition_id
        d.mkdir(parents=True, exist_ok=True)
        got = compose_mod.compose_composition(
            c, conditions, corpus, pairs, direction, elements=elements, g0=args.g0,
            blacklist=blacklist, names=names)
        got["pairs"].to_parquet(d / "atom_pairs.parquet", index=False)
        got["gpr"].to_parquet(d / "gpr.parquet", index=False)
        got["bridges"].to_parquet(d / "bridges.parquet", index=False)
        if got["direction"] is not None:
            got["direction"].to_parquet(d / "direction.parquet", index=False)
        r = dict(got["report"], members="|".join(c.members))
        reports.append(r)
        log(f"[compose] {c.composition_id}: {r['n_orfs']:,} orfs, "
            f"{r['n_reactions']:,} reactions, {r['n_pair_rows']:,} pair rows, "
            f"{r['n_bridge_rows']:,} bridge rows over {r['n_bridged']:,} metabolites"
            + (f", {r['n_declared_bridged']}/{r['n_declared']} declared bridges emitted"
               if "n_declared" in r else ""))

    rep = pd.DataFrame(reports)
    rep["members"] = rep["members"].astype(str)
    rep.to_parquet(out / "compose_report.parquet", index=False)
    (out / "compose_report.json").write_text(json.dumps(reports, indent=2, default=str))
    log(f"[compose] {len(reports)} network(s) -> {out}")
    return 0
