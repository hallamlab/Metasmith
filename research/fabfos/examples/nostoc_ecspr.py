#!/usr/bin/env python3
"""The three-member community as twelve DIRECTED networks, measured twice.

    python research/fabfos/examples/nostoc_ecspr.py --refs           # names + blacklist, from the bake
    python research/fabfos/examples/nostoc_ecspr.py --compose        # build the networks + tests
    python research/fabfos/examples/nostoc_ecspr.py --plan           # plan every unit, run nothing
    python research/fabfos/examples/nostoc_ecspr.py --run            # measure
    python research/fabfos/examples/nostoc_ecspr.py --run --only NOS-ERY_bl-on   # one network

    {NOS,ERY,RHI} gpr_4lane + the reference bake
        -> ecspr::conditions (one per member) + ecspr::compositions (one per network)
        -> ecspr.model.compose  -> per-network atom_pairs + direction + gpr + tests
        -> ecspr_measure  -> ecspr::results

A MEMBER IS A CONDITION, WHICH IS WHY THE MEMBER LIST BECAME TWO TABLES. `--compose`
writes one condition per member and one composition per network, and the community is
then a declaration rather than a Python list of tuples. It buys the case this driver
never had: a member can be PERTURBED, so "Nostoc beside Erythrobacter" and "Nostoc beside
a knockout of Erythrobacter" are two compositions over the same annotation tables and the
knockout is a `subtract` step like every other perturbation in the project.

**WARNING** THIS DRIVER WRITES INTO `data/`, WHICH DVC HARDLINKS. See `_write`.

WHAT `a -> b` MEANS. Glucose is injected into member `a`'s private copy of the network
and the biomass endpoints are read in member `b`'s. Both directions are measured on the
IDENTICAL composed network -- only the terminals move -- which is what makes the pair a
comparison rather than two unrelated numbers. With a, b, c = Nostoc, Erythrobacter,
Allorhizobium the twelve are:

    a->bc  b->ac  c->ab            the three-member network, one injection point each
    a->b   b->a   a->c   c->a      each two-member network, both directions
    b->c   c->b
    a      b      c                each member alone -- the control

Seven composed graphs, twelve directed measurements. Under `ground` there is no endpoint
to place, so what distinguishes the directed measurements there is the injecting copy;
under `two-point` each (source, endpoint) pair is its own solve.

ONLY THE GROUND ARM HAS A TRANSFORM. `--compose` writes BOTH test sets, because
they are one modelling decision and splitting them would let the two drift; but the
transform library declares one measurement step, `ecspr_measure`, and it dispatches
`ecspr ground`. The two-point tests are written and staged and nothing consumes
them. That is deliberate rather than half-finished: the measurement of record for both
probes is the pinned `data/fabfos/nostoc/ecspr` chunk, produced when the library carried
a transform per probe, and declaring the second one again is a library change with a
type question in it -- both probes now write the same `ecspr::results` type, so two
producers of one type need something to tell the planner them apart.

TWELVE UNITS, TIMES TWO ARMS, IS TWENTY-ONE. The carrier blacklist is a modelling choice
and not a fact, so the whole experiment runs with it and without it. The three singletons
have no bridges at all and so are identical across arms -- they are measured once and
shared, which is also what makes them a control for the composition itself.

THE COMPOSED PAIR TABLE IS PER-NETWORK, AND THAT IS THE ONE TENSION HERE
------------------------------------------------------------------------
`ecspr::atom_pairs` is declared network-agnostic and shared, and for the reference bake it
is. A composed table is not: it carries organism-prefixed ids and bridge rows that mean
nothing to another network. Rather than pin the reference type per-experiment -- which
would change the measurement transform and weaken a contract that is right for every
other caller -- this driver invokes the pipeline ONCE PER COMPOSED GRAPH, passing that
graph's tables through `--atom-pairs` / `--direction-ratios`. Composition happens on the
DECODED bake, so what it emits is already in the schema the graph builder reads and
nothing downstream needs to know a bake was involved.

The composition now HAS a transform -- `compose_network`, producing `ecspr::composed_network`
-- and this driver still does the work locally. That is not an oversight. The transform
produces a DIRECTORY type, deliberately: the composed pair table, its direction table and
its GPR table carry member-prefixed ids that only agree with each other, so they are one
product. Chaining it into the measurement inside one plan would mean a transform consuming
`ecspr::atom_pairs` and producing it, and one type would then mean both the shared
reference and a per-network cut with nothing to tell a measurement which it had been
handed. The transform is the route for a plan that composes MANY communities at once; this
driver stays local because the bridge math is still being iterated.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))

import ecspr.model.compose as ec  # noqa: E402
import ecspr.model.compositions as ecomp  # noqa: E402
import ecspr.model.conditions as econd  # noqa: E402
import ecspr.model.tests as etests  # noqa: E402

sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/eydallin"))
import bake_pairs  # noqa: E402

GPR = REPO / "data/fabfos/nostoc/annotation"
OUT = REPO / "data/fabfos/nostoc/ecspr"
NETS = OUT / "networks"
# The members' own GPR tables under names a conditions table can select on. All three are
# called `gpr_4lane.parquet` where they live, and a condition names its table by BASENAME,
# so two of the three would be unreachable in one corpus.
TABLES = OUT / "member_tables"
MEMBERS = ("NOS", "ERY", "RHI")
ELEMENTS = ("C", "N", "P", "S")
_DEFAULT_OUT = str(REPO / "data/scratch/nostoc_ecspr")

# --- the experiment's claim about what it is testing ---------------------------------
#
# M9-style minimal, one growth substrate per element. Glucose cannot be the source in the
# N, P or S graphs -- it has no atom of any of them -- so each element's graph is entered
# through that element's own minimal-medium salt. Every id below was resolved by NAME
# against MetaNetX 4.5 and checked present in the decoded bake FOR ITS OWN ELEMENT; this
# release does not use the ids one would guess (glucose is MNXM1364061, not MNXM1137670,
# and thiamine is MNXM730135, not MNXM662), and a guessed id measures a run of structural
# zeros that looks exactly like a biological finding.
SUBSTRATES = {
    "C": "MNXM1364061",
    "N": "MNXM729302",
    "P": "MNXM9",
    "S": "MNXM58",
}

PRECURSORS = {
    "L-alanine": "MNXM1105732", "L-arginine": "MNXM739527",
    "L-asparagine": "MNXM1107821", "L-aspartate": "MNXM1364497",
    "L-cysteine": "MNXM738068", "L-glutamate": "MNXM741173",
    "L-glutamine": "MNXM37", "glycine": "MNXM29", "L-histidine": "MNXM1107769",
    "L-isoleucine": "MNXM728337", "L-leucine": "MNXM1106761",
    "L-lysine": "MNXM1364268", "L-methionine": "MNXM738804",
    "L-phenylalanine": "MNXM741664", "L-proline": "MNXM114",
    "L-serine": "MNXM737787", "L-threonine": "MNXM142",
    "L-tryptophan": "MNXM741553", "L-tyrosine": "MNXM76", "L-valine": "MNXM199",
    "ATP": "MNXM3", "GTP": "MNXM1103553", "CTP": "MNXM1103718", "UTP": "MNXM1101474",
    "dATP": "MNXM286", "dGTP": "MNXM344", "dCTP": "MNXM360", "dTTP": "MNXM394",
    "UDP-GlcNAc": "MNXM1104529", "sn-glycerol-3-P": "MNXM66",
    "biotin": "MNXM304", "thiamine": "MNXM730135", "chorismate": "MNXM337",
}

# Acetyl-CoA and S-adenosyl-L-methionine are biomass precursors in every GEM's biomass
# reaction and are deliberately NOT endpoints here: both are on the carrier blacklist, and
# a degree-1,100 carrier as a sink makes the P and S readouts a measurement of cofactor
# pool connectivity rather than of biosynthesis.

TWO_TERMINAL = {
    "C": ["L-glutamate", "L-serine", "chorismate", "L-histidine"],
    "N": ["L-glutamate", "L-glutamine", "L-histidine", "L-arginine"],
    "P": ["ATP", "sn-glycerol-3-P", "UDP-GlcNAc"],
    "S": ["L-cysteine", "L-methionine", "biotin"],
}

UNITS = [
    (("NOS", "ERY", "RHI"), "NOS"), (("NOS", "ERY", "RHI"), "ERY"),
    (("NOS", "ERY", "RHI"), "RHI"),
    (("NOS", "ERY"), "NOS"), (("NOS", "ERY"), "ERY"),
    (("NOS", "RHI"), "NOS"), (("NOS", "RHI"), "RHI"),
    (("ERY", "RHI"), "ERY"), (("ERY", "RHI"), "RHI"),
    (("NOS",), "NOS"), (("ERY",), "ERY"), (("RHI",), "RHI"),
]
ARMS = ("bl-on", "bl-off")

# The AGENT image, not the ecspr tool image. metasmith derives its default tag from the
# engine's own version, and 0.20.1 has been pruned from quay -- it fails as a `manifest
# unknown`, then a missing .sif, then a missing `msm_relay`, none of which says "nobody
# built this image". The overlay replaces the container's metasmith package but NOT the
# conda environment under it, so the base has to come from the same minor line as the
# pinned engine or an import the engine makes is simply absent.
#
# `_fir.FIR_CONTAINER` is the same string and is the one the remote path uses; this is
# the local one. They are separate names on purpose -- a local plan does not depend on
# what fir happens to hold -- and both are checked against quay by `_fir.preflight`.
AGENT_CONTAINER = "docker://quay.io/hallamlab/metasmith:0.20.4"

# THE LOCAL RUN USES MAMBA, AND THAT IS NOT A CONVENIENCE. `env::ecspr.env` names
# `quay.io/hallamlab/ecspr:2026.07.14` as its container and its own first line says that
# tag is not yet republished -- the image has the env but not the package, so `ecspr` is
# not on PATH inside it. Under a container runtime the step therefore fails with a bare
# 127 and NOTHING ELSE: nextflow reports `terminated with an error exit status (1) --
# Error is ignored`, the run "completes", zero products are written, and the driver prints
# `1 ok, 0 failed`. So the failure looks exactly like success from here.
#
# `ecspr.env`'s `conda:` key is the working route, which is what `--runtime mamba` takes.
# Under mamba `--agent-env` is a conda env NAME rather than an image (see
# `common.make_agent`), so the two move together and neither may be set without the other.
# `dev/ecspr.sh --iecspr` is what puts the CLI in the `ecspr` env; a fresh checkout has the
# env's console-script shim pointing at no package and fails the same 127 way.
LOCAL_RUNTIME = "mamba"
LOCAL_AGENT_ENV = "msm"


def _fresh(dest: Path) -> Path:
    """Break the link on `dest` so a library writer may create it.

    `ecspr.model.conditions.write` and friends open the destination directly, which is
    right for a run directory and wrong here: under `data/` the path is a DVC hardlink,
    and DVC also leaves it read-only, so the open fails outright. UNLINKING IS THE SAFE
    HALF -- it drops one name from the inode and leaves the cached content alone.
    Truncating it is what would rewrite every other checkout.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() or dest.is_symlink():
        dest.unlink()
    return dest


def _write(df, dest: Path):
    """**WARNING** NEVER WRITE THROUGH A FILE UNDER `data/`.

    DVC's cache type here is `hardlink,symlink`, so a checked-out file is ONE INODE WITH
    MANY NAMES -- the composed tables under `data/fabfos/nostoc/ecspr/` carry link counts
    of 19, 27 and 37. `to_parquet` opens the path for writing and truncates that inode, so
    a plain overwrite edits the cache itself and every other checkout of that content
    silently becomes the new file. `dvc status` stays clean throughout.

    Writing a sibling and renaming over the name breaks the link instead of following it.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".writing")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, dest)
    return dest


def net_id(members, arm):
    return "-".join(members) + ("" if len(members) == 1 else f"_{arm}")


def unit_name(members, src, arm):
    return f"{net_id(members, arm)}__{src}"


# =====================================================================
# Refs -- the two tables cut from the bake's own metabolite universe
# =====================================================================
#
# BOTH ARE FUNCTIONS OF THE BAKE AND MUST BE REBUILT WITH IT. `metabolite_names` is
# chem_prop restricted to the metabolites the bake actually pairs, and the carrier
# blacklist is name RULES materialised against that table -- so a metabolite the bake
# newly reaches is simply absent from both, and absence from the blacklist is the
# dangerous half: an unlisted degree-4,000 carrier gets bridged and every arm reports the
# members as one well-mixed pot while looking entirely healthy. Nothing raises. Hence
# `--refs` before `--compose`, and the coverage guard in `compose_all`.

CHEM_PROP = REPO / "data/fabfos/originals/metanetx/4.5/chem_prop.tsv"
_FORMULA_RE = re.compile(r"([A-Z][a-z]?)(\d*)")


def _parse_formula(formula: str) -> dict:
    # `"C6H12O6"` -> `{"C": 6, "H": 12, "O": 6}`; blank/non-string -> `{}`.
    #
    # A `*` RESIDUE IS KEPT, counting its explicit atoms only. `C28H49N3O10PS*2` is an
    # acyl-ACP whose carrier body MetaNetX leaves unspecified; dropping the whole formula
    # for that would take 7,701 metabolites -- most of the ACP, polymer and protein-bound
    # series -- out of the name table, and with them out of reach of the carrier rules that
    # are the reason the table exists.
    if not isinstance(formula, str) or not formula:
        return {}
    counts = {}
    for el, n in _FORMULA_RE.findall(formula):
        if not el:
            continue
        counts[el] = counts.get(el, 0) + (int(n) if n else 1)
    return counts


def build_refs(elements=ELEMENTS):
    import ecspr.model.compose as _ec
    OUT.mkdir(parents=True, exist_ok=True)
    pairs = pd.read_parquet(bake_pairs.atom_pairs())
    universe = set(pairs.substrate) | set(pairs["product"])
    print(f"bake universe: {len(universe):,} metabolites over "
          f"{pairs.mnxr.nunique():,} reactions", flush=True)

    chem = pd.read_csv(CHEM_PROP, sep="\t", comment="#", usecols=[0, 1, 3], dtype=str,
                       names=["id", "name", "reference", "formula", "charge", "mass",
                              "inchi", "inchikey", "smiles"])
    chem = chem[chem.id.isin(universe)]
    rows = [(m, nm, f, el, n)
            for m, nm, f in zip(chem.id, chem.name, chem.formula)
            for el, n in _parse_formula(f).items() if el in elements]
    names = pd.DataFrame(rows, columns=["mnxm", "name", "formula", "element", "n_atoms"])
    _write(names, OUT / "metabolite_names.parquet")
    print(f"  metabolite_names: {len(names):,} rows, {names.mnxm.nunique():,} metabolites "
          f"carrying a {'/'.join(elements)} atom "
          f"({len(universe - set(names.mnxm)):,} of the universe carry none)")

    bl = _ec.carrier_blacklist(names)
    _write(bl, OUT / "carrier_blacklist.parquet")
    print(f"  carrier_blacklist: {len(bl):,} ids over "
          f"{bl.carrier_class.nunique()} classes")
    return names, bl


def compose_all(g0=1.0, elements=ELEMENTS):
    NETS.mkdir(parents=True, exist_ok=True)
    print("loading the reference bake ...", flush=True)
    pairs = pd.read_parquet(bake_pairs.atom_pairs())
    direction = pd.read_parquet(bake_pairs.direction_ratios())
    names = pd.read_parquet(OUT / "metabolite_names.parquet")
    blacklist = set(pd.read_parquet(OUT / "carrier_blacklist.parquet").mnxm)
    gprs = {o: pd.read_parquet(GPR / o / "gpr_4lane.parquet") for o in MEMBERS}
    print(f"  {len(pairs):,} pair rows, {len(blacklist):,} blacklisted carriers")

    # THE GUARD IS THE SUBSET DIRECTION, and only that direction. `--refs` cuts the name
    # table FROM this universe, so a name the bake does not pair cannot have come from
    # here -- it is the tell that the table was built against a different bake, which is
    # exactly the silent staleness a repin leaves behind (the retired bake's table left 9).
    #
    # The converse is NOT a staleness signal and must not be checked: 4,633 metabolites
    # the bake pairs carry no parseable formula in chem_prop and so appear in no name row
    # by construction -- including hubs like `Ferrocytochrome b5` at degree 3,462. That is
    # a real hole in what the carrier rules can reach, but it is a property of MetaNetX
    # rather than of which bake this is, and refusing on it refuses every run.
    strays = set(names.mnxm) - (set(pairs.substrate) | set(pairs["product"]))
    if strays:
        raise SystemExit(
            f"REFUSING: {len(strays):,} metabolites in "
            f"{OUT/'metabolite_names.parquet'} are absent from the bake "
            f"(e.g. {', '.join(sorted(strays)[:4])}).\n"
            f"  The name table is cut from the bake's own universe, so it was built "
            f"against a different one. Re-run --refs.")

    present = {e: set(g.substrate) | set(g["product"]) for e, g in pairs.groupby("element")}
    prec_by_el, missing = {}, []
    for e in elements:
        keep = [n for n, m in PRECURSORS.items() if m in present.get(e, ())]
        prec_by_el[e] = keep
        if SUBSTRATES[e] not in present.get(e, ()):
            missing.append(f"substrate {SUBSTRATES[e]} absent from element {e}")
        print(f"  element {e}: substrate {SUBSTRATES[e]}, {len(keep)} endpoints "
              f"({', '.join(keep[:6])}{' ...' if len(keep) > 6 else ''})")
    if missing:
        raise SystemExit("REFUSING: " + "; ".join(missing))

    # THE MEMBERS BECOME CONDITIONS AND THE COMMUNITY BECOMES A TABLE. A member is a
    # condition_id, which is what lets one be perturbed: a knockout member is a `subtract`
    # step and nothing else about the composition changes. The corpus is keyed by basename
    # and all three members' tables are called `gpr_4lane.parquet`, so they are written out
    # under distinct names -- a condition names its table by basename and two tables
    # sharing one are unreachable.
    TABLES.mkdir(parents=True, exist_ok=True)
    corpus = {}
    for o in MEMBERS:
        _write(gprs[o], TABLES / f"{o}.parquet")
        corpus[f"{o}.parquet"] = gprs[o]
    member_conditions = [econd.single(o, f"{o}.parquet") for o in MEMBERS]
    econd.write(member_conditions, _fresh(OUT / "member_conditions.parquet"))
    conditions = {c.condition_id: c for c in member_conditions}

    comps = {arm: [] for arm in ARMS}
    seen = set()
    for members, _src in UNITS:
        if len(members) == 1:
            # A SINGLETON IS A ONE-MEMBER COMPOSITION, not a second kind of thing. It has
            # no bridges, so the two arms are identical: it is composed once and shared,
            # which is also what makes it the control for composition itself.
            nid = net_id(members, ARMS[0])
            if nid not in seen:
                seen.add(nid)
                comps[ARMS[0]].append(ecomp.single(nid, members[0]))
            continue
        for arm in ARMS:
            nid = net_id(members, arm)
            if nid in seen:
                continue
            seen.add(nid)
            # A BLANK `on` MEANS BRIDGE ON EVERYTHING the mismatch finds, which is what
            # this experiment does. Naming metabolites instead restricts the bridges to
            # those, and the report then says per named one whether an edge came out.
            comps[arm].append(ecomp.Composition(
                nid, tuple(members),
                {(a, b): () for i, a in enumerate(members) for b in members[i + 1:]}))
    for arm in ARMS:
        if comps[arm]:
            ecomp.write(comps[arm], _fresh(OUT / f"compositions_{arm}.parquet"))

    reports = []
    done = set()
    for arm in ARMS:
        for c in comps[arm]:
            nid = c.composition_id
            d = NETS / nid
            d.mkdir(parents=True, exist_ok=True)
            done.add(nid)
            t0 = time.time()
            out = ec.compose_composition(
                c, conditions, corpus, pairs, direction, elements=elements, g0=g0,
                blacklist=(blacklist if arm == "bl-on" else frozenset()), names=names)
            _write(out["pairs"], d / "atom_pairs.parquet")
            _write(out["direction"], d / "direction.parquet")
            _write(out["gpr"], d / "gpr.parquet")
            _write(out["bridges"], d / "bridges.parquet")
            # WHAT THE MEASUREMENT RESOLVES. The composed GPR table is one network, so its
            # conditions table is one condition over one table. The composition is what
            # BUILT it; this is what the probe reads.
            econd.write([econd.single(nid, "gpr.parquet")],
                        _fresh(d / "conditions.parquet"))
            rep = dict(out["report"], arm=arm, seconds=round(time.time() - t0, 1))
            reports.append(rep)
            print(f"[{nid}] {rep['n_orfs']:,} orfs  {rep['n_reactions']:,} rxn  "
                  f"{rep['n_pair_rows']:,} pair rows  {rep['n_bridge_rows']:,} bridge "
                  f"rows over {rep['n_bridged']:,} metabolites  "
                  f"({rep['n_blocked_blacklist']:,} blocked, "
                  f"{rep['n_one_copy_only']:,} one-copy)  {rep['seconds']}s", flush=True)

    for members, src in UNITS:
        for arm in ARMS:
            nid = net_id(members, arm)
            d = NETS / nid
            sinks = [o for o in members if o != src] or [src]
            ground, two_terminal = ec.make_tests(
                network_id=nid, source_org=src, sink_orgs=sinks,
                substrates=SUBSTRATES,
                precursors={e: [PRECURSORS[n] for n in prec_by_el[e]] for e in elements},
                elements=elements,
                two_terminal_precursors={
                    e: [PRECURSORS[n] for n in TWO_TERMINAL[e] if n in prec_by_el[e]]
                    for e in elements})
            etests.write(ground, _fresh(d / f"tests_{src}.parquet"), "ecspr")
            etests.write(two_terminal, _fresh(d / f"tests_2t_{src}.parquet"), "ecspr")
            if len(members) == 1:
                break

    _write(pd.DataFrame(reports), OUT / "compose_report.parquet")
    print(f"\n{len(done)} composed networks -> {NETS}")
    return reports


def check_tests(path: Path) -> None:
    import pandas as pd
    if not path.exists():
        raise SystemExit(f"no tests at {path} -- run --compose first")
    cols = set(pd.read_parquet(path).columns)
    if "mode" in cols or "background_column" in cols:
        raise SystemExit(
            f"{path} is a PRE-SPLIT conditions table, not a tests table.\n"
            f"  It parses without error and measures the wrong thing, so it must not be "
            f"staged. Re-run --compose.")
    if "test_id" not in cols:
        raise SystemExit(f"{path} carries no test_id column; re-run --compose.")


def invocations():
    by_net = {}
    for members, src in UNITS:
        for arm in ARMS:
            nid = net_id(members, arm)
            by_net.setdefault(nid, []).append(src)
            if len(members) == 1:
                break
    return {n: sorted(set(s)) for n, s in by_net.items()}


def build_cmd(nid, srcs, *, run, threads, outdir, agent_container=LOCAL_AGENT_ENV,
              runtime=LOCAL_RUNTIME):
    d = NETS / nid
    # `--agent-env` is the one lever the CLI has over where the AGENT runs: it becomes
    # `Agent.container`, which is a conda env name under mamba and an image URI under a
    # container runtime. Its help names the mamba case because that is the one nobody
    # can guess; under apptainer it is how you refuse the engine-version-derived default,
    # which names a tag nobody necessarily pushed.
    cmd = [sys.executable, "-m", "fabfos.pipelines.ecspr",
           "--runtime", runtime,
           "--agent-env", agent_container,
           "--atom-pairs", str(d / "atom_pairs.parquet"),
           "--direction-ratios", str(d / "direction.parquet"),
           "--output", str(outdir / nid),
           "--dag", str(REPO / "research/fabfos/reports/dag/ecspr" / nid),
           "--threads", str(threads)]
    for s in srcs:
        check_tests(d / f"tests_{s}.parquet")
        cmd += ["--unit", f"{nid}__{s}:{d/'gpr.parquet'}:{d/'conditions.parquet'}:"
                          f"{d/f'tests_{s}.parquet'}"]
    if run:
        cmd.append("--run")
    return cmd


# =====================================================================
# fir
# =====================================================================
#
# WHY THIS EXISTS. The measurement is compute-bound in one place: the leaky solve.
# A single organism's carbon graph is 162,801 nodes and a three-member composed graph is
# ~490k, and there is one solve per (network, element) on the ground side plus one per
# (network, element, endpoint) on the two-terminal side. Locally that is ten-plus hours on
# a shared workstation. On fir the units are independent, so they fan out as concurrent
# slurm jobs on 192-core / 768 GB nodes and the wall clock collapses to the slowest single
# unit plus queue.
#
# WALLTIMES ARE HALVED FROM WHAT THE TRANSFORM DECLARES, and that is not a guess.
# `slurm.nf` DOUBLES the walltime on attempt >= 2, so `ecspr_measure`'s declared 24 h is a
# 48 h second attempt -- an ask SLURM will not start ahead of a maintenance window, i.e. a
# retry that can never run. `_fir.check_walltimes` enforces the doubled ask and these
# overrides are sized to clear it.
FIR_RESOURCE_OVERRIDES = None


def _fir_overrides():
    # Sized from a MEASURED singleton on fir, not from the transform's declaration.
    #
    # The ground probe on a 162,801-node carbon graph ran 3 m 48 s at 167 MB peak RSS and
    # 2.6% CPU -- the leaky solve is single-threaded and its memory is nothing. So
    # `ecspr_measure`'s 16 cpus / 64 GB / 24 h is an ask that only delays scheduling, and
    # the three-member graphs are ~3x that, nowhere near these ceilings. Walltime is where
    # the headroom goes.
    #
    # Keyed on the transform's file stem, so this dict follows the library: when the two
    # probes were a transform each these were two entries, `measure_ground` and
    # `measure_two_terminal`. A stale key here is silent -- an override that matches no
    # step simply does not apply and the declared 24 h stands.
    from metasmith.python_api import Duration, Resources, Size
    return {
        "ecspr_measure": Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=3)),
    }


def drive_fir(*, only=None, host=None, agent_home=None, container=None,
              account=None, max_concurrent=4, preflight_only=False, wait_only=False,
              outdir=None):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _fir import (FIR_ACCOUNT, FIR_AGENT_HOME, FIR_CONTAINER, FIR_HOST,
                      check_schedulable, check_staged_executor, check_walltimes,
                      envs_from_plan, fir_agent, pin_external_leaf_ids, preflight,
                      provision_dev_overlay_remote, retrieve)
    from metasmith.python_api import Runtime
    from fabfos.pipelines import ecspr as pipeline

    host = host or FIR_HOST
    agent_home = agent_home or FIR_AGENT_HOME
    container = container or FIR_CONTAINER
    account = account or FIR_ACCOUNT
    outdir = outdir or (REPO / "data/scratch/nostoc_ecspr_fir")
    mlib = REPO / "src" / "metasmith_libraries"
    overrides = _fir_overrides()

    inv = invocations()
    if only:
        inv = {k: v for k, v in inv.items() if k in only}
        if not inv:
            raise SystemExit(f"--only matched nothing; networks are {sorted(invocations())}")
    order = sorted(inv.items(), key=lambda kv: (kv[0].count("-"), kv[0]))

    agent = fir_agent(host=host, agent_home=agent_home, container=container)

    print("=== Deploy() ===", flush=True)
    agent.Deploy()
    provision_dev_overlay_remote(host, agent_home)

    tasks = {}
    for nid, srcs in order:
        d = NETS / nid
        work = outdir / nid
        work.mkdir(parents=True, exist_ok=True)
        for s in srcs:
            check_tests(d / f"tests_{s}.parquet")
        units = [pipeline.Unit(name=f"{nid}__{s}",
                               gpr_tables=[d / "gpr.parquet"],
                               conditions=d / "conditions.parquet",
                               tests=d / f"tests_{s}.parquet") for s in srcs]
        _a, task, stubs = pipeline.generate_workflow(
            work, units=units,
            atom_pairs=d / "atom_pairs.parquet",
            direction_ratios=d / "direction.parquet",
            runtime=Runtime.APPTAINER, agent=agent,
            # COPY, not reference. An absolute local path is an EXTERNAL input that
            # metasmith binds verbatim into the remote container and does not transfer,
            # so referencing them makes the remote agent refuse to stage at all. A
            # composed network is tens of megabytes; copying is the cheap answer.
            stage="copy",
            on_inputs=pin_external_leaf_ids,
        )
        assert not stubs, f"{nid}: unexpected stubs {stubs}"
        if not task.ok:
            print(f"\n[{nid}] PLAN DID NOT RESOLVE:\n"
                  f"{getattr(task.plan, 'hints', task.plan)}", file=sys.stderr)
            return 3
        tasks[nid] = (task, work)
        (work / "RUN_KEY").write_text(task.GetKey())
        print(f"[{nid}] {len(srcs)} unit(s), {len(task.plan.steps)} steps, "
              f"key {task.GetKey()}", flush=True)

    envs = sorted({e for t, _ in tasks.values() for e in envs_from_plan(t)})
    if preflight(host, agent_home, container, envs, mlib=mlib):
        print("\nrefusing to run: a compute node has no outbound network, so an image "
              "absent from the store cannot be pulled once a task starts.", file=sys.stderr)
        return 4
    if preflight_only:
        return 0

    if not wait_only:
        in_flight = {}
        for nid, (task, work) in tasks.items():
            agent.StageWorkflow(task, on_exist="update")
            for check, args in ((check_staged_executor, (host, agent_home, task.GetKey())),
                                (check_walltimes, (host, overrides)),
                                (check_schedulable, (host, account, overrides))):
                if check(*args):
                    print(f"[{nid}] refusing to run", file=sys.stderr)
                    return 4
            print(f"=== [{nid}] submitting to slurm, account {account} ===", flush=True)
            agent.RunWorkflow(task,
                              config_file=agent.GetNxfConfigPresets()["slurm"],
                              params={"slurmAccount": account},
                              resource_overrides=overrides)
            in_flight[nid] = (task, work)
            # Each RunWorkflow leaves a nextflow supervisor on the LOGIN node, idling on
            # slurm. Eleven of those is rude on a shared login node even though each one is
            # cheap, so the launch waits for a slot rather than firing them all at once.
            if max_concurrent and len(in_flight) >= max_concurrent:
                print(f"  ({len(in_flight)} in flight; waiting for a slot)", flush=True)
                _wait_for(agent, in_flight, host, agent_home,
                          keep=max_concurrent - 1, retrieve_fn=retrieve)
        return _wait_for(agent, in_flight, host, agent_home, retrieve_fn=retrieve)

    return _wait_for(agent, tasks, host, agent_home, retrieve_fn=retrieve)


def _wait_for(agent, pending, host, agent_home, *, keep=0, retrieve_fn=None):
    # Poll the agent's own run logs until at most `keep` of `pending` are still running.
    #
    # MUTATES `pending`: completed runs are popped, so the caller's in-flight set shrinks as
    # slots free. The runs are DETACHED -- `RunWorkflow` launches nextflow with nohup and
    # returns -- so this process is a spectator and losing it loses nothing; re-attaching
    # works because the task keys are pinned.
    import time
    from _fir import ssh_once

    while len(pending) > keep:
        keys = " ".join(t.GetKey() for t, _ in pending.values())
        out = ssh_once(host, "for k in %s; do "
                             "  f=$(ls -1d %s/runs/$k/_metasmith/logs.* 2>/dev/null | tail -1); "
                             "  if [ -n \"$f\" ] && grep -q 'run completed at' $f/main.log 2>/dev/null; "
                             "  then echo \"DONE $k\"; else echo \"WAIT $k\"; fi; "
                             "done" % (keys, agent_home))
        done = {ln.split()[1] for ln in out.splitlines() if ln.startswith("DONE")}
        for nid in [n for n, (t, _) in pending.items() if t.GetKey() in done]:
            task, work = pending.pop(nid)
            print(f"[{nid}] complete", flush=True)
            if retrieve_fn is not None:
                retrieve_fn(host, agent.GetResultSource(task).GetPath(), work / "results")
        if len(pending) > keep:
            time.sleep(60)
    return 0


def drive(*, run, threads, outdir, only=None):
    inv = invocations()
    if only:
        inv = {k: v for k, v in inv.items() if k in only}
        if not inv:
            raise SystemExit(f"--only matched nothing; networks are {sorted(invocations())}")
    env = dict(**{k: v for k, v in __import__("os").environ.items()})
    env["PYTHONPATH"] = str(REPO / "src") + ":" + env.get("PYTHONPATH", "")
    ok, bad = [], []
    for nid, srcs in sorted(inv.items(), key=lambda kv: (kv[0].count("-"), kv[0])):
        cmd = build_cmd(nid, srcs, run=run, threads=threads, outdir=outdir)
        print(f"\n{'='*70}\n[{nid}] {len(srcs)} unit(s): {', '.join(srcs)}\n{'='*70}", flush=True)
        r = subprocess.run(cmd, cwd=REPO, env=env)
        # EXIT ZERO IS NOT EVIDENCE OF A MEASUREMENT. `slurm.nf`/`local.nf` run the step
        # with `errorStrategy 'ignore'`, so a transform that dies -- a missing binary in
        # the declared image is the one that has actually happened -- is reported as
        # `terminated with an error exit status (1) -- Error is ignored`, the run
        # "completes" with zero outputs, and the driver's exit code is 0. Count the
        # products instead: one per (unit, element), and nothing else distinguishes the
        # two outcomes from out here.
        got = len(list((outdir / nid).rglob("ecspr-ground_results/*.parquet"))) if run else 1
        want = len(srcs) * len(ELEMENTS) if run else 1
        if r.returncode == 0 and got >= want:
            ok.append(nid)
        else:
            bad.append(nid)
            if r.returncode == 0:
                print(f"[{nid}] FAILED SILENTLY: {got} ground products, expected {want}. "
                      f"The step's own stderr is in the run's nxf_work task dir.",
                      file=sys.stderr, flush=True)
    print(f"\n{len(ok)} ok, {len(bad)} failed" + (f": {bad}" if bad else ""))
    return 1 if bad else 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--refs", action="store_true",
                    help="rebuild metabolite_names + carrier_blacklist from the bake")
    p.add_argument("--compose", action="store_true", help="build networks + conditions")
    p.add_argument("--plan", action="store_true", help="plan every unit, run nothing")
    p.add_argument("--run", action="store_true", help="plan and execute")
    p.add_argument("--only", nargs="*", default=None, help="restrict to these network ids")
    p.add_argument("--fir", action="store_true",
                    help="execute on fir over slurm instead of locally")
    p.add_argument("--fir-preflight", action="store_true",
                    help="plan and check what fir must already hold, run nothing")
    p.add_argument("--fir-wait", action="store_true",
                    help="re-attach to runs already executing on fir")
    p.add_argument("--host", default=None)
    p.add_argument("--agent-home", default=None)
    p.add_argument("--slurm-account", default=None)
    p.add_argument("--max-concurrent", type=int, default=4,
                    help="how many nextflow supervisors to leave on fir's login node")
    p.add_argument("--g0", type=float, default=1.0, help="bridge conductance scale")
    p.add_argument("--elements", nargs="*", default=list(ELEMENTS))
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--output", default=_DEFAULT_OUT)
    a = p.parse_args(argv)

    fir = a.fir or a.fir_preflight or a.fir_wait
    if not (a.refs or a.compose or a.plan or a.run or fir):
        p.error("pick one of --refs / --compose / --plan / --run / --fir")
    if a.refs:
        build_refs(elements=tuple(a.elements))
    if a.compose:
        compose_all(g0=a.g0, elements=tuple(a.elements))
    if fir:
        return drive_fir(only=a.only, host=a.host, agent_home=a.agent_home,
                         account=a.slurm_account, max_concurrent=a.max_concurrent,
                         preflight_only=a.fir_preflight, wait_only=a.fir_wait,
                         outdir=Path(a.output).resolve() if a.output != _DEFAULT_OUT else None)
    if a.plan or a.run:
        return drive(run=a.run, threads=a.threads,
                     outdir=Path(a.output).resolve(), only=a.only)
    return 0


if __name__ == "__main__":
    sys.exit(main())
