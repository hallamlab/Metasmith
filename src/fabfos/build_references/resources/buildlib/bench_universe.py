"""The atom universe the BENCHMARK counts, which is not the whole bake.

`in_atom_universe` means "this reaction has atom-pair coverage, so an edge can exist for
it". The bake answers that for every reaction it could map. The benchmark asks a narrower
question, and the difference is transport.

TRANSPORT IS EXCLUDED, and until this module existed three separate comments said so
while no code did it. A transport reaction moves a metabolite between compartments; the
atom pairs it carries come from the energetic coupling -- an ABC transporter's
ATP -> ADP + Pi -- and not from the species that crossed. Those are currency edges,
identical across every transporter in a model, so counting them credits a method for
recovering "this reaction consumes ATP" and calls it knowledge of the metabolite. 9,420
of the bake's 63,621 mapped reactions (14.8%) are transport by MetaNetX's own flag.

THE FILTER LIVES HERE AND NOT AT THE BAKE. `bake_metabolism`'s selftest and
`check_references.py` both assert that the baked reaction vocabulary equals the full
`lookup::reactions` set; filtering deeper trips both and moves `ratio_by_code`'s
addressing besides. Filtering at the universe leaves every baked artifact byte-identical
and simply stops the benchmark from counting reactions it was never going to learn
anything from.

COMPARTMENTS ARE NOT MODELLED ANYWHERE, and that is not a thing this module does -- it is
a thing the tree already did. Equation parsing strips the `@COMP` suffix at every entry
point, no `MNXC` identifier is acquired, and the GEM path is reaction-level. So the
compartment a transporter crosses is invisible here in the first place, which is the
other half of why its atom pairs say nothing: without compartments, "glucose out" and
"glucose in" are the same node and the transport step is a self-edge.

Not the same exclusion as `dir_calibrate`'s. That one drops transport reactions from the
thermodynamic calibration because a standard delta-G-prime omits the membrane potential
term, so the number would be wrong rather than uninformative. It stays, and it is about
directions, not coverage.
"""
from pathlib import Path

import pandas as pd


def transport_mnxrs(reac_prop: str | Path) -> set:
    """Every MNXR MetaNetX flags as transport.

    `reac_prop.tsv` column 6 (0-indexed 5) is the flag, `"T"` or empty -- the same column
    `mnx_lookups.load_reac_prop` carries into `lookup::reactions` as `is_transport`. Read
    from the source rather than through the compiled lookup so the benchmark builders do
    not gain a dependency on the whole reaction table to answer one boolean.
    """
    out = set()
    with open(reac_prop, errors="replace") as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) >= 6 and p[0] not in ("", "EMPTY") and p[5].strip() == "T":
                out.add(p[0])
    if not out:
        raise SystemExit(f"[universe] {reac_prop} flagged no reaction as transport -- "
                         f"either the column moved or this is not reac_prop.tsv")
    return out


def all_mnxrs(reac_prop: str | Path) -> set:
    """Every MNXR MetaNetX defines. The membership test an emitted edge has to pass.

    NOT the bridge's reaction set, which is what a benchmark builder reaches for first
    and which is a different question: the bridge holds only reactions carrying an EC, KO,
    MetaCyc or UniProt key, so a curated id for a reaction with none of those reads as
    "not a reaction" against it. Measured on the seven extractions, 151 of 869 curated ids
    are absent from the bridge and 0 are absent from MetaNetX -- the whole gap was the
    wrong universe.
    """
    out = set()
    with open(reac_prop, errors="replace") as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            mnxr = line.split("\t", 1)[0].strip()
            if mnxr.startswith("MNXR"):
                out.add(mnxr)
    if not out:
        raise SystemExit(f"[universe] {reac_prop} defines no MNXR -- this is not "
                         f"reac_prop.tsv")
    return out


def reac_prop_path(metanetx_dir: str | Path) -> Path:
    """`fabfos_data::metanetx` -> its single release's `reac_prop.tsv`.

    The release directory is read off disk rather than pinned, because the acquisition
    names it after what the server served; hardcoding a number here would silently target
    the wrong snapshot. Same resolution `host_gpr_gem` does for `reac_xref.tsv`.
    """
    mnx = Path(metanetx_dir)
    rels = sorted(p for p in mnx.glob("*") if p.is_dir())
    if len(rels) != 1:
        raise SystemExit(f"[universe] expected one MetaNetX release under {mnx}, found "
                         f"{[p.name for p in rels]}")
    path = rels[0] / "reac_prop.tsv"
    if not path.exists():
        raise SystemExit(f"[universe] no reac_prop.tsv under {rels[0]}")
    return path


def atom_universe(vocab, pairs_path: str | Path, exclude=()) -> tuple:
    """`(universe, stats)` -- the MNXRs a benchmark row may be marked in-universe for.

    `vocab` is the bake's vocabulary, as a DataFrame or a path. Reads `atom_pairs.rxn`,
    a plain vocab code, and no packed node column -- which is why the encoder's version
    gate does not apply here and only the three files being ONE artifact has to hold
    (each caller asserts that itself, before calling this).

    `stats` is returned rather than printed so the caller can put it in its BUILD.json:
    a table built against the full universe and one built against the transport-free
    universe are different claims about the same reactions, and the count is how a reader
    tells them apart.
    """
    V = vocab if isinstance(vocab, pd.DataFrame) else pd.read_parquet(vocab)
    rxn_symbol = V[V["kind"] == "rxn"].set_index("code")["symbol"]
    codes = pd.read_parquet(pairs_path, columns=["rxn"])["rxn"].unique()
    unknown = set(codes) - set(rxn_symbol.index)
    if unknown:
        raise SystemExit(f"[universe] {len(unknown)} rxn codes in atom_pairs are absent "
                         f"from the vocab -- the two files are not one bake")
    mapped = set(rxn_symbol.loc[codes].tolist())
    exclude = set(exclude)
    universe = mapped - exclude
    stats = dict(n_mapped=len(mapped), n_excluded=len(mapped & exclude),
                 n_universe=len(universe), excluded_kind="transport")
    return universe, stats


def universe_line(stats: dict, tag: str) -> str:
    """The one line every builder prints, so two builders cannot describe it differently."""
    return (f"[{tag}] atom universe: {stats['n_universe']:,} reactions "
            f"({stats['n_mapped']:,} mapped by the bake, less {stats['n_excluded']:,} "
            f"{stats['excluded_kind']})")
