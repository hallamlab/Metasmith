from __future__ import annotations
import os
import sys
import time
from collections import Counter
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[3] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from metasmith.models.solver import solve_by_mcts, Transform, Endpoint


P_META   = {"read_metadata"}
P_PAIR   = {"read_pair"}
P_ZFR    = {"zipped_forward_short_reads", "read_length:short", "direction:forward",
            "parity:paired_end", "pair_state:half"}
P_ZRV    = {"zipped_reverse_short_reads", "read_length:short", "direction:reverse",
            "parity:paired_end", "pair_state:half"}
P_SHORT  = {"reads", "read_length:short", "Format:FASTQ", "Data:nucleic_acid_sequence",
            "pair_state:complete", "qc:none", "compression:gzip"}
P_QC     = {"read_qc_stats"}

P_ASM_BASE = {"Format:FASTA", "Data:nucleic_acid_sequence", "ext:fna"}
P_ASM      = P_ASM_BASE | {"origin:assembler"}
P_PG       = P_ASM_BASE | {"genome_scope:single"}
P_BINFA    = lambda m: P_PG | {"bin_fasta", f"method:{m}"}
def asm_props(broken: bool) -> set[str]:
    return P_ASM_BASE if broken else P_ASM
P_TABLE  = lambda m: {"contig_to_bin_table", f"method:{m}"}

P_ASTATS = {"assembly_stats"}
P_CONCOV = {"assembly_per_contig_coverage"}
P_BPCOV  = {"assembly_per_bp_coverage"}
P_BAM    = {"bam"}


def build_world(*, link_asm_to_meta: bool = True,
                bin_fasta_subtypes_assembly: bool = True,
                broken_asm_props: bool = False):
    asm_p = asm_props(broken=broken_asm_props)
    meta_ep = Endpoint(properties=P_META)
    pair_ep = Endpoint(properties=P_PAIR, parents={meta_ep})
    r1_ep   = Endpoint(properties=P_ZFR,  parents={pair_ep})
    r2_ep   = Endpoint(properties=P_ZRV,  parents={pair_ep})
    asm_ep  = Endpoint(properties=asm_p,  parents={r1_ep})

    transforms: list[Transform] = []
    names: dict[Transform, str] = {}

    interleave = Transform()
    pair_d = interleave.AddRequirement(properties=P_PAIR)
    interleave.AddRequirement(properties=P_ZFR, parents={pair_d})
    interleave.AddRequirement(properties=P_ZRV, parents={pair_d})
    interleave.AddProduct(properties=P_SHORT)
    transforms.append(interleave); names[interleave] = "interleave"

    seqkit = Transform()
    seqkit.AddRequirement(properties={"reads"})
    seqkit.AddProduct(properties=P_QC)
    transforms.append(seqkit); names[seqkit] = "seqkit_reads"

    asm_stats = Transform()
    meta_d  = asm_stats.AddRequirement(properties=P_META)
    reads_d = asm_stats.AddRequirement(properties={"reads"}, parents={meta_d})
    qc_d    = asm_stats.AddRequirement(properties=P_QC, parents={meta_d})
    asm_d   = asm_stats.AddRequirement(
        properties=asm_p,
        parents={meta_d} if link_asm_to_meta else {reads_d},
    )
    asm_stats.AddProduct(properties=P_ASTATS)
    asm_stats.AddProduct(properties=P_CONCOV)
    asm_stats.AddProduct(properties=P_BPCOV)
    asm_stats.AddProduct(properties=P_BAM)
    transforms.append(asm_stats); names[asm_stats] = "assembly_stats"

    for method in ("metabat2", "semibin2", "comebin"):
        t = Transform()
        a = t.AddRequirement(properties=P_ASM)
        t.AddRequirement(properties=P_BAM, parents={a})
        if bin_fasta_subtypes_assembly:
            t.AddProduct(properties=P_BINFA(method))
        else:
            t.AddProduct(properties={"bin_fasta", f"method:{method}"})
        t.AddProduct(properties=P_TABLE(method))
        transforms.append(t); names[t] = method

    given = [{meta_ep, pair_ep, r1_ep, r2_ep, asm_ep}]
    return given, transforms, names


def run_repro(label: str,
              target_props: list[set[str]],
              *,
              link_asm_to_meta: bool = True,
              bin_fasta_subtypes_assembly: bool = True,
              broken_asm_props: bool = False,
              quiet: bool = False):
    given, transforms, names = build_world(
        link_asm_to_meta=link_asm_to_meta,
        bin_fasta_subtypes_assembly=bin_fasta_subtypes_assembly,
        broken_asm_props=broken_asm_props,
    )

    target = Transform()
    for props in target_props:
        target.AddRequirement(properties=props)

    print(f"\n=== {label} ===")
    print(f"  link_asm_to_meta={link_asm_to_meta}  "
          f"bin_fasta⊃assembly={bin_fasta_subtypes_assembly}  "
          f"n_targets={len(target_props)}")

    t0 = time.time()
    sol = solve_by_mcts(given=given, target=target, transforms=transforms)
    elapsed = time.time() - t0

    def name_of(app):
        if app.transform is target:
            return "<target>"
        if not app.transform.requires and any(app.produced):
            return "<given>"
        return names.get(app.transform, "?")

    counts = Counter(name_of(s) for s in sol.dependency_plan)
    print(f"  complete={sol.complete}  steps={len(sol.dependency_plan)}  time={elapsed*1000:.1f}ms")
    print(f"  mcts_iterations={sol._iterations}  refiner_iterations={sol._refiner_iterations}")
    print(f"  transform counts: {dict(counts)}")
    if not quiet:
        for i, app in enumerate(sol.dependency_plan):
            n = name_of(app)
            produced = [sorted(e.properties)[:3] for pg in app.produced for e in pg.values()]
            used = [sorted(e.properties)[:3] for e in app.used.values()]
            print(f"    [{i}] {n}: used={used} prod={produced}")
    return sol, counts


def make_8_targets():
    return [
        P_QC,
        P_ASTATS, P_CONCOV, P_BPCOV, P_BAM,
        P_TABLE("metabat2"), P_TABLE("semibin2"), P_TABLE("comebin"),
    ]


if __name__ == "__main__":
    full = make_8_targets()

    run_repro("8 targets — POST-FIX (assembly carries origin:assembler)", full,
              broken_asm_props=False)
