"""The ECSPr measurement: fosmid GPR evidence -> per-condition conductance.

    gpr_table + conditions + atom_pairs + direction_ratios -> ecspr::results

ONE DISPATCH, LIKE EVERY OTHER TRANSFORM
This step invokes `ecspr ground`, a command the `ecspr` package provides, exactly
as the assembly steps invoke megahit and the annotation steps invoke kofamscan.
It used to stage the engine as three loose `lib::` python files and had no
protocol at all. The engine now lives in its own package with its own env, which
is what lets a benchmark run and this transform execute the IDENTICAL command --
an import surface can drift between the two, one command line cannot.

Both reference parquets are NETWORK-AGNOSTIC (static functions of the MNXR/MNXM
id space) and so are unpinned shared inputs.

`metabolite_names` is NOT a requirement here, though it once was. Decoding an
MNXM to a chemical identity is what someone does while WRITING the conditions
table, not while measuring against it: the conditions name their hubs as MNXM ids
already. A name lookup inside this step would put the answer at the mercy of a
synonym table nobody stated, which is the same reason `ecspr` has no `resolve`
verb.

`conditions` IS per-experiment, and pinned to the experiment for that reason:
the condition set is the measurement's own claim about what it is testing. It
carries both the terminals and the MASK selecting each condition's GPR rows --
the engine takes no perturbation argument and has no default condition set.

LINEAGE. `gpr` is pinned to `exp`, which is the constraint this stage was
waiting on. It forces the GPR table to be the one built from THIS run's
recovered inserts rather than any table the planner could otherwise reach, and
in doing so pulls the whole recovery chain -- reads, host filter, assembly,
junction split, dedup, ORFs, the four annotation lanes -- into the plan behind
it. Drop the pin and the measurement silently becomes a measurement of
something else.

THE NULL IS A SEPARATE PIPELINE and is not an input here. Significance is scored
against draws over a metagenomic ORF pool, size-matched to each unit's ORF
count; that basis is built once, frozen, and shared, and it takes no part in
this experiment's lineage. `ecspr draw` emits that pool AS a conditions table, so
the null arm is this same command with a different `--conditions` file, and
`ecspr score` joins the two. What this transform produces is the measurement.
"""
from metasmith.python_api import *

lib        = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model      = Transform()

exp        = model.AddRequirement(lib.GetType("fabfos::experiment"))
# THE seam. Pinned to the experiment -- see LINEAGE above.
gpr        = model.AddRequirement(lib.GetType("annotation::gpr_table"), parents={exp})
# The experiment's own claim about what it is testing.
conditions = model.AddRequirement(lib.GetType("ecspr::conditions"), parents={exp})
# The frozen reference basis: shared, unpinned, static in the MNXR/MNXM id space.
pairs      = model.AddRequirement(lib.GetType("ecspr::atom_pairs"))
direction  = model.AddRequirement(lib.GetType("ecspr::direction_ratios"))
img_ecspr  = model.AddRequirement(lib.GetType("env::ecspr.env"))
out        = model.AddProduct(lib.GetType("ecspr::results"))

# The universal leakage ground, not the two-terminal probe. Under a merged ground
# Kirchhoff forces every non-precursor draw to zero, so a fosmid's effect on
# anything outside the precursor set is not a small number -- it is unaskable.
PROBE = "ground"
ELEMENT = "C"
LEAK = "1e-6"


def protocol(context: ExecutionContext):
    igpr  = context.Input(gpr)
    icond = context.Input(conditions)
    ipair = context.Input(pairs)
    idir  = context.Input(direction)
    iout  = context.Output(out)

    # Sharding to a local work directory, so a job killed mid-run resumes from
    # what it solved rather than starting the condition set over.
    cmd = f"""
        ecspr --where
        ecspr {PROBE} \
            --gpr {igpr.container} \
            --conditions {icond.container} \
            --atom-pairs {ipair.container} \
            --direction {idir.container} \
            --element {ELEMENT} --leak {LEAK} \
            --shard-dir ecspr_shards \
            --log ecspr.log \
            --out {iout.container}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=img_ecspr, cmd=cmd) \
        .ifVirtualEnvDo(env=img_ecspr, cmd=cmd)

    # Non-empty, not merely present: an abstaining condition still writes its
    # diagnostic rows, so a zero-byte file means the command died before writing.
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=16,
        memory=Size.GB(64),
        duration=Duration(hours=24),
    )
)
