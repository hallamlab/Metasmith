"""Encode the direction annotation against the AAM assembly's vocabulary.

THE WHOLE STEP IS ONE CALL, and that is the point of it existing. It used to be the last
four lines of `direction_ensemble.py`, which is tidy until you notice what it costs: the
merged step required `ref::metabolism_vocab`, so the entire direction branch -- the curated
MetaCyc member, the calibration, the three-member fusion -- sat behind the atom-mapping
branch waiting for a vocabulary it barely reads.

BARELY READS IS LITERAL. The encoder touches exactly one of the vocabulary's five spaces,
`rxn`, and `build_vocabulary` fills that space from `sorted(universe)` -- the MetaNetX
reaction list -- not from the mapper output. Nothing the mappers compute reaches the
direction science at all. So the dependency was never on the AAM's WORK, only on its
NAMING, and naming is minutes.

THE IDENTITY GUARANTEE IS UNCHANGED, because it never lived in the step. `bake_direction`
reads the identity block off `vocab.parquet` and writes it through byte for byte; there is
no code path here that could mint a second one, and `cmd_direction` follows the bake with
`selftest_direction`, whose first assertion is `assert_same_bake(vocab, direction)`. That
was true when this ran inside the assembly and it is true now. What the split changes is
scheduling, not the trio's correctness.

IT RUNS IN THE eQUILIBRATOR IMAGE and needs nothing from it but pandas and pyarrow. The
image is chosen so this step lands on the same side of the graph as the annotation it
consumes -- the vocabulary is one small file and travels; the annotation and its evidence
do not need to.

THE ENCODER REFUSES an unknown reaction rather than coding one. MetaNetX's `EMPTY`
sentinel is the one to expect. That refusal matters here more than anywhere: the `rxn`
column is unsigned, so a silent miss would land as 4,294,967,295 instead of raising.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("env::equilibrator.env"))

annot      = model.AddRequirement(lib.GetType("interm::direction_annotation"))
vocab      = model.AddRequirement(lib.GetType("ref::metabolism_vocab"))

encoding   = model.AddRequirement(lib.GetType("buildlib::refs_encoding.py"))
baker      = model.AddRequirement(lib.GetType("buildlib::bake_metabolism.py"))

ratios     = model.AddProduct(lib.GetType("ref::direction_ratios"))


def protocol(context: ExecutionContext):
    iann = context.Input(annot)
    ivoc = context.Input(vocab)
    ilib = context.Input(baker)
    iout = context.Output(ratios)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"
    cmd = f"""
        set -e
        {py} {libdir}/bake_metabolism.py direction \
            --direction {iann.container} \
            --vocab {ivoc.container} \
            --out {iout.container}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    # No evidence product: this step runs no tool, it re-expresses one table in another
    # table's codes. The evidence for the direction call is the assembly's, and the
    # evidence for THIS step is the selftest, which is a non-zero exit rather than a file.
    return ExecutionResult(
        manifest=[{ratios: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # One encode of 83,795 rows against a vocabulary already on disk, plus the selftest.
    # Measured in seconds; the hour is for the queue's sake, not the work's.
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(hours=1)),
)
