from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::carveme.env"))
# `sequences::orfs` -- NOT `sequences::orfs_shard`/`orf_chunk`/`orf_batch` (true
# siblings, not subtypes -- see their comments in `data_types/sequences.yml`) --
# is the whole per-sample ORF FASTA `pprodigal`/`prodigal-gv` emit, and it is
# already PROTEIN: `Data: protein sequence`, `ext: faa`. CarveMe's own `carve`
# takes a protein FASTA by default (`--dna`/`--egg`/`--refseq` are the other
# input modes, none of which apply here), so no nucleotide-to-protein step
# belongs in front of this requirement.
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"))
media   = model.AddRequirement(lib.GetType("modelling::media"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
out     = model.AddProduct(lib.GetType("modelling::carveme_model"))

def protocol(context: ExecutionContext):
    iorfs  = context.Input(orfs)
    imedia = context.Input(media)
    ihelp  = context.Input(helpers)
    iout   = context.Output(out)

    # CAUTION `medium_name` is fixed to "M9" for the same reason
    # `carveme_gapfill.py` fixes it: the only worked example today,
    # `research/metasmith_libraries/carveme_m9_medium.tsv`, carries a single
    # medium named that. A template supplying a multi-medium table must also
    # change this argument.
    _cmd = f"""\
            export XDG_CACHE_HOME=$TMPDIR
            python {ihelp.container}/carveme_from_orfs.py \
                {iorfs.container} {imedia.container} M9 {iout.container}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)
