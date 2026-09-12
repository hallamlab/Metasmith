from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::carveme.env"))
# `sequences::bin_orfs` -- NOT the ambiguous `sequences::orfs`, satisfiable
# from EITHER `metagenomics/prodigal.py` (a whole assembly) or
# `viromics/prodigal_gv.py` (the viral lane's frozen set) -- see
# `sequences::bin_orfs`'s own comment in `data_types/sequences.yml`. metaGEM
# reconstructs per MAG, and pinning to the distinct bin-scoped type is what
# makes exactly one producer, this library's own `prodigal_from_bin.py`,
# answer the slot. It is already PROTEIN: `Data: protein sequence`,
# `ext: faa`. CarveMe's own `carve` takes a protein FASTA by default
# (`--dna`/`--egg`/`--refseq` are the other input modes, none of which apply
# here), so no nucleotide-to-protein step belongs in front of this
# requirement.
orfs    = model.AddRequirement(lib.GetType("sequences::bin_orfs"))
media   = model.AddRequirement(lib.GetType("modelling::media"))
medium  = model.AddRequirement(lib.GetType("modelling::medium_name"))
helpers = model.AddRequirement(lib.GetType("lib::modelling"))
out     = model.AddProduct(lib.GetType("modelling::carveme_model"))

def protocol(context: ExecutionContext):
    iorfs   = context.Input(orfs)
    imedia  = context.Input(media)
    imedium = context.Input(medium)
    ihelp   = context.Input(helpers)
    iout    = context.Output(out)

    _cmd = f"""\
            export XDG_CACHE_HOME=$TMPDIR
            python {ihelp.container}/carveme_from_orfs.py \
                {iorfs.container} {imedia.container} {imedium.container} {iout.container}
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
