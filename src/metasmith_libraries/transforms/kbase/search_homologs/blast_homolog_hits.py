from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::blast.env"))
subject = model.AddRequirement(lib.GetType("sequences::assembly"))
query   = model.AddRequirement(lib.GetType("sequences::orfs"))
hits    = model.AddProduct(lib.GetType("annotation::homolog_hits"))

def protocol(context: ExecutionContext):
    isubject=context.Input(subject)
    iquery=context.Input(query)
    ihits=context.Output(hits)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-num_threads {threads}"
    # The query is protein and the subject is a genome, so the database is `nucl` and
    # the search is tblastn -- the six-frame translation of the subject. Reading the
    # two the other way round builds a protein database out of contigs and finds
    # nothing, with no error.
    _cmd = f"""\
            makeblastdb -in {isubject.container} -dbtype nucl -out subject_db
            tblastn -query {iquery.container} -db subject_db {threads} \
                -evalue 1e-5 -outfmt 6 -max_target_seqs 10000 -out {ihits.container}
        """
    context.ExecWithEnv().ifContainerDo(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{hits: ihits.local}],
        success=ihits.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=subject,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
