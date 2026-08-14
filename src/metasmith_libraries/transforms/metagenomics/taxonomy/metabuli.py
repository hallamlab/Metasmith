from metasmith.python_api import *

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::metabuli.env"))
ref         = model.AddRequirement(lib.GetType("ref::metabuli_ref"))
asm         = model.AddRequirement(lib.GetType("sequences::assembly"))
tax         = model.AddProduct(lib.GetType("taxonomy::metabuli"))
rep         = model.AddProduct(lib.GetType("taxonomy::metabuli_report"))
html        = model.AddProduct(lib.GetType("taxonomy::metabuli_krona"))

def protocol(context: ExecutionContext):
    iref    = context.Input(ref)
    iasm    = context.Input(asm)
    itax    = context.Output(tax)
    irep    = context.Output(rep)
    ihtml   = context.Output(html)

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--threads {threads}"
    mem = context.params.get('memory')
    mem = "" if mem is None else f"--max-ram {int(float(mem))-6}"
    job_name = "metabuli_out"
    context.ExecWithEnv().ifContainerDo(
        env = image,
        cmd = f"""\
            metabuli classify \
                {iasm.container} \
                {iref.container} \
                ./ \
                {job_name} \
                {threads} {mem} \
                --lineage 1 \
                --taxonomy-path ./tmp/taxonomy \
                --seq-mode 3

            mv ./*classifications.tsv {itax.container}
            mv ./*report.tsv {irep.container}
            mv ./*krona.html {ihtml.container}
        """
    )
    
    return ExecutionResult(
        manifest=[
            {
                tax: itax.local,
                rep: irep.local,
                html: ihtml.local,
            },
        ],
        success=itax.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=32,
        memory=Size.GB(180),
        duration=Duration(hours=24),
    )
)
