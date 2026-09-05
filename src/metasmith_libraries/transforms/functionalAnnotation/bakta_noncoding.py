from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::bakta.env"))
assembly = model.AddRequirement(lib.GetType("sequences::assembly"))
db       = model.AddRequirement(lib.GetType("annotation::bakta_db"))
to_gff3  = model.AddRequirement(lib.GetType("lib::piler_cr_to_gff3.py"))
out_gff  = model.AddProduct(lib.GetType("annotation::bakta_gff"))
out_tsv  = model.AddProduct(lib.GetType("annotation::bakta_tsv"))
# bakta writes twelve files under the flags above and this transform declared two of them.
# Three of the other ten are worth having and cost nothing to declare: the .gbff is the
# annotated genome record and the interchange format everything downstream of `annotate`
# reads, the .ffn carries the sequence of every feature called, and the .txt is the
# per-genome summary. The remaining seven are empty or duplicate the input under --skip-cds.
out_gbk  = model.AddProduct(lib.GetType("sequences::gbk"))
out_ffn  = model.AddProduct(lib.GetType("annotation::bakta_feature_nt"))
out_txt  = model.AddProduct(lib.GetType("annotation::bakta_summary"))


def protocol(context: ExecutionContext):
    iasm = context.Input(assembly)
    idb  = context.Input(db)
    igff3 = context.Input(to_gff3)
    ogff = context.Output(out_gff)
    otsv = context.Output(out_tsv)
    ogbk = context.Output(out_gbk)
    offn = context.Output(out_ffn)
    otxt = context.Output(out_txt)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idb.external, "/db")],
        cmd=f"""
            bakta \
                --db /db/db-light \
                --skip-cds --skip-sorf --skip-pseudo --skip-plot --skip-crispr \
                --force \
                --prefix bakta_noncoding \
                --output bakta_out \
                {iasm.container}
        """,
    )

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            pilercr -in {iasm.container} -out crispr_raw.txt -noinfo -quiet || true

            if [ -s crispr_raw.txt ]; then
                python3 {igff3.container} crispr_raw.txt bakta_out/bakta_noncoding.gff3
            fi
        """,
    )

    context.LocalShell(f"cp bakta_out/bakta_noncoding.gff3 {ogff.local}")
    context.LocalShell(f"cp bakta_out/bakta_noncoding.tsv {otsv.local}")
    context.LocalShell(f"cp bakta_out/bakta_noncoding.gbff {ogbk.local}")
    context.LocalShell(f"cp bakta_out/bakta_noncoding.ffn {offn.local}")
    context.LocalShell(f"cp bakta_out/bakta_noncoding.txt {otxt.local}")

    return ExecutionResult(
        manifest=[
            {
                out_gff: ogff.local,
                out_tsv: otsv.local,
                out_gbk: ogbk.local,
                out_ffn: offn.local,
                out_txt: otxt.local,
            },
        ],
        success=all(p.local.exists() for p in (ogff, otsv, ogbk, offn, otxt)),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=assembly,
    resources=Resources(
        cpus=4,
        memory=Size.GB(32),
        duration=Duration(hours=8),
    ),
)
