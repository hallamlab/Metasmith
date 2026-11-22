import json

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()

dram    = model.AddRequirement(lib.GetType("env::dram.env"))
python  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"), parents={asm})
table   = model.AddRequirement(lib.GetType("annotation::annotation_table"), parents={asm})
mag     = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"), parents={asm})
sheets  = model.AddRequirement(lib.GetType("ref::dram_sheets"))
gpr_lib = model.AddRequirement(lib.GetType("lib::fabfos_gpr"))
ev_lib  = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
annotations = model.AddProduct(lib.GetType("annotation::dram_mag_annotations"))
product     = model.AddProduct(lib.GetType("annotation::dram_product_html"))
product_tsv = model.AddProduct(lib.GetType("annotation::dram_product_tsv"))
summary     = model.AddProduct(lib.GetType("annotation::dram_metabolism_summary"))
stats       = model.AddProduct(lib.GetType("annotation::dram_genome_stats"))

SHEETS = ("genome_summary_form", "module_step_form", "etc_module_database",
          "function_heatmap_form")


# The MAG takes the assembly's own annotation table on its contigs, the way pathologic_mag
# does, so nothing is re-predicted and DRAM's distill reads the lanes every other consumer
# reads. Distill needs only its four forms; the config names nothing else.
def protocol(context: ExecutionContext):
    imag    = context.Input(mag)
    isheets = context.Input(sheets)
    iann    = context.Output(annotations)
    outs    = {product: "product.html", product_tsv: "product.tsv",
               summary: "metabolism_summary.xlsx", stats: "genome_stats.tsv"}
    iouts   = {t: context.Output(t) for t in outs}
    name    = imag.local.stem

    with open("DRAM.config", "w") as fh:
        json.dump({
            "search_databases": {}, "database_descriptions": {}, "custom_dbs": None,
            "dram_sheets": {s: f"{isheets.container}/{s}.tsv" for s in SHEETS},
            "description_db": None, "dram_version": "1.5.0",
        }, fh)

    context.ExecWithEnv(env=python, cmd=f"""
        python3 {context.Input(gpr_lib).container}/dram_annotations.py \
            --ev-lib {context.Input(ev_lib).container} \
            --annotation-table {context.Input(table).container} \
            --orfs {context.Input(orfs).container} \
            --mag {imag.container} --name {name} \
            --out {iann.container}
    """)
    context.ExecWithEnv(env=dram, cmd=f"""
        export HOME=$PWD
        DRAM.py distill -i {iann.container} -o distill --config_loc DRAM.config
    """)
    context.LocalShell("\n".join(f"cp distill/{f} {iouts[t].local}" for t, f in outs.items()))

    return ExecutionResult(
        manifest=[{annotations: iann.local, **{t: i.local for t, i in iouts.items()}}],
        success=iann.local.exists() and all(i.local.exists() for i in iouts.values()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=mag,
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
