# module_mag_anchors_absent -- the `off` arm of module_mag_anchors: no module is anchored to a
# MAG. A header-only anchor table, keyed on `Taxon` as the master summary joins it.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
study   = model.AddRequirement(lib.GetType("aspire::study_metadata"))
policy  = model.AddRequirement(lib.GetType("aspire::asv_mag_link_off"), parents={study})
anchors = model.AddProduct(lib.GetType("aspire::module_asv_anchor_table"))


def protocol(context: ExecutionContext):
    ianchors = context.Output(anchors)
    ianchors.local.write_text("Taxon\tmodule_label\thas_mag_pair\n")
    return ExecutionResult(
        manifest=[{anchors: ianchors.local}],
        success=ianchors.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
