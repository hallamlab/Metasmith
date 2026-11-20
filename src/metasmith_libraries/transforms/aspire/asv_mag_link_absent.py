# asv_mag_link_absent -- the `off` arm of asv_mag_link: no ASV is paired with a genome. The
# pairing table is written with the columns the linker gives it when it finds no hit, header
# only, since graph_network and the anchors read it whether or not the link ran. The output
# directory holds the same table where the linker puts it, for the master summary's scan.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
survey  = model.AddRequirement(lib.GetType("amplicon::survey"))
policy  = model.AddRequirement(lib.GetType("aspire::asv_mag_link_off"), parents={survey})
pairing = model.AddProduct(lib.GetType("aspire::asv_mag_pairing"))
out     = model.AddProduct(lib.GetType("aspire::asv_mag_outputs"))

HEADER = "ASV_ID\tasv_uid\tpairing_status\tgenome_id\tgenome_uid\n"


def protocol(context: ExecutionContext):
    ipairing, iout = context.Output(pairing), context.Output(out)
    ipairing.local.write_text(HEADER)
    (iout.local / "tables").mkdir(parents=True, exist_ok=True)
    (iout.local / "tables" / "asv2mag_pairing.tsv").write_text(HEADER)
    return ExecutionResult(
        manifest=[{pairing: ipairing.local, out: iout.local}],
        success=ipairing.local.exists() and iout.local.is_dir(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=survey,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
