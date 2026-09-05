import shlex
# One job over the whole host set, not one per model: the planner binds a
# requirement to ONE concrete type, so three sibling per-host subtypes collapse
# to whichever it picked while the task count still reads correct.
#
# EPI300 borrows DH10B's model and AG1 borrows DH1's, and the borrow is not
# free: `proV` and `fhuA` are pseudogenes in EPI300, both inside AND clauses, so
# seven transport reactions go dark. A pseudogene is a CDS with no
# `/protein_id`, so a check keyed on the protein accession never sees one --
# which is why `check_epi300_identity.py` asserts this exact list rather than
# asserting emptiness.
#
# Needs cobra to open the model, so it cannot run inside the pinned ecspr image.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::cobra.env"))
genomes   = model.AddRequirement(lib.GetType("fabfos_data::genomes"))
bridge    = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
pairs     = model.AddRequirement(lib.GetType("ref::atom_pairs"))
vocab     = model.AddRequirement(lib.GetType("ref::metabolism_vocab"))
direction = model.AddRequirement(lib.GetType("ref::direction_ratios"))
universe_m = model.AddRequirement(lib.GetType("buildlib::bench_universe.py"))
ev_lib     = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
bl        = model.AddRequirement(lib.GetType("buildlib::benchmark"))
out       = model.AddProduct(lib.GetType("ref::gpr_table_gem"))

CHANNEL = "gem_gpr"
LANE_SET = "curated"
EXTENSIONS = ("attribution", "feature", "universe")

GEM_SOURCE = {
    "e_coli_k12":    "e_coli_k12",
    "e_coli_dh10b":  "e_coli_dh10b",
    "e_coli_epi300": "e_coli_dh10b",
    "e_coli_dh1":    "e_coli_dh1",
    "e_coli_ag1":    "e_coli_dh1",
    "e_coli_bw25113": "e_coli_k12",
    "e_coli_lw06":    "e_coli_k12",
}

# host -> the borrowed model's reactions that strain cannot carry, by the MODEL's own
# reaction id. A fact about two genomes, declared here for the same reason the host set
# is declared in acquire/genomes.py: a caller who could pass a different edit list could
# make two runs of "the reference build" mean different things.
#
# EMPTY FOR EPI300, AND THE MEASUREMENT BEHIND IT IS NOT. `proV` and `fhuA` are
# pseudogenes in EPI300 and intact in DH10B, both sit in AND clauses, and seven of the
# model's reactions therefore go dark: PROabcpp, CRNabcpp, CRNDabcpp, CTBTabcpp (the ProU
# osmoprotectant ABC transporter) and FE3HOXtonex, FECRMtonex, FEOXAMtonex (the
# ferrichrome TonB-dependent receptor -- a broken fhuA being the classic
# T1-phage-resistance marker in a cloning strain). check_epi300_identity.py still asserts
# that exact set, because a difference that drifts without failing is the failure mode
# this whole check exists for.
#
# ALL SEVEN ARE TRANSPORT, WHICH IS WHY THE LIST IS EMPTY RATHER THAN THOSE SEVEN. The
# benchmark excludes transport from the atom universe (buildlib::bench_universe.py), so
# all seven are already marked out of universe and carry no edge either way. Editing them
# out would move no measurement while making the two tables differ, so the honest table is
# DH10B's read faithfully under the EPI300 tag.
#
# AG1 IS NOT EMPTY, AND THE CONTRAST IS THE POINT. Its genotype is seven markers and
# `check_ag1_identity.py` measures each against this model: five of them -- recA1, endA1,
# gyrA96, hsdR17, supE44 -- name no gene in it, so they are free. `relA1` names `relA`,
# which carries two reactions: GDPDPK is `relA or spoT` and survives on the isozyme,
# GTPDPK is relA alone and goes dark. One reaction, and it is INSIDE the atom universe,
# so this borrow moves the network where EPI300's could not.
#
# `thi-1` IS DELIBERATELY ABSENT. It is a classical thiamine-auxotrophy allele rather
# than a locus, and the module it lies in is 12 genes over 10 reactions here. Picking one
# would put a fabricated deletion into the background of every eydallin condition, and a
# fabricated deletion is worse than a known gap because nothing downstream can tell.
# What the marker really says is that AG1 needs thiamine in the medium, which is a claim
# about the medium and belongs where the medium is declared.
#
# BW25113 AND LW06 ARE THE ONLY ENTRIES HERE WHOSE DELETIONS ARE ENGINEERED RATHER THAN
# INCIDENTAL, and both lists are measured by `check_lw06_identity.py` by evaluating every
# GPR rule twice rather than by counting deleted genes. The counting answer is wrong in
# both directions and that is the whole reason the check exists:
#
#   BW25113's six catabolic loci (lacZ, araBAD, rhaBAD) darken SEVEN reactions, while
#   hsdR514 and the tolerance host's recA name no gene in the model at all. All seven are
#   sugar catabolism the selections never fed -- arabinose, rhamnose and lactose are
#   absent from both papers' minimal media -- so the edit is real but inert here.
#
#   LW06's four deletions span SEVEN genes and darken only THREE reactions. FRD2 and FRD3
#   go with frdABCD and LDH_D with ldhA, but ACKr survives on purT/tdcD, and ALCD2x,
#   ALCD19 and ACALD all survive adhE on adhP and mhpF. So iML1515 says LW06 can still
#   make ethanol without its engineered pathway. That is a statement about what a curated
#   GEM can express about a strain engineering, and it is reported rather than papered
#   over: deleting the surviving reactions to force the expected topology would be
#   asserting a genome nobody sequenced.
#
# THE HETEROLOGOUS HALF OF LW06 IS NOT HERE AND CANNOT BE. attTn7::pdcZm adhBZm ADDS
# reactions, and this transform only subtracts. The pyruvate decarboxylase edge rides in
# as study GPR rows and concatenates with the host's at solve time.
EDIT_LIST = {
    "e_coli_ag1": ("GTPDPK",),
    "e_coli_bw25113": ("ARAI", "LACZ", "LYXI", "RBK_L1", "RMI", "RMK", "RMPA"),
    "e_coli_lw06": ("ARAI", "FRD2", "FRD3", "LACZ", "LDH_D", "LYXI", "RBK_L1",
                    "RMI", "RMK", "RMPA"),
}

def protocol(context: ExecutionContext):
    iout = context.Output(out)
    ibl = context.Input(bl)
    cmd = f"""
            python3 {ibl.container}/host_gpr_gem.py \
            --universe-m {context.Input(universe_m).container} \
            --genomes {context.Input(genomes).container} \
            --metanetx {context.Input(metanetx).container} \
            --vocab {context.Input(vocab).container} \
            --pairs {context.Input(pairs).container} \
            --direction {context.Input(direction).container} \
            --bridge {context.Input(bridge).container} \
            --channel {CHANNEL} \
            --lane-set {LANE_SET} \
            --extensions '{repr(list(EXTENSIONS))}' \
            --ev-lib {context.Input(ev_lib).container} \
            --gem-source {shlex.quote(repr(GEM_SOURCE))} \
            --edit-list {shlex.quote(repr(EDIT_LIST))} \
            --out {iout.container}
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    made = sorted((iout.local / "hosts").glob("*/gpr_gem.parquet")) \
        if (iout.local / "hosts").exists() else []
    Log.Info(f"gem_gpr: {len(made)}/{len(GEM_SOURCE)} host tables")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(made) == len(GEM_SOURCE) and (iout.local / "BUILD.json").exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=genomes,
    labels=["local"],
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=1)),
)
