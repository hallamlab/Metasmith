"""The deep-learning protein-embedding workflow, stated once.

Three drivers wanted this workflow and each carried its own copy of the target
table, the weight table and the library assembly: the launcher, the seed probe
and a DAG renderer. They had already drifted -- the probe read its weights from
`MSM_HPC_BASE` and the launcher from `MSM_HPC_SCRATCH`, so the two were planning
against different paths while claiming to answer the same question.

What is shared is the *shape*: which targets exist, which weights each one needs,
and how an input library for them is assembled. What is not shared is where the
files are -- the template defers them, the launcher resolves them on a cluster --
so paths are the caller's to supply.
"""

from __future__ import annotations

from pathlib import Path

from metasmith.python_api import DataInstanceLibrary, Spec

import _authoring as A

# (produced_type, gpu_tier, weights_key, transform_module_name)
# transform_module_name is what shows up in Nextflow process names as "p<N>__<name>"
TARGETS: dict[str, tuple[str, str, str | None, str]] = {
    "esmc":          ("annotation::esm_c_embeddings",     "med",   "esmc",         "esm_c"),
    "ankh":          ("annotation::ankh_embeddings",      "med",   "ankh",         "ankh"),
    "prott5":        ("annotation::prott5_embeddings",    "med",   "prott5",       "prott5"),
    "esmfold":       ("sequences::predicted_structures",  "med",   "esmfold",      "esmfold"),
    "foldseek_3di":  ("sequences::structure_3di_tokens",  "cpu",   None,           "foldseek_3di"),
    "saprot":        ("annotation::saprot_embeddings",    "med",   "saprot",       "saprot"),
}

# Weight tarballs, by key. The tarball extension must match the data type's ext.
WEIGHT_TYPES: dict[str, str] = {
    "esmc":    "ref::esm_c_300m_weights",
    "ankh":    "ref::ankh_base_weights",
    "prott5":  "ref::prott5_xl_uniref50_weights",
    "saprot":  "ref::saprot_650m_weights",
    "esmfold": "ref::esmfold_weights",
}
WEIGHT_FILES: dict[str, str] = {
    "esmc": "esmc_300m.tgz", "ankh": "ankh_base.tgz", "prott5": "prott5_xl.tgz",
    "saprot": "saprot_650m.tgz", "esmfold": "esmfold_v1.tgz",
}

# Weights needed transitively when a target is selected. Without them the planner
# introduces download* transforms for the missing ones, which crashes on the
# cgroup-limited login node. foldseek_3di and saprot both consume esmfold's
# predicted_structures, so both need esmfold's weights.
TRANSITIVE_WEIGHTS = {"saprot": ["esmfold"], "foldseek_3di": ["esmfold"]}

# Transforms the planner pulls in as transitive producers. Each must also appear
# in TARGETS so its GPU tier is known: without this, `--only saprot` plans
# esmfold + foldseek_3di whose clusterOptions fall through to the CPU default,
# and ESMFold runs on CPU and never finishes before walltime.
TRANSITIVE_TRANSFORMS = {"saprot": ["esmfold", "foldseek_3di"], "foldseek_3di": ["esmfold"]}

# GPU tier -> SLURM --gpus= MIG slice
GPU_SLICE = {
    "small": "nvidia_h100_80gb_hbm3_1g.10gb:1",
    "med":   "nvidia_h100_80gb_hbm3_3g.40gb:1",
    "cpu":   None,
}

TYPE_LIBRARIES = ("sequences.yml", "annotation.yml", "ref.yml")


def needed_weights(target_keys: list[str]) -> list[str]:
    """Weight keys for these targets, transitive deps included, in order."""
    seen, out = set(), []
    for t in target_keys:
        _, _, w_key, _ = TARGETS[t]
        for w in [w_key, *TRANSITIVE_WEIGHTS.get(t, [])]:
            if w and w not in seen:
                seen.add(w)
                out.append(w)
    return out


def gpu_map(target_keys: list[str]) -> dict[str, str | None]:
    """Nextflow transform name -> SLURM gpu slice, transitive steps included."""
    out: dict[str, str | None] = {}
    for t in target_keys:
        for tt in [t, *TRANSITIVE_TRANSFORMS.get(t, [])]:
            _, tier, _, tname = TARGETS[tt]
            out[tname] = GPU_SLICE[tier]
    return out


def add_inputs(lib: DataInstanceLibrary, orfs, weights: dict[str, object]) -> None:
    """Type libraries, the ORFs, and one row per weight tarball.

    `orfs` and each value of `weights` is a path or `DEFERRED`; `AddItem` takes
    either, so a template and a real run assemble the same library.
    """
    for tl in TYPE_LIBRARIES:
        lib.AddTypeLibrary(A.TYPES / tl)
    lib.AddItem(orfs, "sequences::orfs")
    for key, path in weights.items():
        lib.AddItem(path, WEIGHT_TYPES[key])


def spec(input_library, target_keys: list[str]) -> Spec:
    # No sample type. Splitting on `sequences::orfs` would mask the library down
    # to that one row's lineage, and the weight tarballs are nobody's ancestor --
    # they would vanish from the plan and come back as `download*` steps. The
    # library is one run's inputs, so it is planned as it stands.
    return Spec(
        input_library=input_library,
        target_types=[TARGETS[t][0] for t in target_keys],
        transform_libraries=A.transforms("functionalAnnotation", "logistics"),
        resource_libraries=[A.envs()],
    )
