"""Variant 4: full real-shape topology.
- ORFs (sequences::orfs) -> shardFasta -> orfs_shard
- 3 encoders consume orfs_shard + their weights
- esmfold(orfs_shard + esmfold_weights) -> predicted_structures
- foldseek_3di(predicted_structures) -> structure_3di_tokens
- saprot(orfs_shard + 3di_tokens[parents=orfs_shard] + saprot_weights) -> saprot_emb
- Five downloader transforms (container -> weights), one per model
- All weights share 'protein_model_weights' property
- Container image given
"""
import sys
from pathlib import Path

SRC = Path("/home/tony/agentic_workspace/projects/metasmith/mcts-prefer-given-bias/src")
sys.path.insert(0, str(SRC))

from metasmith.models.solver import solve_by_mcts, Transform, Endpoint


PMW = "protein_model_weights"


def build_problem():
    transforms: list[Transform] = []
    names: dict[Transform, str] = {}

    # shardFasta: orfs -> orfs_shard
    t = Transform()
    t.AddRequirement(properties={"container_image"})
    t.AddRequirement(properties={"orfs"})
    t.AddProduct(properties={"orfs", "orfs_shard"})  # subtype: orfs_shard IsA orfs
    transforms.append(t)
    names[t] = "shardFasta"

    # encoders
    def encoder(weight_prop: str, out_prop: str, name: str):
        t = Transform()
        t.AddRequirement(properties={"container_image"})
        t.AddRequirement(properties={PMW, weight_prop})
        t.AddRequirement(properties={"orfs", "orfs_shard"})
        t.AddProduct(properties={out_prop})
        transforms.append(t)
        names[t] = name

    encoder("esmc_weights",   "esmc_emb",   "esm_c")
    encoder("ankh_weights",   "ankh_emb",   "ankh")
    encoder("prott5_weights", "prott5_emb", "prott5")

    # esmfold
    t = Transform()
    t.AddRequirement(properties={"container_image"})
    t.AddRequirement(properties={PMW, "esmfold_weights"})
    t.AddRequirement(properties={"orfs", "orfs_shard"})
    t.AddProduct(properties={"predicted_structures"})
    transforms.append(t)
    names[t] = "esmfold"

    # foldseek_3di
    t = Transform()
    t.AddRequirement(properties={"container_image"})
    t.AddRequirement(properties={"predicted_structures"})
    t.AddProduct(properties={"structure_3di_tokens"})
    transforms.append(t)
    names[t] = "foldseek_3di"

    # saprot: lineage parents={orfs_shard}
    t = Transform()
    t.AddRequirement(properties={"container_image"})
    t.AddRequirement(properties={PMW, "saprot_weights"})
    orfs_dep = t.AddRequirement(properties={"orfs", "orfs_shard"})
    t.AddRequirement(properties={"structure_3di_tokens"}, parents={orfs_dep})
    t.AddProduct(properties={"saprot_emb"})
    transforms.append(t)
    names[t] = "saprot"

    # 5 downloaders
    download_trs: dict[str, Transform] = {}
    for w in ["esmc_weights", "ankh_weights", "prott5_weights",
              "esmfold_weights", "saprot_weights"]:
        t = Transform()
        t.AddRequirement(properties={"container_image"})
        t.AddProduct(properties={PMW, w})
        transforms.append(t)
        names[t] = f"download_{w}"
        download_trs[w] = t

    target = Transform()
    target.AddRequirement(properties={"esmc_emb"})
    target.AddRequirement(properties={"ankh_emb"})
    target.AddRequirement(properties={"prott5_emb"})
    target.AddRequirement(properties={"predicted_structures"})
    target.AddRequirement(properties={"structure_3di_tokens"})
    target.AddRequirement(properties={"saprot_emb"})

    given = {
        Endpoint(properties={"orfs"}),
        Endpoint(properties={"container_image"}),
        Endpoint(properties={PMW, "esmc_weights"}),
        Endpoint(properties={PMW, "ankh_weights"}),
        Endpoint(properties={PMW, "prott5_weights"}),
        Endpoint(properties={PMW, "esmfold_weights"}),
        Endpoint(properties={PMW, "saprot_weights"}),
    }
    return given, target, transforms, download_trs, names


def run_one(seed: int):
    given, target, transforms, download_trs, names = build_problem()
    sol = solve_by_mcts(given=[given], target=target, transforms=transforms, seed=seed)
    if not sol.complete:
        return None, []
    plan_trs = [a.transform for a in sol.dependency_plan]
    used_dl = [w for w, tr in download_trs.items() if tr in plan_trs]
    plan_names = [names.get(t, "?") for t in plan_trs if t in names]
    return len(plan_trs), used_dl, plan_names


if __name__ == "__main__":
    print(f"{'seed':>6}  {'steps':>6}  downloaders | plan")
    print("-" * 90)
    for seed in [1, 7, 13, 42, 99, 2024]:
        result = run_one(seed)
        if result[0] is None:
            print(f"{seed:>6}  FAILED")
        else:
            steps, used, plan = result
            tag = "DOWNLOAD" if used else "ok"
            print(f"{seed:>6}  {steps:>6}  {tag:10s} | {plan}")
