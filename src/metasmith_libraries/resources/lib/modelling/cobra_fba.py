"""Maximise biomass under a medium, once per condition.

    cobra_fba.py <model.xml> <media.tsv> <conditions.parquet> <out.csv>

A conditions table is one row per medium and mask, which is a KBase PhenotypeSet
in this repository's own idiom -- so `predict_phenotype` is absorbed here rather
than becoming a transform of its own.

`ecspr::conditions` is what round 3 named, and it is an ecspr GPR-mask schema
rather than an FBA one. Two of its columns carry over to a cobra model and the
rest do not, so this reads exactly those two and says so rather than inventing a
meaning for the others:

  `media`   selects rows of the media table by name; blank means the whole table.
  `drop_*`  withholds GPR rows, which over a reconstruction is a gene knockout --
            every id in a drop column is knocked out for that condition.

`background_*` and `mask_*` select which GPR rows are IN, which is a statement
about how a model is BUILT and not about how a built one is solved. They are
ignored, and the count of ignored columns is printed so a caller reading a
suspiciously flat result can see why.
"""
import sys

import cobra
import pandas as pd

import media as media_mod
import mnx

IGNORED_PREFIXES = ("background_", "mask_")


def knockouts(row: dict) -> list[str]:
    out = []
    for column, value in row.items():
        if not column.startswith("drop_") or value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            out.extend(str(v) for v in value)
            continue
        text = str(value).strip()
        if text and text.lower() not in ("nan", "none"):
            out.extend(p.strip() for p in text.replace("|", ",").split(",") if p.strip())
    return out


def main(argv: list[str]) -> int:
    model_path, media_path, conditions_path, out_path = argv

    model = cobra.io.read_sbml_model(model_path)
    biomass = mnx.biomass_reaction(model)
    assert biomass is not None, (
        f"[{model_path}] has neither an objective nor a reaction named for biomass; "
        "there is no growth to maximise and a zero here would be an artefact of "
        "the model, not a phenotype"
    )
    model.objective = biomass

    table = media_mod.load(media_path)
    conditions = pd.read_parquet(conditions_path)
    ignored = [c for c in conditions.columns if c.startswith(IGNORED_PREFIXES)]
    if ignored:
        print(f"ignoring {len(ignored)} GPR-selection columns, which say how a model is "
              f"built rather than how one is solved: {ignored}")

    rows = []
    for record in conditions.to_dict(orient="records"):
        condition = str(record.get("condition_id", len(rows)))
        medium = record.get("media")
        medium = None if medium is None or str(medium).strip() in ("", "nan") else str(medium)
        dropped = knockouts(record)
        with model:
            media_mod.apply(model, table, medium)
            missing = []
            for gene in dropped:
                if gene in model.genes:
                    model.genes.get_by_id(gene).knock_out()
                else:
                    missing.append(gene)
            solution = model.optimize()
            objective = solution.objective_value if solution.status == "optimal" else 0.0
            fluxes = solution.fluxes.to_dict() if solution.status == "optimal" else {}
        if missing:
            print(f"condition [{condition}] names {len(missing)} genes this model does "
                  f"not carry: {missing[:8]}{' ...' if len(missing) > 8 else ''}")
        rows.append({
            "condition_id": condition,
            "media": medium or "",
            "status": solution.status,
            "objective": objective,
            "n_knockouts": len(dropped) - len(missing),
            **fluxes,
        })
        print(f"condition [{condition}] medium=[{medium}] knockouts={len(dropped)} "
              f"status={solution.status} objective={objective}")

    pd.DataFrame(rows).to_csv(out_path, index=False)
    print(f"{len(rows)} conditions -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
