"""Fetch a curated model from BiGG and write it as SBML.

    fetch_bigg_model.py <bigg_id.txt> <out.xml>

A curated biomass equation and curated GPR rules are the sound baseline a draft
reconstruction is measured against, and the id is the accession the way
`ncbi::assembly_accession` is.
"""
import sys

import cobra
import requests

BIGG = "http://bigg.ucsd.edu/static/models/{model_id}.json"


def main(argv: list[str]) -> int:
    id_path, out_path = argv
    with open(id_path) as fh:
        model_id = fh.read().strip()
    assert model_id, f"[{id_path}] is empty; there is no model to fetch"

    url = BIGG.format(model_id=model_id)
    print(f"fetching {url}")
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    with open("bigg_model.json", "wb") as fh:
        fh.write(response.content)

    model = cobra.io.load_json_model("bigg_model.json")
    cobra.io.write_sbml_model(model, out_path)
    print(f"{model_id}: {len(model.reactions)} reactions, "
          f"{len(model.metabolites)} metabolites, {len(model.genes)} genes "
          f"-> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
