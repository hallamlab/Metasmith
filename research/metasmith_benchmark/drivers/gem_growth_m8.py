#!/usr/bin/env python3
"""Growth of each GEM on the medium its gap fill used, applied as CarveMe's gapfill applies it.

    python gem_growth_m8.py <lib::modelling dir> <medium tsv> <medium name> <out tsv> <models dir>...

Run it inside the CarveMe 1.6.6 image. Each models dir holds <lane>/<bin>.xml.gz, and each row is
lane, bin, biomass flux, and whether it is above 1e-6.
"""

import gzip
import sys
import tempfile
from pathlib import Path


def main(helpers, medium_tsv, medium_name, out, *dirs):
    sys.path.insert(0, helpers)
    from carveme_from_orfs import build_carveme_media_db
    from carveme import config
    from carveme.reconstruction.utils import load_media_db, medium_to_constraints
    import reframed
    from reframed import FBA, load_cbmodel

    reframed.solvers.set_default_solver("scip")
    media_db_path = Path(tempfile.mkdtemp()) / "media_db.tsv"
    build_carveme_media_db(medium_tsv, medium_name, str(media_db_path))
    compounds = load_media_db(str(media_db_path))[medium_name]
    max_uptake = config.getint("gapfill", "max_uptake")
    with open(out, "w") as fh:
        fh.write("lane\tbin\tbiomass_flux\tgrows\n")
        for d in dirs:
            for path in sorted(Path(d).glob("*/*.xml.gz")):
                lane, b = path.parent.name, path.name.removesuffix(".xml.gz")
                with tempfile.NamedTemporaryFile(suffix=".xml") as tmp:
                    tmp.write(gzip.decompress(path.read_bytes()))
                    tmp.flush()
                    model = load_cbmodel(tmp.name, flavor="fbc2")
                medium_to_constraints(model, compounds, max_uptake=max_uptake, inplace=True)
                sol = FBA(model)
                flux = sol.fobj if sol is not None and sol.fobj is not None else 0.0
                fh.write(f"{lane}\t{b}\t{flux:.6g}\t{int(flux > 1e-6)}\n")
                fh.flush()


if __name__ == "__main__":
    main(*sys.argv[1:])
