#!/usr/bin/env python3
import sys

import _authoring as A
import engcyc_metagenomics as M
import engcyc_metagenomics_lite as L
import engcyc_prep_references as P

NAME = "engcyc_prep_references_lite"
DESCRIPTION = """
Fetches and builds every reference database and tool image that
engcyc_metagenomics_lite needs, and takes no samples. Run it once on an agent before
the real run; engcyc_metagenomics_lite then finds these products in the cache. Its
targets are engcyc_metagenomics_lite's references case. The plan summary shows the
download total.
"""


def build_template(rebuild: bool = False):
    return P.prep_for(L.build_template(rebuild=rebuild), M.LITE_REFERENCES, NAME, DESCRIPTION,
                      rebuild)


if __name__ == "__main__":
    A.cli(sys.modules[__name__])
