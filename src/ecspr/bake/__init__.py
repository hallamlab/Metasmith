"""The metabolism bake: MNXref in, the tables :mod:`ecspr.model` reads out.

Two lanes over one reaction universe. :mod:`~ecspr.bake.aam` maps atoms across
each reaction and banks the (mnxr, element, substrate, product) pairs;
:mod:`~ecspr.bake.direction` decides which way each reaction runs. The four
modules here are what both lanes share:

  :mod:`~ecspr.bake.atom_pairs`  equations, templates, the extractor that turns a
                                 mapped reaction into pair rows
  :mod:`~ecspr.bake.encoding`    the packed on-disk form, and the vocabulary that
                                 makes it decodable
  :mod:`~ecspr.bake.metabolism`  the bake itself -- both lanes' products into one
                                 versioned reference
  :mod:`~ecspr.bake.evidence`    the tool-output ledger every lane writes into

Every module with a command line keeps it, and a metasmith transform reaches it
as ``python3 -m ecspr.bake.<lane>.<module> <verb>``. That is the whole coupling:
the package is staged as one hashed input and the transform names a module path,
so a lane's requirement list stops being a hand-maintained copy of its import
graph.

Deliberately empty of imports, and it must stay that way. The bake images carry
rdkit or a torch stack and nothing else -- no scipy, no cobra -- so anything
:mod:`ecspr.model` needs is unavailable here by construction rather than by
accident. :mod:`~ecspr.bake.encoding` is the one module that reaches across, and
it does so inside the one function that needs it.
"""
