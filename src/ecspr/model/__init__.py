"""The measurement model: the network, the solve, and the experiment over it.

Deliberately empty of imports. `ecspr.bake` is installed into images that carry
neither scipy nor cobra, and anything imported here would be pulled in by an
`import ecspr.model.<x>` from a module that only wanted one leaf. Reach for the
submodule you want; the package docstring in :mod:`ecspr` maps them.
"""
