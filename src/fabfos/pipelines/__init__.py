"""The three FabFos pipeline drivers: assembly, annotation, ecspr.

Each module (``assembly.py``, ``annotation.py``, ``ecspr.py``) resolves one
metasmith workflow end to end and is runnable standalone
(``python -m fabfos.pipelines.<name> --help``). ``common.py`` holds what all
three share -- locating the library, staging references with a stub fallback,
rendering, and running.
"""

# The planner's candidate space, as the union of what the three drivers load.
# It is a component of the method id (``fabfos.method``), so it lives here as a
# literal rather than being derived by importing the drivers: describing a
# method must not require importing metasmith. ``tests/test_domains.py`` is what
# keeps it honest -- add a domain to a driver and that test fails until this
# list follows.
DOMAINS = ["assembly", "fabfos", "functionalAnnotation", "logistics"]
