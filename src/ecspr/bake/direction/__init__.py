"""Directionality: which way each reaction runs, and how strongly.

:mod:`~ecspr.bake.direction.drive` runs a thermodynamic member over a shard of
the universe (:mod:`~ecspr.bake.direction.thermo_eq` or
:mod:`~ecspr.bake.direction.thermo_dgbyg`),
:mod:`~ecspr.bake.direction.curated` reads the curated calls out of the MetaCyc
flat file, and :mod:`~ecspr.bake.direction.combine` fuses them into one ratio per
reaction under the calibration :mod:`~ecspr.bake.direction.calibrate` fits.

Empty of imports on purpose: see :mod:`ecspr.bake`.
"""
