"""Atom-atom mapping: the reaction universe in, banked (mnxr, element) pairs out.

:mod:`~ecspr.bake.aam.worklist` adjudicates every reaction once and hands the
lanes one todo list. The members map it -- :mod:`~ecspr.bake.aam.metacyc_member`
from curated mappings, :mod:`~ecspr.bake.aam.neural_members` and
:mod:`~ecspr.bake.aam.indigo_member` by inference --
:mod:`~ecspr.bake.aam.curation` rescues what they could not reach, and
:mod:`~ecspr.bake.aam.layers` stacks the result into one table with each pair
attributed to the layer that claimed it.

Empty of imports on purpose: see :mod:`ecspr.bake`.
"""
