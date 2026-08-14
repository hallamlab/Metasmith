"""One module per verb. Each is thin on purpose: the argument surface lives in
:mod:`ecspr.cli` and the method lives in the experiment layer, so a command file
is only the wiring between them."""
from .draw import draw
from .probe import ground, two_point
from .score import score

__all__ = ["two_point", "ground", "draw", "score"]
