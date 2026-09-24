"""Re-export, not a copy: colouring is untouched by the bundling work.

The two frozen modules beside this one import `.dag_colour` relatively, so the
name has to resolve inside this package. Pointing it at the live module keeps
one implementation rather than a second that can drift.
"""

from metasmith.models.dag_colour import *  # noqa: F401,F403
from metasmith.models.dag_colour import Colouring, colour_layout  # noqa: F401
