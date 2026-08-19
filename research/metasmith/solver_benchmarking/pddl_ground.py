"""Ground a PlanBench Blocksworld instance via pyperplan's own parser/grounder.

No hand-rolled PDDL parsing here -- pyperplan already has a correct one, and
reusing it means grounding bugs can't be blamed on us reinventing that wheel.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyperplan.planner import _parse, _ground


@dataclass(frozen=True)
class GroundedAction:
    name: str
    preconditions: frozenset[str]
    add_effects: frozenset[str]
    del_effects: frozenset[str]


@dataclass(frozen=True)
class GroundedInstance:
    name: str
    initial_state: frozenset[str]
    goals: frozenset[str]
    actions: tuple[GroundedAction, ...]


def ground_instance(domain_file: Path, problem_file: Path) -> GroundedInstance:
    """Parse + fully ground one PDDL instance, canonicalized to plain atom strings.

    pyperplan's own `Task.facts`/`initial_state`/`goals` are already atom
    strings like `(on a b)` -- we pass those straight through so every later
    consumer (metasmith's `Endpoint.properties`, pyperplan's own search) sees
    byte-identical strings for "the same" atom, with no second encoding to
    drift out of sync with the first.
    """
    problem = _parse(str(domain_file), str(problem_file))
    task = _ground(problem)
    actions = tuple(
        GroundedAction(
            name=op.name,
            preconditions=frozenset(op.preconditions),
            add_effects=frozenset(op.add_effects),
            del_effects=frozenset(op.del_effects),
        )
        for op in task.operators
    )
    return GroundedInstance(
        name=problem_file.stem,
        initial_state=frozenset(task.initial_state),
        goals=frozenset(task.goals),
        actions=actions,
    )
