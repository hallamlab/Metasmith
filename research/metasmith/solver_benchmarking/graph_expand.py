"""Pre-expand a grounded Blocksworld instance into a pure state graph, then
package it as a metasmith `SolverProblem`.

This is the honest workaround for metasmith's `Transform` model: a produced
`Endpoint`'s properties come from the transform's own static template, never
from the matched input's content (`solver.py:generate_applications_of_transform`),
so delete effects can't be computed dynamically per-application. Instead we
fully enumerate the grounded state-transition graph *here*, outside metasmith,
and give every reachable world state its own fully-materialized `Endpoint` --
one `Transform` per STRIPS action application (state-transition edge).

Every `Endpoint`/`Dependency` built below passes only `properties=`, never
`parents=`. That is the whole "pure state" contract: nothing here is ever
reconstructed by walking ancestry, so a delete effect can never be silently
undone by a state's lineage.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from metasmith.models.solver import Endpoint, Transform
from metasmith.testing.solver_verification import SolverProblem

from pddl_ground import GroundedInstance

#: Refuse to enumerate an instance whose reachable-state graph blows past
#: this size, rather than hang. Blocksworld with 3-4 blocks needs low
#: hundreds of states; anything landing near this cap needs a smaller
#: instance, not a bigger cap.
MAX_STATES = 20_000


class StateGraphTooLarge(RuntimeError):
    pass


@dataclass(frozen=True)
class StateGraph:
    instance_name: str
    states: list[frozenset[str]]  # index -> full atom set for that state
    edges: list[tuple[int, int, str]]  # (from_index, to_index, action_name)
    initial_index: int
    goals: frozenset[str]


def expand_state_graph(instance: GroundedInstance, *, max_states: int = MAX_STATES) -> StateGraph:
    """Full BFS successor-generation over the grounded action set.

    Standard STRIPS successor rule: an action is applicable when its
    preconditions are a subset of the state, and produces
    `(state - del_effects) | add_effects`.
    """
    states: list[frozenset[str]] = [instance.initial_state]
    index_of: dict[frozenset[str], int] = {instance.initial_state: 0}
    edges: list[tuple[int, int, str]] = []

    queue: deque[int] = deque([0])
    while queue:
        i = queue.popleft()
        state = states[i]
        for action in instance.actions:
            if not action.preconditions <= state:
                continue
            nxt = (state - action.del_effects) | action.add_effects
            j = index_of.get(nxt)
            if j is None:
                if len(states) >= max_states:
                    raise StateGraphTooLarge(
                        f"[{instance.name}] exceeded {max_states} reachable states; "
                        "pick a smaller instance rather than raising this cap"
                    )
                j = len(states)
                states.append(nxt)
                index_of[nxt] = j
                queue.append(j)
            edges.append((i, j, action.name))

    return StateGraph(
        instance_name=instance.name,
        states=states,
        edges=edges,
        initial_index=0,
        goals=instance.goals,
    )


def _state_marker(index: int) -> str:
    """Per-state uniqueness tag.

    Blocksworld states don't all have the same atom count (a `holding`
    state has fewer `on`/`ontable`/`clear` facts than a fully-settled one),
    so under plain subset (`IsA`) matching a larger state could spuriously
    satisfy a smaller state's dependency. The marker makes every state's
    property set mutually non-comparable by content alone, so a
    `Transform`'s edge can only ever match the one state it was built from.
    """
    return f"__state__:{index}"


def build_solver_problem(graph: StateGraph) -> SolverProblem:
    """One `Endpoint` per state, one `Transform` per edge. No `parents=` anywhere."""
    state_endpoints = [
        Endpoint(properties=set(atoms) | {_state_marker(i)})
        for i, atoms in enumerate(graph.states)
    ]

    given = [{state_endpoints[graph.initial_index]}]

    transforms: list[Transform] = []
    for from_i, to_i, action_name in graph.edges:
        tr = Transform()
        tr.AddRequirement(properties=set(graph.states[from_i]) | {_state_marker(from_i)})
        tr.AddProduct(properties=set(graph.states[to_i]) | {_state_marker(to_i)})
        transforms.append(tr)

    target = Transform()
    target.AddRequirement(properties=set(graph.goals))

    return SolverProblem(
        given=given,
        transforms=transforms,
        target=target,
        name=graph.instance_name,
    )
