"""Solve a grounded instance with pyperplan (a real classical planner) as the
on-machine timing baseline -- no published per-instance timing exists at
Blocksworld's scale, so this is the honest apples-to-apples comparison.
"""

from __future__ import annotations

import heapq
import time
from dataclasses import dataclass

from pyperplan.heuristics.relaxation import hFFHeuristic
from pyperplan.search import searchspace
from pyperplan.search.a_star import ordered_node_astar
from pyperplan.task import Operator, Task

from pddl_ground import GroundedInstance


@dataclass
class PyperplanResult:
    solved: bool
    plan_length: int
    states_expanded: int
    wall_seconds: float


def _as_task(instance: GroundedInstance) -> Task:
    operators = [
        Operator(a.name, a.preconditions, a.add_effects, a.del_effects)
        for a in instance.actions
    ]
    facts = set(instance.initial_state) | set(instance.goals)
    for op in operators:
        facts |= op.preconditions | op.add_effects | op.del_effects
    return Task(
        instance.name, facts, instance.initial_state, instance.goals, operators
    )


def _astar_with_expansion_count(task: Task, heuristic):
    """`pyperplan.search.a_star.astar_search`, minus the logging, plus a count.

    Copied rather than imported because upstream only exposes expansion count
    through a log line -- not worth scraping logs for a number the search
    already has in hand.
    """
    open_list = []
    state_cost = {task.initial_state: 0}
    tiebreaker = 0
    root = searchspace.make_root_node(task.initial_state)
    heapq.heappush(open_list, ordered_node_astar(root, heuristic(root), tiebreaker))
    expansions = 0
    while open_list:
        (_f, _h, _tie, node) = heapq.heappop(open_list)
        if state_cost[node.state] != node.g:
            continue
        expansions += 1
        if task.goal_reached(node.state):
            return node.extract_solution(), expansions
        for op, succ_state in task.get_successor_states(node.state):
            succ_g = node.g + 1
            if succ_state not in state_cost or succ_g < state_cost[succ_state]:
                state_cost[succ_state] = succ_g
                tiebreaker += 1
                child = searchspace.make_child_node(node, op, succ_state)
                heapq.heappush(
                    open_list, ordered_node_astar(child, heuristic(child), tiebreaker)
                )
    return None, expansions


def solve_with_pyperplan(instance: GroundedInstance) -> PyperplanResult:
    task = _as_task(instance)
    heuristic = hFFHeuristic(task)
    t0 = time.perf_counter()
    plan, expansions = _astar_with_expansion_count(task, heuristic)
    wall = time.perf_counter() - t0
    return PyperplanResult(
        solved=plan is not None,
        plan_length=len(plan) if plan is not None else -1,
        states_expanded=expansions,
        wall_seconds=wall,
    )
