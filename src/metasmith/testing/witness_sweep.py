"""Holding the proved witness against real workflows rather than generated ones.

`test_plan_witness` adjudicates eight generated problems and three stress
variants. Nothing there is a workflow anybody runs, and the scope's own record
says a generated corpus will endorse a rule the eleven shipped templates refuse.
This module is the same two questions -- does the witness accept what the engine
returns, and does it reject a plan built to be wrong *by the clause that was
broken* -- asked of the real ones.

Two things it does that `witness_check.solve_and_check` does not, and both are
the reason it exists rather than being a loop in a test:

- It keeps the `EncodedProblem`, so the reply can be decoded and fingerprinted.
  A sweep re-solves the problem it recovered from a template's plan, and without
  that comparison it could be adjudicating a different plan from the one the
  template ships and reporting a pass for it.
- It separates three outcomes that a boolean folds into two: solved-and-accepted,
  solved-and-refused, and did not solve. The gate's contract is `complete ->
  sound`, so an incomplete reply is an honest "no answer" and must not be counted
  in either column.

**CAUTION** An accepted complete plan is close to a tautology and must not be
reported as though it were not. `msm_solver solve` runs the same audit before it
emits, so a complete plan that failed would have come back as a solve error
rather than as a plan. What the accepted column says is that the search never
produced a complete plan its own gate refused. The two claims that do stand on
their own are `solver_spec.check_spec` agreeing on the same bytes -- a second
statement of the specification, in another language, over the raw JSON -- and the
decoys, which are the only evidence here that the witness rejects anything.
"""

from __future__ import annotations

import time
import traceback
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterator

from ..models.solver_backend import Backend
from ..models.solver_engine import SOLVER_WIRE_VERSION, CallEngine, EngineFor
from ..models.solver_wire import decode_plan, encode_problem
from .solver_spec import CLAUSES, check_spec
from .solver_verification import SolverProblem, plan_fingerprint, problem_of_plan
from .witness_check import DECOYS, apply_decoy, require_rust, witness_check

__all__ = [
    "CAUGHT",
    "MISSED",
    "UNHOSTABLE",
    "CaseResult",
    "sweep_problem",
    "templates_corpus",
    "decoy_matrix",
    "summarise",
]

#: `unhostable` is not a weaker `caught`. Five of the ten conditions are
#: unfalsifiable on real solves -- no shipped plan uses a product slot declaring
#: lineage, no decoded payload names a slot twice -- so a clause no real case can
#: host is a fact about the corpus, and `test_spec_fixtures` is what covers it.
CAUGHT = "caught"
MISSED = "missed"
UNHOSTABLE = "unhostable"


@dataclass
class CaseResult:
    name: str
    solved: bool = False
    complete: bool = False
    ok: bool = False
    steps: int = 0
    fingerprint: str | None = None
    clauses: list[str] = field(default_factory=list)
    reference_ok: bool | None = None
    reference_violations: list[str] = field(default_factory=list)
    reference_agrees: bool | None = None
    decoys: dict[str, str] = field(default_factory=dict)
    decoy_disagreements: list[str] = field(default_factory=list)
    seconds: float = 0.0
    error: str | None = None
    note: str | None = None

    @property
    def verdict(self) -> str:
        if self.error is not None: return "error"
        if not self.solved: return "unsolved"
        if not self.complete: return "incomplete"
        return "accepted" if self.ok else "REFUSED"


def _solve_capturing(problem: SolverProblem, *, seed: int, max_iter: int, max_refine: int):
    """Solve on the engine, keeping the request, the raw reply and the decode.

    `witness_check.solve_and_check` throws the `EncodedProblem` away, which is
    what makes the fingerprint comparison impossible from outside.
    """
    encoded = encode_problem(
        [set(g) for g in problem.given],
        list(problem.transforms),
        problem.target,
        seed=seed, max_iter=max_iter, max_refine=max_refine,
        wire_version=SOLVER_WIRE_VERSION,
    )
    info = EngineFor("solve")
    assert info is not None, "no engine advertising 'solve'"
    reply = CallEngine(info, "solve", encoded.payload)
    return encoded, reply, decode_plan(encoded, reply)


def sweep_problem(
    name: str,
    problem: SolverProblem,
    *,
    expect_fingerprint: str | None = None,
    decoys: bool = True,
    seed: int = 42,
    max_iter: int = 256,
    max_refine: int = 256,
) -> CaseResult:
    """Solve one real problem on the engine, adjudicate it, then break it ten ways."""
    require_rust()
    res = CaseResult(name=name)
    t0 = time.perf_counter()
    try:
        encoded, reply, solution = _solve_capturing(
            problem, seed=seed, max_iter=max_iter, max_refine=max_refine
        )
        request = encoded.payload
        res.solved = True
        res.steps = len(solution.dependency_plan)
        res.fingerprint = plan_fingerprint(solution)
        if expect_fingerprint is not None and res.fingerprint != expect_fingerprint:
            res.error = (
                f"the re-solve returned a different plan from the one handed in: "
                f"{res.fingerprint} vs {expect_fingerprint}"
            )
            return res

        verdict = witness_check(request, reply)
        res.complete, res.ok, res.clauses = verdict.complete, verdict.ok, verdict.clauses
        reference = check_spec(request, reply)
        res.reference_ok = reference.ok
        res.reference_violations = list(reference.violations)
        res.reference_agrees = reference.ok == verdict.ok

        if decoys and verdict.complete and verdict.ok:
            for clause in sorted(DECOYS):
                bad = apply_decoy(clause, request, reply)
                if bad is None:
                    res.decoys[clause] = UNHOSTABLE
                    continue
                got = witness_check(*bad)
                res.decoys[clause] = CAUGHT if got.violated(clause) else (
                    f"{MISSED}:{','.join(got.clauses) or 'nothing'}"
                )
                ref = check_spec(*bad)
                if ref.ok != got.ok or ref.violated(clause) != got.violated(clause):
                    res.decoy_disagreements.append(
                        f"{clause}: engine {got.clauses} vs reference {ref.violations}"
                    )
    except Exception as e:  # a case that cannot be solved is an outcome, not a crash
        res.error = f"{type(e).__name__}: {e}"
        res.__dict__["_traceback"] = traceback.format_exc()
    finally:
        res.seconds = round(time.perf_counter() - t0, 3)
    return res


def templates_corpus(root: Path) -> Iterator[tuple[str, SolverProblem, str]]:
    """Every shipped template, as a problem the sweep can take.

    Yields the fingerprint of the plan the template itself solved to, so the
    sweep's own re-solve can be held against it.
    """
    from ..agents import Template

    for template in Template.Discover(root):
        task = template.spec.Solve()
        result = getattr(task.plan, "_solver_result", None)
        problem = problem_of_plan(task.plan, name=template.name)
        if problem is None or result is None:
            continue
        yield template.name, problem, plan_fingerprint(result)


def decoy_matrix(results: list[CaseResult]) -> dict[str, dict[str, int]]:
    """Per clause, how many real cases caught it, missed it, or could not host it."""
    out: dict[str, dict[str, int]] = {
        c: {CAUGHT: 0, MISSED: 0, UNHOSTABLE: 0} for c in CLAUSES
    }
    for r in results:
        for clause, outcome in r.decoys.items():
            out[clause][CAUGHT if outcome == CAUGHT else (
                UNHOSTABLE if outcome == UNHOSTABLE else MISSED
            )] += 1
    return out


def summarise(results: list[CaseResult]) -> dict[str, Any]:
    by_verdict: dict[str, list[str]] = {}
    for r in results:
        by_verdict.setdefault(r.verdict, []).append(r.name)
    missed = [
        f"{r.name}/{c}={o}"
        for r in results for c, o in r.decoys.items() if o.startswith(MISSED)
    ]
    return {
        "backend": Backend("solve"),
        "cases": len(results),
        "by_verdict": {k: sorted(v) for k, v in sorted(by_verdict.items())},
        "decoy_matrix": decoy_matrix(results),
        "decoys_missed": missed,
        "reference_disagreements": [
            f"{r.name}: engine ok={r.ok} reference ok={r.reference_ok}"
            for r in results if r.reference_agrees is False
        ] + [f"{r.name}/{d}" for r in results for d in r.decoy_disagreements],
        "total_seconds": round(sum(r.seconds for r in results), 2),
        "results": [dict(asdict(r), verdict=r.verdict) for r in results],
    }
