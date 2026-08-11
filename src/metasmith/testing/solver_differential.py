"""The differential gate: the engine and the Python solver, on the same corpus.

Acceptance criterion 9 of the port -- *Rust and Python produce topologically
equivalent plans on 100% of the generated corpus given the same seed* -- is a
claim about a population, not about a handful of cases, so it needs a sweep
rather than a test file. This module is that sweep, and `tests/solver` and
`tests/perf` both drive it at different sizes.

Three things make it a gate rather than a demo.

**The reference has to actually be the reference.** Both implementations are
reached through the same `SolverProblem.solve()`, so the moment the engine
advertises `solve`, a naive comparison runs the engine twice and agrees with
itself. `_reference` forces the Python path *and asserts it got it*; nothing
here calls `problem.solve()` directly.

**More than one stream per problem.** A port diverges quietly in the stream --
a rule that consumes one word too many is right until the day the draw before
it lands differently -- and one seed per problem exercises exactly one path
through the search. Every problem is solved under `SOLVE_SEEDS`.

**A slow case is named, not swallowed.** Some draws lead the search to a much
larger plan than the same problem yields under a different seed -- `sink` at
problem seed 24 solves to 7 steps under seed 42 and 57 under seed 2^31-1 -- and
the refiner's cost climbs steeply with plan size. That makes the instance
expensive for *both* sides, not for the reference in particular; the engine is
merely fifteen times cheaper about it. So both sides run under the same cap and
a cap hit is recorded as **not adjudicated**, with the two seeds needed to
reproduce it, rather than being counted as either agreement or disagreement.
Tolerating it as a statistic is how the one case worth looking at stays
unexamined.

Usage::

    python -m metasmith.testing.solver_differential --problems 16000 --out sweep.json
    python -m metasmith.testing.solver_differential --problems 40 --seeds 42 --verbose

and, for a case the report named as unadjudicated, the same command with
``--timeout 0`` and that case's seed. Sixteen of sixteen thousand needed it, all
in the `sink` profile. Fifteen then finished and agreed; the sixteenth,
`sink-178` under seed 7, is a problem on which one refiner iteration costs the
engine 22 seconds, so at the default budget of 256 neither implementation
finishes. It agrees at the budgets that do.
"""

from __future__ import annotations

import argparse
import json
import signal
import subprocess
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterator

from ..models.solver_backend import Backend, UsePythonSolver
from ..models.solver_engine import (
    SOLVER_WIRE_VERSION,
    CallEngine,
    EngineError,
    EngineFor,
    EngineInfo,
)
from ..models.solver_wire import decode_plan, encode_problem
from .solver_verification import (
    GeneratorDials,
    SolverProblem,
    check_plan,
    generate_problem,
    plan_fingerprint,
    plan_shape,
)

__all__ = [
    "SWEEP_PROFILES",
    "SOLVE_SEEDS",
    "CaseResult",
    "compare",
    "cases",
    "run_sweep",
]


#: The generated corpus, by pressure. Wider than `solver_bench.CORPUS`, which is
#: eight fixed instances pinned to a fingerprint -- this one varies the problem
#: seed to sweep a *population* of each shape.
SWEEP_PROFILES: list[tuple[str, GeneratorDials]] = [
    ("plain", GeneratorDials(n_types=6, n_extra_transforms=3)),
    ("cyclic", GeneratorDials(n_types=7, n_extra_transforms=5, cycle_density=0.8)),
    (
        "lineage",
        GeneratorDials(
            n_types=7, n_extra_transforms=5, lineage_density=0.9, target_lineage=1.0
        ),
    ),
    ("dupes", GeneratorDials(n_types=7, n_extra_transforms=3, n_duplicate_transforms=5)),
    ("pgroups", GeneratorDials(n_types=7, n_extra_transforms=4, product_group_density=0.9)),
    ("multi", GeneratorDials(n_types=7, n_given=2, n_given_groups=3, n_extra_transforms=4)),
    (
        "tiny",
        GeneratorDials(n_types=4, n_extra_transforms=2, lineage_density=0.6, target_lineage=0.6),
    ),
    (
        "sink",
        GeneratorDials(
            n_types=9,
            n_given=2,
            n_given_groups=2,
            n_extra_transforms=6,
            cycle_density=0.4,
            lineage_density=0.7,
            n_duplicate_transforms=2,
            product_group_density=0.5,
            target_lineage=0.8,
        ),
    ),
]

#: The streams every problem is solved under. Deliberately spread: the default,
#: two small values, and one at the top of the 32-bit range, since a port that
#: truncates or sign-extends a seed agrees on small numbers and nothing else.
SOLVE_SEEDS: tuple[int, ...] = (42, 7, 1234, 2**31 - 1)

#: Seconds each side gets. Both are capped, and for different reasons: the
#: reference because its tail is unbounded, the engine because an uncapped
#: subprocess turns a hang in the port into a sweep that never returns. A cap
#: hit on either side is an unadjudicated case, never a verdict.
CASE_TIMEOUT = 20.0


@dataclass
class CaseResult:
    """One (problem, stream) comparison."""

    case: str
    profile: str
    problem_seed: int
    solve_seed: int
    #: `identical` or the first thing the two disagreed about.
    outcome: str
    engine_seconds: float = 0.0
    reference_seconds: float = 0.0
    detail: str = ""

    @property
    def agreed(self) -> bool:
        return self.outcome == "identical"


class _Timeout(Exception):
    pass


class _Alarm:
    """SIGALRM, when this is the main thread of the main interpreter.

    A no-op elsewhere rather than an error: the sweep is still worth running
    from a worker thread, it just cannot bound the reference there.
    """

    def __init__(self, seconds: float):
        self.seconds = seconds
        self.armed = False

    def __enter__(self):
        if self.seconds <= 0:
            return self
        try:
            signal.signal(signal.SIGALRM, self._fire)
            signal.setitimer(signal.ITIMER_REAL, self.seconds)
            self.armed = True
        except ValueError:
            pass  # not the main thread
        return self

    def __exit__(self, *exc):
        if self.armed:
            signal.setitimer(signal.ITIMER_REAL, 0)
            self.armed = False
        return False

    def _fire(self, signum, frame):
        # Cancelling the timer does not un-deliver a signal already queued, and
        # Python runs the handler at the next bytecode boundary -- which can be
        # *after* the `with` block. Without this guard that stray `_Timeout`
        # escapes into the sweep loop and ends a two-hour run over a case that
        # actually finished.
        if not self.armed:
            return
        raise _Timeout()


def _sequence(plan) -> list:
    """The plan as an ordered list of (transform, sorted input slots).

    Coarser than a fingerprint and about a different property: the fingerprint's
    canonical form deliberately forgets step order, so two plans can be
    topologically equivalent and still be different sequences. Equivalence is
    what was promised; identical sequences are what the port has actually been
    delivering, and a regression from one to the other is worth seeing.
    """
    return [
        (s.transform.key, sorted((d.key, e.key) for d, e in s.used.items()))
        for s in plan.dependency_plan
    ]


def _reference(problem: SolverProblem, **kwargs):
    """Solve on the Python path, having checked that it is the Python path.

    The assertion is the whole point. Without it this function silently returns
    the engine's answer the moment the engine advertises `solve`, every
    comparison below passes, and the sweep proves nothing while reporting 100%.
    """
    with UsePythonSolver():
        assert Backend("solve") == "python", "the reference side must be python"
        return problem.solve(**kwargs)


def compare(
    engine: EngineInfo,
    problem: SolverProblem,
    *,
    seed: int = 42,
    max_iter: int = 256,
    max_refine: int = 256,
    timeout: float = CASE_TIMEOUT,
    check: bool = True,
) -> tuple[str, str, float, float]:
    """`(outcome, detail, engine_seconds, reference_seconds)` for one comparison.

    Reports the *first* disagreement, most structural first, so a case that
    differs in topology is not also counted as differing in order.
    """
    encoded = encode_problem(
        problem.given,
        problem.transforms,
        problem.target,
        seed=seed,
        max_iter=max_iter,
        max_refine=max_refine,
        wire_version=SOLVER_WIRE_VERSION,
    )
    t0 = time.perf_counter()
    try:
        theirs = decode_plan(
            encoded, CallEngine(engine, "solve", encoded.payload, timeout=timeout or None)
        )
    except EngineError as e:
        engine_seconds = time.perf_counter() - t0
        if isinstance(e.__cause__, subprocess.TimeoutExpired):
            return (
                "engine_timeout",
                f"the engine exceeded {timeout:g}s, so neither side was adjudicated",
                engine_seconds,
                0.0,
            )
        return "engine_error", repr(e)[:300], engine_seconds, 0.0
    except Exception as e:  # noqa: BLE001 -- the sweep classifies, it does not raise
        return "engine_error", repr(e)[:300], time.perf_counter() - t0, 0.0
    engine_seconds = time.perf_counter() - t0

    t1 = time.perf_counter()
    try:
        with _Alarm(timeout):
            mine = _reference(problem, seed=seed, max_iter=max_iter, max_refine=max_refine)
    except _Timeout:
        return (
            "reference_timeout",
            f"the python solver exceeded {timeout:g}s; the engine answered the same"
            f" case in {engine_seconds:.3f}s",
            engine_seconds,
            time.perf_counter() - t1,
        )
    except Exception as e:  # noqa: BLE001
        return "reference_error", repr(e)[:300], engine_seconds, time.perf_counter() - t1
    reference_seconds = time.perf_counter() - t1

    def done(outcome: str, detail: str = "") -> tuple[str, str, float, float]:
        return outcome, detail, engine_seconds, reference_seconds

    if mine.complete != theirs.complete:
        return done("complete_differs", f"python={mine.complete} engine={theirs.complete}")
    fa, fb = plan_fingerprint(mine), plan_fingerprint(theirs)
    if fa != fb:
        return done("fingerprint_differs", f"python={fa} engine={fb}")
    if plan_shape(mine) != plan_shape(theirs):
        return done("shape_differs", f"python={plan_shape(mine)} engine={plan_shape(theirs)}")
    if _sequence(mine) != _sequence(theirs):
        return done("order_differs", "same topology, different step order")
    if check and mine.complete:
        # The checker shares no code with either implementation, so this asks a
        # question neither side can answer about itself: not "do they agree" but
        # "are they both wrong in the same way". Parity of verdict is the claim
        # -- some generated instances are unsound under both, which is the
        # pinned laundering defect, not a divergence.
        a, b = check_plan(problem, mine), check_plan(problem, theirs)
        if a.ok != b.ok:
            return done("verdict_differs", f"python ok={a.ok} engine ok={b.ok}")
    return done("identical")


def cases(
    *,
    problems: int,
    solve_seeds: tuple[int, ...] = SOLVE_SEEDS,
    profiles: list[tuple[str, GeneratorDials]] | None = None,
    first_seed: int = 0,
) -> Iterator[tuple[str, str, int, int, GeneratorDials]]:
    """`(case, profile, problem_seed, solve_seed, dials)`, deterministically.

    `problems` is the total number of *comparisons*, split evenly across
    profiles and streams, so the caller sizes the sweep in the unit it pays for.
    """
    profiles = profiles if profiles is not None else SWEEP_PROFILES
    per = max(1, problems // (len(profiles) * len(solve_seeds)))
    for name, dials in profiles:
        for problem_seed in range(first_seed, first_seed + per):
            for solve_seed in solve_seeds:
                yield (
                    f"{name}-{problem_seed}/s{solve_seed}",
                    name,
                    problem_seed,
                    solve_seed,
                    dials,
                )


#: Outcomes that mean "this case was never judged", as against "the two
#: disagreed". Keeping them apart is the point: a sweep that folded them in
#: either direction would be reporting a number it did not measure.
UNADJUDICATED = frozenset({"engine_timeout", "reference_timeout"})


@dataclass
class SweepReport:
    tally: Counter = field(default_factory=Counter)
    disagreements: list[CaseResult] = field(default_factory=list)
    unadjudicated: list[CaseResult] = field(default_factory=list)
    total: int = 0
    #: Summed over *adjudicated* cases only. A capped side contributes the cap
    #: rather than what it would have taken, so including those would report a
    #: speedup floored by the cap -- an understatement dressed as a measurement.
    engine_seconds: float = 0.0
    reference_seconds: float = 0.0

    @property
    def clean(self) -> bool:
        """Nothing disagreed. Says nothing about the unadjudicated cases -- read
        `unadjudicated` for those, and run them off the clock."""
        return not self.disagreements

    @property
    def agreed(self) -> int:
        return self.tally["identical"]

    def as_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "agreed": self.agreed,
            "tally": dict(self.tally),
            "engine_seconds": round(self.engine_seconds, 3),
            "reference_seconds": round(self.reference_seconds, 3),
            "speedup": (
                round(self.reference_seconds / self.engine_seconds, 2)
                if self.engine_seconds
                else None
            ),
            "disagreements": [asdict(c) for c in self.disagreements],
            "unadjudicated": [asdict(c) for c in self.unadjudicated],
        }


def run_sweep(
    engine: EngineInfo | None = None,
    *,
    problems: int = 4000,
    solve_seeds: tuple[int, ...] = SOLVE_SEEDS,
    profiles: list[tuple[str, GeneratorDials]] | None = None,
    timeout: float = CASE_TIMEOUT,
    check: bool = True,
    first_seed: int = 0,
    on_case=None,
) -> SweepReport:
    engine = engine or EngineFor("solve")
    if engine is None:
        raise RuntimeError(
            "no msm_solver advertising `solve` -- there is nothing to differ from"
            " (build and stage it with ./dev.sh -bel)"
        )
    report = SweepReport()
    for case, profile, problem_seed, solve_seed, dials in cases(
        problems=problems, solve_seeds=solve_seeds, profiles=profiles, first_seed=first_seed
    ):
        try:
            problem = generate_problem(problem_seed, dials, name=profile)
            outcome, detail, es, rs = compare(
                engine, problem, seed=solve_seed, timeout=timeout, check=check
            )
        except Exception as e:  # noqa: BLE001
            # Generation, encoding and the fingerprint sit outside `compare`'s
            # own guards. One bad case must not end a sweep that takes hours --
            # it becomes a reported outcome like any other.
            outcome, detail, es, rs = "harness_error", repr(e)[:300], 0.0, 0.0
        result = CaseResult(
            case=case,
            profile=profile,
            problem_seed=problem_seed,
            solve_seed=solve_seed,
            outcome=outcome,
            engine_seconds=round(es, 4),
            reference_seconds=round(rs, 4),
            detail=detail,
        )
        report.total += 1
        report.tally[outcome] += 1
        if outcome in UNADJUDICATED:
            report.unadjudicated.append(result)
        else:
            report.engine_seconds += es
            report.reference_seconds += rs
            if not result.agreed:
                report.disagreements.append(result)
        if on_case is not None:
            on_case(result)
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--problems", type=int, default=4000, help="total comparisons")
    ap.add_argument(
        "--first-seed",
        type=int,
        default=0,
        help="first problem seed; move it to sweep a window the checked-in"
        " gates never reach",
    )
    ap.add_argument(
        "--seeds",
        type=int,
        nargs="+",
        default=list(SOLVE_SEEDS),
        help="solve seeds; every problem is run under each",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=CASE_TIMEOUT,
        help="per-side cap in seconds; 0 removes it, which is how a case the"
        " sweep reported as unadjudicated gets settled",
    )
    ap.add_argument("--no-check", action="store_true", help="skip the semantic checker")
    ap.add_argument("--out", type=Path, help="write the report JSON here")
    ap.add_argument("--verbose", action="store_true", help="one line per case")
    args = ap.parse_args(argv)

    def echo(r: CaseResult):
        if args.verbose or not r.agreed:
            print(
                f"{'ok ' if r.agreed else 'BAD'} {r.case:28s}"
                f" {r.engine_seconds:7.3f}s / {r.reference_seconds:7.3f}s"
                f"  {r.outcome} {r.detail}"
            )

    report = run_sweep(
        problems=args.problems,
        solve_seeds=tuple(args.seeds),
        timeout=args.timeout,
        check=not args.no_check,
        first_seed=args.first_seed,
        on_case=echo,
    )
    summary = report.as_dict()
    print(json.dumps(
        summary | {"disagreements": len(report.disagreements),
                   "unadjudicated": len(report.unadjudicated)}, indent=1))
    for r in report.unadjudicated:
        print(
            f"UNADJUDICATED {r.case}: {r.detail}"
            f"  (rerun: --problems 1 --seeds {r.solve_seed} --timeout 0)"
        )
    if args.out:
        args.out.write_text(json.dumps(summary, indent=1), encoding="utf-8")
    return 0 if report.clean else 1


if __name__ == "__main__":
    raise SystemExit(main())
