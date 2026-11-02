"""Run MEMOTE's test suite ONCE and derive all three of its usual outputs from
that single run, rather than the two independent runs MEMOTE's own CLI does.

`memote run` writes a raw result (`ResultManager.store`) and `memote report
snapshot` renders an HTML report -- but `report snapshot` never reads `run`'s
result.json.gz back. It calls `memote.suite.api.test_model` a SECOND time,
confirmed by reading `memote/suite/cli/reports.py::snapshot`. Neither result
carries a top-level score: `Report.compute_score()` is called only from the
report path, and it MUTATES the very results dict it renders from (adds a
`score` key with `total_score` and a per-section breakdown) rather than
returning a copy.

Calling `test_model` here once and handing that SAME results object to both
`ResultManager.store` and `api.snapshot_report` gets the raw result, the HTML
report and the score from one test execution. `ResultManager.store` runs
BEFORE `snapshot_report`, deliberately: the mutation happens in place, so
storing after would put a `score` key into the "raw" result that a real
`memote run` file never carries, which is a lie about what that command alone
produces.

    memote_score.py <model.xml> <report.html> <result.json.gz> <score.json>
"""
import json
import sys

import memote.suite.api as api
import memote.utils as utils
from memote.suite.reporting import ReportConfiguration
from memote.suite.results.result_manager import ResultManager


def main(argv: list[str]) -> int:
    model_path, report_path, results_path, score_path = argv

    model_obj, sbml_ver, notifications = api.validate_model(model_path)
    if model_obj is None:
        utils.stdout_notifications(notifications)
        raise SystemExit(f"MEMOTE could not load [{model_path}] as an SBML model")

    config = ReportConfiguration.load()
    _, results = api.test_model(
        model_obj,
        sbml_version=sbml_ver,
        results=True,
        pytest_args=["--tb", "no"],
    )

    ResultManager().store(results, filename=results_path)

    html = api.snapshot_report(results, config)
    with open(report_path, "w", encoding="utf-8") as fh:
        fh.write(html)

    score = results["score"]
    with open(score_path, "w", encoding="utf-8") as fh:
        json.dump(score, fh, indent=2)
    print(f"MEMOTE total score: {score['total_score']:.4f}")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
