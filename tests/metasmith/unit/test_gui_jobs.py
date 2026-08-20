from metasmith.gui.jobs import Job


def _make_job() -> Job:
    return Job(id="t", kind="test", label="test")


def test_progress_ticks_collapse_into_one_line():
    job = _make_job()
    job.emit("pulling layer\r")
    job.emit("pulling layer 10%\r")
    job.emit("pulling layer 55%\r")
    assert job.lines() == ["pulling layer 55%\r"]


def test_progress_run_commits_on_a_real_line():
    job = _make_job()
    job.emit("pulling layer 10%\r")
    job.emit("pulling layer 100%\r")
    job.emit("layer pulled\n")
    assert job.lines() == ["layer pulled"]


def test_a_fresh_progress_run_after_a_committed_line_gets_its_own_entry():
    job = _make_job()
    job.emit("layer a pulled\n")
    job.emit("layer b 10%\r")
    job.emit("layer b 90%\r")
    assert job.lines() == ["layer a pulled", "layer b 90%\r"]


def test_ordinary_lines_are_unaffected():
    job = _make_job()
    job.emit("first\nsecond\nthird")
    assert job.lines() == ["first", "second", "third"]
