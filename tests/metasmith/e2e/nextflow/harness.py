# Runs a hand-written workflow of mock steps against the real
# Orchestrator.groovy and reads back what happened: the Nextflow trace, what
# each task appended to the receive log, and the run's exit status and error
# text. The workflow either runs on the host's Nextflow or, given an image,
# inside that container with the workspace mounted at /ws.
#
# Every timing claim a test makes is an ORDERING relation between trace times,
# never an absolute threshold, so scheduler noise cannot flip it.

import json
import os
import shutil
import signal
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

from metasmith.constants import MODULE_PATH

FIXTURES = Path(__file__).parent / "fixtures"
ORCHESTRATOR_ENV = "MSM_TEST_ORCHESTRATOR"
RUN_TIMEOUT_S = 240


def orchestrator_src() -> Path:
    override = os.environ.get(ORCHESTRATOR_ENV)
    return Path(override) if override else MODULE_PATH / "nextflow_config/Orchestrator.groovy"


def host_nextflow() -> Path | None:
    found = shutil.which("nextflow")
    if found:
        return Path(found)
    beside = Path(sys.executable).parent / "nextflow"
    return beside if beside.exists() else None


@dataclass
class Task:
    process: str
    tag: str
    status: str
    exit: str
    attempt: int
    submit: int
    start: int
    complete: int


@dataclass
class Recv:
    process: str
    attempt: int
    tokens: list[str]
    slots: list[list[str]]

    @property
    def token(self) -> str:
        return ",".join(self.tokens)


@dataclass
class RunResult:
    returncode: int | None
    stdout: str
    stderr: str
    timed_out: bool
    trace: list[Task]
    recv: list[Recv]

    @property
    def output(self) -> str:
        return (self.stdout or "") + "\n" + (self.stderr or "")

    @property
    def tail(self) -> str:
        return self.output[-3000:]

    def tasks(self, process: str, tag: str | None = None, status: str | None = None) -> list[Task]:
        return [
            t for t in self.trace
            if t.process == process
            and (tag is None or t.tag == tag)
            and (status is None or t.status == status)
        ]

    def completed(self, process: str, tag: str) -> Task:
        rows = self.tasks(process, tag, "COMPLETED")
        assert len(rows) == 1, (
            f"expected exactly one completed {process} ({tag}) task, found "
            f"{[(t.status, t.attempt) for t in self.tasks(process, tag)]}\n{self.tail}"
        )
        return rows[0]

    def received(self, process: str) -> list[Recv]:
        return [r for r in self.recv if r.process == process]

    def members(self, process: str, expected_tokens: set[str]) -> dict[str, Recv]:
        rows = self.received(process)
        by_token: dict[str, list[Recv]] = {}
        for r in rows:
            by_token.setdefault(r.token, []).append(r)
        assert set(by_token) == set(expected_tokens), (
            f"{process} ran for members {sorted(by_token)}, expected exactly "
            f"{sorted(expected_tokens)}\n{self.tail}"
        )
        dupes = {k: len(v) for k, v in by_token.items() if len(v) != 1}
        assert not dupes, (
            f"{process} ran more than once for a member: {dupes}. A key must "
            f"produce exactly one member.\n{self.tail}"
        )
        return {k: v[0] for k, v in by_token.items()}


def assert_finished(result: RunResult) -> None:
    assert not result.timed_out, (
        f"the run HUNG: it did not finish within {RUN_TIMEOUT_S}s. A key whose "
        f"count is never reached must flush at close, not wait forever.\n{result.tail}"
    )


def assert_ok(result: RunResult) -> None:
    assert_finished(result)
    assert result.returncode == 0, f"nextflow exited {result.returncode}\n{result.tail}"


def assert_stopped(result: RunResult, *needles: str) -> None:
    assert_finished(result)
    assert result.returncode != 0, f"the run exited 0, but it had to stop\n{result.tail}"
    for needle in needles:
        assert needle in result.output, f"the error must name {needle!r}\n{result.tail}"


def assert_slot(recv: Recv, slot: int, names: set[str] | None = None, count: int | None = None, token: str | None = None) -> None:
    got = recv.slots[slot]
    if names is not None:
        assert set(got) == names and len(got) == len(names), (
            f"{recv.process} member {recv.token} slot {slot + 1} received {sorted(got)}, "
            f"expected exactly {sorted(names)}"
        )
    if count is not None:
        assert len(got) == count, (
            f"{recv.process} member {recv.token} slot {slot + 1} received {len(got)} files "
            f"{sorted(got)}, expected {count}"
        )
    if token is not None:
        strays = [n for n in got if f".{token}-" not in n]
        assert not strays, (
            f"{recv.process} member {recv.token} slot {slot + 1} received files of "
            f"another member: {strays}"
        )


def _render_trace(result: RunResult) -> str:
    return "\n".join(
        f"  {t.process:12s} {t.tag:10s} {t.status:9s} submit={t.submit} start={t.start} complete={t.complete}"
        for t in result.trace
    )


def assert_started_before(result: RunResult, early: tuple[str, str], late: tuple[str, str]) -> None:
    a = result.completed(*early)
    b = result.completed(*late)
    assert a.start < b.complete, (
        f"{early[0]} ({early[1]}) started at {a.start} but {late[0]} ({late[1]}) had "
        f"already completed at {b.complete}: the finished sample waited on the slow one "
        f"(no early release).\ntrace:\n{_render_trace(result)}"
    )


def assert_started_after(result: RunResult, late: tuple[str, str], early: tuple[str, str]) -> None:
    a = result.completed(*late)
    b = result.completed(*early)
    assert a.start >= b.complete, (
        f"{late[0]} ({late[1]}) started at {a.start}, before {early[0]} ({early[1]}) "
        f"completed at {b.complete}.\ntrace:\n{_render_trace(result)}"
    )


def assert_runs_ahead(result: RunResult, early: Task, late: Task) -> None:
    assert early.start < late.complete, (
        f"{early.process} ({early.tag}) started at {early.start} but {late.process} "
        f"({late.tag}) had already completed at {late.complete}: it waited on a task "
        f"it does not depend on.\ntrace:\n{_render_trace(result)}"
    )


def assert_slow_task_was_slow(result: RunResult, process: str, tag: str, seconds: int) -> None:
    t = result.completed(process, tag)
    assert t.complete - t.start >= (seconds - 2) * 1000, (
        f"{process} ({tag}) took {t.complete - t.start}ms; the fixture's slow task "
        f"must sleep ~{seconds}s for the ordering assertions to mean anything"
    )


class Workspace:
    def __init__(self, root: Path, image: str | None = None):
        self.root = root
        self.image = image
        self.mount = "/ws" if image else str(root)
        self.lineage: dict[str, list[dict]] = {}
        self.child2parent: dict[str, set[str]] = {}
        self.cases: dict[str, list[str]] = {}
        self.params: dict = {"spec": {}}
        root.mkdir(parents=True, exist_ok=True)
        (root / "lib").mkdir(exist_ok=True)
        (root / "inputs").mkdir(exist_ok=True)
        (root / "late").mkdir(exist_ok=True)
        shutil.copy(orchestrator_src(), root / "lib" / "Orchestrator.groovy")
        shutil.copy(FIXTURES / "mocks.nf", root / "mocks.nf")
        shutil.copy(FIXTURES / "early_release.config", root / "early_release.config")
        shutil.copytree(FIXTURES / "helpers", root / "helpers", dirs_exist_ok=True)

    def path(self, *parts: str) -> str:
        return "/".join([self.mount, *parts])

    def given(
        self, name: str, ids: list[str], parents: dict[str, str] | None = None,
        cases: dict[str, list[str]] | None = None,
    ) -> None:
        self.cases.update(cases or {})
        rows = []
        paths = []
        for i in ids:
            p = self.root / "inputs" / f"{name}_{i}.txt"
            p.write_text(i)
            paths.append(self.path("inputs", p.name))
            row = {k: [v] for k, v in (parents or {}).items()}
            row["__self__"] = [i]
            rows.append(row)
        (self.root / "inputs" / name).write_text("\n".join(paths) + "\n")
        self.lineage[f"inputs/{name}"] = rows
        if parents:
            self.child2parent.setdefault(name, set()).update(parents)

    def spec(self, process: str, **behaviour) -> None:
        self.params["spec"][process] = behaviour

    def shard(self, sample: str, filename: str) -> None:
        out = self.root / "shards" / sample / "out"
        out.mkdir(parents=True, exist_ok=True)
        (out / filename).write_text(f"cached {sample}")

    def run(self, fixture: str | None = None, script: str | None = None, timeout: int = RUN_TIMEOUT_S) -> RunResult:
        if script is not None:
            (self.root / "main.nf").write_text(script)
        else:
            shutil.copy(FIXTURES / fixture, self.root / "main.nf")
        for stale in ("trace.txt", "recv.log"):
            (self.root / stale).unlink(missing_ok=True)
        (self.root / "workflow.lineage_of_given.json").write_text(json.dumps({
            "lineage": self.lineage,
            "child2parent": {k: sorted(v) for k, v in self.child2parent.items()},
            **({"cases": self.cases} if self.cases else {}),
        }))
        params = {"recv_log": self.path("recv.log"), "late_dir": self.path("late"), **self.params}
        (self.root / "params.json").write_text(json.dumps(params))
        nxf_args = [
            "run", "main.nf",
            "-lib", "./lib",
            "-c", "early_release.config",
            "-params-file", "params.json",
            "-ansi-log", "false",
        ]
        if self.image:
            name = f"msm-early-release-{uuid.uuid4().hex[:12]}"
            cmd = [
                "docker", "run", "--rm", "--name", name,
                "-v", f"{self.root}:/ws", "-w", "/ws",
                self.image, "nextflow", *nxf_args,
            ]
            kill = lambda proc: subprocess.run(["docker", "kill", name], capture_output=True)
        else:
            nextflow = host_nextflow()
            assert nextflow is not None, "no nextflow on PATH or beside this python"
            cmd = [str(nextflow), *nxf_args, "-w", str(self.root / "work")]
            kill = lambda proc: os.killpg(proc.pid, signal.SIGKILL)
        env = {**os.environ, "NXF_ANSI_LOG": "false"}
        proc = subprocess.Popen(
            cmd, cwd=self.root, env=env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
        )
        timed_out = False
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            kill(proc)
            stdout, stderr = proc.communicate()
        return RunResult(
            returncode=None if timed_out else proc.returncode,
            stdout=stdout, stderr=stderr, timed_out=timed_out,
            trace=self._read_trace(), recv=self._read_recv(),
        )

    def _read_trace(self) -> list[Task]:
        p = self.root / "trace.txt"
        if not p.exists():
            return []
        lines = [l for l in p.read_text().splitlines() if l.strip()]
        if not lines:
            return []
        header = lines[0].split("\t")
        rows = []
        for line in lines[1:]:
            d = dict(zip(header, line.split("\t")))
            n = {k: int(d[k]) if d[k] not in ("", "-") else 0 for k in ("attempt", "submit", "start", "complete")}
            rows.append(Task(process=d["process"], tag=d["tag"], status=d["status"], exit=d["exit"], **n))
        return rows

    def _read_recv(self) -> list[Recv]:
        p = self.root / "recv.log"
        if not p.exists():
            return []
        rows = []
        for line in p.read_text().splitlines():
            if not line.startswith("RECV|"):
                continue
            _, process, attempt, tokens, slots = line.split("|", 4)
            rows.append(Recv(
                process=process, attempt=int(attempt),
                tokens=tokens.split(",") if tokens else [],
                slots=[s.split(",") if s else [] for s in slots.split(";")],
            ))
        return rows


def reads(i: str) -> str:
    return f"reads_{i}.txt"


def product(token: str, label: str, item: int = 1, pos: int = 1, branch: int = 1) -> str:
    return f"{pos}-{item}-{branch}.{token}-{label}.out"
