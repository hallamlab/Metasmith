# Black-box tests of Orchestrator early release. Each test runs a hand-written
# workflow from fixtures/early_release/ against the real Orchestrator.groovy in
# the repo's docker image, with mock processes whose behaviour depends on the
# sample they receive. No planner, no codegen, no reading of the Groovy: the
# evidence is the Nextflow trace, what each task appended to the receive log,
# and the run's exit status and error text.
#
# Every timing claim is an ORDERING relation with a 20 s sleep behind it, never
# an absolute threshold, so scheduler noise cannot flip it.

import json
import shutil
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

from metasmith.constants import MODULE_PATH

pytestmark = [pytest.mark.docker, pytest.mark.nextflow, pytest.mark.slow]

FIXTURES = Path(__file__).parent / "fixtures" / "early_release"
ORCHESTRATOR_SRC = MODULE_PATH / "nextflow_config/Orchestrator.groovy"

SAMPLES = ["s0", "s1", "s2"]
SLOW = "s0"
FAST = [s for s in SAMPLES if s != SLOW]
SLOW_S = 20
RUN_TIMEOUT_S = 240


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


def assert_started_before(result: RunResult, early: tuple[str, str], late: tuple[str, str]) -> None:
    a = result.completed(*early)
    b = result.completed(*late)
    assert a.start < b.complete, (
        f"{early[0]} ({early[1]}) started at {a.start} but {late[0]} ({late[1]}) had "
        f"already completed at {b.complete}: the finished sample waited on the slow one "
        f"(no early release).\n"
        f"trace:\n" + "\n".join(
            f"  {t.process:12s} {t.tag:10s} {t.status:9s} submit={t.submit} start={t.start} complete={t.complete}"
            for t in result.trace
        )
    )


def assert_slow_task_was_slow(result: RunResult, process: str, tag: str) -> None:
    t = result.completed(process, tag)
    assert t.complete - t.start >= (SLOW_S - 2) * 1000, (
        f"{process} ({tag}) took {t.complete - t.start}ms; the fixture's slow task "
        f"must sleep ~{SLOW_S}s for the ordering assertions to mean anything"
    )


class Workspace:
    def __init__(self, root: Path, image: str):
        self.root = root
        self.image = image
        self.lineage: dict[str, list[dict]] = {}
        self.child2parent: dict[str, set[str]] = {}
        self.params: dict = {"spec": {}}
        root.mkdir(parents=True, exist_ok=True)
        (root / "lib").mkdir(exist_ok=True)
        (root / "inputs").mkdir(exist_ok=True)
        (root / "late").mkdir(exist_ok=True)
        shutil.copy(ORCHESTRATOR_SRC, root / "lib" / "Orchestrator.groovy")
        shutil.copy(FIXTURES / "mocks.nf", root / "mocks.nf")
        shutil.copy(FIXTURES / "early_release.config", root / "early_release.config")
        shutil.copytree(FIXTURES / "helpers", root / "helpers")

    def given(self, name: str, ids: list[str], parents: dict[str, str] | None = None) -> None:
        rows = []
        paths = []
        for i in ids:
            p = self.root / "inputs" / f"{name}_{i}.txt"
            p.write_text(i)
            paths.append(f"/ws/inputs/{p.name}")
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

    def run(self, fixture: str, timeout: int = RUN_TIMEOUT_S) -> RunResult:
        shutil.copy(FIXTURES / fixture, self.root / "main.nf")
        (self.root / "workflow.lineage_of_given.json").write_text(json.dumps({
            "lineage": self.lineage,
            "child2parent": {k: sorted(v) for k, v in self.child2parent.items()},
        }))
        (self.root / "params.json").write_text(json.dumps(self.params))
        name = f"msm-early-release-{uuid.uuid4().hex[:12]}"
        cmd = [
            "docker", "run", "--rm", "--name", name,
            "-v", f"{self.root}:/ws", "-w", "/ws",
            self.image,
            "nextflow", "run", "main.nf",
            "-lib", "./lib",
            "-c", "early_release.config",
            "-params-file", "params.json",
            "-ansi-log", "false",
        ]
        timed_out = False
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            returncode, stdout, stderr = proc.returncode, proc.stdout, proc.stderr
        except subprocess.TimeoutExpired as e:
            subprocess.run(["docker", "kill", name], capture_output=True)
            timed_out = True
            returncode = None
            stdout = e.stdout.decode("utf-8", errors="replace") if isinstance(e.stdout, bytes) else (e.stdout or "")
            stderr = e.stderr.decode("utf-8", errors="replace") if isinstance(e.stderr, bytes) else (e.stderr or "")
        return RunResult(
            returncode=returncode, stdout=stdout, stderr=stderr, timed_out=timed_out,
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
            rows.append(Task(
                process=d["process"], tag=d["tag"], status=d["status"], exit=d["exit"],
                attempt=int(d["attempt"] or 0),
                submit=int(d["submit"] or 0), start=int(d["start"] or 0), complete=int(d["complete"] or 0),
            ))
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


@pytest.fixture
def ws(tmp_path, docker_image) -> Workspace:
    return Workspace(tmp_path / "ws", docker_image)


def _reads(i: str) -> str:
    return f"reads_{i}.txt"


def _product(token: str, label: str, item: int = 1, pos: int = 1, branch: int = 1) -> str:
    return f"{pos}-{item}-{branch}.{token}-{label}.out"


# ---------------------------------------------------------------- early release happens


def test_fast_samples_run_ahead_of_a_slow_sample(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")

    result = ws.run("linear_chain.nf")
    assert_ok(result)

    assert_slow_task_was_slow(result, "p01", SLOW)
    for s in FAST:
        assert_started_before(result, ("p02", s), ("p01", SLOW))
        assert_started_before(result, ("p03", s), ("p01", SLOW))

    p02 = result.members("p02", set(SAMPLES))
    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 0, names={_reads(s)})
        assert_slot(p02[s], 1, names={_product(s, "asm")})
        assert_slot(p03[s], 0, names={_reads(s)})
        assert_slot(p03[s], 1, names={_product(s, "bins")})


def test_fast_samples_run_ahead_through_a_cache_twin(ws):
    hit = "s1"
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")
    ws.shard(hit, _product(hit, "bins"))
    ws.params["cacheable"] = True
    ws.params["helper"] = ["python3", "/ws/helpers/cache_helper.py", "/ws/shards", hit]

    result = ws.run("cache_twin.nf")
    assert_ok(result)

    assert_slow_task_was_slow(result, "p01", SLOW)
    for s in FAST:
        assert_started_before(result, ("p03", s), ("p01", SLOW))

    result.members("p02_cached", {hit})
    result.members("p02", set(SAMPLES) - {hit})
    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p03[s], 0, names={_reads(s)})
        assert_slot(p03[s], 1, names={_product(s, "bins")})


def test_a_finished_batch_runs_ahead_of_an_unfinished_one(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")
    ws.params["batch"] = 2

    result = ws.run("batched_producer.nf")
    assert_ok(result)

    p01_tasks = result.tasks("p01", status="COMPLETED")
    assert len(p01_tasks) == 2, f"batch_size 2 over 3 samples is 2 tasks, got {[(t.tag) for t in p01_tasks]}"
    slow_batch = next(t for t in p01_tasks if SLOW in t.tag.split(","))
    fast_batch = next(t for t in p01_tasks if SLOW not in t.tag.split(","))
    assert_slow_task_was_slow(result, "p01", slow_batch.tag)
    for s in fast_batch.tag.split(","):
        assert_started_before(result, ("p02", s), ("p01", slow_batch.tag))

    p02 = result.members("p02", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 0, names={_reads(s)})
        assert_slot(p02[s], 1, count=1, token=s)


# ------------------------------------------- whole members where release must wait


def test_fan_out_member_holds_every_file(ws):
    fan = "s1"
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", fan={fan: 3})
    ws.spec("p02", label="bins")
    ws.params["batch"] = 1

    result = ws.run("batched_producer.nf")
    assert_ok(result)

    p02 = result.members("p02", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 0, names={_reads(s)})
        n = 3 if s == fan else 1
        assert_slot(p02[s], 1, names={_product(s, "asm", item=i + 1) for i in range(n)})


def test_collector_gets_one_whole_member(ws):
    ws.given("proj", ["p"])
    ws.given("reads", SAMPLES, parents={"proj": "p"})
    ws.spec("p01", label="asm", slow={SLOW: SLOW_S})
    ws.spec("p02", label="pan", by="proj")

    result = ws.run("collector.nf")
    assert_ok(result)

    p02 = result.members("p02", {"p"})
    assert_slot(p02["p"], 0, names={"proj_p.txt"})
    assert_slot(p02["p"], 1, names={_product(s, "asm") for s in SAMPLES})
    collector = result.completed("p02", "+".join(SAMPLES))
    slow = result.completed("p01", SLOW)
    assert collector.start >= slow.complete, "the collector ran before its slowest input existed"


def test_aggregate_then_distribute_hands_every_product_to_every_sample(ws):
    ws.given("proj", ["p"])
    ws.given("reads", SAMPLES, parents={"proj": "p"})
    ws.spec("p01", label="asm")
    ws.spec("p02", label="merged", by="proj", fan={"+".join(SAMPLES): 2})
    ws.spec("p03", label="dist")

    result = ws.run("aggregate_distribute.nf")
    assert_ok(result)

    result.members("p02", {"p"})
    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p03[s], 0, names={_reads(s)})
        assert_slot(p03[s], 1, names={_product("p", "merged", item=1), _product("p", "merged", item=2)})


def test_two_producers_mixed_into_one_stream_both_reach_every_member(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01a", label="asm")
    ws.spec("p01b", label="asmb", slow={SLOW: SLOW_S})
    ws.spec("p02", label="bins")

    result = ws.run("mixed_producers.nf")
    assert_ok(result)

    p02 = result.members("p02", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 0, names={_reads(s)})
        assert_slot(p02[s], 1, names={_product(s, "asm"), _product(s, "asmb")})


def test_producer_grouped_by_another_key_waits_for_every_item(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", fan={s: 2 for s in SAMPLES})
    ws.spec("p02", label="bins", by="asm", slow_in={rf".*-2-1\.{SLOW}-asm\.out": SLOW_S})
    ws.spec("p03", label="qc")

    result = ws.run("other_key_producer.nf")
    assert_ok(result)

    assert len(result.received("p02")) == 6, "one p02 task per asm file"
    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p03[s], 0, names={_reads(s)})
        assert_slot(p03[s], 1, count=2)
        feeders = [r for r in result.received("p02") if r.slots[0] and f".{s}-asm" in r.slots[0][0]]
        assert {r.tokens[0] for r in feeders} == {
            n.split(".", 1)[1].rsplit("-", 1)[0] for n in p03[s].slots[1]
        }, f"p03 ({s}) did not receive exactly the bins of its own two asm files"


# ----------------------------------------------------------------------- no hangs


def test_a_dropped_task_does_not_hang_the_other_samples(ws):
    dropped = "s1"
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", fail=[dropped])
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")

    result = ws.run("linear_chain.nf")
    assert_ok(result)

    failed = result.tasks("p01", dropped, "FAILED")
    assert len(failed) == 2 and all(t.exit == "1" for t in failed), (
        f"p01 ({dropped}) should fail twice (retry, then ignore), got {[(t.status, t.attempt) for t in result.tasks('p01', dropped)]}"
    )
    survivors = set(SAMPLES) - {dropped}
    p02 = result.members("p02", survivors)
    p03 = result.members("p03", survivors)
    for s in survivors:
        assert_slot(p02[s], 1, names={_product(s, "asm")})
        assert_slot(p03[s], 1, names={_product(s, "bins")})


def test_an_empty_optional_branch_does_not_hang_the_other_samples(ws):
    empty = "s1"
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm", empty=[empty])
    ws.spec("p02", label="qc")

    result = ws.run("optional_branch.nf")
    assert_ok(result)

    others = set(SAMPLES) - {empty}
    p02 = result.members("p02", others)
    for s in others:
        assert_slot(p02[s], 0, names={_reads(s)})
        assert_slot(p02[s], 1, names={_product(s, "asm2", branch=2)})


def test_a_broken_cache_probe_runs_every_member(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm")
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")
    ws.params["cacheable"] = True
    ws.params["helper"] = ["bash", "/ws/helpers/broken_helper.sh"]

    result = ws.run("cache_twin.nf")
    assert_ok(result)

    assert "helper" in result.output, "a failed cache probe must be reported, not swallowed"
    assert not result.received("p02_cached"), "nothing can be a hit when the probe cannot answer"
    p02 = result.members("p02", set(SAMPLES))
    p03 = result.members("p03", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 1, names={_product(s, "asm")})
        assert_slot(p03[s], 1, names={_product(s, "bins")})


# ------------------------------------------------------- SIBLING with two ancestors


@pytest.mark.parametrize("root", ["cfg", "proj"])
@pytest.mark.parametrize("order", [["r1", "r2"], ["r2", "r1"]])
def test_coassembly_sibling_join_emits_one_whole_member(ws, root, order):
    # `cfg` has no lineage relation to the reads, so `reads` is the ONLY
    # ancestor the clean reads and the coassembly share. `proj` is a real
    # parent of the reads, so both it and `reads` are shared and the join
    # key is whichever _firstSharedAncestor happens to pick.
    ws.given(root, ["c"])
    ws.given("reads", order, parents={"proj": "c"} if root == "proj" else None)
    ws.spec("p01", label="clean")
    ws.spec("p02", label="coasm", by=root)
    ws.spec("p03", label="bins", by="coasm")
    ws.params["root"] = root

    result = ws.run("sibling_coassembly.nf")
    assert_ok(result)

    coasm = result.members("p02", {"c"})
    assert_slot(coasm["c"], 1, names={_reads(r) for r in order})
    p03 = result.received("p03")
    assert len(p03) == 1, (
        f"one coassembly must yield ONE member, got {len(p03)}: "
        f"{[r.slots for r in p03]}. Two members with one clean read each is the "
        f"SIBLING by-side fan-out bug.\n{result.tail}"
    )
    assert_slot(p03[0], 0, names={_product("c", "coasm")})
    assert_slot(p03[0], 1, names={_product(r, "clean") for r in order})


# --------------------------------------------------------------------------- seal


def test_an_unsealed_workflow_refuses_to_run(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm")
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc")

    result = ws.run("linear_chain_unsealed.nf")
    assert_finished(result)

    assert result.returncode != 0, (
        "a workflow body that never calls o.seal() ran to completion; every "
        "registry read happened before a seal that never came, and that must be "
        f"a loud failure, not a quiet count.\n{result.tail}"
    )
    assert "seal" in result.output.lower(), f"the refusal must name the seal\n{result.tail}"


def test_every_source_kind_sees_the_seal(ws):
    ws.given("reads", SAMPLES)
    ws.child2parent["meta"] = {"reads"}
    meta = []
    for s in SAMPLES:
        p = ws.root / "inputs" / f"meta_{s}.txt"
        p.write_text(s)
        meta.append({"id": f"m-{s}", "reads": s, "path": f"/ws/inputs/{p.name}"})
    (ws.root / "inputs" / "ref.txt").write_text("ref")
    ws.params["meta"] = meta
    ws.params["ref"] = "/ws/inputs/ref.txt"
    ws.spec("p01", label="asm")
    ws.spec("p02", label="bins")
    ws.spec("p03", label="qc", by="bins")

    result = ws.run("seal_sources.nf")
    assert_ok(result)

    p02 = result.members("p02", set(SAMPLES))
    for s in SAMPLES:
        assert_slot(p02[s], 0, names={_reads(s)})
        assert_slot(p02[s], 1, names={f"meta_{s}.txt"})
        assert_slot(p02[s], 2, names={_product(s, "asm")})
    p03 = result.received("p03")
    assert len(p03) == 3 and len({r.token for r in p03}) == 3, f"one p03 member per bins item, got {p03}"
    assert {r.slots[0][0] for r in p03} == {_product(s, "bins") for s in SAMPLES}
    for r in p03:
        assert_slot(r, 1, names={"ref.txt"})


# ------------------------------------------------------------- late item is a crash


def test_a_late_item_for_a_released_key_crashes_the_run(ws):
    ws.given("reads", SAMPLES)
    ws.spec("p01", label="asm")
    ws.spec("p02", label="bins")
    ws.params["late_ms"] = 10000

    result = ws.run("late_item.nf")
    assert_finished(result)

    assert result.returncode != 0, (
        "a second, distinct `asm` item arrived for every sample after its member "
        "was released, and the run still exited 0. Under the sibling-stamp "
        "invariant that item cannot exist, so it must stop the run.\n"
        f"p02 received: {[r.slots for r in result.received('p02')]}\n{result.tail}"
    )
    for needle, what in (("[asm]", "the stream"), ("[reads]", "the by-key")):
        assert needle in result.output, f"the error must name {what} ({needle})\n{result.tail}"
    seen: dict[str, int] = {}
    for r in result.received("p02"):
        seen[r.token] = seen.get(r.token, 0) + 1
        assert not any(n.endswith("-late.out") for n in r.slots[1]), (
            f"a late item was folded into a member instead of stopping the run: {r.slots[1]}"
        )
    assert all(n == 1 for n in seen.values()), (
        f"a late item was emitted as a second member: {seen}"
    )
