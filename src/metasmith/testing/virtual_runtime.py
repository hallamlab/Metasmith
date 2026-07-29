from __future__ import annotations

import json
import os
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from hashlib import md5
from pathlib import Path
from typing import Any

import yaml

from ..constants import AgentPaths
from ..models.lineage import LinPayload
from ..models.remote import Source


TRACE_ENV = "MSM_VIRTUAL_E2E_TRACE"
HOME_ENV = "MSM_VIRTUAL_AGENT_HOME"
HOST_ENV = "MSM_VIRTUAL_HOST"
FORCE_BOUNCE_ENV = "MSM_VIRTUAL_FORCE_BOUNCE"
IN_CONTAINER_ENV = "MSM_VIRTUAL_IN_CONTAINER"


def hash15(value: str) -> int:
    return int(md5(value.encode()).hexdigest()[:15], 16)


def _trace_path() -> Path | None:
    raw = os.environ.get(TRACE_ENV)
    if not raw:
        return None
    return Path(raw)


def write_trace(event: dict[str, Any]) -> None:
    path = _trace_path()
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    row = dict(event)
    row.setdefault("ts", time.time())
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, separators=(",", ":")) + "\n")


def read_trace(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


@dataclass
class VirtualE2ERuntime:
    root: Path
    host: str = "virtual-host"
    force_bounce: bool = False

    def __post_init__(self) -> None:
        self.root = self.root.resolve()
        self.home = self.root / "agent_home"
        self.bin_dir = self.root / "fake_bin"
        self.trace_file = self.root / "virtual_runtime.trace.jsonl"

    def setup(self, monkeypatch) -> "VirtualE2ERuntime":
        self.home.mkdir(parents=True, exist_ok=True)
        self.bin_dir.mkdir(parents=True, exist_ok=True)
        self._install_wrappers()
        self._install_bootstrap()
        self._install_agent_definition()

        old_path = os.environ.get("PATH", "")
        monkeypatch.setenv("PATH", f"{self.bin_dir}:{old_path}")
        monkeypatch.setenv(TRACE_ENV, str(self.trace_file))
        monkeypatch.setenv(HOME_ENV, str(self.home))
        monkeypatch.setenv(HOST_ENV, self.host)
        monkeypatch.setenv(FORCE_BOUNCE_ENV, "1" if self.force_bounce else "0")

        # Prefer this checkout's source tree for child CLI processes.
        repo_root = Path(__file__).resolve().parents[3]
        src = repo_root / "src"
        old_pp = os.environ.get("PYTHONPATH", "")
        if old_pp:
            monkeypatch.setenv("PYTHONPATH", f"{src}:{old_pp}")
        else:
            monkeypatch.setenv("PYTHONPATH", str(src))

        return self

    def _write_wrapper(self, name: str, tool: str) -> None:
        # Pin the python interpreter to the one driving the test, not
        # whatever `python3` resolves to on PATH — otherwise the wrapper
        # picks up /usr/bin/python3 which lacks numpy + the metasmith
        # editable install.
        path = self.bin_dir / name
        py = sys.executable
        path.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env bash",
                    f'exec {py} -m metasmith.testing.virtual_runtime __tool__ {tool} "$@"',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        path.chmod(0o755)

    def _install_wrappers(self) -> None:
        self._write_wrapper("docker", "docker")
        self._write_wrapper("nextflow", "nextflow")
        self._write_wrapper("msm_relay", "relay")
        self._write_wrapper("hostname", "hostname")
        self._write_wrapper("metasmith", "metasmith")

    def _install_bootstrap(self) -> None:
        lib = self.home / "lib"
        lib.mkdir(parents=True, exist_ok=True)
        bootstrap = lib / "msm_bootstrap"
        bootstrap.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env bash",
                    "set -euo pipefail",
                    "TASK_DIR=$1",
                    "STEP=$2",
                    "HOST_NAME=$3",
                    'CWD=${4:-$(pwd -P)}',
                    'cd "$CWD"',
                    f'if [ "${{{IN_CONTAINER_ENV}:-0}}" = "1" ]; then',
                    f'  CMD="{IN_CONTAINER_ENV}=0 ${{{HOME_ENV}}}/lib/msm_bootstrap \"$TASK_DIR\" \"$STEP\" \"$HOST_NAME\" \"$CWD\""',
                    '  msm_relay --io "_metasmith/relay/$HOST_NAME" bounce "$CMD"',
                    "  exit $?",
                    "fi",
                    "msm_relay start --local",
                    'metasmith api execute_transform -a step_index="$STEP" -a workspace="$TASK_DIR" host=$(hostname)',
                    "msm_relay stop",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        bootstrap.chmod(0o755)

    def _install_agent_definition(self) -> None:
        from ..agents import Agent
        from ..env import Runtime

        agent = Agent(
            home=Source.FromLocal(self.home),
            runtime=Runtime.DOCKER,
            container="virtual/metasmith:test",
        )
        lib = self.home / "lib"
        lib.mkdir(parents=True, exist_ok=True)
        with open(lib / "agent.yml", "w", encoding="utf-8") as f:
            yaml.safe_dump(agent.Pack(), f)

    def parse_trace(self) -> list[dict[str, Any]]:
        return read_trace(self.trace_file)


# ---------------------------------------------------------------------------
# Fake CLI implementations
# ---------------------------------------------------------------------------


def _parse_option_map(argv: list[str]) -> tuple[list[str], dict[str, str | bool]]:
    positional: list[str] = []
    opts: dict[str, str | bool] = {}
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok.startswith("--"):
            if "=" in tok:
                k, v = tok.split("=", 1)
                opts[k] = v
                i += 1
                continue
            if i + 1 < len(argv) and not argv[i + 1].startswith("-"):
                opts[tok] = argv[i + 1]
                i += 2
            else:
                opts[tok] = True
                i += 1
            continue
        if tok.startswith("-"):
            if i + 1 < len(argv) and not argv[i + 1].startswith("-"):
                opts[tok] = argv[i + 1]
                i += 2
            else:
                opts[tok] = True
                i += 1
            continue
        positional.append(tok)
        i += 1
    return positional, opts


def cli_hostname(argv: list[str]) -> int:
    _ = argv
    print(os.environ.get(HOST_ENV, socket.gethostname()))
    return 0


def cli_metasmith(argv: list[str]) -> int:
    # The virtual runtime mocks transform execution via the nextflow stub
    # and `virtual_output_synthesized`; the real `metasmith api` call would
    # try to load /msm_home/lib/agent.yml and fail. We just record the
    # invocation in the trace and return 0 so the bootstrap script's
    # downstream steps (e.g. `msm_relay stop`) keep running.
    write_trace({"type": "metasmith_call", "argv": argv})
    return 0


def _docker_extract_command(argv: list[str]) -> tuple[str | None, list[str]]:
    if not argv:
        return None, []
    image: str | None = None
    cmd_start: int | None = None

    options_with_value = {
        "--platform",
        "-v",
        "--volume",
        "-w",
        "--workdir",
        "-e",
        "--env",
        "--mount",
        "--entrypoint",
        "-u",
        "--user",
        "--name",
        "--io",
    }

    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok in {"run", "--rm", "-it", "-i", "-t", "--network", "--network=host"}:
            i += 1
            continue
        if tok in options_with_value:
            i += 2
            continue
        if tok.startswith("--") and "=" in tok:
            i += 1
            continue
        if tok.startswith("-"):
            i += 1
            continue
        image = tok
        cmd_start = i + 1
        break
    if cmd_start is None:
        return image, []
    return image, argv[cmd_start:]


def cli_docker(argv: list[str]) -> int:
    if not argv:
        return 0
    sub = argv[0]

    if sub == "info":
        write_trace({"type": "docker_info"})
        return 0

    if sub in {"pull", "build", "inspect"}:
        write_trace({"type": f"docker_{sub}", "argv": argv[1:]})
        if sub == "inspect":
            print("0")
        return 0

    if sub == "run":
        image, cmd = _docker_extract_command(argv)
        write_trace({"type": "docker_run", "image": image, "cmd": cmd})
        if not cmd:
            return 0
        env = os.environ.copy()
        res = subprocess.run(cmd, check=False, env=env)
        return int(res.returncode)

    write_trace({"type": "docker_unknown", "argv": argv})
    return 0


def _load_task_from_workspace(workspace: Path):
    from ..models.workflow import WorkflowTask

    task_path = workspace / AgentPaths.INTERNALS / AgentPaths.TASK
    return WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data()])


def _seed_lineage(inst) -> dict[str, list[int]]:
    lineage: dict[str, list[int]] = {}
    stack = [inst]
    seen: set[tuple[str, str]] = set()
    while stack:
        curr = stack.pop()
        mark = (curr.parent_lib.GetKey(), str(curr.path))
        if mark in seen:
            continue
        seen.add(mark)
        p = curr.ResolvePath()
        lineage[curr.dtype.key] = lineage.get(curr.dtype.key, []) + [hash15(str(p))]
        parents = curr.parent_lib.parents.get(curr.path, [])
        for pm in parents:
            if pm.path in curr.parent_lib.manifest:
                stack.append(curr.parent_lib.Get(pm.path))
    return {k: sorted(set(v)) for k, v in lineage.items()}


def _select_instances(insts: list, start: int, end: int) -> list:
    if len(insts) == 0:
        return []
    if len(insts) == 1:
        return list(insts)
    chunk = list(insts[start:end])
    if chunk:
        return chunk
    if start < len(insts):
        return [insts[start]]
    return [insts[-1]]


def _instance_mark(inst) -> tuple[str, str]:
    return (inst.parent_lib.GetKey(), str(inst.path))


def _ancestor_marks(inst) -> set[tuple[str, str]]:
    """Every instance `inst` descends from, itself included."""
    marks: set[tuple[str, str]] = set()
    stack = [inst]
    while stack:
        curr = stack.pop()
        m = _instance_mark(curr)
        if m in marks:
            continue
        marks.add(m)
        for pm in curr.parent_lib.parents.get(curr.path, []):
            if pm.path in curr.parent_lib.manifest:
                stack.append(curr.parent_lib.Get(pm.path))
    return marks


def _select_for_key(dep_insts: list, key_inst, key_idx: int) -> list:
    """The instances of one dependency that belong to one group_by key.

    Lineage first, position only as a fallback. A positional window is right
    for a dependency that fans out ALONGSIDE the grouping key (instance i of
    each lines up), and wrong for one that COLLECTS into it — where N
    instances all descend from the same key and the key's member must hold
    every one of them. Slicing there is what hands a collecting transform a
    single item.
    """
    if not dep_insts:
        return []
    if len(dep_insts) == 1:
        # A shared reference DB / container: broadcast to every key.
        return list(dep_insts)
    if key_inst is None:
        return _select_instances(dep_insts, key_idx, key_idx + 1)

    key_mark = _instance_mark(key_inst)
    key_ancestors = _ancestor_marks(key_inst)
    related = [
        inst
        for inst in dep_insts
        # Either direction counts: the dep may descend from the key (the
        # collecting case) or the key may descend from the dep (grouping by
        # a fan-out output while still needing its shared parent).
        if key_mark in _ancestor_marks(inst) or _instance_mark(inst) in key_ancestors
    ]
    if related:
        return related
    return _select_instances(dep_insts, key_idx, key_idx + 1)


def _merge_lineage(maps: list[dict[str, list[int]]]) -> dict[str, list[int]]:
    out: dict[str, list[int]] = {}
    for m in maps:
        for k, vals in m.items():
            out[k] = out.get(k, []) + list(vals)
    return {k: sorted(set(v)) for k, v in out.items()}


def _write_metadata_file(step, invocation_dir: Path, lineages: list[dict[str, Any]]) -> None:
    from ..models.workflow import METADATA_FILE

    used = [step.dependency_map[d][0] for d in step.transform.model.requires if len(step.dependency_map.get(d, [])) > 0]
    produced: list[list] = []
    for dep_group in step.transform.model.produces:
        g = []
        for dep in dep_group:
            insts = step.dependency_map.get(dep, [])
            if insts:
                g.append(insts[0])
        produced.append(g)

    dep_in = {
        dep.key: [inst.instance_id for inst in step.dependency_map.get(dep, [])]
        for dep in step.transform.model.requires
    }
    dep_out = [
        {
            dep.key: [inst.instance_id for inst in step.dependency_map.get(dep, [])]
            for dep in dep_group
        }
        for dep_group in step.transform.model.produces
    ]
    structure_arity = {
        dep.key: len(step.dependency_map.get(dep, []))
        for dep in list(step.transform.model.requires)
        + [d for group in step.transform.model.produces for d in group]
    }

    inp = ",".join(x.dtype.key for x in used)
    out = ";".join(",".join(x.dtype.key for x in group) for group in produced)

    with open(invocation_dir / METADATA_FILE, "w", encoding="utf-8") as f:
        f.write("res 1/1.GB/1\n")
        # Same envelope the Groovy emitter puts on the wire — bootstrap parses
        # both through `LinPayload.from_json`, so a bare list here would be a
        # silent divergence between the two runtimes.
        f.write(f"lin {LinPayload(v=LinPayload.VERSION, entries=lineages).to_json()}\n")
        f.write("fmt 2\n")
        f.write(f"din {json.dumps(dep_in, separators=(',', ':'))}\n")
        f.write(f"dot {json.dumps(dep_out, separators=(',', ':'))}\n")
        f.write(f"sar {json.dumps(structure_arity, separators=(',', ':'))}\n")
        f.write(f"par {len(step.group_by_instances)}\n")
        f.write(f"inp {inp}\n")
        f.write(f"out {out}\n")


def _manifest_name_for_target(target) -> str:
    spec = target.instance.dtype_name.replace("::", "-").replace(" ", "_")
    return f"{spec}.{target.instance.dtype.key}.{target.instance.instance_id}.json"


def _read_hit_decisions(workspace: Path) -> dict[int, dict]:
    """Probe cache.sqlite for each step's cache_key from workflow.step_*.meta.

    Mirrors the codegen path's compile-time probe: a step is "hit" when
    its meta file declares ``cacheable true`` AND the cache_key resolves
    to an entry whose output_root is still on disk. The virtual nextflow
    runtime uses this to skip the bootstrap call entirely and emit the
    cached files as if they had just been produced — the symmetric pin
    of the synthetic ``Channel.of(...)`` path that real Nextflow uses.
    """
    home = Path(os.environ.get(HOME_ENV, str(AgentPaths.HOME_ROOT)))
    cache_root = home / "task_cache"
    if not cache_root.exists():
        return {}
    if os.environ.get("METASMITH_CACHE", "1").lower() in {
        "0", "false", "off", "no"
    }:
        return {}
    meta_specs: dict[int, dict] = {}
    for meta_path in sorted(workspace.glob("workflow.step_*.meta")):
        try:
            order = int(meta_path.stem.rsplit("_", 1)[1])
        except (IndexError, ValueError):
            continue
        cache_key_hex: str | None = None
        cacheable = True
        for line in meta_path.read_text().splitlines():
            if line.startswith("cache_key "):
                cache_key_hex = line.split(" ", 1)[1].strip()
            elif line.startswith("cacheable "):
                cacheable = line.split(" ", 1)[1].strip().lower() == "true"
        if cache_key_hex is None:
            continue
        meta_specs[order] = {
            "cache_key": bytes.fromhex(cache_key_hex),
            "cacheable": cacheable,
        }
    if not meta_specs:
        return {}
    from ..caching.store import CacheStore

    hits: dict[int, dict] = {}
    store = CacheStore.open(cache_root)
    try:
        for order, spec in meta_specs.items():
            if not spec["cacheable"]:
                continue
            entry = store.probe(spec["cache_key"])
            if entry is None or not store.files_exist(entry):
                continue
            hits[order] = {
                "cache_key": spec["cache_key"],
                "output_dir": entry.output_root / "out",
            }
            store.touch(spec["cache_key"])
    finally:
        store.close()
    return hits


def _populate_hit_outputs(
    step,
    hit: dict,
    lineage_by_instance: dict[str, dict[str, list[int]]],
    produced_by_dep: dict[str, list],
) -> None:
    """Build produced_by_dep + lineage_by_instance from a cached step's outputs.

    The cached output directory holds per-branch files matching the
    ``1-1-{branch+1}.*-{dtype_key}{ext}`` shape that virtual nextflow
    emits at miss time. Group them by (branch, dep) and route each into
    the corresponding produced DataInstance just as the miss path would.
    """
    output_dir: Path = hit["output_dir"]
    cached_files = sorted(p for p in output_dir.glob("*") if p.is_file())

    input_maps: list[dict[str, list[int]]] = []
    for dep in step.transform.model.requires:
        dep_insts = list(step.dependency_map.get(dep, []))
        if not dep_insts:
            continue
        lineages = [
            lineage_by_instance.get(inst.instance_id, _seed_lineage(inst))
            for inst in dep_insts
        ]
        input_maps.append(_merge_lineage(lineages))
    merged_inputs = _merge_lineage(input_maps)

    for branch_idx, dep_group in enumerate(step.transform.model.produces):
        for dep in dep_group:
            insts = list(step.dependency_map.get(dep, []))
            if not insts:
                continue
            out_inst = insts[0]
            ext = out_inst.dtype.GetPreferredFileExtension()
            suffix = f"-{out_inst.dtype.key}{ext}"
            branch_prefix = f"1-1-{branch_idx + 1}."
            matching = [
                f for f in cached_files
                if f.name.startswith(branch_prefix) and f.name.endswith(suffix)
            ]
            for fpath in matching:
                curr = dict(merged_inputs)
                curr[out_inst.dtype.key] = [hash15(str(fpath))]
                curr = {k: sorted(set(v)) for k, v in curr.items()}
                lineage_by_instance[out_inst.instance_id] = curr
                produced_by_dep.setdefault(out_inst.dtype.key, []).append(
                    (fpath.resolve(), curr, out_inst.instance_id)
                )


def cli_nextflow(argv: list[str]) -> int:
    write_trace({"type": "nextflow_call", "argv": argv})

    # Split global options from `run ...` options.
    run_idx = None
    for i, tok in enumerate(argv):
        if tok == "run":
            run_idx = i
            break
    if run_idx is None or run_idx + 1 >= len(argv):
        return 1

    script = argv[run_idx + 1]
    _global_args = argv[:run_idx]
    run_args = argv[run_idx + 2 :]
    _pos, opts = _parse_option_map(run_args)

    host = str(opts.get("--hostName", os.environ.get(HOST_ENV, socket.gethostname())))
    output_name = str(opts.get("--output", "results"))

    workspace = Path.cwd()
    output_root = (workspace / output_name).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    task = _load_task_from_workspace(workspace)
    bootstrap = Path(os.environ.get(HOME_ENV, str(AgentPaths.HOME_ROOT))) / "lib/msm_bootstrap"

    lineage_by_instance: dict[str, dict[str, list[int]]] = {}
    for inst in task.plan.given:
        lineage_by_instance[inst.instance_id] = _seed_lineage(inst)

    produced_by_dep: dict[str, list[tuple[Path, dict[str, list[int]], str]]] = {}

    nxf_work = workspace / "nxf_work"
    nxf_work.mkdir(exist_ok=True)

    # S3 — probe cache before walking steps. On hit, the step's outputs
    # are sourced from <cache_root>/<key>/out/ and the executor never
    # fires. Misses fall through to the existing bootstrap path and the
    # post-exec promote (S5) deposits their outputs into the cache.
    # The `_metasmith/trace.jsonl` rows for hits are written at compile
    # time by `_compute_cache_decisions`; the executor only emits its
    # own virtual-runtime trace event for diagnostics here.
    hit_decisions = _read_hit_decisions(workspace)

    for step in task.plan.steps:
        if step.order in hit_decisions:
            hit = hit_decisions[step.order]
            write_trace(
                {
                    "type": "cache_hit",
                    "step": step.order,
                    "step_name": step.transform.name,
                    "cache_key": hit["cache_key"].hex(),
                    "host": host,
                }
            )
            _populate_hit_outputs(
                step, hit, lineage_by_instance, produced_by_dep
            )
            continue

        group_total = max(1, len(step.group_by_instances))
        batch_size = max(1, int(step.transform.batch_size))

        for start in range(0, group_total, batch_size):
            end = min(group_total, start + batch_size)
            invocation_dir = nxf_work / f"step_{step.order:02}" / f"batch_{start:04}_{end:04}"
            invocation_dir.mkdir(parents=True, exist_ok=True)

            # One lineage member per group key in the window — the same arity
            # the real runtime puts on the wire (`Orchestrator._collateBatch`
            # builds one index per member, `LinPayload.entries` carries them
            # all). Writing one member for the whole window would make
            # `context.AsBatch()` yield once here and `batch_size` times under
            # Nextflow, and this runtime is what pins the contract cheaply.
            members: list[dict[str, Any]] = []
            input_maps: list[dict[str, list[int]]] = []
            group_insts = step.group_by_instances

            for key_idx in range(start, end):
                key_inst = group_insts[key_idx] if key_idx < len(group_insts) else None
                lineage_entry: dict[str, Any] = {}
                files: list[list[str]] = []

                for dep in step.transform.model.requires:
                    dep_insts = list(step.dependency_map.get(dep, []))
                    selected = _select_for_key(dep_insts, key_inst, key_idx)
                    dtype_key = selected[0].dtype.key if selected else dep.key
                    paths = [str(inst.ResolvePath()) for inst in selected]
                    files.append(paths)
                    hashes = [hash15(str(Path(p))) for p in paths]
                    lineage_entry[dtype_key] = hashes

                    lineages = [
                        lineage_by_instance.get(inst.instance_id, {dtype_key: hashes})
                        for inst in selected
                    ]
                    input_maps.append(_merge_lineage(lineages))

                lineage_entry["FILES"] = files
                members.append(lineage_entry)

            _write_metadata_file(step, invocation_dir, members)

            dep_arity = {
                dep.key: len(step.dependency_map.get(dep, []))
                for dep in list(step.transform.model.requires)
                + [d for group in step.transform.model.produces for d in group]
            }
            write_trace(
                {
                    "type": "bootstrap_call",
                    "step": step.order,
                    "step_name": step.transform.name,
                    "batch_start": start,
                    "batch_end": end,
                    "host": host,
                    "dep_arity": dep_arity,
                    "sample_arity": len(step.group_by_instances),
                }
            )

            env = os.environ.copy()
            env[IN_CONTAINER_ENV] = "1" if env.get(FORCE_BOUNCE_ENV, "0") == "1" else "0"
            res = subprocess.run(
                [str(bootstrap), str(workspace), str(step.order), host],
                cwd=invocation_dir,
                env=env,
                check=False,
                capture_output=True,
                text=True,
            )
            write_trace(
                {
                    "type": "bootstrap_result",
                    "step": step.order,
                    "code": int(res.returncode),
                    "stdout_tail": res.stdout[-500:],
                    "stderr_tail": res.stderr[-500:],
                }
            )
            if res.returncode != 0:
                return int(res.returncode)

            merged_inputs = _merge_lineage(input_maps)
            for branch_idx, dep_group in enumerate(step.transform.model.produces):
                for dep in dep_group:
                    insts = list(step.dependency_map.get(dep, []))
                    if not insts:
                        continue
                    out_inst = _select_instances(insts, start, end)[0]
                    ext = out_inst.dtype.GetPreferredFileExtension()
                    pattern = f"*-*-{branch_idx + 1}.*-{out_inst.dtype.key}{ext}"
                    files = sorted(invocation_dir.glob(pattern))
                    if len(files) == 0:
                        # Some transforms emit custom names instead of context.Output().
                        # Synthesize the canonical Nextflow-shaped output so publish/collect
                        # logic can still be validated end-to-end.
                        from ..hashing import KeyGenerator

                        base_lin = {k: sorted(v) for k, v in merged_inputs.items()}
                        _, lin_hash = KeyGenerator.FromStr(
                            json.dumps(base_lin, separators=(",", ":"), sort_keys=True),
                            l=16,
                        )
                        synth = invocation_dir / (
                            f"1-1-{branch_idx + 1}.{lin_hash}-{out_inst.dtype.key}{ext}"
                        )
                        synth.write_text(
                            f"virtual output for step {step.order} {out_inst.dtype.key}\\n",
                            encoding="utf-8",
                        )
                        files = [synth]
                        write_trace(
                            {
                                "type": "virtual_output_synthesized",
                                "step": step.order,
                                "dep_key": out_inst.dtype.key,
                                "path": str(synth),
                            }
                        )
                    for fpath in files:
                        curr = dict(merged_inputs)
                        curr[out_inst.dtype.key] = [hash15(str(fpath))]
                        curr = {k: sorted(set(v)) for k, v in curr.items()}
                        lineage_by_instance[out_inst.instance_id] = curr
                        produced_by_dep.setdefault(out_inst.dtype.key, []).append(
                            (fpath.resolve(), curr, out_inst.instance_id)
                        )

    # Publish target outputs (lineage now rides on trace.jsonl).
    for target in task.plan.targets:
        dep_key = target.instance.dtype.key
        entries = produced_by_dep.get(dep_key, [])
        if not entries:
            continue

        out_dir = output_root / target.name.replace(" ", "_")
        out_dir.mkdir(parents=True, exist_ok=True)
        for i, (src, _lineage, _inst_id) in enumerate(entries):
            dest = out_dir / f"{i + 1:04}_{src.name}"
            shutil.copy2(src, dest)

    # Produce optional report files if requested.
    for k in ["-with-report", "-with-dag", "-with-timeline", "-with-trace"]:
        v = opts.get(k)
        if isinstance(v, str):
            p = Path(v)
            p.parent.mkdir(parents=True, exist_ok=True)
            if k == "-with-report":
                p.write_text("<html><body>virtual nextflow report</body></html>\n", encoding="utf-8")
            else:
                p.write_text("virtual\n", encoding="utf-8")

    # If -log is provided in global args, create it.
    if "-log" in _global_args:
        idx = _global_args.index("-log")
        if idx + 1 < len(_global_args):
            lp = Path(_global_args[idx + 1])
            lp.parent.mkdir(parents=True, exist_ok=True)
            lp.write_text("virtual nextflow log\n", encoding="utf-8")

    write_trace({"type": "nextflow_done", "script": script, "workspace": str(workspace)})
    return 0


def _relay_paths(host: str) -> tuple[Path, Path, Path, Path]:
    root = Path("_metasmith") / "relay"
    watcher = root / host
    pidf = root / f"{host}.pid"
    stopf = root / f"{host}.stop"
    logf = root / f"{host}.log"
    return watcher, pidf, stopf, logf


def _relay_daemon(watcher: Path, stopf: Path, logf: Path) -> int:
    watcher.mkdir(parents=True, exist_ok=True)
    logf.parent.mkdir(parents=True, exist_ok=True)

    def _log(msg: str) -> None:
        with open(logf, "a", encoding="utf-8") as f:
            f.write(msg + "\n")

    _log("relay daemon started")
    while not stopf.exists():
        starts = sorted(watcher.glob("*.start"))
        for startf in starts:
            stem = startf.stem
            outp = watcher / f"{stem}.out"
            errp = watcher / f"{stem}.err"
            donep = watcher / f"{stem}.done"
            script = startf.read_text(encoding="utf-8")
            startf.unlink(missing_ok=True)
            proc = subprocess.run(
                ["bash", "-lc", script],
                capture_output=True,
                text=True,
                check=False,
            )
            outp.write_text(proc.stdout or "", encoding="utf-8")
            errp.write_text(proc.stderr or "", encoding="utf-8")
            donep.write_text(str(proc.returncode), encoding="utf-8")
            write_trace(
                {
                    "type": "relay_job",
                    "watcher": str(watcher),
                    "job": stem,
                    "returncode": int(proc.returncode),
                }
            )
        time.sleep(0.05)

    _log("relay daemon stopped")
    return 0


def cli_relay(argv: list[str]) -> int:
    if not argv:
        return 0

    if argv[0] == "--io":
        if len(argv) < 4:
            return 1
        io_path = Path(argv[1])
        action = argv[2]
        if action != "bounce":
            return 1
        cmd = argv[3]
        write_trace({"type": "relay_bounce", "io": str(io_path), "cmd": cmd})
        env = os.environ.copy()
        env[IN_CONTAINER_ENV] = "0"
        res = subprocess.run(cmd, shell=True, check=False, env=env)
        return int(res.returncode)

    command = argv[0]
    host = os.environ.get(HOST_ENV, socket.gethostname())
    watcher, pidf, stopf, logf = _relay_paths(host)

    if command == "start":
        watcher.mkdir(parents=True, exist_ok=True)
        stopf.unlink(missing_ok=True)
        if pidf.exists():
            try:
                old_pid = int(pidf.read_text(encoding="utf-8").strip())
                os.kill(old_pid, 0)
                write_trace({"type": "relay_start", "host": host, "watcher": str(watcher), "status": "already_running"})
                return 0
            except Exception:
                pidf.unlink(missing_ok=True)

        proc = subprocess.Popen(
            [sys.executable, "-m", "metasmith.testing.virtual_runtime", "__tool__", "relay-daemon", str(watcher), str(stopf), str(logf)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        pidf.write_text(str(proc.pid), encoding="utf-8")
        write_trace({"type": "relay_start", "host": host, "watcher": str(watcher), "pid": proc.pid})
        return 0

    if command == "stop":
        stopf.parent.mkdir(parents=True, exist_ok=True)
        stopf.write_text("1", encoding="utf-8")
        if pidf.exists():
            try:
                pid = int(pidf.read_text(encoding="utf-8").strip())
                for _ in range(40):
                    try:
                        os.kill(pid, 0)
                        time.sleep(0.05)
                    except ProcessLookupError:
                        break
                else:
                    os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
            pidf.unlink(missing_ok=True)
        write_trace({"type": "relay_stop", "host": host, "watcher": str(watcher)})
        return 0

    if command == "logs":
        if logf.exists():
            sys.stdout.write(logf.read_text(encoding="utf-8"))
        write_trace({"type": "relay_logs", "host": host})
        return 0

    write_trace({"type": "relay_unknown", "argv": argv})
    return 0


def main() -> int:
    if len(sys.argv) < 3 or sys.argv[1] != "__tool__":
        print("usage: python -m metasmith.testing.virtual_runtime __tool__ <tool> [args...]", file=sys.stderr)
        return 2

    tool = sys.argv[2]
    argv = sys.argv[3:]

    if tool == "docker":
        return cli_docker(argv)
    if tool == "nextflow":
        return cli_nextflow(argv)
    if tool == "relay":
        return cli_relay(argv)
    if tool == "relay-daemon":
        if len(argv) != 3:
            return 2
        return _relay_daemon(Path(argv[0]), Path(argv[1]), Path(argv[2]))
    if tool == "hostname":
        return cli_hostname(argv)
    if tool == "metasmith":
        return cli_metasmith(argv)

    print(f"unknown tool [{tool}]", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
