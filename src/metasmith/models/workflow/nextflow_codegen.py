"""Compiling a task into Nextflow: the .nf, the configs, and the orchestrator wiring.

The bulk of this module is one function, and it was one 800-line method before
that -- a third of the old `workflow.py`. It reads almost nothing off the task
(`plan`, and three helpers), which is what let it move out whole.

Two invariants the generated Groovy depends on, both easy to break silently:

Every tuple must re-enter `o.post(...)` before any downstream `o.group(...)`
observes it. A cache hit rewrites a step's emission into a synthetic channel,
and routing that channel around `o.post` deadlocks the orchestrator rather than
failing.

The output targets a strict-syntax parser (nextflow 26.04.1). Single-element
parenthesized assignment and range-based `for` are both rejected there, so an
edit that reads fine as Groovy can still refuse to compile. No unit test sees
it; the virtual e2e axis does.

`NextflowProcessName` lives here because the compiler writes it and every
`withName:` selector-builder has to agree with what was written.
"""

from __future__ import annotations

import itertools
import json
import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from ...constants import AgentPaths
from ...env import ContainerDef, Environment, Runtime
from ...logging import Log
from ..libraries import DataInstance, GPU_LABEL
from ..paths import PathMap
from ..solver import Endpoint
from .steps import WorkflowStep



METADATA_FILE = ".command.metadata"
BIND_FILE = ".command.binds"

def NextflowProcessName(order: int, transform_name) -> str:
    """The name nextflow knows a step by.

    One function because two callers need it to agree: the compiler, which
    writes it into the .nf, and anything building a `withName:` selector to
    retarget that step's resources. The position prefix is what makes a
    selector address *one* step rather than every step running the transform.
    """
    name = str(transform_name).replace('/', '_')
    return f"p{order:02}__{name}"


@dataclass
class NextflowGenContext:
    workflow_file: str
    work_dir: Path
    external_work: Path
    home_dir: Path
    external_home: Path
    runtime: Runtime
    resources_file: str
    external_home_var: str = "${params.home}"
    external_work_var: str = "${params.workspace}"
    bootstrap_var: str = "${params.bootstrap_def}"
    # S3 — lineage-addressed task cache. cache_root defaults to
    # <external_home>/task_cache; set to None to disable cache integration
    # (synthetic channels, publishDir-to-cache, probe). The env var
    # METASMITH_CACHE=0 also disables, regardless of this setting.
    cache_root: Path | None = None
    # Materialization strategy for publishDir into the cache: 'link'
    # (hardlink, local FS) or 'copy' (network FS). S6 picks this from
    # mountinfo; for now the default is 'link'.
    cache_hit_strategy: str = "link"

def _read_env_declarations(step) -> dict[str, list[str]]:
    """Which of `container:` / `conda:` each env resource this step names carries.

    Keyed by the resource's own file name. A resource that cannot be read (not
    yet staged, binary, unparseable) is recorded as `null` -- unknown, which the
    preflight must not read as absent, or a workspace staged before the resource
    landed would fail for the wrong reason.
    """
    found: dict[str, list[str]|None] = {}
    for dep in getattr(step.transform, "_env_deps", []):
        for inst in step.dependency_map.get(dep, []):
            try:
                p = inst.ResolvePath()
                name = Path(p).name
                if name in found: continue
                with open(p) as f:
                    parsed = yaml.safe_load(f.read())
            except Exception:
                found[Path(str(getattr(inst, "dtype_name", dep.key))).name] = None
                continue
            if isinstance(parsed, dict):
                found[name] = sorted(k for k in ("container", "conda") if parsed.get(k))
            else:
                # A legacy bare-URI (*.oci) resource is a container image and
                # nothing else -- that is exactly what ResolveEnvImage does with it.
                found[name] = ["container"]
    return found

def apply_fs_strategy(context: NextflowGenContext) -> None:
    """Refuse straddle-mounts; pick publishDir mode from mountinfo.

    S6 contract:
    - If cache_root and work_dir live on different mounts, force the
      publishDir 'copy' strategy. The only cross-mount-fragile op is
      Nextflow's publishDir hardlink ('link'); copy works across
      mounts. promote's own staging uses shutil.move/copy2
      (cross-mount safe) and its atomic tmp->shard rename is *within*
      cache_root (always same mount), so the loser-of-race contract is
      unaffected by a work_dir/cache_root straddle. This is the
      containerized-agent case: agent_home is bound at both /ws
      (WORK_ROOT) and its literal path, which are distinct bind mounts
      that reject cross-mount rename/hardlink (EXDEV) despite sharing a
      host device.
    - Otherwise set context.cache_hit_strategy to 'copy' on a network
      FS (Lustre / NFS / GPFS / BeeGFS / etc.) and 'link' on a local
      FS. The default was 'link'; this only widens it when needed.

    Silently skipped when cache_root is None or METASMITH_CACHE is
    falsy (cache integration disabled).
    """
    if context.cache_root is None:
        return
    if os.environ.get("METASMITH_CACHE", "1").lower() in {
        "0", "false", "off", "no"
    }:
        return
    from ...caching.fs import (
        StraddleMountError,
        assert_same_mount,
        detect_strategy,
    )

    # cache_root may not exist yet on a fresh workspace; resolve()
    # walks up to the first existing parent for the mountinfo match.
    anchor = context.cache_root
    while not anchor.exists() and anchor != anchor.parent:
        anchor = anchor.parent
    try:
        assert_same_mount(anchor, context.work_dir)
    except StraddleMountError as e:
        # Containerized agent: work_dir (/ws) and cache_root (literal
        # home bind) are distinct bind mounts of the same host dir.
        # Hardlink publishDir would EXDEV; fall back to copy.
        Log.Warn(
            "cache_root and work_dir straddle mounts; forcing "
            f"publishDir 'copy' strategy. ({e})"
        )
        context.cache_hit_strategy = "copy"
        return
    context.cache_hit_strategy = detect_strategy(
        anchor, default=context.cache_hit_strategy
    )


def prepare_nextflow(task, context: NextflowGenContext):
    TAB = "\t"
    def _strip_var(s: str):
        return s[2:-1]
    if context.cache_root is None:
        context.cache_root = context.external_home / "task_cache"
    task._apply_fs_strategy(context)
    cache_decisions = task._compute_cache_decisions(context)
    # Derive task key from the per-task workspace name. external_work
    # is always <external_home>/runs/<task_key> by the StageWorkflow
    # invariant (agents.py:874-878), so the basename IS the task key.
    path_map = PathMap(
        extern_home=context.external_home,
        task_key=context.external_work.name,
    )
    bootstrap = [
        f"{_strip_var(context.bootstrap_var)} = '''",
        f"CONTAINER={context.home_dir}",
        f"DIRECT={context.external_home}",
        "function bootstrap {",
        TAB+f"if [ -e $CONTAINER ]; then",
        TAB+TAB+f"$CONTAINER/lib/msm_bootstrap $@",
        TAB+f"elif [ -e $DIRECT ]; then",
        TAB+TAB+f"$DIRECT/lib/msm_bootstrap $@",
        TAB+f"else",
        TAB+TAB+'echo "critical error: could not find metasmith bootstrap script"',
        TAB+f"fi",
        "}",
        f"'''",
        "",
        "def in(f, l) {",
        "    def rows = Channel.fromPath(f).splitCsv(header: false)",
        "    if (f in l) {",
        "        rows = Channel.fromList(l[f]).merge(rows)",
        "    }",
        "    return rows.map { row ->",
        "        if (row.size()>1) {",
        "            def (ri, rx) = row",
        "            return tuple(ri, file(rx))",
        "        } else {",
        "            def i = [:]",
        "            return tuple(i, file(row[0]))",
        "        }",
        "    }",
        "}",
        "",
        "",
    ]
    # The Nextflow HEADER assigns the params.home / params.workspace
    # variables. params.home is the literal host path; params.workspace
    # is rendered via the groovy dialect so the substitution is
    # prefix-aware (not str.replace, which would corrupt inner
    # occurrences — see tests/path_overhaul/test_str_replace_path_overlap.py).
    HEADER = "\n".join([
        "params.testSpread=1",
        f"{_strip_var(context.external_home_var)} = '{context.external_home}'",
        f'{_strip_var(context.external_work_var)} = "{path_map.Render(context.external_work, dialect="groovy")}"',
    ]+bootstrap)
    MAX_FILE_SIZE = int(2**16 * 0.95) # nextflow is 65536

    _archetypes: dict[DataInstance, DataInstance] = {}
    def get_archetype(candidates: list[DataInstance]):
        """Pick a stable representative for a set of equivalent instances.

        The closure body is the canonical-name dance: if any candidate
        already has a recorded archetype, reuse it (transitive merge);
        otherwise the first candidate becomes the archetype. The
        `_archetypes` dict is closure state — extraction would force
        it to become a class attribute on NextflowGenContext, adding
        indirection with no behavioral win (A3 decision).
        """
        a = None
        for c in candidates:
            if c not in _archetypes: continue
            a = _archetypes[c]
        if a is None:
            a = candidates[0]
        for c in candidates:
            _archetypes[c] = a
        return a
    # except for given data instances
    # intermediates are "collapsed"
    # we do not know the arity as steps can produce multiples instances,
    # at which point nextflow will branch automatically and flow into gropby junctions, etc.
    def get_io_signature(step: WorkflowStep):
        used_archetypes: list[DataInstance] = []
        for d in step.transform.model.requires:
            insts = step.dependency_map[d]
            archetype = get_archetype(insts)
            used_archetypes.append(archetype)
        produced_archetypes: list[list[DataInstance]] = []
        for dg in step.transform.model.produces:
            g = []
            for d in dg:
                insts = step.dependency_map[d]
                archetype = get_archetype(insts)
                g.append(archetype)
            produced_archetypes.append(g)
        return used_archetypes, produced_archetypes

    def prepare_step(step: WorkflowStep):
        process_name = NextflowProcessName(step.order, step.transform.name)
        src = [f"process {process_name}"+" {"]
        src += [
            TAB+f"label 'x{step.transform.GetKey()}x'",
        ] + [
            TAB+f"label 'x{x}x'"
            for x in step.transform.labels
        ]
        # S3 — emit a publishDir directive into <cache_root>/<key>.tmp/
        # for cacheable miss steps so Nextflow itself stages outputs
        # into the cache staging area as it normally would for
        # publishDir. The post-exec promote step (S5) then validates
        # and renames the .tmp directory into its final cache slot.
        # `cacheable=False` and the env kill-switch skip this entirely.
        decision = cache_decisions.get(step.order)
        if decision is not None and decision.get("cacheable", True):
            cache_tmp = (
                context.cache_root / f"{decision['cache_key'].hex()}.tmp"
            )
            src += [
                TAB + (
                    f"publishDir \"{cache_tmp}\", "
                    f"mode: '{context.cache_hit_strategy}', "
                    "overwrite: true, "
                    "failOnError: true, "
                    "pattern: '*'"
                )
            ]

        def _make_bind_var(i: int, is_assignment=False):
            s = "\\$" if not is_assignment else ""
            return f"{s}b{i+1}"
        raw_external_binds = set()
        for inst in step.uses:
            p = inst.ResolvePath()
            if not p.is_absolute(): continue
            p = path_map.LocalToExternal(p)
            raw_external_binds.add(p.parent)
        external_binds = task._get_common_folders(raw_external_binds)
        external_binds_param = ""
        if len(external_binds)>0:
            external_binds_param = Environment(
                image="",
                runtime=context.runtime,
                container=ContainerDef(binds=[
                    (_make_bind_var(i), _make_bind_var(i))
                    for i, _ in enumerate(external_binds)
                ]),
            ).MakeBindsParam()

        res = step.transform.resources
        src_res = [] # goes to config to not mess with caching
        if res is not None:
            src_res += [x for x in res.AsNextflowFormat(is_config=True)] # config!
        # GPU need is declared on the resources, not on transform.labels, so
        # an author writes it once. The label is the shared channel (the
        # `xlocalx` precedent in slurm.nf); the per-step device count cannot
        # be rendered here because stage time does not know what a device is
        # on the target -- that lands in workflow.config.nf at run time.
        gpu_req = None
        if res is not None and res.wants_gpu:
            src.append(TAB+f"label 'x{GPU_LABEL}x'")
            gpu_req = {
                "step": step.order,
                "transform": str(step.transform.name),
                "process": process_name,
                "gpus": res.gpus.value,
                "gpu_memory_gb": None if res.gpu_memory is None else res.gpu_memory.value_gb,
            }
            gpu_requirements[process_name] = gpu_req
        # Which worlds this step can run in. `arms` is a fact about the
        # transform's source; `envs` is a fact about the resource it names.
        # Both are needed: an arm with no matching field in the declaration
        # has nothing to run, and a declaration with no arm to use it is
        # equally unrunnable. `arms: null` means the source could not be
        # scanned -- unknown, not "declared nothing".
        _scan = step.transform._env_scan
        env_requirements[process_name] = {
            "step": step.order,
            "transform": str(step.transform.name),
            "process": process_name,
            "arms": None if _scan is None else _scan.arms,
            "envs": _read_env_declarations(step),
        }
        duration_is_strict = res is not None and res.duration is not None and res.duration.strict
        memory_is_strict = res is not None and res.memory is not None and res.memory.strict
        if duration_is_strict and memory_is_strict:
            src_res += [
                "errorStrategy 'ignore'" # no point in retrying if not changing resource requests
            ]
        used_archetypes, produced_archetypes = get_io_signature(step)
        dep_in = {
            d.key: [inst.instance_id for inst in step.dependency_map.get(d, [])]
            for d in step.transform.model.requires
        }
        dep_out = [
            {
                d.key: [inst.instance_id for inst in step.dependency_map.get(d, [])]
                for d in dep_group
            }
            for dep_group in step.transform.model.produces
        ]
        structure_arity = {
            d.key: len(step.dependency_map.get(d, []))
            for d in itertools.chain(
                step.transform.model.requires,
                [d for g in step.transform.model.produces for d in g],
            )
        }
        sample_arity = len(step.group_by_instances)
        step_meta_file = f"workflow.step_{step.order}.meta"
        cache_decision = cache_decisions.get(step.order)
        with open(context.work_dir / step_meta_file, "w") as f:
            f.write(f"din {json.dumps(dep_in, separators=(',',':'))}\n")
            f.write(f"dot {json.dumps(dep_out, separators=(',',':'))}\n")
            f.write(f"sar {json.dumps(structure_arity, separators=(',',':'))}\n")
            f.write(f"par {sample_arity}\n")
            # Static per-step GPU declaration, surfaced to the protocol as
            # context.params["gpus"]. Only written when the transform asked
            # for a GPU, so previously staged workspaces (and every non-GPU
            # step) produce byte-identical metadata to before.
            if gpu_req is not None:
                _gpu_meta = {k: gpu_req[k] for k in ("gpus", "gpu_memory_gb")}
                f.write(f"gpu {json.dumps(_gpu_meta, separators=(',',':'))}\n")
            # S3 — cache_key + per-output instance_ids land in the
            # step meta so the post-exec promote step (S5) can locate
            # what to write, and `msm status <key>` (S8) can render
            # per-task provenance. cacheable comes from the
            # TransformInstance (S4 default True); the post-exec
            # promote skips write when False.
            if cache_decision is not None:
                f.write(
                    f"cache_key {cache_decision['cache_key'].hex()}\n"
                )
                out_ids_serialized = {
                    f"{slot}::{branch}": iid
                    for (slot, branch), iid
                    in cache_decision["out_instance_ids"].items()
                }
                f.write(
                    "out_identities "
                    f"{json.dumps(out_ids_serialized, separators=(',',':'))}\n"
                )
                f.write(
                    f"cacheable {'true' if cache_decision['cacheable'] else 'false'}\n"
                )
                f.write(f"transform_key {cache_decision['transform_key']}\n")
                # S4a (Bug E): persist step_name so promote_run can
                # populate InvocationEvent.step_name on the promote
                # route. Cache-hit already populates it from
                # step.transform.name; promote couldn't see that.
                f.write(f"step_name {step.transform.name or ''}\n")
                # C0: persist the compile-time sorted_inputs so the
                # post-exec promote route can populate InvocationEvent.consumes
                # with the same dict shape as the cache-hit route at
                # workflow.py:1419-1422. Without this, promote-side events
                # carry consumes={} and BFS over trace.jsonl has no edges.
                # C0-amend: serialize as [slot_key, [hex,...]]; promote-side
                # parser tolerates the legacy single-string form via split("+").
                sorted_inputs_serialized = [
                    [slot_key, list(ids)]
                    for slot_key, ids in cache_decision["sorted_inputs"]
                ]
                f.write(
                    "sorted_inputs "
                    f"{json.dumps(sorted_inputs_serialized, separators=(',',':'))}\n"
                )
                # C0.5: persist per-output-slot file naming info so
                # the post-exec promote can match output files to slots
                # unambiguously. The canonical filename (bootstrap.py:196,
                # virtual_runtime.py:651/665) is
                #   "{batch+1}-{i+1}-{branch+1}.{_hash}-{dtype.key}{ext}"
                # Critically: the filename embeds the DataInstance's
                # `dtype.key` (DataType hash), NOT the Dependency's `key`
                # (which is a different hash). The slot_id for a produced
                # file is keyed by (dep.key, branch_idx) in out_identities.
                # We persist (dtype_key, ext, branch_idx, slot_id) per
                # produced slot so promote can match by filename and
                # recover the slot_id without re-deriving any hashes.
                slot_files: list[dict] = []
                for branch_idx, dep_group in enumerate(
                    step.transform.model.produces
                ):
                    for dep in dep_group:
                        slot_id = cache_decision[
                            "out_instance_ids"
                        ].get((dep.key, branch_idx), "")
                        insts = step.dependency_map.get(dep, [])
                        if insts:
                            dtype_key = insts[0].dtype.key
                            ext = (
                                insts[0].dtype.GetPreferredFileExtension()
                                or ""
                            )
                        else:
                            dtype_key = dep.key
                            ext = dep.GetPreferredFileExtension() or ""
                        slot_files.append({
                            "dtype_key": dtype_key,
                            "ext": ext,
                            "branch_idx": branch_idx,
                            "slot_id": slot_id,
                        })
                f.write(
                    "slot_files "
                    f"{json.dumps(slot_files, separators=(',',':'))}\n"
                )
                # S3: persist per-batch decomposition so promote can
                # emit one InvocationEvent per batch (= per task) in
                # S4. Single-batch steps still emit one event each;
                # multi-batch steps stop aggregating across siblings
                # (the C2 structural defect from session #268).
                # Shape: [{batch_idx, start, end, sorted_inputs:
                # [[slot_key, [hex,...]], ...]}, ...]
                f.write(
                    "batches "
                    f"{json.dumps(cache_decision.get('batches', []), separators=(',',':'))}\n"
                )
        mock_outputs = [
            f'"1-1-{branch+1}.test$hash-{x.dtype.key}{x.dtype.GetPreferredFileExtension()}"'
            for branch, g in enumerate(produced_archetypes) for x in g
        ]

        if len(produced_archetypes)>1: # if there is branching, outputs must be set to optional
            optional = ", optional: true"
        else:
            optional = ""
        src += [
            "input:",
            TAB+f'tuple '+','.join(['val(index)']+[f'path(_{i+1:02})' for i, x in enumerate(used_archetypes)])
        ] + [
            "output:",
        ] + [
            # TAB+f'tuple val(index),path("{add_prefix(x.path)}")'
            TAB+f'tuple val(index),path("*-{branch+1}.*-{x.dtype.key}{x.dtype.GetPreferredFileExtension()}"){optional}'
            for branch, g in enumerate(produced_archetypes) for x in g
        ] + [
            "script:",
            '"""',
            f'echo "step {step.order}, sample $index"',    # this is used to extract logs in agent.RunWorkflow()
            f'echo "{step.transform.name}"',
            f'echo "res $task.cpus/$task.memory/$task.attempt" >>{METADATA_FILE}',
            # C4 — wrap the channel's per-task lineage MAP in the
            # LinPayload v2 envelope `{"v": 2, "entries": <index_map>}` and
            # serialise the WHOLE envelope through Orchestrator.JsonforEcho
            # so the outer `"v"`/`"entries"` keys are bash-escaped (`\"`)
            # identically to the nested entries. Two prior bugs here:
            #  (1) hand-escaping only the envelope at the Groovy level
            #      collapsed those quotes to bare `"` in the bash
            #      `echo "..."`, producing invalid JSON; and
            #  (2) `entries` was the whole channel value `index` — a
            #      length-1 LIST wrapping the map — but LinPayload.entries
            #      is a dict and Bootstrap (C5) re-wraps it into a list
            #      itself, so the wire must carry `index[0]` (the map).
            # `index[0]` matches the stub's own access pattern below.
            # Bootstrap parses this via `LinPayload.from_json`.
            f'echo "lin ${{Orchestrator.JsonforEcho([v:2, entries:index[0]])}}" >>{METADATA_FILE}',
            f'echo "fmt 2" >>{METADATA_FILE}',
            f'cat ${{params.workspace}}/{step_meta_file} >>{METADATA_FILE}',
            f'echo "inp {",".join(x.dtype.key for x in used_archetypes)}" >>{METADATA_FILE}',
            f'echo "out {";".join(",".join(x.dtype.key for x in g) for g in produced_archetypes)}" >>{METADATA_FILE}',
        # ] + [
        #     f'echo "i{i+1:02} $_{i+1:02}">>{METADATA_FILE}'
        #     for i, x in enumerate(used_archetypes)
        ] + [
            f'{_make_bind_var(i, is_assignment=True)}="{p}"'
            for i, p in enumerate(external_binds)
        ] + [
            f'echo "{external_binds_param}" >{BIND_FILE}',
            f'{context.bootstrap_var}',
            f'bootstrap {context.external_work_var} "{step.order}" ${{params.hostName}}',
            f'[ -e .command.success ] && exit 0 || exit 1', # in case slurm silently kills proc from oom/timeout
            '"""',
            'stub:',
            'def dt = new Random().nextFloat()*params.testSpread',
            'def hash = "${index[0].sort().collectEntries { k, v -> [k, v.sort()] }}".md5()[0..11]', # 12 characters
            f'"""',
            f'sleep $dt',
            f'touch {" ".join(mock_outputs)}',
            f'"""',
            "}",
            ""
        ]
        return process_name, "\n".join(src), src_res

    def ensure_local_folder(n):
        d = context.work_dir/n
        d.mkdir(exist_ok=True)
        return d
    
    the_plan = task.plan
    _given = set(the_plan.given)
    used_given = {x for s in the_plan.steps for x in s.uses if x in _given}
    given_endpoints = {x.dtype for x in used_given}

    # find merges
    inputs_dir = ensure_local_folder("inputs")
    e2producer: dict[Endpoint, list[WorkflowStep]] = {}
    for step in the_plan.steps:
        for pg in step.produces:
            for inst in pg:
                e2producer[inst.dtype] = e2producer.get(inst.dtype, [])+[step]
    final_steps_for_merging: dict[int, set[Endpoint]] = {}
    for e, steps in e2producer.items():
        if e not in given_endpoints and len(steps)<2: continue
        k = max(s.order for s in steps)
        final_steps_for_merging[k] = final_steps_for_merging.get(k, set())|{e}
    output_copies: dict[str, int] = {}
    to_merge_names: dict[Endpoint, list[str]] = {}
    def get_prod_name(x: Endpoint, force_singular=False):
        k = x.key
        arity = len(e2producer.get(x, []))+int(x in given_endpoints)
        if not force_singular and arity>1:
            i = output_copies.get(k, 0)+1
            output_copies[k] = i
            name = f"{k}_{i}"
            _curr = to_merge_names.get(x, [])
            if name not in _curr: _curr.append(name)
            to_merge_names[x] = _curr
        else:
            name = f"{k}"
        return name
    
    # goal:
    # _tK9GI0FH = (o.post([in("inputs/tK9GI0FH")], ["tK9GI0FH"]))[0] // lib::pangenome_heatmap.py
    # _7A15qSzL = (o.post([in("inputs/7A15qSzL")], ["7A15qSzL"]))[0] // env::python_for_data_science.env
    # _urCt2PG9 = (o.post([in("inputs/urCt2PG9")], ["urCt2PG9"]))[0] // sequences::gbk
    # _seen = set()
    unsorted_input_channels: dict[Endpoint, list[DataInstance]] = {}
    _child2parents: dict[Endpoint, set[Endpoint]] = {}
    for inst in the_plan.given:
        e = inst.dtype
        unsorted_input_channels[e] = unsorted_input_channels.get(e, [])+[inst]
        _child2parents[e] = _child2parents.get(e, set())|e.parents # type: ignore
    input_channels: dict[Endpoint, list[DataInstance]] = {}
    # sort givens by lineage
    # parents must be registered (and processed) first!
    while len(_child2parents)>0:
        to_add = []
        for ce, pes in _child2parents.items():
            if len(pes)>0: continue
            to_add.append(ce)
        to_add = sorted(to_add, key=lambda e: unsorted_input_channels[e][0].dtype_name)
        for e in to_add:
            input_channels[e] = unsorted_input_channels[e]
            del _child2parents[e]
        added = set(to_add)
        for e in _child2parents:
            _child2parents[e] = _child2parents[e]-added

    given2order = {}
    for i, x in enumerate(the_plan.given):
        given2order[x] = i
    prepared_given: list[tuple[Path, str, str]] = []
    _seen_paths = set()
    _given_by_prod_name: dict[str, list[DataInstance]] = {}
    _path2prod_name = {}
    # path -> the given DataInstance written at that path. Used to source a
    # parent-ref's canonical instance_id from the SAME given object the
    # parent leaf itself carries on-channel (so a child's parent-ref equals
    # the parent's own task-id). See the given-seed below.
    _path2given_inst: dict[Path, DataInstance] = {}
    for i, (_, lst) in enumerate(input_channels.items()):
        inst = get_archetype(lst)
        p = inputs_dir/f"{get_prod_name(inst.dtype, force_singular=True)}"
        if p in _seen_paths: continue
        _seen_paths.add(p)
        v = get_prod_name(inst.dtype)
        k = p, v, "/".join({i.dtype_name for i in lst})
        prepared_given.append(k)
        with open(p, "w") as f:
            unique_lst = set(lst)
            if len(unique_lst)==1:
                to_write = [inst]
            else:
                to_write = sorted(lst, key=lambda x: given2order[x])
            _given_by_prod_name[v] = to_write
            for i, x in enumerate(to_write):
                _path = x.ResolvePath()
                _path2prod_name[_path] = v
                _path2given_inst[_path] = x
                f.write(f"{_path}\n")
        # Note: the old input_ids/ sidecar (<path>\t<instance_id>) is gone —
        # agents.py now derives path->instance_id straight from the given
        # DataInstances (the record), so there is no second copy to keep in
        # sync. inputs_dir stays path-CSV only for Nextflow's splitCsv.

    # SELF_ID_KEY must match Orchestrator.SELF_ID_KEY: postIn relocates the
    # value at this reserved key to index[<name>] and strips it, so a leaf's
    # on-channel per-file identity is its canonical instance_id (single point
    # of provenance) rather than the legacy Long(md5(path)[:15]).
    SELF_ID_KEY = "__self__"
    _given_lineage = {}
    given_lineage_by_keys = {}
    for prod_name, to_write in _given_by_prod_name.items():
        # Parent-refs per row (instance_id-valued). These drive both the
        # on-channel join and child2parent (classify()); their VALUE changes
        # Long->instance_id (injective, byte-exact joins) but their PRESENCE
        # is gated exactly as before to preserve results.
        _parent_indexes: list[dict[str, list[str]]] = []
        _parent_keys: set[str] = set()
        for x in to_write:
            _pi: dict[str, list[str]] = {}
            for pm in x.parent_lib.parents.get(x.path, []):
                _pp = x.parent_lib.Get(pm.path).ResolvePath()
                _parent_inst = _path2given_inst.get(_pp)
                if _parent_inst is None: continue # spurious parent, not a workflow input
                _prod_name = _path2prod_name[_pp]
                # Parent-ref = the parent given instance's canonical id, which
                # is exactly the parent leaf's own task-id (both read from the
                # same DataInstance).
                _pi[_prod_name] = _pi.get(_prod_name, [])+[_parent_inst.instance_id]
                _parent_keys.add(_prod_name)
            _parent_indexes.append(_pi)
        # Preserve the legacy gate: parent-refs ride the channel (and seed
        # child2parent) only when EVERY row of this input has ≥1 in-workflow
        # parent — otherwise they were dropped pre-refactor, so keep dropping
        # them to hold join behaviour byte-identical.
        _all_have_parents = all(len(pi) > 0 for pi in _parent_indexes)
        _indexes = []
        for x, _pi in zip(to_write, _parent_indexes):
            _row = dict(_pi) if _all_have_parents else {}
            # Always carry the leaf's own canonical instance_id so the merge in
            # in() fires and postIn mints the canonical (not Long) task-id.
            _row[SELF_ID_KEY] = [x.instance_id]
            _indexes.append(_row)
        k = prod_name.split('_')[0] # in case this will be merged and has a "_1" suffix
        _given_lineage[str(inputs_dir.relative_to(context.work_dir)/k)] = _indexes
        # child2parent keys only on real parent prod_names — never SELF_ID_KEY.
        if _all_have_parents and _parent_keys:
            given_lineage_by_keys[k] = given_lineage_by_keys.get(k, set())|_parent_keys
    LINEAGE_FILE = "workflow.lineage_of_given.json"
    _lineage_file_data = {
        "lineage": _given_lineage,
        "child2parent": {k: sorted(v) for k, v in given_lineage_by_keys.items()},
    }
    with open(context.work_dir/LINEAGE_FILE, "w") as f:
        json.dump(_lineage_file_data, f, separators=(',', ':'))

    # goal:
    # k = ['h']
    # h = (o.post([*p1(o.group('f', o.using([f], k)))], k))[0]
    # or this for when batching
    # y = (o.post(o.debatch([*b1(o.batch(o.group('g', o.using([g], k)), 3))]), k))[0]
    # (multi-output processes still use parenthesized destructure, e.g.
    #  (h, y) = o.post([*p1(...)], k) — strict syntax accepts >=2 vars)
    target_endpoints = {x.instance.dtype for x in the_plan.targets}
    src_process = []
    wf_main = []
    wf_publish = set()       
    published_channels: dict[str, tuple[int, DataInstance]] = {}
    resources = {}
    gpu_requirements: dict[str, dict] = {}
    env_requirements: dict[str, dict] = {}

    # NOTE: DSL2 implicitly forks channels even when wrapped in [name, channel]
    # tuples and consumed inside Orchestrator.group(). multiMap forking was
    # added in a94a3d1 but proven unnecessary — see tests:
    #   test_channel_reuse_across_group_calls
    #   test_stream_reuse_works_in_orchestrator
    # Do not re-add multiMap here.

    for step in the_plan.steps:
        decision = cache_decisions.get(step.order)
        is_hit = bool(decision and decision.get("hit"))
        used_archetypes, produced_archetypes = get_io_signature(step)
        produced_names = [get_prod_name(x.dtype) for g in produced_archetypes for x in g]
        produced_snames = [get_prod_name(x.dtype, force_singular=True) for g in produced_archetypes for x in g]
        produced = ", ".join(f"_{x}" for x in produced_names)
        produced_k = [f"'{x}'" for x in produced_snames]
        produced_k = ", ".join(produced_k)
        wf_main.append(f"k = [{produced_k}]")

        # T2: per-produced-slot identity threaded into o.post so the
        # on-channel id becomes md5("<slot_id>::<filename>"), byte-identical
        # to the canonical file_instance_id (LinPayload.mint_file_id).
        # Order matches produced_names: get_io_signature iterates
        # step.transform.model.produces, so (branch_idx, dep) enumeration
        # here is 1:1 with the produced streams/names.
        _out_ids = (decision or {}).get("out_instance_ids", {})
        produced_slot_ids = [
            _out_ids.get((dep.key, branch_idx), "")
            for branch_idx, dep_group in enumerate(step.transform.model.produces)
            for dep in dep_group
        ]
        slot_ids_literal = (
            "[" + ", ".join(f"'{s}'" for s in produced_slot_ids) + "]"
        )

        if is_hit:
            # S3 — synthetic Channel.of for cache hits. Replace the
            # process call with N channels (one per produced dep,
            # ordered by branch then dep) where each channel emits
            # `(index, file)` tuples for the cached files matching
            # the canonical `1-1-{branch+1}.*-{dtype_key}{ext}`
            # filename shape. The tuple re-enters o.post() exactly
            # as a real process output would (Critic E#1 pin).
            cache_out = (
                context.cache_root
                / decision["cache_key"].hex()[:2]
                / decision["cache_key"].hex()[2:]
                / "out"
            )
            cached_channels: list[str] = []
            cached_channel_var = f"__cached_step_{step.order}"
            channel_exprs: list[str] = []
            for branch_idx, dep_group in enumerate(step.transform.model.produces):
                for dep in dep_group:
                    insts = step.dependency_map.get(dep, [])
                    if not insts:
                        channel_exprs.append("Channel.empty()")
                        continue
                    out_inst = insts[0]
                    ext = out_inst.dtype.GetPreferredFileExtension()
                    suffix = f"-{out_inst.dtype.key}{ext}"
                    branch_prefix = f"1-1-{branch_idx + 1}."
                    cached_files = sorted(
                        f for f in (cache_out.glob("*") if cache_out.exists() else [])
                        if f.is_file()
                        and f.name.startswith(branch_prefix)
                        and f.name.endswith(suffix)
                    )
                    if not cached_files:
                        channel_exprs.append("Channel.empty()")
                        continue
                    tuples = ", ".join(
                        f"[[:], file('{fp}')]" for fp in cached_files
                    )
                    channel_exprs.append(f"Channel.of({tuples})")
            wf_main.append(
                f"def {cached_channel_var} = [{', '.join(channel_exprs)}]"
            )
            if len(produced_names) == 1:
                wf_main.append(
                    f"_{produced_names[0]} = "
                    f"(o.post(o.asStreams({cached_channel_var}), k, {slot_ids_literal}))[0]"
                )
            else:
                wf_main.append(
                    f"({produced}) = "
                    f"o.post(o.asStreams({cached_channel_var}), k, {slot_ids_literal})"
                )
            # Final-step merging still applies if the cached step is
            # the producer of a target.
            if step.order in final_steps_for_merging:
                for e in final_steps_for_merging[step.order]:
                    names = to_merge_names[e]
                    to_mix = [f"_{x}" for x in names]
                    name = get_prod_name(e, force_singular=True)
                    wf_main.append(
                        f"_{name} = o.mix([{', '.join(to_mix)}])"
                    )
            if the_plan.publish_intermediates:
                to_pubish = [x for g in produced_archetypes for x in g]
            else:
                to_pubish = [
                    x for g in produced_archetypes for x in g
                    if x.dtype in target_endpoints
                ]
            for inst in to_pubish:
                k = inst.dtype.key
                wf_publish.add(k)
                published_channels[k] = (step.order, inst)
            continue

        process_name, src, src_res = prepare_step(step)
        resources[process_name] = src_res
        src_process.append(src)
        if len(used_archetypes)>0:
            _inst = step.group_by_instances
            _dtypes = {x.dtype.key for x in _inst}
            if len(_dtypes)>1:
                # The group_by requirement bound instances of >1 dtype that
                # were NOT collapsed onto a single canonical dtype upstream
                # (see the merged-endpoints collapse in Generate). Emitting
                # o.group here would key the channel by one dtype while
                # wiring a different one -> runtime NullPointerException in
                # Orchestrator.group. Fail loudly at stage time instead.
                _detail = ", ".join(f"{x.dtype_name}({x.dtype.key})" for x in _inst)
                raise ValueError(
                    f"group_by for [{step.transform.name}] bound multiple "
                    f"un-collapsed dtypes {sorted(_dtypes)} -> cannot emit a "
                    f"valid o.group (would NPE at runtime). instances: [{_detail}]"
                )
            _inst = _inst[0]
            gb = _inst.dtype.key
            using_symbols = ", ".join(f"_{x.dtype.key}" for x in used_archetypes)
            used = f"o.group('{gb}', [{using_symbols}], k, {step.transform.batch_size})"
        else:
            used = ""
        if len(produced_names) == 1:
            # Nextflow 26.04+ strict syntax rejects single-element parenthesized
            # multiple-assignment `(_x) = expr`; use indexed access instead.
            # It also rejects `[*proc(...)]` (spread in list literal), so we
            # route through `o.asStreams(...)` (defined in Orchestrator.groovy,
            # which is loaded via -lib and not subject to strict syntax).
            wf_main.append(
                f"_{produced_names[0]} = (o.post(o.asStreams({process_name}({used})), k, {slot_ids_literal}))[0]"
            )
        else:
            wf_main.append(
                f"({produced}) = o.post(o.asStreams({process_name}({used})), k, {slot_ids_literal})"
            )
        if step.order in final_steps_for_merging:
            for e in final_steps_for_merging[step.order]:
                names = to_merge_names[e]
                to_mix = [f"_{x}" for x in names]
                name = get_prod_name(e, force_singular=True)
                wf_main.append(
                    f"_{name} = o.mix([{', '.join(to_mix)}])"
                )

        if the_plan.publish_intermediates:
            to_pubish = [x for g in produced_archetypes for x in g]
        else:
            to_pubish = [x for g in produced_archetypes for x in g if x.dtype in target_endpoints]
        for inst in to_pubish:
            k = inst.dtype.key
            wf_publish.add(k)
            published_channels[k] = (step.order, inst)

    with open(context.work_dir/context.resources_file, "w") as f:
        _src = [
            "process {"
        ]
        for name, lines in resources.items():
            _src += [
                TAB+f"withName: '{name}' "+"{"
            ] + [
                TAB+TAB+l for l in lines
            ] + [
                TAB+"}"
            ]
        _src.append("}")
        for line in _src:
            f.write(line+"\n")

    # Always (re)written, including empty, so an `update_workflow` re-stage
    # that drops a GPU transform cannot leave a stale requirement behind for
    # the run-time preflight to trip over.
    # One compact line plus a trailing newline, deliberately. RunWorkflow
    # reads this back by `cat`-ing it over the agent shell, and a line
    # reader drops a final line with no newline -- which silently truncated
    # the JSON and turned the whole preflight into a no-op.
    with open(context.work_dir/AgentPaths.GPU_MANIFEST, "w") as f:
        json.dump({"schema": 1, "steps": gpu_requirements}, f, separators=(",", ":"))
        f.write("\n")

    # Same contract as the GPU manifest above, for the same reasons: always
    # (re)written so a re-stage cannot strand a requirement, and one compact
    # line with a trailing newline so `cat`-ing it back over the agent shell
    # cannot silently truncate.
    with open(context.work_dir/AgentPaths.ENV_MANIFEST, "w") as f:
        json.dump({"schema": 1, "steps": env_requirements}, f, separators=(",", ":"))
        f.write("\n")

    wf_output = []
    _e2target = {x.instance.dtype:x for x in the_plan.targets}
    for ch, (step_order, inst) in published_channels.items():
        spec_name = inst.dtype_name.replace(' ', '_').replace("::", "-")
        if inst.dtype in _e2target:
            out_name = _e2target[inst.dtype].name.replace(' ', '_').replace("::", "-")
        else:
            out_name = f"{step_order}_{spec_name}"
        wf_output += [
            TAB+f"_{ch}"+"{",
            TAB+TAB+f"path '{out_name}'",
            TAB+"}",
        ]
        
    content = [
        f"workflow"+" {",
        "main:",
        f'o = new Orchestrator(Channel.fromList([null])) // cant create channels in groovy',
        f'_lf = new groovy.json.JsonSlurper().parseText(file("{LINEAGE_FILE}").text)',
        f'l = _lf.lineage',
        f'o.seedParents(_lf.child2parent)',
    ] + [
        f'_{v} = (o.postIn([in("{p.relative_to(context.work_dir)}", l)], ["{p.name}"]))[0] // {n}'
        for p, v, n in prepared_given # this must be (and is) sorted in lineage order
    ] + [
        line for line in wf_main
    ] + [
        "",
        "publish:",
    ] + [
        f"_{k} = o.publish(_{k})"
        for k in wf_publish
    ] + [
        "}",
        "",
        "output {",
    ] + [
        line for line in wf_output
    ] + [
        "}",
    ]
    
    with open(context.work_dir/context.workflow_file, "w") as f:
        f.write("\n".join([HEADER]+src_process+content))
