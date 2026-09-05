from __future__ import annotations

import itertools
import json
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import yaml

from ...caching.keys import LIN_PAYLOAD_VERSION
from ...caching.layout import default_cache_root
from ...constants import AgentPaths
from ...env import ContainerDef, Environment, Rootfs, Runtime
from ...logging import Log
from ..libraries import DataInstance, GPU_LABEL, ResolveEnvImage
from ..lineage import LinPayload
from ..paths import PathMap
from ..solver import Endpoint
from .grouping import expected_per_key
from .steps import WorkflowStep


METADATA_FILE = ".command.metadata"
BIND_FILE = ".command.binds"
# One JSON line per batch member, written by the task after its protocol ran:
# what the member was keyed on, whether it was promoted, and what it produced.
CACHE_RECORD_FILE = ".command.cache"
# One JSON line per member the orchestrator served from a shard.
CACHE_HITS_LOG = "_metasmith/cache_hits.jsonl"
DEFAULT_CACHE_HELPER = ("python", "-m", "metasmith.caching.invocation")


def AssertBindPathsAreShellSafe(paths: Iterable[Path], step_name: str):
    for p in paths:
        if any(c.isspace() for c in str(p)):
            raise ValueError(
                f"cannot mount external path [{p}] for step [{step_name}]:"
                " bind paths must not contain whitespace"
            )


LIN_ECHO_EXPR = (
    f"${{Orchestrator.JsonforEcho([v:{LIN_PAYLOAD_VERSION}, entries:index])}}"
)

_RESERVED_KEYS_GROOVY = (
    "[" + ", ".join(f"'{k}'" for k in sorted(LinPayload.RESERVED_KEYS)) + "]"
)

def branch_name_pattern(branch_idx: int) -> "re.Pattern[str]":
    return re.compile(rf"^\d+-\d+-{branch_idx + 1}\.")


def NextflowProcessName(order: int, transform_name) -> str:
    name = str(transform_name).replace('/', '_')
    return f"p{order:02}__{name}"


def CachedProcessName(process_name: str) -> str:
    return f"{process_name}_cached"


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
    cache_root: Path | None = None
    cache_hit_strategy: str = "link"
    cache_helper: tuple[str, ...] = DEFAULT_CACHE_HELPER
    rootfs: "Rootfs|None" = None

def _read_env_declarations(step) -> dict[str, dict[str, str]|None]:
    # Keyed by the resource file name; the value maps `container` / `conda` to what that
    # world resolves to, through the same `ResolveEnvImage` execution uses. A resource
    # that cannot be read is `None` -- UNKNOWN, which the preflight must not read as
    # absent, or a workspace staged before the resource landed fails for the wrong reason.
    found: dict[str, dict[str, str]|None] = {}
    for dep in getattr(step.transform, "_env_deps", []):
        for inst in step.dependency_map.get(dep, []):
            try:
                p = inst.ResolvePath()
                name = Path(p).name
                if name in found: continue
                with open(p) as f:
                    content = f.read()
                parsed = yaml.safe_load(content)
            except Exception:
                found[Path(str(getattr(inst, "dtype_name", dep.key))).name] = None
                continue
            if isinstance(parsed, dict):
                found[name] = {
                    key: ResolveEnvImage(content, runtime, p)
                    for key, runtime in (("container", Runtime.APPTAINER), ("conda", Runtime.MAMBA))
                    if parsed.get(key)
                }
            else:
                # A legacy bare-URI (*.oci) resource is a container image and nothing
                # else; asking `ResolveEnvImage` for its conda form returns the same URI.
                found[name] = {"container": ResolveEnvImage(content, Runtime.APPTAINER, p)}
    return found

def apply_fs_strategy(context: NextflowGenContext) -> None:
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

    anchor = context.cache_root
    while not anchor.exists() and anchor != anchor.parent:
        anchor = anchor.parent
    try:
        assert_same_mount(anchor, context.work_dir)
    except StraddleMountError as e:
        Log.Warn(
            "cache_root and work_dir straddle mounts; forcing "
            f"publishDir 'copy' strategy. ({e})"
        )
        context.cache_hit_strategy = "copy"
        return
    context.cache_hit_strategy = detect_strategy(
        anchor, default=context.cache_hit_strategy
    )


_REF_NAMESPACE = "ref::"
_ENV_NAMESPACE = "env::"


def _dtype_names(step, deps) -> set[str]:
    return {
        str(getattr(inst, "dtype_name", d.key))
        for d in deps for inst in step.dependency_map.get(d, [])
    }


def DownloadSteps(plan) -> list:
    """The steps of `plan` that fetch a reference artifact from the internet.

    A step qualifies when everything it produces is a `ref::` type and
    everything it consumes is an `env::` one -- it takes no sample input, so
    there is nothing for it to do but fetch. That is worth saying out loud
    before a run starts: the difference between a workflow that finishes in
    twenty-five minutes and one that spends ninety on databases is whether the
    input set carried them, and nothing else announces it.
    """
    found = []
    for step in plan.steps:
        produces = [d for g in step.transform.model.produces for d in g]
        requires = list(step.transform.model.requires)
        made = _dtype_names(step, produces)
        taken = _dtype_names(step, requires)
        if not made or not taken:
            continue
        if all(n.startswith(_REF_NAMESPACE) for n in made) and \
                all(n.startswith(_ENV_NAMESPACE) for n in taken):
            found.append(step)
    return found


def _dep_type_name(step, dep) -> str:
    """The declared name of a slot's type, for the branch that has no instance.

    get_io_signature indexes dependency_map directly, so a produced slot always
    has one by the time this runs; this is the belt to that brace. A dep that
    carries lineage signs differently from the bare type it was declared from
    and will not be found, which is a missing name rather than an error.
    """
    lib = getattr(step, "transform_library", None)
    if lib is None:
        return ""
    try:
        return lib.GetName(dep)
    except KeyError:
        return ""


def prepare_nextflow(task, context: NextflowGenContext):
    TAB = "\t"
    downloads = DownloadSteps(task.plan)
    if downloads:
        Log.Info(
            f"[{len(downloads)}] step(s) in this plan fetch a reference"
            f" database rather than reading one that was given:"
            f" {', '.join(s.transform.name for s in downloads)}"
        )
    def _strip_var(s: str):
        return s[2:-1]
    if context.cache_root is None:
        context.cache_root = default_cache_root(context.external_home)
    task._apply_fs_strategy(context)
    cache_decisions = task._compute_cache_decisions(context)
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
    HEADER = "\n".join([
        "params.testSpread=1",
        f"{_strip_var(context.external_home_var)} = '{context.external_home}'",
        f'{_strip_var(context.external_work_var)} = "{path_map.Render(context.external_work, dialect="groovy")}"',
    ]+bootstrap)
    MAX_FILE_SIZE = int(2**16 * 0.95)

    _archetypes: dict[DataInstance, DataInstance] = {}
    def get_archetype(candidates: list[DataInstance]):
        a = None
        for c in candidates:
            if c not in _archetypes: continue
            a = _archetypes[c]
        if a is None:
            a = candidates[0]
        for c in candidates:
            _archetypes[c] = a
        return a
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
        AssertBindPathsAreShellSafe(external_binds, str(step.transform.name))
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
        src_res = []
        if res is not None:
            src_res += [x for x in res.AsNextflowFormat(is_config=True)]
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
                "errorStrategy 'ignore'"
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
        slot_channels = {
            d.key: a.dtype.key
            for d, a in zip(step.transform.model.requires, used_archetypes)
        }
        sample_arity = len(step.group_by_instances)
        step_meta_file = f"workflow.step_{step.order}.meta"
        cache_decision = cache_decisions.get(step.order)
        with open(context.work_dir / step_meta_file, "w") as f:
            f.write(f"din {json.dumps(dep_in, separators=(',',':'))}\n")
            f.write(f"dot {json.dumps(dep_out, separators=(',',':'))}\n")
            f.write(f"sar {json.dumps(structure_arity, separators=(',',':'))}\n")
            f.write(f"slk {json.dumps(slot_channels, separators=(',',':'))}\n")
            f.write(f"par {sample_arity}\n")
            if gpu_req is not None:
                _gpu_meta = {k: gpu_req[k] for k in ("gpus", "gpu_memory_gb")}
                f.write(f"gpu {json.dumps(_gpu_meta, separators=(',',':'))}\n")
            if context.rootfs is not None:
                f.write(f"rootfs {context.rootfs.value}\n")
            if cache_decision is not None:
                f.write(f"transform_key {cache_decision['transform_key']}\n")
                f.write(f"signature {cache_decision['signature']}\n")
                f.write(f"step_name {step.transform.name or ''}\n")
                f.write(
                    f"cacheable {'true' if cache_decision['cacheable'] else 'false'}\n"
                )
                f.write(f"session {cache_decision.get('session', 0)}\n")
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
                            dtype_name = insts[0].dtype_name
                            ext = (
                                insts[0].dtype.GetPreferredFileExtension()
                                or ""
                            )
                        else:
                            dtype_key = dep.key
                            dtype_name = _dep_type_name(step, dep)
                            ext = dep.GetPreferredFileExtension() or ""
                        slot_files.append({
                            "dtype_key": dtype_key,
                            # The name is what makes a promoted product
                            # describable as a data instance later. The key is
                            # a property-set fingerprint and cannot be turned
                            # back into a type, so it has to travel separately.
                            "dtype_name": dtype_name,
                            "ext": ext,
                            "branch_idx": branch_idx,
                            "slot_id": slot_id,
                        })
                f.write(
                    "slot_files "
                    f"{json.dumps(slot_files, separators=(',',':'))}\n"
                )
        mock_outputs = [
            f'"1-1-{branch+1}.test$hash-{x.dtype.key}{x.dtype.GetPreferredFileExtension()}"'
            for branch, g in enumerate(produced_archetypes) for x in g
        ]

        if len(produced_archetypes)>1:
            optional = ", optional: true"
        else:
            optional = ""
        src += [
            "input:",
            TAB+f'tuple '+','.join(['val(index)']+[f'path(_{i+1:02})' for i, x in enumerate(used_archetypes)])
        ] + [
            "output:",
        ] + [
            TAB+f'tuple val(index),path("*-{branch+1}.*-{x.dtype.key}{x.dtype.GetPreferredFileExtension()}"){optional}'
            for branch, g in enumerate(produced_archetypes) for x in g
        ] + [
            "script:",
            '"""',
            f'echo "step {step.order}, sample $index"',
            f'echo "{step.transform.name}"',
            f'echo "res $task.cpus/$task.memory/$task.attempt" >>{METADATA_FILE}',
            f'echo "lin {LIN_ECHO_EXPR}" >>{METADATA_FILE}',
            f'echo "fmt 2" >>{METADATA_FILE}',
            f'cat ${{params.workspace}}/{step_meta_file} >>{METADATA_FILE}',
            f'echo "inp {",".join(x.dtype.key for x in used_archetypes)}" >>{METADATA_FILE}',
            f'echo "out {";".join(",".join(x.dtype.key for x in g) for g in produced_archetypes)}" >>{METADATA_FILE}',
            f'__msm_wd="\\$(cd "\\$(dirname "\\$0")" && pwd)"',
            f'[ "\\$__msm_wd" = "\\$PWD" ] || cp -f {METADATA_FILE} "\\$__msm_wd/" || true',
        ] + [
            f'{_make_bind_var(i, is_assignment=True)}="{p}"'
            for i, p in enumerate(external_binds)
        ] + [
            f'echo "{external_binds_param}" >{BIND_FILE}',
            f'{context.bootstrap_var}',
            f'bootstrap {context.external_work_var} "{step.order}" ${{params.hostName}}',
            f'[ "\\$__msm_wd" = "\\$PWD" ] || cp -f {CACHE_RECORD_FILE} "\\$__msm_wd/" 2>/dev/null || true',
            f'[ -e .command.success ] && exit 0 || exit 1',
            '"""',
            'stub:',
            'def dt = new Random().nextFloat()*params.testSpread',
            f'def hash = "${{index[0].findAll {{ k, v -> !({_RESERVED_KEYS_GROOVY}.contains(k)) }}'
            '.sort().collectEntries { k, v -> [k, v.sort()] }}".md5()[0..11]',
            f'"""',
            f'sleep $dt',
            # The stub lane is the only lane the trace tests run in. Without the
            # same metadata the script lane writes, a stub run records no
            # provenance and every shard it promotes is demoted on the next hit.
            f'echo "res 1/1.GB/1" >>{METADATA_FILE}',
            f'echo "lin {LIN_ECHO_EXPR}" >>{METADATA_FILE}',
            f'echo "fmt 2" >>{METADATA_FILE}',
            f'cat ${{params.workspace}}/{step_meta_file} >>{METADATA_FILE}',
            f'echo "inp {",".join(x.dtype.key for x in used_archetypes)}" >>{METADATA_FILE}',
            f'echo "out {";".join(",".join(x.dtype.key for x in g) for g in produced_archetypes)}" >>{METADATA_FILE}',
            f'touch {" ".join(mock_outputs)}',
            f'"""',
            "}",
            ""
        ]
        return process_name, "\n".join(src), src_res

    def prepare_twin(step: WorkflowStep):
        # A hit runs as a task, on the driver's own host, that links the shard's
        # products into its work dir under the member's position. Nextflow then
        # publishes and traces it like any other task, and no scheduler saw it.
        # No `stub:` block: under `-stub` the copy is still the twin's whole job,
        # and a touched stand-in cannot serve a directory product.
        process_name = NextflowProcessName(step.order, step.transform.name)
        twin_name = CachedProcessName(process_name)
        _used, produced_archetypes = get_io_signature(step)
        optional = ", optional: true" if len(produced_archetypes) > 1 else ""
        src = [
            f"process {twin_name}"+" {",
            TAB+"executor 'local'",
            TAB+"cache false",
            "input:",
            TAB+"tuple val(index), val(sources)",
            "output:",
        ] + [
            TAB+f'tuple val(index),path("*-{branch+1}.*-{x.dtype.key}{x.dtype.GetPreferredFileExtension()}"){optional}'
            for branch, g in enumerate(produced_archetypes) for x in g
        ] + [
            "script:",
            '"""',
            f'echo "step {step.order} (cached), sample $index"',
            "${sources.collect { s -> \"ln -f '${s[1]}' '${s[0]}-${s[2]}' 2>/dev/null || cp -r '${s[1]}' '${s[0]}-${s[2]}'\" }.join('\\n')}",
            '"""',
            "}",
            "",
        ]
        return twin_name, "\n".join(src), ["cpus = 1", "memory = '256 MB'"]

    def ensure_local_folder(n):
        d = context.work_dir/n
        d.mkdir(exist_ok=True)
        return d
    
    the_plan = task.plan
    _given = set(the_plan.given)
    used_given = {x for s in the_plan.steps for x in s.uses if x in _given}
    given_endpoints = {x.dtype for x in used_given}

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
    
    unsorted_input_channels: dict[Endpoint, list[DataInstance]] = {}
    _child2parents: dict[Endpoint, set[Endpoint]] = {}
    for inst in the_plan.given:
        e = inst.dtype
        unsorted_input_channels[e] = unsorted_input_channels.get(e, [])+[inst]
        _child2parents[e] = _child2parents.get(e, set())|e.parents # type: ignore
    input_channels: dict[Endpoint, list[DataInstance]] = {}
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

    SELF_ID_KEY = "__self__"
    _given_lineage = {}
    given_lineage_by_keys = {}
    for prod_name, to_write in _given_by_prod_name.items():
        _parent_indexes: list[dict[str, list[str]]] = []
        _parent_keys: set[str] = set()
        for x in to_write:
            _pi: dict[str, list[str]] = {}
            for pm in x.parent_lib.parents.get(x.path, []):
                _pp = x.parent_lib.Get(pm.path).ResolvePath()
                _parent_inst = _path2given_inst.get(_pp)
                if _parent_inst is None: continue
                _prod_name = _path2prod_name[_pp]
                _pi[_prod_name] = _pi.get(_prod_name, [])+[_parent_inst.instance_id]
                _parent_keys.add(_prod_name)
            _parent_indexes.append(_pi)
        _all_have_parents = all(len(pi) > 0 for pi in _parent_indexes)
        if not _all_have_parents and any(len(pi) > 0 for pi in _parent_indexes):
            _orphans = [
                str(x.path) for x, pi in zip(to_write, _parent_indexes) if len(pi) == 0
            ]
            Log.Warn(
                f"[{prod_name}]: {len(_orphans)} of {len(to_write)} rows declare "
                f"no in-workflow parent, so parent lineage is dropped for ALL of "
                f"them -- anything downstream asking which input one of these "
                f"descends from will get no answer. Unparented: "
                f"{', '.join(_orphans[:5])}{' ...' if len(_orphans) > 5 else ''}"
            )
        _indexes = []
        for x, _pi in zip(to_write, _parent_indexes):
            _row = dict(_pi) if _all_have_parents else {}
            _row[SELF_ID_KEY] = [x.instance_id]
            _indexes.append(_row)
        k = prod_name.split('_')[0]
        _given_lineage[str(inputs_dir.relative_to(context.work_dir)/k)] = _indexes
        if _all_have_parents and _parent_keys:
            given_lineage_by_keys[k] = given_lineage_by_keys.get(k, set())|_parent_keys
    LINEAGE_FILE = "workflow.lineage_of_given.json"
    _lineage_file_data = {
        "lineage": _given_lineage,
        "child2parent": {k: sorted(v) for k, v in given_lineage_by_keys.items()},
    }
    with open(context.work_dir/LINEAGE_FILE, "w") as f:
        json.dump(_lineage_file_data, f, separators=(',', ':'))

    target_endpoints = {x.instance.dtype for x in the_plan.targets}
    src_process = []
    wf_main = []
    wf_publish = set()       
    published_channels: dict[str, tuple[int, DataInstance]] = {}
    resources = {}
    gpu_requirements: dict[str, dict] = {}
    env_requirements: dict[str, dict] = {}


    helper_literal = "[" + ", ".join(f"'{t}'" for t in context.cache_helper) + "]"
    cache_root_literal = path_map.Render(context.cache_root, dialect="groovy")
    hits_log_literal = f"{context.external_work_var}/{CACHE_HITS_LOG}"

    for step in the_plan.steps:
        decision = cache_decisions.get(step.order)
        used_archetypes, produced_archetypes = get_io_signature(step)
        produced_names = [get_prod_name(x.dtype) for g in produced_archetypes for x in g]
        produced_snames = [get_prod_name(x.dtype, force_singular=True) for g in produced_archetypes for x in g]
        produced = ", ".join(f"_{x}" for x in produced_names)
        produced_k = [f"'{x}'" for x in produced_snames]
        produced_k = ", ".join(produced_k)
        wf_main.append(f"k = [{produced_k}]")

        _out_ids = (decision or {}).get("out_instance_ids", {})
        produced_slot_ids = [
            _out_ids.get((dep.key, branch_idx), "")
            for branch_idx, dep_group in enumerate(step.transform.model.produces)
            for dep in dep_group
        ]
        slot_ids_literal = (
            "[" + ", ".join(f"'{s}'" for s in produced_slot_ids) + "]"
        )

        process_name, src, src_res = prepare_step(step)
        resources[process_name] = src_res
        src_process.append(src)
        if len(used_archetypes)>0:
            twin_name, twin_src, twin_res = prepare_twin(step)
            resources[twin_name] = twin_res
            src_process.append(twin_src)
            _inst = step.group_by_instances
            _dtypes = {x.dtype.key for x in _inst}
            if len(_dtypes)>1:
                _detail = ", ".join(f"{x.dtype_name}({x.dtype.key})" for x in _inst)
                raise ValueError(
                    f"group_by for [{step.transform.name}] bound multiple "
                    f"un-collapsed dtypes {sorted(_dtypes)} -> cannot emit a "
                    f"valid o.group (would NPE at runtime). instances: [{_detail}]"
                )
            _inst = _inst[0]
            gb = _inst.dtype.key
            using_symbols = ", ".join(f"_{x.dtype.key}" for x in used_archetypes)
            _expected: dict[str, int] = {}
            for dep in step.transform.model.requires:
                dep_insts = step.dependency_map.get(dep, [])
                if not dep_insts:
                    continue
                sname = dep_insts[0].dtype.key
                if sname == gb:
                    continue
                n = expected_per_key(list(dep_insts), list(step.group_by_instances))
                if n is not None and n > 0:
                    _expected[sname] = n
            expected_literal = (
                "["
                + (
                    ", ".join(f"'{k}': {v}" for k, v in sorted(_expected.items()))
                    if _expected
                    else ":"
                )
                + "]"
            )
            slk_literal = "[" + ", ".join(f"'{x.dtype.key}'" for x in used_archetypes) + "]"
            cacheable = bool(decision and decision.get("cacheable"))
            cache_literal = (
                f"[tk: '{(decision or {}).get('transform_key', '')}', "
                f"sig: '{(decision or {}).get('signature', '')}', "
                f"slk: {slk_literal}, "
                f"cache_root: \"{cache_root_literal}\", "
                f"cacheable: {'true' if cacheable else 'false'}, "
                f"helper: {helper_literal}, "
                f"hits_log: \"{hits_log_literal}\", "
                f"step: {step.order}, step_name: '{step.transform.name}']"
            )
            miss_var = f"__miss_{step.order}"
            hit_var = f"__hit_{step.order}"
            out_var = f"__out_{step.order}"
            wf_main.append(
                f"({miss_var}, {hit_var}) = o.group('{gb}', [{using_symbols}], k, "
                f"{step.transform.batch_size}, {expected_literal}, {cache_literal})"
            )
            wf_main.append(
                f"{out_var} = o.mixOuts(o.asStreams({process_name}({miss_var})), "
                f"o.asStreams({twin_name}({hit_var})))"
            )
            streams_expr = out_var
        else:
            streams_expr = f"o.asStreams({process_name}())"
        if len(produced_names) == 1:
            wf_main.append(
                f"_{produced_names[0]} = (o.post({streams_expr}, k, {slot_ids_literal}))[0]"
            )
        else:
            wf_main.append(
                f"({produced}) = o.post({streams_expr}, k, {slot_ids_literal})"
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

    with open(context.work_dir/AgentPaths.GPU_MANIFEST, "w") as f:
        json.dump({"schema": 1, "steps": gpu_requirements}, f, separators=(",", ":"))
        f.write("\n")

    with open(context.work_dir/AgentPaths.ENV_MANIFEST, "w") as f:
        json.dump(
            {
                "schema": AgentPaths.ENV_MANIFEST_SCHEMA,
                # The per-task rootfs override, recorded beside the images
                # because whoever materialises them ahead of a run has to
                # produce the artifact the steps will actually look for. It is
                # otherwise reachable only from per-step meta, which the
                # launching host would have to parse a step at a time. `null`
                # means none was declared, so the agent's own tendency stands.
                "rootfs": None if context.rootfs is None else context.rootfs.value,
                "steps": env_requirements,
            },
            f, separators=(",", ":"),
        )
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
        for p, v, n in prepared_given
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
