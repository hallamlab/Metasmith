"""Transform isolation harness for testing without Nextflow.

Replicates what Nextflow + bootstrap does for a single step, without
requiring Nextflow, relay, or containers. Useful for testing transform
protocols in isolation.
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from ..coms.containers import ContainerRuntime
from ..coms.terminals import ShellResult
from ..hashing import KeyGenerator
from ..logging import Log
from ..models.libraries import (
    ContextData,
    ContextPath,
    ExecutionContext,
    ExecutionResult,
    DataInstance,
)
from ..models.solver import Dependency, Endpoint
from ..models.workflow import WorkflowStep, WorkflowTask, METADATA_FILE


class MockShell:
    """Mock RemoteShell that satisfies the interface without actual shell execution.

    Sufficient for transforms that don't call context.ExecWithContainer().
    """

    def __init__(self):
        self._out_callbacks: list[Callable[[str], None]] = []
        self._err_callbacks: list[Callable[[str], None]] = []

    def RegisterOnOut(self, callback: Callable[[str], None]):
        self._out_callbacks.append(callback)

    def RegisterOnErr(self, callback: Callable[[str], None]):
        self._err_callbacks.append(callback)

    def RemoveOnOut(self, callback: Callable[[str], None]):
        if callback in self._out_callbacks:
            self._out_callbacks.remove(callback)

    def RemoveOnErr(self, callback: Callable[[str], None]):
        if callback in self._err_callbacks:
            self._err_callbacks.remove(callback)

    def Exec(self, cmd: str, timeout=None, history: bool = False) -> ShellResult:
        return ShellResult(out=[], err=[])

    def ExecAsync(self, cmd: str) -> str:
        return "mock_key"

    def AwaitDone(self, timeout=None, _key=None):
        pass

    def Dispose(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


@dataclass
class TransformHarness:
    """Runs a single workflow step's transform protocol in isolation.

    Replicates the bootstrap logic from StageAndRunTransform without
    requiring Nextflow, relay, or containers.

    Args:
        task: The WorkflowTask containing the step to run.
        step_index: 1-based index of the step to execute.
        work_dir: Working directory for the step. Uses a temp dir if None.
    """

    task: WorkflowTask
    step_index: int
    work_dir: Path | None = None
    _original_cwd: Path = field(default_factory=Path.cwd, init=False, repr=False)

    @property
    def step(self) -> WorkflowStep:
        return self.task.plan.steps[self.step_index - 1]

    def _ensure_work_dir(self) -> Path:
        if self.work_dir is None:
            raise ValueError("work_dir must be set before running")
        self.work_dir.mkdir(parents=True, exist_ok=True)
        return self.work_dir

    def write_metadata(self) -> Path:
        """Write .command.metadata matching NXF process script format.

        Format matches PrepareNextflow lines 729-735 of workflow.py:
            res <cpus>/<memory>/<attempt>
            lin <json of index with FILES entry>
            inp <comma-separated input keys>
            out <semicolon-separated groups of comma-separated output keys>

        Returns:
            Path to the metadata file.
        """
        work_dir = self._ensure_work_dir()
        step = self.step

        # Build used/produced archetypes (matching get_io_signature logic)
        used_archetypes = []
        for d in step.transform.model.requires:
            insts = step.dependency_map[d]
            used_archetypes.append(insts[0])

        produced_archetypes = []
        for dg in step.transform.model.produces:
            g = []
            for d in dg:
                insts = step.dependency_map[d]
                g.append(insts[0])
            produced_archetypes.append(g)

        # Build lineage index
        lineages = self._build_lineages(used_archetypes)

        inp_keys = ",".join(x.dtype.key for x in used_archetypes)
        out_keys = ";".join(
            ",".join(x.dtype.key for x in g) for g in produced_archetypes
        )
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
            for dep in (
                list(step.transform.model.requires)
                + [d for g in step.transform.model.produces for d in g]
            )
        }
        sample_arity = len(step.group_by_instances)

        meta_path = work_dir / METADATA_FILE
        with open(meta_path, "w") as f:
            f.write(f"res 1/1.GB/1\n")
            f.write(f"lin {json.dumps(lineages)}\n")
            f.write(f"fmt 2\n")
            f.write(f"din {json.dumps(dep_in, separators=(',', ':'))}\n")
            f.write(f"dot {json.dumps(dep_out, separators=(',', ':'))}\n")
            f.write(f"sar {json.dumps(structure_arity, separators=(',', ':'))}\n")
            f.write(f"par {sample_arity}\n")
            f.write(f"inp {inp_keys}\n")
            f.write(f"out {out_keys}\n")

        return meta_path

    def _build_lineages(self, used_archetypes: list[DataInstance]) -> list[dict]:
        """Build lineage index matching what Orchestrator.groovy would produce."""
        step = self.step
        batch_size = step.transform.batch_size

        # Get the group_by instances
        group_by_insts = step.dependency_map[step.transform.group_by]

        # For each batch (or single item), build an index
        lineages = []
        batch_count = max(1, len(group_by_insts) // max(1, batch_size))
        for batch_idx in range(batch_count):
            index = {}
            # Add key for each input endpoint
            file_groups = []
            for inst in used_archetypes:
                e = inst.dtype
                dep_insts = [x for x in step.uses if x.dtype.key == e.key]
                # Collect file paths for this input
                start = batch_idx * batch_size
                end = start + batch_size
                batch_insts = dep_insts[start:end] if batch_size > 1 else dep_insts[batch_idx:batch_idx + 1]
                if not batch_insts:
                    batch_insts = dep_insts  # fallback: use all

                files = []
                for di in batch_insts:
                    p = di.ResolvePath()
                    files.append(str(p))
                    # Add hash to index
                    from hashlib import md5
                    h = md5(str(p).encode()).hexdigest()
                    h_val = int(h[:15], 16)
                    index[e.key] = index.get(e.key, []) + [h_val]
                file_groups.append(files)

            index["FILES"] = file_groups
            lineages.append(index)

        return lineages

    def setup_inputs(self) -> dict[Dependency, list[Path]]:
        """Symlink input files from data libraries into work_dir.

        Returns:
            Dict mapping dependencies to lists of symlinked paths in work_dir.
        """
        work_dir = self._ensure_work_dir()
        step = self.step
        result: dict[Dependency, list[Path]] = {}

        for dep in step.transform.model.requires:
            insts = step.dependency_map[dep]
            paths = []
            for inst in insts:
                src = inst.ResolvePath()
                if not src.exists():
                    Log.Warn(f"input file not found: {src}")
                    paths.append(src)
                    continue

                dest = work_dir / src.name
                # Handle name collisions
                if dest.exists():
                    dest = work_dir / f"{inst.dtype.key}_{src.name}"
                if not dest.exists():
                    dest.symlink_to(src)
                paths.append(dest)
            result[dep] = paths

        return result

    def build_context(self) -> ExecutionContext:
        """Construct ExecutionContext matching bootstrap logic.

        Returns:
            ExecutionContext ready for transform protocol execution.
        """
        work_dir = self._ensure_work_dir()
        step = self.step

        # Build inputs
        lineages = self._build_lineages(
            [step.dependency_map[d][0] for d in step.transform.model.requires]
        )

        # Build input map
        input_map: dict[Endpoint, list[DataInstance]] = {}
        for d in step.transform.model.requires:
            insts = step.dependency_map[d]
            input_map[insts[0].dtype] = insts

        input2dep: dict[Endpoint, Dependency] = {}
        for e, d in zip(input_map, step.transform.model.requires):
            input2dep[e] = d

        # Build dep2output
        dep2output: list[dict[Dependency, Endpoint]] = []
        for dep_group in step.transform.model.produces:
            dgroup = {}
            inst_group = [e for d in dep_group for e in step.dependency_map[d]]
            for dep in dep_group:
                insts = [x for x in inst_group if x in step.dependency_map[dep]]
                if insts:
                    dgroup[dep] = insts[0].dtype
            dep2output.append(dgroup)

        # Build context inputs. The harness has no real host/container
        # distinction, so all three views collapse to the same absolute
        # path. ContextPath enforces absoluteness; the harness anchors
        # relative inputs against the working directory.
        def _absolutize(p: str | Path) -> Path:
            q = Path(p)
            return q if q.is_absolute() else (work_dir / q).resolve()

        inputs: list[dict[Dependency, ContextData]] = []
        for batch_lineage in lineages:
            g: dict[Dependency, ContextData] = {}
            file_groups = batch_lineage["FILES"]
            for (e, dep_insts), file_names in zip(input_map.items(), file_groups):
                input_group = []
                for p in file_names:
                    abs_p = _absolutize(p)
                    input_group.append(ContextPath(local=abs_p, external=abs_p, container=abs_p))
                g[input2dep[e]] = ContextData(
                    input_group=input_group,
                    endpoint=e,
                    type_name=dep_insts[0].dtype_name,
                )
            inputs.append(g)

        # Output path generator
        _hashes: dict[int, str] = {}

        def _get_output_paths(key: Dependency, i: int, batch: int) -> ContextPath:
            found = False
            branch = 0
            dtype = None
            for b, d2e in enumerate(dep2output):
                if key in d2e:
                    dtype = d2e[key]
                    branch = b
                    found = True
                    break
            assert found, f"[{key}] not found in [{dep2output}]"

            if batch not in _hashes:
                lin = lineages[batch]
                slin = {k: sorted(lin[k]) for k in sorted(lin.keys())}
                _, _hash = KeyGenerator.FromStr(json.dumps(slin), l=8)
                _hashes[batch] = _hash
            _hash = _hashes[batch]
            ext = dtype.GetPreferredFileExtension()
            dest = (work_dir / f"{batch + 1}-{i + 1}-{branch + 1}.{_hash}-{dtype.key}{ext}").resolve()
            return ContextPath(local=dest, external=dest, container=dest)

        mock_shell = MockShell()

        return ExecutionContext(
            _inputs=inputs,
            _get_output_paths=_get_output_paths,
            external_shell=mock_shell,
            external_cwd=work_dir,
            external_agent_home=work_dir,
            container_runtime=ContainerRuntime.DOCKER,
            params={"cpus": 1, "memory": 1, "attempt": 1},
        )

    def run(self) -> ExecutionResult:
        """Execute the transform protocol in isolation.

        Sets up inputs, builds context, changes to work_dir, runs protocol,
        and restores the original cwd.

        Returns:
            ExecutionResult from the transform protocol.
        """
        work_dir = self._ensure_work_dir()
        self._original_cwd = Path.cwd()

        self.write_metadata()
        self.setup_inputs()
        context = self.build_context()

        os.chdir(work_dir)
        try:
            results = self.step.transform.protocol(context)
            if not isinstance(results, list):
                results = [results]
            success = any(r.success for r in results)
            # Merge manifests
            merged_manifest = []
            for r in results:
                merged_manifest.extend(r.manifest)
            return ExecutionResult(success=success, manifest=merged_manifest)
        except Exception as e:
            Log.Error(f"transform failed: {e}")
            return ExecutionResult(success=False)
        finally:
            os.chdir(self._original_cwd)

    @property
    def metadata_path(self) -> Path:
        """Path to the .command.metadata file in work_dir."""
        assert self.work_dir is not None
        return self.work_dir / METADATA_FILE
