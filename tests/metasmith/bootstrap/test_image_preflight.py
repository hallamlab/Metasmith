# Filling the image store before a run, and saying so before a launch.
#
# Tool images materialise lazily -- inside the first task that needs each one.
# On a cluster whose compute nodes have no route to a registry that is exactly
# where it fails. A setup run's pull steps fetch on the driver's host through
# `ExecutionContext.MaterialiseImage`, the call a task makes; the launch path only
# reports, from the stage-time env manifest. Nothing here opens a connection: the
# shell is a stub.
#
# The same two halves for an agent that runs no containers at all: a mamba agent
# needs the conda envs its steps name, and the library ships one recipe per tool
# to build them from. What it cannot build it reports -- an env with no recipe, a
# step with no `conda:` entry at all (named with the container it does have), a
# host with no conda frontend -- because each of those is a fact about the agent
# the person pressing the button has to act on, not an error in the workflow.

from pathlib import Path

import pytest

from metasmith.agents.conda import (
    NO_RECIPE,
    CondaEnvError,
    _conda_frontend,
    _create_conda_envs,
    _find_recipes,
    _manifest_envs,
)
from metasmith.agents.images import (
    _check_image_store,
    _manifest_images,
    _tool_environment_for,
)
from metasmith.env import Environment, Rootfs, Runtime
from metasmith.models.libraries import ExecutionContext


HOME = Path("/msm_home")
KRAKEN = "docker://quay.io/biocontainers/kraken2:2.1.3--h43eeafb_0"
SYLPH = "docker://quay.io/biocontainers/sylph:0.6.1--h919a2d8_0"
METAPHLAN = "docker://quay.io/biocontainers/metaphlan:4.1.1--pyhca03a8a_0"


def _step(process, transform, envs):
    return {
        "step": 1, "transform": transform, "process": process,
        "runs": 1, "envs": envs,
    }


def _doc(steps, rootfs=None, schema=2):
    return {"schema": schema, "rootfs": rootfs, "steps": steps}


def _apptainer() -> Environment:
    return Environment(image="msm", runtime=Runtime.APPTAINER)


def _mamba() -> Environment:
    return Environment(image="msm", runtime=Runtime.MAMBA)


class _StubShell:
    def __init__(self, present=(), fails=()):
        self.present = set(present)
        self.fails = set(fails)
        self.commands: list[str] = []

    def Exec(self, cmd, history=False, quiet=False, timeout=None, idle_timeout=None, what=None):
        import re
        from metasmith.coms.terminals import ShellResult
        self.commands.append(cmd)
        # The success token is read out of the command rather than restated
        # here, so the stub cannot drift from the flag the caller looks for.
        m = re.search(r'echo "([^"]+)"', cmd)
        ready = ShellResult(out=[m.group(1)], err=[]) if m else ShellResult(out=[], err=[])
        # Which image a command is about is decided by the store path in it,
        # derived from the image URI the same way the run command derives it --
        # so a stub cannot agree with a spelling the runtime does not use.
        for image in (KRAKEN, SYLPH, METAPHLAN):
            stem = Environment(image=image, runtime=Runtime.APPTAINER)._cached_name()
            if stem not in cmd: continue
            fetching = "apptainer pull" in cmd or "apptainer build" in cmd
            if fetching and image in self.fails:
                return ShellResult(out=[], err=["materialise failed"])
            if image in self.present or fetching:
                self.present.add(image)
                return ready
            return ShellResult(out=[], err=[])
        return ShellResult(out=[], err=[])

    def materialise_commands(self) -> list[str]:
        return [c for c in self.commands if "apptainer pull" in c or "apptainer build" in c]


class TestWhichImages:
    def test_distinct_images_across_steps(self):
        steps = {
            f"P{i}": _step(f"P{i}", "t", {"a.env": {"container": img}})
            for i, img in enumerate([KRAKEN, SYLPH, KRAKEN, METAPHLAN, SYLPH, KRAKEN, KRAKEN, SYLPH])
        }
        images, unknown = _manifest_images(_doc(steps))
        assert sorted(images) == sorted({KRAKEN, SYLPH, METAPHLAN})
        assert unknown == []

    def test_a_resource_recorded_as_unknown_is_reported_not_ignored(self):
        # `null` means the resource could not be read at stage time. Treating it
        # as "no image needed" would let the pre-flight claim a complete store.
        steps = {"P1": _step("P1", "mystery", {"x.env": None})}
        images, unknown = _manifest_images(_doc(steps))
        assert images == []
        assert unknown == ["mystery"]

    def test_a_schema_1_manifest_is_unknown_rather_than_empty(self):
        # The previous generation recorded only which keys were present, so it
        # cannot answer this question at all -- and must say so rather than
        # report an empty image set, which reads as "nothing to fetch".
        steps = {"P1": _step("P1", "gtdbtk", {"g.env": ["container"]})}
        images, unknown = _manifest_images(_doc(steps, schema=1))
        assert images == []
        assert unknown == ["gtdbtk"]

    def test_a_conda_only_resource_contributes_no_image(self):
        steps = {"P1": _step("P1", "diamond", {"d.env": {"conda": "diamond-2.1"}})}
        assert _manifest_images(_doc(steps)) == ([], [])

    def test_no_manifest_is_nothing_to_do(self):
        assert _manifest_images({}) == ([], [])


class TestArtifactChoice:
    def test_the_staged_rootfs_override_is_honoured(self):
        # The pre-flight must produce the artifact the steps will look for.
        #
        # A workspace staged `rootfs=sandbox` whose store was filled with SIFs is
        # a pre-flight that did nothing: the first task finds no sandbox and
        # unpacks one itself, on the node that cannot reach a registry.
        env = _tool_environment_for(KRAKEN, _apptainer(), HOME, rootfs="sandbox")
        assert env.rootfs is Rootfs.SANDBOX

    def test_the_agents_own_tendency_stands_when_none_was_staged(self):
        agent_env = _apptainer()
        agent_env.rootfs = Rootfs.SIF
        env = _tool_environment_for(KRAKEN, agent_env, HOME, rootfs=None)
        assert env.rootfs is Rootfs.SIF

    def test_the_image_lands_in_the_agent_home_store(self):
        env = _tool_environment_for(KRAKEN, _apptainer(), HOME, rootfs=None)
        assert str(env.GetLocalPath()).startswith("${APPTAINER_CACHEDIR:-/msm_home/container_images}")


def _materialise(sh, image, agent_env) -> bool:
    ctx = ExecutionContext(
        _inputs=[{}], _get_output_paths=lambda *a: None, external_shell=sh,
        external_cwd=HOME, external_agent_home=HOME, _environment=agent_env,
    )
    return ctx.MaterialiseImage(_tool_environment_for(image, agent_env, HOME, rootfs=None))


class TestMaterialise:
    def test_a_missing_image_is_fetched_with_the_execution_paths_command(self):
        # A setup pull and a task cannot disagree about what a materialised image
        # is -- including the mount test and its stamp -- and the pull takes the
        # task's lock, so one racing a task that started early does not fetch twice.
        sh = _StubShell()
        assert _materialise(sh, KRAKEN, _apptainer())
        env = _tool_environment_for(KRAKEN, _apptainer(), HOME, rootfs=None)
        issued, = sh.materialise_commands()
        assert env.MakeMaterialiseCommand() in issued.replace("'\\''", "'")
        assert "flock" in issued

    def test_a_present_image_does_no_work(self):
        sh = _StubShell(present=[KRAKEN])
        assert _materialise(sh, KRAKEN, _apptainer())
        assert sh.materialise_commands() == []

    def test_an_image_that_cannot_be_materialised_says_so(self):
        assert not _materialise(_StubShell(fails=[SYLPH]), SYLPH, _apptainer())

    def test_a_runtime_with_no_image_store_has_nothing_to_fetch(self):
        sh = _StubShell()
        assert not _materialise(sh, KRAKEN, _mamba())
        assert sh.commands == []


class TestLaunchReport:
    def test_it_names_what_is_missing_without_fetching(self):
        sh = _StubShell(present=[KRAKEN])
        missing = _check_image_store(sh, [KRAKEN, SYLPH], _apptainer(), HOME, rootfs=None)
        assert missing == [SYLPH]
        assert sh.materialise_commands() == [], "the launch path fetched an image"

    def test_a_full_store_reports_nothing(self):
        sh = _StubShell(present=[KRAKEN, SYLPH])
        assert _check_image_store(sh, [KRAKEN, SYLPH], _apptainer(), HOME, rootfs=None) == []

    def test_no_images_to_check_is_not_a_failure(self):
        sh = _StubShell()
        assert _check_image_store(sh, [], _apptainer(), HOME, rootfs=None) == []
        assert sh.commands == []


BLAST = "blast-2.16"
DIAMOND = "diamond-2.1"


def _conda_step(process, transform, envs):
    return {
        "step": 1, "transform": transform, "process": process,
        "runs": 1, "envs": envs,
    }


class _CondaShell:
    # A host with a conda frontend and some envs already on it. The env that
    # answers `run -n <name> true` is the env the wrapper every task is launched
    # under would find, which is what the pre-flight consults.
    def __init__(self, present=(), frontend="mamba", fails=()):
        self.present = set(present)
        self.frontend = frontend
        self.fails = set(fails)
        self.commands: list[str] = []
        self.written: dict[str, str] = {}

    def Exec(self, cmd, history=False, quiet=False, timeout=None, idle_timeout=None, what=None):
        import base64
        from metasmith.coms.terminals import ShellResult
        self.commands.append(cmd)
        if "command -v" in cmd:
            if self.frontend is None: return ShellResult(out=[], err=[])
            return ShellResult(out=[f"MSM_FRONTEND={self.frontend}"], err=[])
        if " run -n " in cmd:
            name = cmd.split(" run -n ")[1].split()[0]
            return ShellResult(out=["env-ready"] if name in self.present else [], err=[])
        if "base64 -d" in cmd:
            blob = cmd.split('echo "')[1].split('"')[0]
            path = cmd.rsplit("> ", 1)[1].strip('"')
            self.written[path] = base64.b64decode(blob).decode()
            return ShellResult(out=[], err=[])
        if "env remove" in cmd:
            self.present.discard(cmd.split("-n ")[1].split()[0])
            return ShellResult(out=[], err=[])
        if "env create" in cmd:
            name = cmd.split("-n ")[1].split()[0]
            if name not in self.fails: self.present.add(name)
            return ShellResult(out=[], err=[])
        return ShellResult(out=[], err=[])

    def created(self) -> list[str]:
        return [c.split("-n ")[1].split()[0] for c in self.commands if "env create" in c]


class TestWhichEnvs:
    def test_distinct_envs_across_steps(self):
        steps = {
            f"P{i}": _conda_step(f"P{i}", "t", {"a.env": {"container": KRAKEN, "conda": env}})
            for i, env in enumerate([BLAST, DIAMOND, BLAST, DIAMOND])
        }
        envs, without, unknown = _manifest_envs(_doc(steps))
        assert envs == sorted({BLAST, DIAMOND})
        assert (without, unknown) == ([], [])

    def test_a_step_with_no_conda_entry_is_named_with_what_it_does_have(self):
        # The report this exists for: the workflow is runnable, just not here.
        steps = {"P1": _conda_step("P1", "gtdbtk", {"g.env": {"container": KRAKEN}})}
        envs, without, _ = _manifest_envs(_doc(steps))
        assert envs == []
        assert without == [{
            "transform": "gtdbtk", "step": 1, "resource": "g.env", "container": KRAKEN,
        }]

    def test_a_resource_recorded_as_unknown_is_reported_not_ignored(self):
        steps = {"P1": _conda_step("P1", "mystery", {"x.env": None})}
        assert _manifest_envs(_doc(steps)) == ([], [], ["mystery"])

    def test_no_manifest_is_nothing_to_do(self):
        assert _manifest_envs({}) == ([], [], [])


class TestRecipes:
    def test_the_workflows_own_library_is_read_before_the_installed_one(self, tmp_path):
        near, far = tmp_path/"near", tmp_path/"far"
        for root, text in ((near, "name: blast # near"), (far, "name: blast # far")):
            (root/"envs"/"tools").mkdir(parents=True)
            (root/"envs"/"tools"/f"{BLAST}.yml").write_text(text)
        found = _find_recipes([BLAST], [near/"envs"/"tools", far/"envs"/"tools"])
        assert found[BLAST].endswith("near")

    def test_an_env_with_no_recipe_reads_as_absent_rather_than_missing_file(self, tmp_path):
        (tmp_path/"envs"/"tools").mkdir(parents=True)
        assert _find_recipes([BLAST], [tmp_path/"envs"/"tools"]) == {BLAST: None}


class TestCreateEnvs:
    def test_a_missing_env_is_built_from_its_recipe(self):
        sh = _CondaShell()
        report = _create_conda_envs(sh, {BLAST: "name: blast"}, "mamba", HOME)
        assert report == [{"env": BLAST, "ok": True, "skipped": False, "reason": None}]
        assert sh.created() == [BLAST]
        # the recipe is pushed as it stands, not rewritten from the env name
        assert list(sh.written.values()) == ["name: blast"]

    def test_an_env_that_is_already_there_is_left_alone(self):
        sh = _CondaShell(present=[BLAST])
        report = _create_conda_envs(sh, {BLAST: "name: blast"}, "mamba", HOME)
        assert report[0]["skipped"] is True
        assert sh.created() == []

    def test_force_rebuilds_what_is_already_there(self):
        sh = _CondaShell(present=[BLAST])
        _create_conda_envs(sh, {BLAST: "name: blast"}, "mamba", HOME, force=True)
        # removed first: `env create` refuses a name that already exists
        assert any("env remove" in c for c in sh.commands)
        assert sh.created() == [BLAST]

    def test_an_env_with_no_recipe_is_reported_and_does_not_stop_the_rest(self):
        sh = _CondaShell()
        report = _create_conda_envs(
            sh, {BLAST: None, DIAMOND: "name: diamond"}, "mamba", HOME,
        )
        assert [r["reason"] for r in report] == [NO_RECIPE, None]
        assert sh.created() == [DIAMOND]

    def test_an_env_that_will_not_build_fails_the_setup_after_trying_the_others(self):
        sh = _CondaShell(fails=[BLAST])
        with pytest.raises(CondaEnvError) as e:
            _create_conda_envs(sh, {BLAST: "x", DIAMOND: "y"}, "mamba", HOME)
        assert BLAST in str(e.value) and DIAMOND not in str(e.value)
        assert sh.created() == [BLAST, DIAMOND]

    def test_nothing_to_build_is_not_a_failure(self):
        sh = _CondaShell()
        assert _create_conda_envs(sh, {}, "mamba", HOME) == []


class TestFrontend:
    def test_it_reports_which_one_the_host_has(self):
        assert _conda_frontend(_CondaShell(frontend="conda")) == "conda"

    def test_a_host_with_neither_answers_none_rather_than_failing_a_create(self):
        # The answer the caller reports, rather than an opaque `env create` error
        # for every env in the workflow.
        assert _conda_frontend(_CondaShell(frontend=None)) is None
