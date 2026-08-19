from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.constants import AgentPaths
from metasmith.models.paths import ContextPath, PathMap


class TestContextPathInvariants:
    def test_accepts_absolute_clean_paths(self) -> None:
        cp = ContextPath(
            local=Path("/msm_home/runs/K/out.fa"),
            external=Path("/scratch/agent/runs/K/out.fa"),
            container=Path("/msm_home/runs/K/out.fa"),
        )
        assert cp.local == Path("/msm_home/runs/K/out.fa")

    def test_rejects_relative_local(self) -> None:
        with pytest.raises(ValueError, match="local must be absolute"):
            ContextPath(
                local=Path("relative/out.fa"),
                external=Path("/scratch/agent/out.fa"),
                container=Path("/ws/out.fa"),
            )

    def test_rejects_relative_external(self) -> None:
        with pytest.raises(ValueError, match="external must be absolute"):
            ContextPath(
                local=Path("/msm_home/out.fa"),
                external=Path("relative/out.fa"),
                container=Path("/ws/out.fa"),
            )

    def test_rejects_relative_container(self) -> None:
        with pytest.raises(ValueError, match="container must be absolute"):
            ContextPath(
                local=Path("/msm_home/out.fa"),
                external=Path("/scratch/agent/out.fa"),
                container=Path("relative/out.fa"),
            )

    def test_rejects_dotdot_in_local(self) -> None:
        with pytest.raises(ValueError, match=r"local must not contain '\.\.'"):
            ContextPath(
                local=Path("/msm_home/../ws/out.fa"),
                external=Path("/scratch/agent/out.fa"),
                container=Path("/ws/out.fa"),
            )

    def test_rejects_dotdot_in_external(self) -> None:
        with pytest.raises(ValueError, match=r"external must not contain '\.\.'"):
            ContextPath(
                local=Path("/msm_home/out.fa"),
                external=Path("/scratch/agent/../ws/out.fa"),
                container=Path("/ws/out.fa"),
            )

    def test_rejects_dotdot_in_container(self) -> None:
        with pytest.raises(ValueError, match=r"container must not contain '\.\.'"):
            ContextPath(
                local=Path("/msm_home/out.fa"),
                external=Path("/scratch/agent/out.fa"),
                container=Path("/ws/../foo/out.fa"),
            )

    def test_rejects_non_path_types(self) -> None:
        with pytest.raises(TypeError, match="must be a Path"):
            ContextPath(local="/msm_home/out.fa", external=Path("/scratch/out.fa"), container=Path("/ws/out.fa"))  # type: ignore

    def test_is_pinned(self) -> None:
        cp = ContextPath(
            local=Path("/msm_home/out.fa"),
            external=Path("/scratch/agent/out.fa"),
            container=Path("/ws/out.fa"),
        )
        with pytest.raises(Exception):
            cp.local = Path("/elsewhere")  # type: ignore


class TestPathMapConstruction:
    def test_from_agent(self) -> None:
        class _StubSource:
            def GetPath(self) -> Path:
                return Path("/scratch/agent")

        class _StubAgent:
            home = _StubSource()

        pm = PathMap.FromAgent(_StubAgent(), task_key="TESTKEY")
        assert pm.extern_home == Path("/scratch/agent")
        assert pm.task_key == "TESTKEY"
        assert pm.extern_work == Path("/scratch/agent/runs/TESTKEY")

    def test_from_external_cwd_picks_correct_run_key(self) -> None:
        class _StubSource:
            def GetPath(self) -> Path:
                return Path("/scratch/agent")

        class _StubAgent:
            home = _StubSource()

        pm = PathMap.FromExternalCwd(
            cwd=Path("/scratch/agent/runs/REALKEY/ws/nxf_work/aa/bb"),
            agent=_StubAgent(),
        )
        assert pm.task_key == "REALKEY"
        assert pm.extern_work == Path("/scratch/agent/runs/REALKEY")

    def test_from_external_cwd_extra_subdir(self) -> None:
        class _StubSource:
            def GetPath(self) -> Path:
                return Path("/scratch/agent")

        class _StubAgent:
            home = _StubSource()

        pm = PathMap.FromExternalCwd(
            cwd=Path("/scratch/agent/runs/REALKEY/sample_a/nxf_work/aa/bb"),
            agent=_StubAgent(),
        )
        assert pm.task_key == "REALKEY"

    def test_from_external_cwd_rejects_outside_staged(self) -> None:
        class _StubSource:
            def GetPath(self) -> Path:
                return Path("/scratch/agent")

        class _StubAgent:
            home = _StubSource()

        with pytest.raises(ValueError, match="is not under agent's STAGED root"):
            PathMap.FromExternalCwd(
                cwd=Path("/tmp/random"),
                agent=_StubAgent(),
            )

    def test_rejects_non_absolute_extern_home(self) -> None:
        with pytest.raises(ValueError, match="extern_home must be absolute"):
            PathMap(extern_home=Path("relative/home"), task_key="K")

    def test_rejects_bad_task_key(self) -> None:
        with pytest.raises(ValueError, match="task_key must be a bare key"):
            PathMap(extern_home=Path("/scratch/agent"), task_key="K/with/slash")
        with pytest.raises(ValueError, match="task_key must be a bare key"):
            PathMap(extern_home=Path("/scratch/agent"), task_key="")


class TestPathMapConversionMatrix:
    @pytest.fixture
    def pm(self) -> PathMap:
        return PathMap(extern_home=Path("/scratch/agent"), task_key="K")

    @pytest.mark.parametrize("rel_tail", ["inputs/x.fa", "data/lib.xgdb", "deep/sub/dir/file"])
    def test_local_home_root_to_external(self, pm: PathMap, rel_tail: str) -> None:
        local = AgentPaths.HOME_ROOT / rel_tail
        assert pm.LocalToExternal(local) == pm.extern_home / rel_tail

    @pytest.mark.parametrize("rel_tail", ["work/aa/bb/out.fa", "nxf_work/cd/ef"])
    def test_local_work_root_to_external(self, pm: PathMap, rel_tail: str) -> None:
        local = AgentPaths.WORK_ROOT / rel_tail
        assert pm.LocalToExternal(local) == pm.extern_work / rel_tail

    @pytest.mark.parametrize("foreign", ["/project/refdb/x.tsv", "/data/external/foo.bam"])
    def test_foreign_absolute_passthrough(self, pm: PathMap, foreign: str) -> None:
        p = Path(foreign)
        assert pm.LocalToExternal(p) == p
        assert pm.ExternalToLocal(p) == p

    def test_external_to_local_under_extern_home(self, pm: PathMap) -> None:
        external = pm.extern_home / "inputs/x.fa"
        assert pm.ExternalToLocal(external) == AgentPaths.HOME_ROOT / "inputs/x.fa"

    def test_container_to_local(self, pm: PathMap) -> None:
        container = AgentPaths.WORK_ROOT / "work/aa/bb/out.fa"
        assert pm.ContainerToLocal(container) == (
            AgentPaths.HOME_ROOT / "runs/K/work/aa/bb/out.fa"
        )

    def test_local_to_external_rejects_relative(self, pm: PathMap) -> None:
        with pytest.raises(ValueError, match="expects absolute"):
            pm.LocalToExternal(Path("relative/x"))


class TestPathMapParse:
    @pytest.fixture
    def pm(self) -> PathMap:
        return PathMap(extern_home=Path("/scratch/agent"), task_key="K")

    def test_parses_absolute_ws_prefix(self, pm: PathMap) -> None:
        cp = pm.Parse(Path("/ws/work/aa/bb/file"))
        assert cp.local == AgentPaths.HOME_ROOT / "runs/K/work/aa/bb/file"
        assert cp.external == pm.extern_work / "work/aa/bb/file"
        assert cp.container == cp.local

    def test_parses_relative_ws_prefix(self, pm: PathMap) -> None:
        cp = pm.Parse(Path("../ws/work/aa/bb/file"))
        assert cp.local == AgentPaths.HOME_ROOT / "runs/K/work/aa/bb/file"
        assert cp.external == pm.extern_work / "work/aa/bb/file"
        assert cp.container == cp.local
        assert ".." not in cp.local.parts
        assert ".." not in cp.external.parts
        assert ".." not in cp.container.parts

    def test_parses_foreign_absolute(self, pm: PathMap) -> None:
        cp = pm.Parse(Path("/data/external/foo.bam"))
        assert cp.local == Path("/data/external/foo.bam")

    def test_parses_symlink_into_home(self, tmp_path: Path, pm: PathMap) -> None:
        link = tmp_path / "input.fa"
        link.symlink_to(AgentPaths.HOME_ROOT / "runs/K/inputs/some.fa")
        cp = pm.Parse(link)
        assert cp.external == pm.extern_home / "runs/K/inputs/some.fa"
        assert cp.local == AgentPaths.HOME_ROOT / "runs/K/inputs/some.fa"

    def test_parses_symlink_foreign(self, tmp_path: Path, pm: PathMap) -> None:
        foreign = Path("/project/refdb/tax.tsv")
        link = tmp_path / "tax.tsv"
        link.symlink_to(foreign)
        cp = pm.Parse(link)
        assert cp.external == foreign
        assert cp.container == foreign

    def test_rejects_naked_relative_path(self, pm: PathMap) -> None:
        with pytest.raises(ValueError, match="non-absolute, non-symlink"):
            pm.Parse(Path("just/some/relative/path.fa"))

    def test_honors_container_override(self, pm: PathMap) -> None:
        cp = pm.Parse(Path("/ws/out.fa"), container_override=AgentPaths.WORK_ROOT / "out.fa")
        assert cp.container == AgentPaths.WORK_ROOT / "out.fa"

    def test_home_rooted_absolute_resolves_all_three_views(self, pm: PathMap) -> None:
        cp = pm.Parse(AgentPaths.HOME_ROOT / "task_cache/1e/20ab/out/f.gbk")
        assert cp.local == AgentPaths.HOME_ROOT / "task_cache/1e/20ab/out/f.gbk"
        assert cp.external == pm.extern_home / "task_cache/1e/20ab/out/f.gbk"
        assert cp.container == cp.local

    def test_host_rooted_absolute_stays_foreign(self, pm: PathMap) -> None:
        host_path = pm.extern_home / "task_cache/1e/20ab/out/f.gbk"
        cp = pm.Parse(host_path)
        assert cp.local == host_path
        assert cp.external == host_path
        assert cp.container == host_path

    def test_direct_run_input_under_cwd_is_untouched(self, tmp_path: Path) -> None:
        real_input = tmp_path / "my_reads.fq"
        real_input.write_text("ACGT\n")
        pm = PathMap(
            extern_home=tmp_path, task_key=tmp_path.name, host_local=True
        )
        cp = pm.Parse(real_input)
        assert cp.local == real_input
        assert cp.local.exists()

    def test_relay_free_arm_is_the_identity(self) -> None:
        pm = PathMap(extern_home=AgentPaths.HOME_ROOT, task_key="K")
        p = AgentPaths.HOME_ROOT / "task_cache/1e/20ab/out/f.gbk"
        assert pm.ExternalToLocal(p) == p
        cp = pm.Parse(p)
        assert cp.local == p
        assert cp.external == p


class TestPathMapRender:
    @pytest.fixture
    def pm(self) -> PathMap:
        return PathMap(extern_home=Path("/scratch/agent"), task_key="K")

    @pytest.mark.parametrize(
        "dialect,expected_prefix",
        [
            ("bash", "$AGENT_HOME"),
            ("brace", "{agent_home}"),
            ("groovy", "${params.home}"),
        ],
    )
    def test_render_prefix_substitution(self, pm: PathMap, dialect: str, expected_prefix: str) -> None:
        p = pm.extern_home / "runs/K/inputs/x.fa"
        rendered = pm.Render(p, dialect=dialect)  # type: ignore[arg-type]
        assert rendered == f"{expected_prefix}/runs/K/inputs/x.fa"

    def test_render_only_replaces_prefix(self, pm: PathMap) -> None:
        p = pm.extern_home / "data/scratch_agent_backup/lib.xgdb"
        rendered = pm.Render(p, dialect="groovy")
        assert rendered == "${params.home}/data/scratch_agent_backup/lib.xgdb"
        assert "scratch_agent_backup" in rendered

    def test_render_foreign_path_unchanged(self, pm: PathMap) -> None:
        p = Path("/project/refdb/tax.tsv")
        assert pm.Render(p, dialect="groovy") == "/project/refdb/tax.tsv"

    def test_render_root_only(self, pm: PathMap) -> None:
        assert pm.Render(pm.extern_home, dialect="brace") == "{agent_home}"


class TestContextPathClassmethods:
    @pytest.fixture
    def pm(self) -> PathMap:
        return PathMap(extern_home=Path("/scratch/agent"), task_key="K")

    def test_from_local_under_home(self, pm: PathMap) -> None:
        cp = ContextPath.FromLocal(AgentPaths.HOME_ROOT / "runs/K/inputs/x.fa", pm)
        assert cp.external == pm.extern_home / "runs/K/inputs/x.fa"
        assert cp.container == cp.local

    def test_for_output(self, pm: PathMap) -> None:
        cp = ContextPath.ForOutput("1-1-1.hash-namespace--type.txt", pm)
        assert cp.container == AgentPaths.WORK_ROOT / "1-1-1.hash-namespace--type.txt"
        assert cp.local == cp.container
        assert cp.external == pm.extern_work / "1-1-1.hash-namespace--type.txt"

    def test_for_output_with_extern_cwd(self) -> None:
        pm = PathMap(
            extern_home=Path("/scratch/agent"),
            task_key="K",
            extern_cwd=Path("/scratch/agent/runs/K/nxf_work/aa/bb"),
        )
        cp = ContextPath.ForOutput("out.fa", pm)
        assert cp.external == Path("/scratch/agent/runs/K/nxf_work/aa/bb/out.fa")
        assert cp.local == AgentPaths.WORK_ROOT / "out.fa"
        assert cp.container == AgentPaths.WORK_ROOT / "out.fa"

    def test_for_output_rejects_path_with_slash(self, pm: PathMap) -> None:
        with pytest.raises(ValueError, match="bare filename"):
            ContextPath.ForOutput("subdir/file.fa", pm)

    def test_for_output_collapses_all_three_views_host_local(self) -> None:
        pm = PathMap(
            extern_home=Path("/scratch/agent"),
            task_key="K",
            extern_cwd=Path("/scratch/agent/runs/K/nxf_work/aa/bb"),
            host_local=True,
        )
        cp = ContextPath.ForOutput("out.fa", pm)
        expected = Path("/scratch/agent/runs/K/nxf_work/aa/bb/out.fa")
        assert cp.external == expected
        assert cp.local == expected
        assert cp.container == expected

    def test_for_output_host_local_falls_back_to_extern_work(self) -> None:
        pm = PathMap(extern_home=Path("/scratch/agent"), task_key="K", host_local=True)
        cp = ContextPath.ForOutput("out.fa", pm)
        assert cp.local == cp.container == cp.external == pm.extern_work / "out.fa"
