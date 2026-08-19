"""Filling the image store ahead of a run, and reporting on it before a launch.

Tool images materialise lazily, inside the first task that needs each one. That
is right on a cluster whose nodes can reach a registry and wrong on one whose
compute nodes cannot: the fetch happens exactly where there is no route. The
answer is not to move every pull onto the launch path -- that would make every
run on a connected cluster pay for it -- but to split the two halves:

  * `_materialise_images` fetches, and is reached only when asked (the
    `workflow materialise` verb, run from a login node);
  * `_check_image_store` only looks, and rides in `RunWorkflow`'s existing
    preflight so an incomplete store is named before anything is transferred.

Both read the images out of the stage-time env manifest rather than a transform
library, because the host doing the launching may hold neither. Both build the
same `Environment` the execution path builds and issue `MakeMaterialiseCommand`
under the same lock, so a pre-flight and a task can never disagree about what a
materialised image is -- including its mount test and its stamp.

Free functions taking a shell, in the shape of `gpu.py` and `portability.py`:
the agent method is the thing that opens a connection, and everything worth
testing sits on this side of that.
"""

from __future__ import annotations

from pathlib import Path

from ..constants import AgentPaths
from ..env import ContainerDef, Environment, Rootfs
from ..logging import Log
from ..models.libraries.execution import _materialised_test


class ImageMaterialiseError(Exception):
    """One or more of a staged workflow's images could not be materialised."""


_READY = "image-ready"


def _manifest_images(doc: dict) -> tuple[list[str], list[str]]:
    """The distinct container images a staged workspace needs, and what is unknown.

    Distinct by image, not by step: a library where eight steps share three
    images has three to fetch.

    The second list names transforms whose requirement could not be read. Two
    quite different things land there and both must be reported rather than
    silently dropped -- a resource recorded as `null` (unreadable at stage time)
    and a manifest from the previous schema, which recorded only *which* keys a
    resource carried and so cannot answer this question at all. Reporting an
    empty image set for either would read as "the store is complete".
    """
    images: list[str] = []
    unknown: set[str] = set()
    for _, step in sorted((doc.get("steps") or {}).items()):
        for _, fields in sorted((step.get("envs") or {}).items()):
            if isinstance(fields, dict):
                image = fields.get("container")
                if image and image not in images:
                    images.append(image)
                continue
            # None (unreadable) or a bare key list (schema 1).
            unknown.add(str(step.get("transform")))
    return sorted(images), sorted(unknown)


def _tool_environment_for(
    image: str, agent_env: Environment, agent_home: Path, *, rootfs: "str|Rootfs|None" = None,
) -> Environment:
    """The environment a tool image is materialised into on this agent.

    The store is the agent home's container directory, which is what both deploy
    and execution already resolve to, so a pre-flight fills the same place a task
    will look.

    `rootfs` is the workspace's staged override and wins when there is one, else
    the agent's own standing tendency -- the same precedence bootstrap applies
    per step. It matters here more than anywhere: a workspace staged
    `rootfs=sandbox` whose store was filled with SIFs has been pre-flighted for
    nothing, and the first task unpacks a sandbox on the node that cannot fetch.
    """
    return Environment(
        image=image,
        runtime=agent_env.runtime,
        native=agent_env.native,
        container=ContainerDef(cache=agent_home/AgentPaths.CONTAINER_CACHE),
        rootfs=Rootfs.Parse(rootfs) if rootfs is not None else agent_env.rootfs,
        gpu_args=list(agent_env.gpu_args),
    )


def _has_image_store(env: Environment) -> bool:
    # Only the runtimes that keep a metasmith-managed artifact on disk have
    # anything to pre-fill. Mamba/native run tools on the host filesystem, and
    # docker keeps its own store which its own pull policy and the unconditional
    # deploy-time refresh already cover -- there is no path here to fill.
    return env.GetLocalPath() is not None


def _is_materialised(shell, env: Environment) -> bool:
    res = shell.Exec(f'{_materialised_test(env)} && echo "{_READY}"', history=True, quiet=True)
    return _READY in res.out


def _materialise_images(
    shell, images: list[str], agent_env: Environment, agent_home: Path,
    *, rootfs: "str|Rootfs|None" = None, force: bool = False,
) -> list[dict]:
    """Fetch every image a staged workspace needs, on the agent's own host.

    Idempotent by construction rather than by bookkeeping: each image is skipped
    when the artifact-and-stamp test already passes, which is the same gate every
    task consults. Running this twice does nothing the second time.

    One image failing does not stop the others. On the cluster this exists for,
    leaving the rest of the store empty because the first pull failed would mean
    another trip to the login node for each remaining image.
    """
    if not _has_image_store(agent_env):
        return []
    report: list[dict] = []
    for image in images:
        env = _tool_environment_for(image, agent_env, agent_home, rootfs=rootfs)
        if not force and _is_materialised(shell, env):
            report.append({"image": image, "ok": True, "skipped": True})
            continue
        sif, sandbox = env.GetLocalPath(), env.GetSandboxPath()
        materialise = env.MakeMaterialiseCommand(force=force).replace("'", "'\\''")
        # The same lock the execution path takes, on the same path, so a
        # pre-flight racing a task that started early does not fetch twice.
        res = shell.Exec(
            f'mkdir -p "{sif.parent}"; '
            f'flock "{sandbox}.lock" -c \'{materialise}\'; '
            f'{_materialised_test(env)} && echo "{_READY}"',
            history=True,
        )
        ok = _READY in res.out
        report.append({"image": image, "ok": ok, "skipped": False})
        Log.Info(f"{'materialised' if ok else 'FAILED to materialise'} [{image}]")
    failed = [r["image"] for r in report if not r["ok"]]
    if failed:
        raise ImageMaterialiseError(
            f"[{len(failed)}] of [{len(report)}] image(s) could not be materialised "
            f"on this agent:\n  " + "\n  ".join(failed)
        )
    return report


def _check_image_store(
    shell, images: list[str], agent_env: Environment, agent_home: Path,
    *, rootfs: "str|Rootfs|None" = None,
) -> list[str]:
    """Which of these images the agent's store does not already hold.

    Looks and does not fetch. On a connected cluster the lazy path is fine and
    putting pulls on every launch's critical path would be a regression; on one
    whose compute nodes are offline this report is the thing that says to run the
    pre-flight first.
    """
    if not images or not _has_image_store(agent_env):
        return []
    missing: list[str] = []
    for image in images:
        env = _tool_environment_for(image, agent_env, agent_home, rootfs=rootfs)
        if not _is_materialised(shell, env):
            missing.append(image)
    return missing
