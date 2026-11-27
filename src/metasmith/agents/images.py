# Reporting on the image store before a launch.
#
# Tool images materialise lazily, inside the first task that needs each one. That
# is right on a cluster whose nodes can reach a registry and wrong on one whose
# compute nodes cannot: the fetch happens exactly where there is no route. A setup
# run pulls every image on the driver's host first (`pullContainer`), and
# `_check_image_store` rides in `RunWorkflow`'s preflight to name an incomplete
# store before anything is transferred. It only looks.
#
# It reads the images out of the stage-time env manifest rather than a transform
# library, because the host doing the launching may hold neither, and builds the
# same `Environment` the execution path builds, so it judges an image by the same
# artifact-and-stamp test a task does.

from __future__ import annotations

from pathlib import Path

from ..constants import AgentPaths
from ..env import ContainerDef, Environment, Rootfs
from ..models.libraries.execution import _materialised_test


_READY = "image-ready"


def _manifest_images(doc: dict) -> tuple[list[str], list[str]]:
    # The distinct container images a staged workspace needs, and what is unknown.
    #
    # Distinct by image, not by step: a library where eight steps share three
    # images has three to fetch.
    #
    # The second list names transforms whose requirement could not be read. Two
    # quite different things land there and both must be reported rather than
    # silently dropped -- a resource recorded as `null` (unreadable at stage time)
    # and a manifest from the previous schema, which recorded only *which* keys a
    # resource carried and so cannot answer this question at all. Reporting an
    # empty image set for either would read as "the store is complete".
    images: list[str] = []
    unknown: set[str] = set()
    for _, step in sorted((doc.get("steps") or {}).items()):
        for _, fields in sorted((step.get("envs") or {}).items()):
            if isinstance(fields, dict):
                image = fields.get("container")
                if image and image not in images:
                    images.append(image)
                continue
            unknown.add(str(step.get("transform")))
    return sorted(images), sorted(unknown)


def _tool_environment_for(
    image: str, agent_env: Environment, agent_home: Path, *, rootfs: "str|Rootfs|None" = None,
) -> Environment:
    # The environment a tool image is materialised into on this agent.
    #
    # The store is the agent home's container directory, which is what both deploy
    # and execution already resolve to, so a pre-flight fills the same place a task
    # will look.
    #
    # `rootfs` is the workspace's staged override and wins when there is one, else
    # the agent's own standing tendency -- the same precedence bootstrap applies
    # per step. It matters here more than anywhere: a workspace staged
    # `rootfs=sandbox` whose store was filled with SIFs has been pre-flighted for
    # nothing, and the first task unpacks a sandbox on the node that cannot fetch.
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


def _check_image_store(
    shell, images: list[str], agent_env: Environment, agent_home: Path,
    *, rootfs: "str|Rootfs|None" = None,
) -> list[str]:
    if not images or not _has_image_store(agent_env):
        return []
    missing: list[str] = []
    for image in images:
        env = _tool_environment_for(image, agent_env, agent_home, rootfs=rootfs)
        if not _is_materialised(shell, env):
            missing.append(image)
    return missing
