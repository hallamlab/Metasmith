def make_exe():
    dist = default_python_distribution()

    policy = dist.make_python_packaging_policy()
    # policy.resources_location = "in-memory"
    # policy.resources_location_fallback = "filesystem-relative:prefix"

    python_config = dist.make_python_interpreter_config()
    # python_config.run_command = "from uuid import uuid4; print(uuid4())"


    exe = dist.to_python_executable(
        name="test_pk",
        packaging_policy=policy,
        config=python_config,
    )

    return exe

register_target("exe", make_exe)
resolve_targets()
