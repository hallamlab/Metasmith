from metasmith.python_api import Agent, Runtime, Source

AGENT_HOME = "/scratch/phyberos/msm_e2e/run-1"
smith = Agent(
    home=Source.FromSsh(host="nibi", path=AGENT_HOME),
    runtime=Runtime.APPTAINER,
    setup_commands=[
        "module load apptainer/1.3.5",
        f"export TMPDIR={AGENT_HOME}/tmp",
        "mkdir -p $TMPDIR",
        "export APPTAINER_TMPDIR=$TMPDIR",
    ],
)
smith.Deploy(rootfs="sandbox")
print("deployed")
