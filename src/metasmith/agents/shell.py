"""A held-open connection to the agent host.

`AgentShell` is a context manager, not a shell: entering runs the agent's setup
(source the env, cd to the home root, make the relay reachable) and exiting runs
its cleanup, so everything in between can assume it is standing in a working
metasmith installation on the far side.

Its own module because both `Agent` and the mixins that hang off it need to
construct one, and a class the whole package reaches for should not live inside
the class it serves.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..coms.terminals import LiveShell, PROBE_TIMEOUT
from ..logging import Log

if TYPE_CHECKING:
    from .agent import Agent
class AgentShell:
    def __init__(self, agent: Agent):
        self.agent = agent
        self.shell: LiveShell | None = None

    def __enter__(self):
        shell = LiveShell()
        try:
            def _on_out(x: str):
                Log.Info(f"> {x}\x1b[0;m", timestamp=False) # to escape nextflow colours
            def _on_err(x: str):
                Log.Error(f"> {x}", timestamp=False)
            Log.Info(f"connecting to deployed agent")
            self.agent._run_setup(shell)
            shell.RegisterOnOut(_on_out)
            shell.RegisterOnErr(_on_err)
            shell.Exec(
                f"cd {self.agent.home.GetPath()}",
                idle_timeout=PROBE_TIMEOUT, what="cd to agent home",
            )
            # mamba/native cross no container boundary, so there is no relay to
            # find or start; requiring one would make those runtimes
            # undeployable rather than merely un-bounced.
            if self.agent._environment().needs_relay:
                res = shell.Exec(
                    '[ -e ./relay/msm_relay ] && echo "relay-present"', history=True,
                    idle_timeout=PROBE_TIMEOUT, what="relay presence check",
                )
                assert "relay-present" in res.out, (
                    f"relay binary not present at [{self.agent.home.GetPath()}/relay/msm_relay]; "
                    f"agent home may be partially deployed — rerun Agent.Deploy()"
                )
                Log.Info(f"starting relay service")
                shell.Exec(
                    f'./relay/msm_relay start',
                    idle_timeout=PROBE_TIMEOUT, what="relay start",
                )
            self.shell = shell
            return self.shell
        except BaseException:
            shell.Dispose()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.shell is None: return
        Log.Info(f"closing connection")
        self.agent._run_cleanup(self.shell)
        # A command that timed out is still running on the far end with an
        # unclaimed marker, so this shell is spent: the polite `exit` would
        # only burn its own timeout before we disposed it anyway.
        spent = exc_type is not None and issubclass(exc_type, TimeoutError)
        if self.agent._is_ssh() and not spent:
            try:
                self.shell.Exec("exit", timeout=5)
            except (KeyboardInterrupt, TimeoutError):
                pass
        self.shell.__exit__(exc_type, exc_val, exc_tb)
        self.shell = None
