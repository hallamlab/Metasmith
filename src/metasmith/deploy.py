from .process_management import LiveShell

def Deploy():
    with LiveShell(silent=False) as ssh, LiveShell(silent=False) as local:
        shell.Exec(
            f"""\
            ssh sockeye
            if [ -n "$SSH_CLIENT" ] || [ -n "$SSH_TTY" ]; then
                echo "You are connected via SSH"
            else
                echo "You are not connected via SSH"
            fi
            """
        )

        shell.Exec(
            f"""\
            echo exiting
            exit
            """
        )

        shell.Exec(
            f"""\
            if [ -n "$SSH_CLIENT" ] || [ -n "$SSH_TTY" ]; then
                echo "You are connected via SSH"
            else
                echo "You are not connected via SSH"
            fi
            """
        )
