from os.path import basename, splitext
from sys import argv, stderr

GREEN   = "\x1b[92m"
CYAN    = "\x1b[96m"
RED     = "\x1b[91m"
YELLOW  = "\x1b[93m"
BOLD    = "\x1b[1m"
RESET   = "\x1b[0m"

def parse_args(expected_inputs: tuple[tuple[str, str], ...]) -> list[str]:
    try:
        arg1 = argv[1]
        if arg1 == "--help":
            print(generate_help_message(expected_inputs))
            exit(0)
    except:
        pass

    input_args = argv[1:]
    N = len(expected_inputs)

    def eprint(msg: str):
        print(msg, file=stderr)

    if len(input_args) < N:
        eprint(f"{RED}{BOLD}Expected {N} positional arguments!{RESET}\n\n{generate_help_message(expected_inputs)}")
    elif len(input_args) > N:
        eprint(f"{YELLOW}{BOLD}WARNING{RESET}{YELLOW}: Trailing arguments have been detected, and will be ignored!{RESET}")

    outputs: list[str] = []
    for i in range(N):
        outputs.append(input_args[i])
    return outputs


def generate_help_message(expected_inputs: tuple[tuple[str, str], ...]) -> str:
    prog_basename = basename(__file__)
    prog_name = splitext(prog_basename)[0]

    out = f"{GREEN}{BOLD}Usage: {CYAN}{prog_name}{RESET}{CYAN}"
    for id, _ in expected_inputs:
        out += f" [{id}]"
    out += f"{GREEN}{BOLD}Arguments:{RESET}\n"

    def get_len(item: tuple[str, str]) -> int:
        id, _ = item
        return len(id)
    max_arg_size = max(map(get_len, expected_inputs))

    for id, desc in expected_inputs:
        out += (f"    {CYAN}{BOLD}{id:<max_arg_size}{RESET}    {desc}")

    return out
