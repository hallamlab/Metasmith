from pathlib import Path
from ..models.libraries import ExecutionContext, ExecutionResult, TransformInstance
from ..transforms import LoadTransform

def BootstrapTransform():
    print("> Bootstrap Transform")
    inputs: dict[str, Path] = {}
    with open("./inputs.txt") as f:
        for l in f:
            p, name = l[:-1].split(",")
            inputs[name] = Path(p)

    for k, p in inputs.items():
        print(k)
        with open(p) as f:
            for l in f:
                print(l)
        # if p.is_symlink():
        #     print(p)
        #     print(p.resolve())
        # else:
        #     with open(p) as f:
        #         for l in f:
        #             print(l)
        print()
