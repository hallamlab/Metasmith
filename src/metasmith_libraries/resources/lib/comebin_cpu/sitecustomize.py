import os
import sys

if os.path.basename(sys.argv[0]) == "main.py" and sys.argv[1:2] == ["train"]:
    import torch

    if not torch.cuda.is_available():
        sys.path.insert(0, os.path.dirname(os.path.abspath(sys.argv[0])))
        import comebin_cpu_patch

        comebin_cpu_patch.apply()
