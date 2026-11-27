"""Write ESM-1b's checkpoint without its optimizer state, so each worker's torch.load peaks at the
weights (2.6 GB) instead of the shipped 7.8 GB file. Also copies the contact-regression file that
fair-esm insists on finding beside the model.

Usage: python slim_ckpt.py <out dir>
"""
import os
import shutil
import sys

import torch

SRC = "/opt/torch_cache/hub/checkpoints/esm1b_t33_650M_UR50S"
out = sys.argv[1]
os.makedirs(out, exist_ok=True)
d = torch.load(f"{SRC}.pt", map_location="cpu")
print({k: type(v).__name__ for k, v in d.items()})
torch.save({k: d[k] for k in ("args", "cfg", "model") if k in d}, f"{out}/esm1b_t33_650M_UR50S.pt")
shutil.copy(f"{SRC}-contact-regression.pt", out)
print(os.path.getsize(f"{out}/esm1b_t33_650M_UR50S.pt"))
