import os
import sys


# MKL runs its AVX-512 sgemm only on CPUs whose vendor string is Intel and gives AMD the AVX2 kernels. On Zen 4/5,
# which execute AVX-512, overriding MKL's vendor test sped the training step's GEMMs up 1.5-2x. MKL takes the
# override only through LD_PRELOAD, not from a library loaded with RTLD_GLOBAL before torch, so the process
# re-executes itself.
def _reexec_with_mkl_vendor_override():
    override = os.path.join(os.path.dirname(os.path.abspath(__file__)), "libmkl_intel_cpu.so")
    preloaded = os.environ.get("LD_PRELOAD", "")
    if override in preloaded.split(":"):
        return
    with open("/proc/cpuinfo") as f:
        cpu = f.read(8192)
    flags = next((line.split(":", 1)[1].split() for line in cpu.splitlines() if line.startswith("flags")), [])
    if "AuthenticAMD" in cpu and "avx512f" in flags:
        os.environ["LD_PRELOAD"] = override + (":" + preloaded if preloaded else "")
        os.execv(sys.executable, [sys.executable] + sys.argv)


_subcommand = sys.argv[1:2] if os.path.basename(sys.argv[0]) == "main.py" else []

# The bin step's Leiden sweep forks its worker pool after k-means and hnswlib have started threads. A worker
# forked while one of them held a lock waits on it forever, so the sweep hangs at random with no output.
if _subcommand == ["bin"]:
    import multiprocessing

    multiprocessing.set_start_method("spawn")

if _subcommand == ["train"] and not os.environ.get("CUDA_VISIBLE_DEVICES"):
    _reexec_with_mkl_vendor_override()
    import torch

    if not torch.cuda.is_available():
        sys.path.insert(0, os.path.dirname(os.path.abspath(sys.argv[0])))
        import comebin_cpu_patch

        comebin_cpu_patch.apply()
