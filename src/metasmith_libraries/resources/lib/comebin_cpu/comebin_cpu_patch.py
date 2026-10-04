import sys
from concurrent.futures import ThreadPoolExecutor

import torch
import torch.nn as nn
import torch.nn.functional as F

_pool = None
_generators = []
_stash = {}
_indices = {}


def _threads():
    if "--num_threads" in sys.argv:
        return int(sys.argv[sys.argv.index("--num_threads") + 1])
    return torch.get_num_threads()


# torch 1.10 takes MKL's parallel bernoulli only on Intel CPUs. On AMD every dropout mask is drawn on one
# thread, ~86 ms for each of the six views in every step while the other cores wait.
class ParallelDropout(nn.Module):
    def __init__(self, p=0.5, inplace=False):
        super().__init__()
        self.p = p

    def forward(self, x):
        if not self.training or self.p == 0:
            return x
        q = 1.0 - self.p
        mask = torch.empty(x.shape, dtype=x.dtype)
        chunks = mask.view(-1).chunk(len(_generators))
        list(_pool.map(lambda cg: cg[0].bernoulli_(q, generator=cg[1]), zip(chunks, _generators)))
        return x * mask * (1.0 / q)


def _loss_indices(batch, views):
    if (batch, views) not in _indices:
        n = batch * views
        label = torch.arange(n) % batch
        same_contig = torch.zeros(n, n)
        same_contig.masked_fill_(label[:, None] == label[None, :], float("-inf"))
        view = torch.arange(n).div(batch, rounding_mode="floor")
        cols = label[:, None] + batch * torch.arange(views)[None, :]
        other_view = torch.arange(views)[None, :] != view[:, None]
        _indices[(batch, views)] = (same_contig, cols[other_view].view(n, views - 1))
    return _indices[(batch, views)]


# COMEBin selects positives and negatives by boolean-mask indexing, which parallelises badly. Cross-entropy
# over [pos, neg...] equals cross-entropy over [pos, logsumexp(neg)], so each positive needs two logits.
def info_nce_loss(self, features):
    batch, views, tau = self.args.batch_size, self.args.n_views, self.args.temperature
    same_contig, pos_idx = _loss_indices(batch, views)
    f = F.normalize(features, dim=1)
    sim = torch.matmul(f, f.T) / tau
    pos = sim.gather(1, pos_idx)
    neg = sim + same_contig
    lse = torch.logsumexp(neg, dim=1, keepdim=True)
    with torch.no_grad():
        _stash["top1"] = (pos > neg.max(dim=1, keepdim=True).values).float().mean().reshape(1) * 100.0
    logits = torch.cat([pos.reshape(-1, 1), lse.expand(-1, views - 1).reshape(-1, 1)], dim=1)
    return logits, torch.zeros(logits.shape[0], dtype=torch.long)


def apply():
    global _pool, _generators
    import simclr

    threads = _threads()
    _pool = ThreadPoolExecutor(threads)
    _generators = [torch.Generator().manual_seed(7919 + k) for k in range(threads)]
    nn.Dropout = ParallelDropout

    original_accuracy = simclr.accuracy

    # The two-column logits make COMEBin's top-5 meaningless, so its logged accuracy reads the true top-1.
    def accuracy(output, target, topk=(1,)):
        if output.shape[1] == 2 and "top1" in _stash:
            return [_stash["top1"] for _ in topk]
        return original_accuracy(output, target, topk)

    simclr.accuracy = accuracy
    simclr.SimCLR.info_nce_loss = info_nce_loss
    print(f"comebin_cpu_patch: parallel dropout on {threads} threads and a logsumexp InfoNCE", file=sys.stderr, flush=True)
