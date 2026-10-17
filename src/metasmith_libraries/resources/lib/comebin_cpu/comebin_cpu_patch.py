import ctypes
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


# A 1024-contig batch makes every activation 6144 x 2048 floats, 50 MB. glibc serves anything over 32 MB with a
# fresh mmap and unmaps it on free, so each op's output was page-faulted in, 12k faults, and a 0.2 ms multiply
# took 5 ms. Keeping freed memory in the heap makes the allocations reuse warm pages.
def _keep_freed_memory():
    libc = ctypes.CDLL("libc.so.6")
    M_TRIM_THRESHOLD, M_MMAP_MAX = -1, -4
    libc.mallopt(M_MMAP_MAX, 0)
    libc.mallopt(M_TRIM_THRESHOLD, 2**31 - 1)


def _parallel_dropout():
    def draw(chunk_gen_q):
        chunk, gen, q = chunk_gen_q
        chunk.bernoulli_(q, generator=gen)

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
            list(_pool.map(draw, [(c, g, q) for c, g in zip(chunks, _generators)]))
            return x * mask.div_(q)

    return ParallelDropout


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


# In training, mlp2.EmbeddingNet runs the coverage network a second time only to return an embedding that the
# loss ignores unless --addcovloss is set. Its one lasting effect is a second update of each BatchNorm's
# running statistics, which the exported embeddings use. Replaying the network up to its last BatchNorm,
# without gradients and with fresh dropout masks, keeps those updates and drops the rest of the pass.
def _single_coverage_pass(original_forward):
    def replay_batchnorm(cov_model, x2):
        layers = list(cov_model.fc)
        last_bn = max(i for i, layer in enumerate(layers) if isinstance(layer, nn.BatchNorm1d))
        h = x2
        with torch.no_grad():
            for layer in layers[: last_bn + 1]:
                h = layer(h)

    def forward(self, x, x2=None):
        if not self.training or self.pretrained_model is not None or self.cov_model is None:
            return original_forward(self, x, x2)
        cov = self.cov_model(x2)
        x = torch.cat([x, cov if self.covmodel_notl2normalize else F.normalize(cov)], dim=-1)
        output = self.fc(x)
        replay_batchnorm(self.cov_model, x2)
        return output, cov

    return forward


# A shuffled TensorDataset can be fetched a whole batch per index list. Five worker processes, forked anew every
# epoch, stacked 1024 single-row tensors instead. The sampler is the one DataLoader builds for shuffle=True and
# draws from the same generator, so the batches are the ones the original loader yields.
def _batched_loader(original):
    from torch.utils.data import BatchSampler, RandomSampler, TensorDataset

    def loader(dataset, batch_size=1, shuffle=False, drop_last=False, **kwargs):
        if not (isinstance(dataset, TensorDataset) and shuffle):
            return original(dataset, batch_size=batch_size, shuffle=shuffle, drop_last=drop_last, **kwargs)
        return original(dataset, batch_size=None, sampler=BatchSampler(RandomSampler(dataset), batch_size, drop_last))

    return loader


def apply():
    global _pool, _generators
    _keep_freed_memory()
    import simclr
    from models import mlp2

    threads = _threads()
    _pool = ThreadPoolExecutor(threads)
    _generators = [torch.Generator().manual_seed(7919 + k) for k in range(threads)]
    nn.Dropout = _parallel_dropout()

    original_accuracy = simclr.accuracy

    # The two-column logits make COMEBin's top-5 meaningless, so its logged accuracy reads the true top-1.
    def accuracy(output, target, topk=(1,)):
        if output.shape[1] == 2 and "top1" in _stash:
            return [_stash["top1"] for _ in topk]
        return original_accuracy(output, target, topk)

    simclr.accuracy = accuracy
    simclr.SimCLR.info_nce_loss = info_nce_loss
    torch.utils.data.DataLoader = _batched_loader(torch.utils.data.DataLoader)
    if "--addcovloss" not in sys.argv:
        mlp2.EmbeddingNet.forward = _single_coverage_pass(mlp2.EmbeddingNet.forward)
    print(f"comebin_cpu_patch: {threads} threads, parallel dropout, logsumexp InfoNCE, one coverage pass, batched loader, heap-kept buffers",
          file=sys.stderr, flush=True)
