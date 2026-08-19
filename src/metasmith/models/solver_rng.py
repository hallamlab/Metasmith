from __future__ import annotations
from math import inf, isnan
from typing import Sequence

# Bump when the stream or any decision rule changes. A plan solved under one
# version is not reproducible under another, and the Rust side reports the
# version it implements so a desync is loud rather than a quiet divergence in
# results. This repo has scar tissue from a version constant that drifted from
# its second implementation and failed silently.
SOLVER_RNG_VERSION = 1

_MASK32 = 0xFFFFFFFF
_2_32 = 1 << 32
_SIGMA = (0x61707865, 0x3320646E, 0x79622D32, 0x6B206574)
_ZERO_NONCE = bytes(12)
CHACHA_ROUNDS = 8

def _rotl32(v: int, c: int) -> int:
    return ((v << c) & _MASK32) | (v >> (32 - c))

def _quarter_round(x: list[int], a: int, b: int, c: int, d: int):
    x[a] = (x[a] + x[b]) & _MASK32; x[d] = _rotl32(x[d] ^ x[a], 16)
    x[c] = (x[c] + x[d]) & _MASK32; x[b] = _rotl32(x[b] ^ x[c], 12)
    x[a] = (x[a] + x[b]) & _MASK32; x[d] = _rotl32(x[d] ^ x[a], 8)
    x[c] = (x[c] + x[d]) & _MASK32; x[b] = _rotl32(x[b] ^ x[c], 7)

def chacha_block(key: bytes, counter: int, nonce: bytes=_ZERO_NONCE, rounds: int=CHACHA_ROUNDS) -> list[int]:
    assert len(key) == 32, f"key must be 32 bytes, got {len(key)}"
    assert len(nonce) == 12, f"nonce must be 12 bytes, got {len(nonce)}"
    assert rounds % 2 == 0, "ChaCha rounds come in column/diagonal pairs"
    state = list(_SIGMA)
    state += [int.from_bytes(key[i:i+4], "little") for i in range(0, 32, 4)]
    state.append(counter & _MASK32)
    state += [int.from_bytes(nonce[i:i+4], "little") for i in range(0, 12, 4)]
    w = state.copy()
    for _ in range(rounds // 2):
        _quarter_round(w, 0, 4,  8, 12)
        _quarter_round(w, 1, 5,  9, 13)
        _quarter_round(w, 2, 6, 10, 14)
        _quarter_round(w, 3, 7, 11, 15)
        _quarter_round(w, 0, 5, 10, 15)
        _quarter_round(w, 1, 6, 11, 12)
        _quarter_round(w, 2, 7,  8, 13)
        _quarter_round(w, 3, 4,  9, 14)
    return [(w[i] + state[i]) & _MASK32 for i in range(16)]

def seed_to_key(seed: int) -> bytes:
    return (seed & 0xFFFFFFFFFFFFFFFF).to_bytes(8, "little") + bytes(24)

class ChaCha8:
    def __init__(self, seed: int):
        self._key = seed_to_key(seed)
        self._counter = 0
        self._block: list[int] = []
        self._i = 16

    def next_u32(self) -> int:
        if self._i >= 16:
            self._block = chacha_block(self._key, self._counter)
            self._counter = (self._counter + 1) & _MASK32
            self._i = 0
        word = self._block[self._i]
        self._i += 1
        return word

# A NaN would make the orderings below non-total, and a non-total order is a
# divergence waiting to happen rather than a crash. The rule is that NaN is
# always *worst*, which means it has to sort in opposite directions depending on
# which way "best" points -- one shared mapping quietly makes NaN the winner of
# every `argmin_index`.
def _rank_high(score: float) -> float:
    return -inf if isnan(score) else score

def _rank_low(value: float) -> float:
    return inf if isnan(value) else value

def top_k_indices(scores: Sequence[float], k: int) -> list[int]:
    k = min(k, len(scores))
    if k <= 0: return []
    if k == 1: return [argmax_index(scores)]
    return sorted(range(len(scores)), key=lambda i: (-_rank_high(scores[i]), i))[:k]

def argmax_index(scores: Sequence[float]) -> int:
    best, best_v = 0, _rank_high(scores[0])
    for i in range(1, len(scores)):
        v = scores[i]
        if v != v: continue
        if v > best_v: best, best_v = i, v
    return best

def argmin_index(values: Sequence[float]) -> int:
    best, best_v = 0, _rank_low(values[0])
    for i in range(1, len(values)):
        v = values[i]
        if v != v: continue
        if v < best_v: best, best_v = i, v
    return best

class DecisionStream:
    def __init__(self, seed: int):
        self._words = ChaCha8(seed)
        self.draws = 0

    def bounded_int(self, n: int) -> int:
        if n <= 1: return 0
        limit = _2_32 - (_2_32 % n)
        while True:
            v = self._words.next_u32()
            self.draws += 1
            if v < limit: return v % n

    def weighted_index(self, weights: Sequence[int]) -> int:
        assert len(weights) > 0, "cannot choose from no options"
        if len(weights) == 1: return 0
        total = sum(weights)
        assert total > 0, "weights must not sum to zero"
        r = self.bounded_int(total)
        acc = 0
        for i, w in enumerate(weights):
            acc += w
            if r < acc: return i
        return len(weights) - 1

    def pick_top_k(self, scores: Sequence[float], k: int) -> int:
        candidates = top_k_indices(scores, k)
        assert len(candidates) > 0, "cannot pick from an empty frontier"
        return candidates[self.bounded_int(len(candidates))]
