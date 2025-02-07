import numpy as np
from hashlib import sha256

class KeyGenerator:
    def __init__(self, full=False, seed=None) -> None:
        ascii_vocab = [(48, 57), (65, 90), (97, 122)]
        vocab = [chr(i) for g in [range(a, b+1) for a, b in ascii_vocab] for i in g]
        if full: vocab += [c for c in "-_"]
        self.vocab = vocab
        self._generator = np.random.default_rng(seed)

    def GenerateUID(self, l:int=8, blacklist: set[str]=set()) -> str:
        key: str|None = None
        while key is None or key in blacklist:
            digits = self._generator.integers(0, len(self.vocab), l)
            key = "".join([self.vocab[i] for i in digits])
        return key
    
    def FromInt(self, i: int, l: int=8, little_endian=False):
        chunks = [self.vocab[0]]*l
        place = 0
        while i > 0 and place < l:
            chunk_k = i % len(self.vocab)
            i = (i - chunk_k) // len(self.vocab)
            chunks[place] = self.vocab[chunk_k]
            place += 1
        if not little_endian: chunks.reverse()
        return "".join(chunks)
    
    def FromStr(self, s: str, l: int=8):
        i = int(sha256(s.encode("utf-8", "replace")).hexdigest(), 16)
        return self.FromInt(i, l)
    
    def FromHex(self, hex: str, l: int=8, little_endian=False):
        return self.FromInt(int(hex, 16), l, little_endian)
