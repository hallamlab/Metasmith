import numpy as np
from hashlib import sha256

_ASCII_VOCAB_62 = [(48, 57), (65, 90), (97, 122)]
_ASCII_VOCAB_62 = [i for a, b in _ASCII_VOCAB_62 for i in list(range(a, b+1))]
_ASCII_VOCAB_62 = [chr(i) for i in _ASCII_VOCAB_62] # +["+", "!"]
class KeyGenerator:
    vocab = _ASCII_VOCAB_62
    def __init__(self, seed=None) -> None:
        self._generator = np.random.default_rng(seed)

    def GenerateUID(self, l:int=8, blacklist: set[str]=set()) -> str:
        key: str|None = None
        while key is None or key in blacklist:
            digits = self._generator.integers(0, len(self.vocab), l)
            key = "".join([self.vocab[i] for i in digits])
        return key
    
    @classmethod
    def FromInt(cls, i: int, l: int=8, little_endian=False):
        chunks = [cls.vocab[0]]*l
        place = 0
        while i > 0 and place < l:
            chunk_k = i % len(cls.vocab)
            i = (i - chunk_k) // len(cls.vocab)
            chunks[place] = cls.vocab[chunk_k]
            place += 1
        if not little_endian: chunks.reverse()
        return "".join(chunks)
    
    @classmethod
    def FromHex(cls, hex: str, l: int=8, little_endian=False):
        i = int(hex, 16)
        return i, cls.FromInt(i, l, little_endian)

    @classmethod
    def FromStr(cls, s: str, l: int=8, little_endian=False):
        _hex = sha256(s.encode("utf-8", "replace")).hexdigest()
        return cls.FromHex(_hex, l, little_endian)
