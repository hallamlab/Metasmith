# A local-LLM chat client that cannot return malformed JSON, and that bills every call.
#
# Vendored from `index_scrape.llm` on capella (which is now offline; the rescued copy
# lives at ~/capella-rescue/index-scrape). Two things changed, and one deliberately
# did not.
#
# **The constraint moved dialects.** capella served vLLM and used its `guided_json`
# extension; fir serves llama.cpp, which spells the same capability
# `response_format: {"type": "json_schema", ...}`. Both are supported here because the
# choice is a property of the server, not of the caller — `LLMConfig.dialect` picks
# one and nothing else in the package knows which is in use. The capability is the
# point: under constrained decoding a response cannot be malformed or wrongly shaped,
# so a parse failure is a harness bug rather than a scored chemistry outcome.
#
# **Qwen3 thinks out loud by default**, and the preamble is billed. `enable_thinking`
# is off by default for that reason, and `_extract_json` still tolerates a preamble
# if some server ignores the flag — a response that cost tokens should not also cost
# the reaction.
#
# **Usage is returned, not logged.** Every call hands back `prompt_tokens`,
# `completion_tokens` and wall-clock alongside the parsed object, so cost per reaction
# is measured from the first run rather than reconstructed afterwards. A caller that
# drops it is choosing to; a client that never reported it would leave no choice.
#
# `RetryableHTTP` is redefined here rather than imported, and the tenacity backoff is
# hand-rolled: capella's `http` module drags in a SQLite response cache this package
# has no use for, and tenacity is in none of this workspace's environments — an
# exponential sleep is ten lines and not worth a shared-env install. A 400 is
# `SchemaRejected` and is NOT retried, since a server refusing to compile a schema
# will refuse it again; 429/5xx and transport errors back off.
#
# httpx lives in `ecspr` and `msm` but not in `rdkit-scratch`, and rdkit lives only in
# `rdkit-scratch`. So this client and `arbiter.py` cannot share a process, which is
# why the runner writes JSONL and the arbiter reads it rather than calling it.

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field

import httpx


class RetryableHTTP(Exception):
    # 429/5xx — transient, so tenacity backs off rather than giving up.
    pass


class SchemaRejected(Exception):
    # 400 — the server will not compile this request. Retrying repeats it.
    pass


class BadJSON(Exception):
    # Content did not parse despite constrained decoding: a harness bug, not a result.
    pass


@dataclass
class LLMConfig:
    base_url: str                     # e.g. http://127.0.0.1:8080/v1
    model: str                        # served id; llama.cpp accepts any string
    alias: str                        # short tag recorded in the scoreboard
    dialect: str = "llamacpp"         # llamacpp -> response_format; vllm -> guided_json
    max_tokens: int = 4096
    temperature: float = 0.0
    timeout: float = 900.0
    enable_thinking: bool = False
    extra_body: dict = field(default_factory=dict)


@dataclass
class Usage:
    prompt_tokens: int
    completion_tokens: int
    seconds: float

    def __add__(self, other: "Usage") -> "Usage":
        return Usage(self.prompt_tokens + other.prompt_tokens,
                     self.completion_tokens + other.completion_tokens,
                     self.seconds + other.seconds)


ZERO_USAGE = Usage(0, 0, 0.0)

_FENCE = re.compile(r"```(?:json)?\s*(.*?)```", re.S)


def _extract_json(text: str) -> str:
    # The JSON in `text`, whether or not the model wrapped it in prose or a fence.
    #
    # Constrained decoding should make this the identity function. It exists because
    # a server that silently ignores the constraint would otherwise cost every
    # reaction in the batch, and because Qwen3's reasoning preamble is emitted before
    # the object rather than in a separate field on some builds.
    s = text.strip()
    if s.startswith("{") or s.startswith("["):
        return s
    m = _FENCE.search(s)
    if m:
        return m.group(1).strip()
    start = min((i for i in (s.find("{"), s.find("[")) if i >= 0), default=-1)
    if start < 0:
        return s
    opener = s[start]
    closer = {"{": "}", "[": "]"}[opener]
    depth, in_str, esc = 0, False, False
    for i in range(start, len(s)):
        c = s[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == opener:
            depth += 1
        elif c == closer:
            depth -= 1
            if depth == 0:
                return s[start:i + 1]
    return s[start:]


class LLMClient:
    def __init__(self, cfg: LLMConfig, *, transport: httpx.BaseTransport | None = None):
        self.cfg = cfg
        self._client = httpx.Client(base_url=cfg.base_url, timeout=cfg.timeout,
                                    transport=transport)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        self._client.close()

    def health(self) -> bool:
        try:
            return self._client.get("/models").status_code == 200
        except httpx.HTTPError:
            return False

    def wait_for_server(self, *, attempts: int = 240, delay: float = 5.0) -> bool:
        # Poll `/models` until it answers — a 23 GB model takes minutes to load.
        for _ in range(attempts):
            if self.health():
                return True
            time.sleep(delay)
        return False

    def _post_once(self, payload: dict) -> dict:
        resp = self._client.post("/chat/completions", json=payload)
        if resp.status_code == 400:
            raise SchemaRejected(resp.text[:500])
        if resp.status_code == 429 or resp.status_code >= 500:
            raise RetryableHTTP(f"{resp.status_code} from /chat/completions")
        resp.raise_for_status()
        return resp.json()

    def _post_chat(self, payload: dict, *, attempts: int = 5) -> dict:
        # `_post_once` with exponential backoff on the transient failures only.
        for i in range(attempts):
            try:
                return self._post_once(payload)
            except (RetryableHTTP, httpx.TransportError):
                if i == attempts - 1:
                    raise
                time.sleep(min(60.0, 2.0 * 1.5 ** i))
        raise AssertionError("unreachable")

    def _constrain(self, payload: dict, schema: dict, name: str) -> None:
        if self.cfg.dialect == "vllm":
            payload["guided_json"] = schema
        else:
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": name, "schema": schema, "strict": True},
            }

    def complete_json(self, *, system: str, user: str, schema: dict,
                      name: str = "response") -> tuple[dict, Usage]:
        # One chat completion constrained to `schema`, with its cost.
        payload = {
            "model": self.cfg.model,
            "temperature": self.cfg.temperature,
            "max_tokens": self.cfg.max_tokens,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        self._constrain(payload, schema, name)
        if not self.cfg.enable_thinking:
            payload["chat_template_kwargs"] = {"enable_thinking": False}
        payload.update(self.cfg.extra_body)

        t0 = time.perf_counter()
        data = self._post_chat(payload)
        seconds = time.perf_counter() - t0

        u = data.get("usage") or {}
        usage = Usage(int(u.get("prompt_tokens", 0)),
                      int(u.get("completion_tokens", 0)), seconds)
        try:
            content = data["choices"][0]["message"]["content"]
            obj = json.loads(_extract_json(content))
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as e:
            raise BadJSON(f"{e}; usage={usage}") from e
        return obj, usage
