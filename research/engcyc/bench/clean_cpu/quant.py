"""Precision and quantisation variants of ESM-1b for embed.py.

prepare(model, mode, calib) returns the model to run and the autocast context to run it under.
`calib` is a list of token tensors used only by the static or smoothed modes.

Modes:
  fp32, bf16            stock weights, bf16 under CPU autocast
  bf16pure              bf16 weights and activations without autocast, LayerNorms in fp32
  int8, int8ffn         torch dynamic int8, per-tensor weights (round 1)
  int8pc                torch dynamic int8, per-channel weights, every Linear
  pc_fc1, pc_ffn        per-channel dynamic int8 on fc1, or on fc1 and fc2; the rest bf16
  sq_int8               SmoothQuant (alpha 0.5) folded into the LayerNorms before q/k/v and fc1,
                        then per-channel dynamic int8 on q/k/v/fc1; out_proj, fc2 and the rest bf16
  ao_int8wo, ao_int4wo  torchao weight-only int8 / int4 (group 128), bf16 activations (torch 2.5)
  ao_int8dyn            torchao int8 per-token activations with int8 per-channel weights (torch 2.5)
"""
import contextlib

import torch
from torch import nn

LAYER_LINEARS = ("self_attn.q_proj", "self_attn.k_proj", "self_attn.v_proj", "self_attn.out_proj", "fc1", "fc2")


class Float32In(nn.Module):
    """Runs a quantised module on fp32 input outside autocast, so it can sit inside a bf16 model."""

    def __init__(self, inner):
        super().__init__()
        self.inner = inner

    def forward(self, x):
        with torch.autocast("cpu", enabled=False):
            return self.inner(x.float())


def per_projection_attention(model):
    # A quantised Linear's .bias is a method and its weight is packed, which ESM's fused attention
    # call cannot take; onnx_trace routes attention through the per-projection calls instead.
    for m in model.modules():
        if type(m).__name__ == "MultiheadAttention":
            m.onnx_trace = True


def layer_linears(model, suffixes):
    return [f"layers.{i}.{s}" for i in range(len(model.layers)) for s in suffixes]


def dynamic_pc(model, names):
    from torch.ao.quantization import per_channel_dynamic_qconfig, quantize_dynamic
    model = quantize_dynamic(model, {n: per_channel_dynamic_qconfig for n in names}, dtype=torch.qint8)
    for n in names:
        parent, _, leaf = n.rpartition(".")
        owner = model.get_submodule(parent)
        setattr(owner, leaf, Float32In(getattr(owner, leaf)))
    return model


def bf16_weights(model):
    # ESM-1b's token-dropout rescale divides by an fp32 ratio, which promotes the activations back to
    # fp32 before the first LayerNorm; LayerNorms therefore stay fp32 and hand bf16 on.
    model = model.bfloat16()
    for m in model.modules():
        if isinstance(m, nn.LayerNorm):
            m.float()
            m.register_forward_pre_hook(lambda _, inp: (inp[0].float(),))
            m.register_forward_hook(lambda _, __, out: out.bfloat16())
    return model


@torch.no_grad()
def smooth(model, calib, alpha=0.5):
    stats = {}

    def hook(name):
        def f(_, __, out):
            m = out.detach().abs().reshape(-1, out.shape[-1]).amax(0).float()
            stats[name] = torch.maximum(stats[name], m) if name in stats else m
        return f

    hs = []
    for i, layer in enumerate(model.layers):
        hs.append(layer.self_attn_layer_norm.register_forward_hook(hook(f"{i}.attn")))
        hs.append(layer.final_layer_norm.register_forward_hook(hook(f"{i}.ffn")))
    for toks in calib:
        model(toks, repr_layers=[33])
    for h in hs:
        h.remove()
    for i, layer in enumerate(model.layers):
        groups = ((layer.self_attn_layer_norm, [layer.self_attn.q_proj, layer.self_attn.k_proj, layer.self_attn.v_proj], "attn"),
                  (layer.final_layer_norm, [layer.fc1], "ffn"))
        for ln, lins, key in groups:
            act = stats[f"{i}.{key}"].clamp(min=1e-5)
            w = torch.stack([l.weight.abs().amax(0) for l in lins]).amax(0).clamp(min=1e-5)
            s = (act.pow(alpha) / w.pow(1 - alpha)).clamp(min=1e-5)
            ln.weight.div_(s)
            ln.bias.div_(s)
            for l in lins:
                l.weight.mul_(s)


def prepare(model, mode, calib=()):
    bf16 = torch.autocast("cpu", dtype=torch.bfloat16)
    off = contextlib.nullcontext()
    if mode == "fp32":
        return model, off
    if mode == "bf16":
        return model, bf16
    if mode == "bf16pure":
        return bf16_weights(model), off
    if mode == "int8":
        per_projection_attention(model)
        return torch.ao.quantization.quantize_dynamic(model, {nn.Linear}, dtype=torch.qint8), off
    if mode == "int8ffn":
        names = {n for n, _ in model.named_modules() if n.endswith((".fc1", ".fc2"))}
        return torch.ao.quantization.quantize_dynamic(model, names, dtype=torch.qint8), off
    if mode == "int8pc":
        per_projection_attention(model)
        return dynamic_pc(model, layer_linears(model, LAYER_LINEARS)), off
    if mode == "pc_fc1":
        return dynamic_pc(model, layer_linears(model, ("fc1",))), bf16
    if mode == "pc_ffn":
        return dynamic_pc(model, layer_linears(model, ("fc1", "fc2"))), bf16
    if mode == "sq_int8":
        smooth(model, calib)
        per_projection_attention(model)
        names = layer_linears(model, ("self_attn.q_proj", "self_attn.k_proj", "self_attn.v_proj", "fc1"))
        return dynamic_pc(model, names), bf16
    if mode.startswith("ao_"):
        from torchao.quantization import (int4_weight_only, int8_dynamic_activation_int8_weight,
                                          int8_weight_only, quantize_)
        per_projection_attention(model)
        model = bf16_weights(model)
        names = set(layer_linears(model, LAYER_LINEARS))
        scheme = {"ao_int8wo": int8_weight_only, "ao_int4wo": lambda: int4_weight_only(group_size=128),
                  "ao_int8dyn": int8_dynamic_activation_int8_weight}[mode]()
        quantize_(model, scheme, filter_fn=lambda m, fqn: fqn in names)
        return model, off
    raise ValueError(mode)


MODES = ["fp32", "bf16", "bf16pure", "int8", "int8ffn", "int8pc", "pc_fc1", "pc_ffn", "sq_int8",
         "ao_int8wo", "ao_int4wo", "ao_int8dyn"]
