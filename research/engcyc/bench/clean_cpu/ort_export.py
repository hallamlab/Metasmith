"""Export ESM-1b's layer-33 representation to ONNX, then write ONNX Runtime dynamic int8 copies.

Usage: python ort_export.py <slim or shipped checkpoint> <out dir> [<calibration FASTA>]
Writes <out>/esm1b_fp32.onnx (+ one external-data file), esm1b_int8pc.onnx (per-channel weights) and
esm1b_int8pt.onnx (per-tensor weights). With a calibration FASTA it also folds SmoothQuant scales
(quant.smooth, 32 sequences, seed 1007) into a second export and writes esm1b_sqall.onnx (every MatMul
int8) and esm1b_sqpart.onnx (out_proj and fc2, whose inputs have no LayerNorm to fold into, left fp32).
Needs the ONNX overlay (onnx, onnxruntime, protobuf) on PYTHONPATH.
"""
import os
import sys

import random

import esm
import onnx
import torch
from onnxruntime.quantization import QuantType, quantize_dynamic

ckpt, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
model, alphabet = esm.pretrained.load_model_and_alphabet_local(ckpt)
model.eval()
for m in model.modules():
    if type(m).__name__ == "MultiheadAttention":
        m.onnx_trace = True


class Rep(torch.nn.Module):
    def __init__(self, m):
        super().__init__()
        self.m = m

    def forward(self, toks):
        return self.m(toks, repr_layers=[33])["representations"][33]


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
convert = alphabet.get_batch_converter()
# Two sequences of different length, so the trace keeps the padding-mask branch.
_, _, toks = convert([("a", "MKTAYIAKQRQISFVKSHFSRQ"), ("b", "MKTAYIAKQR")])


def export(tag):
    raw = f"{out}/raw_{tag}"
    os.makedirs(raw, exist_ok=True)
    with torch.no_grad():
        torch.onnx.export(Rep(model), (toks,), f"{raw}/esm1b.onnx", input_names=["toks"], output_names=["rep"],
                          dynamic_axes={"toks": {0: "b", 1: "L"}, "rep": {0: "b", 1: "L"}}, opset_version=17)
    m = onnx.load(f"{raw}/esm1b.onnx")
    onnx.save_model(m, f"{out}/esm1b_{tag}.onnx", save_as_external_data=True, all_tensors_to_one_file=True,
                    location=f"esm1b_{tag}.onnx.data")
    return m


export("fp32")
for name, pc in (("int8pc", True), ("int8pt", False)):
    quantize_dynamic(f"{out}/esm1b_fp32.onnx", f"{out}/esm1b_{name}.onnx", per_channel=pc,
                     weight_type=QuantType.QInt8, use_external_data_format=False)
if len(sys.argv) > 3:
    from embed import read_fasta, TRUNC
    import quant
    items = random.Random(1007).sample(list(read_fasta(sys.argv[3])), 32)
    calib = [convert([(n, q[:TRUNC]) for n, q in items[i:i + 8]])[2] for i in range(0, 32, 8)]
    for mod in model.modules():
        if type(mod).__name__ == "MultiheadAttention":
            mod.onnx_trace = False
    quant.smooth(model, calib)
    for mod in model.modules():
        if type(mod).__name__ == "MultiheadAttention":
            mod.onnx_trace = True
    g = export("sq_fp32")
    skip = [n.name for n in g.graph.node if n.op_type == "MatMul" and ("fc2" in n.name or "out_proj" in n.name)]
    print("sqpart leaves", len(skip), "MatMuls fp32, e.g.", skip[:2])
    quantize_dynamic(f"{out}/esm1b_sq_fp32.onnx", f"{out}/esm1b_sqall.onnx", per_channel=True,
                     weight_type=QuantType.QInt8, use_external_data_format=False)
    quantize_dynamic(f"{out}/esm1b_sq_fp32.onnx", f"{out}/esm1b_sqpart.onnx", per_channel=True,
                     weight_type=QuantType.QInt8, use_external_data_format=False, nodes_to_exclude=skip)
for f in sorted(os.listdir(out)):
    if os.path.isfile(f"{out}/{f}"):
        print(f, os.path.getsize(f"{out}/{f}"))
