"""Build experiments.html from experiments.src.html, the tool table and the DAG SVGs."""
import re
from pathlib import Path

import tool_table

HERE = Path(__file__).resolve().parent

src = (HERE / "experiments.src.html").read_text()
src = tool_table.insert(src)
svg = lambda key: (lambda s: s[s.index("<svg"):])((HERE / "dags" / f"{key}.dag.svg").read_text())
src = re.sub(r"\{\{SVG:(\w+)\}\}", lambda m: svg(m.group(1)), src)
assert "{{" not in src, "unfilled placeholder"
(HERE / "experiments.html").write_text(src)
print(HERE / "experiments.html")
