"""Check that the generated readout contains executed, error-free consistency checks."""
import hashlib
import json
from pathlib import Path

import nbformat
from PIL import Image


root = Path(__file__).resolve().parents[1]
notebook = nbformat.read(root / "notebooks/ab_test_readout.ipynb", as_version=4)
nbformat.validate(notebook)
code = [cell for cell in notebook.cells if cell.cell_type == "code"]
if not code or any(cell.execution_count is None for cell in code):
    raise ValueError("notebook has unexecuted cells")
outputs = [output for cell in code for output in cell.outputs]
if any(output.output_type == "error" for output in outputs):
    raise ValueError("notebook contains an execution error")
text = "\n".join(output.get("text", "") for output in outputs)
if "All notebook/report consistency checks passed." not in text:
    raise ValueError("notebook did not finish its report consistency checks")
paths = [root / "reports/results.json", root / "notebooks/ab_test_readout.ipynb",
         root / "notebooks/ab_test_readout.html", *sorted((root / "reports/figures").glob("*.png"))]
for path in paths:
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"missing output: {path.name}")
figures = {}
for path in (root / "reports/figures").glob("*.png"):
    with Image.open(path) as figure:
        figures[path.name] = list(figure.size)
report = {
    "executed_notebook_cells": len(code), "notebook_errors": 0,
    "consistency_checks": "fixed tests, absolute/relative bootstrap CIs, CUPED, sequential replay",
    "figures": figures,
    "sha256": {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths},
}
(root / "reports/validation.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report, indent=2))
