"""Ensure all example scripts are syntactically valid Python."""
import ast
from pathlib import Path

EXAMPLES = Path(__file__).parent.parent / "examples"


def test_all_examples_parse():
    py_files = list(EXAMPLES.glob("*.py"))
    assert len(py_files) >= 3, "expected at least 3 example scripts"
    for path in py_files:
        src = path.read_text()
        ast.parse(src, filename=str(path))
