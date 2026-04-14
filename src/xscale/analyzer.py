"""Static AST analysis: find data-parallel loops, detect parallelism blockers."""
from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class LoopCandidate:
    """A top-level for-loop that looks like `for x in items: f(x)`."""
    line: int
    iter_name: str           # e.g. "files"
    var_name: str            # e.g. "f"
    call_func: str           # e.g. "process_file"


@dataclass
class DataFrameApply:
    """A statement like `df[col] = df.apply(func, axis=1)`."""
    line: int
    df_name: str             # "df"
    target_col: str          # "score"
    func_name: str           # "compute_score"


@dataclass
class Analysis:
    path: Path
    pattern: str                              # parallel_loop | dataframe_apply | unknown
    loop: LoopCandidate | None = None
    df_apply: DataFrameApply | None = None
    blockers: list[str] = field(default_factory=list)
    recommended_framework: str = "ray"
    notes: list[str] = field(default_factory=list)

    @property
    def supports_parallel(self) -> bool:
        return self.pattern != "unknown" and not self.blockers


def _find_parallel_loop(tree: ast.Module) -> LoopCandidate | None:
    """Look for `for x in items: f(x)` inside `if __name__ == '__main__':` or at module top."""
    bodies: list[list[ast.stmt]] = [tree.body]
    for node in tree.body:
        if (
            isinstance(node, ast.If)
            and isinstance(node.test, ast.Compare)
            and isinstance(node.test.left, ast.Name)
            and node.test.left.id == "__name__"
        ):
            bodies.append(node.body)

    for body in bodies:
        for stmt in body:
            if not isinstance(stmt, ast.For):
                continue
            if not isinstance(stmt.target, ast.Name):
                continue
            if len(stmt.body) != 1:
                continue
            first = stmt.body[0]
            # Accept either `f(x)` (Expr→Call) or `y = f(x)` (Assign→Call)
            call = None
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Call):
                call = first.value
            elif isinstance(first, ast.Assign) and isinstance(first.value, ast.Call):
                call = first.value
            if call is None or not isinstance(call.func, ast.Name):
                continue
            # Ensure it passes the loop variable (anywhere in args)
            if not any(isinstance(a, ast.Name) and a.id == stmt.target.id for a in call.args):
                continue
            iter_name = ast.unparse(stmt.iter)
            return LoopCandidate(
                line=stmt.lineno,
                iter_name=iter_name,
                var_name=stmt.target.id,
                call_func=call.func.id,
            )
    return None


def _find_global_mutation_blockers(tree: ast.Module) -> list[str]:
    """Detect functions that mutate module-level names → parallelism blocker."""
    module_names = {
        n.targets[0].id
        for n in tree.body
        if isinstance(n, ast.Assign)
        and len(n.targets) == 1
        and isinstance(n.targets[0], ast.Name)
    }
    blockers: list[str] = []
    for fn in (n for n in tree.body if isinstance(n, ast.FunctionDef)):
        for sub in ast.walk(fn):
            # explicit `global x`
            if isinstance(sub, ast.Global):
                blockers.append(f"function `{fn.name}` declares `global {', '.join(sub.names)}`")
            # mutate module-level dict/list via subscript: counter["k"] = ...
            if isinstance(sub, ast.Assign):
                for tgt in sub.targets:
                    if (
                        isinstance(tgt, ast.Subscript)
                        and isinstance(tgt.value, ast.Name)
                        and tgt.value.id in module_names
                    ):
                        blockers.append(
                            f"function `{fn.name}` mutates module-level `{tgt.value.id}` at line {sub.lineno}"
                        )
            # counter["k"] += 1
            if isinstance(sub, ast.AugAssign):
                tgt = sub.target
                if (
                    isinstance(tgt, ast.Subscript)
                    and isinstance(tgt.value, ast.Name)
                    and tgt.value.id in module_names
                ):
                    blockers.append(
                        f"function `{fn.name}` mutates module-level `{tgt.value.id}` at line {sub.lineno}"
                    )
    return blockers


def _find_dataframe_apply(tree: ast.Module) -> DataFrameApply | None:
    """Look for `df[col] = df.apply(func, axis=1)` at module top or __main__."""
    bodies: list[list[ast.stmt]] = [tree.body]
    for node in tree.body:
        if (
            isinstance(node, ast.If)
            and isinstance(node.test, ast.Compare)
            and isinstance(node.test.left, ast.Name)
            and node.test.left.id == "__name__"
        ):
            bodies.append(node.body)

    for body in bodies:
        for stmt in body:
            if not isinstance(stmt, ast.Assign) or len(stmt.targets) != 1:
                continue
            tgt = stmt.targets[0]
            # df[col] = ...
            if not (isinstance(tgt, ast.Subscript) and isinstance(tgt.value, ast.Name)):
                continue
            df_name = tgt.value.id
            col_node = tgt.slice if not isinstance(tgt.slice, ast.Index) else tgt.slice.value  # type: ignore[attr-defined]
            if isinstance(col_node, ast.Constant) and isinstance(col_node.value, str):
                target_col = col_node.value
            else:
                continue
            # RHS: df.apply(func, axis=1)
            call = stmt.value
            if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)):
                continue
            if call.func.attr != "apply":
                continue
            if not (isinstance(call.func.value, ast.Name) and call.func.value.id == df_name):
                continue
            if not call.args or not isinstance(call.args[0], ast.Name):
                continue
            return DataFrameApply(
                line=stmt.lineno,
                df_name=df_name,
                target_col=target_col,
                func_name=call.args[0].id,
            )
    return None


def analyze_file(path: Path) -> Analysis:
    source = Path(path).read_text()
    tree = ast.parse(source, filename=str(path))

    blockers = _find_global_mutation_blockers(tree)
    loop = _find_parallel_loop(tree)
    df_apply = _find_dataframe_apply(tree)

    if blockers:
        return Analysis(
            path=Path(path),
            pattern="unknown",
            loop=loop,
            df_apply=df_apply,
            blockers=blockers,
            recommended_framework="ray",
            notes=["Shared mutable state would cause races under parallel execution."],
        )
    if loop:
        return Analysis(
            path=Path(path),
            pattern="parallel_loop",
            loop=loop,
            recommended_framework="ray",
            notes=[
                f"Top-level for-loop on line {loop.line} calls `{loop.call_func}({loop.var_name})` "
                f"over `{loop.iter_name}` — good candidate for ray.remote fan-out."
            ],
        )
    if df_apply:
        return Analysis(
            path=Path(path),
            pattern="dataframe_apply",
            df_apply=df_apply,
            recommended_framework="ray",
            notes=[
                f"DataFrame apply on line {df_apply.line}: `{df_apply.df_name}[{df_apply.target_col!r}] "
                f"= {df_apply.df_name}.apply({df_apply.func_name}, axis=1)` — can be chunked across workers."
            ],
        )
    return Analysis(
        path=Path(path),
        pattern="unknown",
        notes=["No obvious data-parallel pattern found at module top-level."],
    )
