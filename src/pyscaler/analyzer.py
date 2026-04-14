"""Static AST analysis: find data-parallel loops, detect parallelism blockers."""
from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class LoopCandidate:
    """A fan-out pattern — one function call per element of an iterable.

    Unifies four source shapes, all convertible to
    `[result =] ray.get([f.remote(*vars) for *vars in iter_expr])`:

      kind="for_loop"  : for x in xs: f(x)
      kind="zip_loop"  : for x, y in zip(a, b): f(x, y)
      kind="list_comp" : results = [f(x) for x in xs]
      kind="map_call"  : results = list(map(f, xs))
    """
    line: int
    kind: str                # for_loop | zip_loop | list_comp | map_call
    iter_name: str           # the original iterable expression (unparsed)
    var_names: list[str]     # loop variables, in order (len 1 for most, >1 for zip)
    call_func: str           # function being called on each element
    assign_target: str | None = None  # None for bare statement; variable name if result is assigned

    # Back-compat: older code uses .var_name (singular)
    @property
    def var_name(self) -> str:
        return self.var_names[0] if self.var_names else ""


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


def _collect_bodies(tree: ast.Module) -> list[list[ast.stmt]]:
    """Return module-top body + __main__ body, since those are the candidate scopes."""
    bodies: list[list[ast.stmt]] = [tree.body]
    for node in tree.body:
        if (
            isinstance(node, ast.If)
            and isinstance(node.test, ast.Compare)
            and isinstance(node.test.left, ast.Name)
            and node.test.left.id == "__name__"
        ):
            bodies.append(node.body)
    return bodies


def _call_uses_all_vars(call: ast.Call, var_names: list[str]) -> bool:
    """Every loop variable must appear as a positional arg of the call."""
    arg_names = {a.id for a in call.args if isinstance(a, ast.Name)}
    return all(v in arg_names for v in var_names)


def _find_parallel_loop(tree: ast.Module) -> LoopCandidate | None:
    """Match four fan-out shapes. First one found wins."""
    for body in _collect_bodies(tree):
        for stmt in body:
            # Shape 1 & 2: for-loop (single var) or for-with-zip (tuple unpack)
            if isinstance(stmt, ast.For) and len(stmt.body) == 1:
                cand = _match_for_loop(stmt)
                if cand:
                    return cand
            # Shape 3: results = [f(x) for x in xs]
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 \
                    and isinstance(stmt.targets[0], ast.Name):
                cand = _match_list_comp(stmt)
                if cand:
                    return cand
                cand = _match_map_call(stmt)
                if cand:
                    return cand
    return None


def _match_for_loop(stmt: ast.For) -> LoopCandidate | None:
    """for x in xs: f(x)        →  for_loop
    for x, y in zip(a, b): f(x, y)  →  zip_loop
    Body must be a single call that uses every loop variable.
    """
    # Single var: `for x in xs:`
    var_names: list[str]
    kind: str
    iter_expr = stmt.iter
    if isinstance(stmt.target, ast.Name):
        var_names = [stmt.target.id]
        # If iter is `zip(...)` with a Name target, that's invalid Python; skip
        kind = "for_loop"
    elif isinstance(stmt.target, ast.Tuple) and all(isinstance(e, ast.Name) for e in stmt.target.elts):
        # Only consider zip unpack if iter IS a zip(...) call
        if not (isinstance(iter_expr, ast.Call)
                and isinstance(iter_expr.func, ast.Name)
                and iter_expr.func.id == "zip"):
            return None
        var_names = [e.id for e in stmt.target.elts]  # type: ignore[union-attr]
        kind = "zip_loop"
    else:
        return None

    first = stmt.body[0]
    call: ast.Call | None = None
    assign_target: str | None = None
    if isinstance(first, ast.Expr) and isinstance(first.value, ast.Call):
        call = first.value
    elif isinstance(first, ast.Assign) and len(first.targets) == 1 \
            and isinstance(first.targets[0], ast.Name) \
            and isinstance(first.value, ast.Call):
        call = first.value
        # intra-loop assignment is discarded when we fan out; skip if it looks intentional
        # (the user probably wanted to collect results — tell them to use list-comp instead)
        return None
    if call is None or not isinstance(call.func, ast.Name):
        return None
    if not _call_uses_all_vars(call, var_names):
        return None

    return LoopCandidate(
        line=stmt.lineno,
        kind=kind,
        iter_name=ast.unparse(iter_expr),
        var_names=var_names,
        call_func=call.func.id,
        assign_target=assign_target,
    )


def _match_list_comp(stmt: ast.Assign) -> LoopCandidate | None:
    """results = [f(x) for x in xs]  →  list_comp
    Only accept single-generator, single-var comprehensions where the elt is a pure call.
    """
    target = stmt.targets[0]
    if not isinstance(target, ast.Name):
        return None
    value = stmt.value
    if not isinstance(value, ast.ListComp) or len(value.generators) != 1:
        return None
    gen = value.generators[0]
    if gen.ifs or gen.is_async:
        return None
    if not isinstance(gen.target, ast.Name):
        return None
    if not isinstance(value.elt, ast.Call) or not isinstance(value.elt.func, ast.Name):
        return None
    var = gen.target.id
    if not _call_uses_all_vars(value.elt, [var]):
        return None
    return LoopCandidate(
        line=stmt.lineno,
        kind="list_comp",
        iter_name=ast.unparse(gen.iter),
        var_names=[var],
        call_func=value.elt.func.id,
        assign_target=target.id,
    )


def _match_map_call(stmt: ast.Assign) -> LoopCandidate | None:
    """results = list(map(f, xs))  →  map_call (strictly list(map(...)), 2-arg map)."""
    target = stmt.targets[0]
    if not isinstance(target, ast.Name):
        return None
    v = stmt.value
    if not (isinstance(v, ast.Call)
            and isinstance(v.func, ast.Name)
            and v.func.id == "list"
            and len(v.args) == 1
            and isinstance(v.args[0], ast.Call)):
        return None
    inner = v.args[0]
    if not (isinstance(inner.func, ast.Name) and inner.func.id == "map"):
        return None
    if len(inner.args) != 2 or not isinstance(inner.args[0], ast.Name):
        return None
    return LoopCandidate(
        line=stmt.lineno,
        kind="map_call",
        iter_name=ast.unparse(inner.args[1]),
        var_names=["_x"],  # synthetic — we'll emit [f.remote(_x) for _x in xs]
        call_func=inner.args[0].id,
        assign_target=target.id,
    )


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
        pretty = {
            "for_loop": "for-loop",
            "zip_loop": "for/zip loop",
            "list_comp": "list comprehension",
            "map_call": "list(map(...))",
        }.get(loop.kind, loop.kind)
        return Analysis(
            path=Path(path),
            pattern="parallel_loop",
            loop=loop,
            recommended_framework="ray",
            notes=[
                f"{pretty} on line {loop.line} calls `{loop.call_func}(...)` over "
                f"`{loop.iter_name}` — good candidate for ray.remote fan-out."
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
