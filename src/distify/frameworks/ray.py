"""Ray framework plugin — AST-based conversion for parallel-loop pattern."""
from __future__ import annotations

import ast
import difflib

from .base import ConversionResult, Framework


class RayFramework(Framework):
    name = "ray"

    def supports(self, analysis: dict) -> bool:
        return analysis.get("pattern") == "parallel_loop"

    def runtime_dependencies(self) -> list[str]:
        return ["ray[default]>=2.9"]

    def convert(self, source: str, analysis: dict, workers: int = 8) -> ConversionResult:
        if analysis.get("pattern") != "parallel_loop":
            raise NotImplementedError(
                f"Ray converter can't handle pattern={analysis.get('pattern')!r} yet."
            )
        loop = analysis["loop"]
        call_func = loop["call_func"]
        var_name = loop["var_name"]
        iter_name = loop["iter_name"]

        tree = ast.parse(source)
        self._decorate_function(tree, call_func)
        self._replace_loop(tree, call_func, var_name, iter_name)
        self._ensure_ray_import_and_init(tree, workers)

        converted = ast.unparse(tree) + "\n"
        diff = "".join(
            difflib.unified_diff(
                source.splitlines(keepends=True),
                converted.splitlines(keepends=True),
                fromfile="original",
                tofile="converted",
                n=3,
            )
        )
        summary = {
            "framework": "ray",
            "pattern": "parallel_loop",
            "decorated_function": call_func,
            "iter": iter_name,
            "workers": workers,
        }
        return ConversionResult(source=source, converted=converted, diff=diff, summary=summary)

    # ---------- AST passes ----------

    @staticmethod
    def _decorate_function(tree: ast.Module, name: str) -> None:
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name == name:
                already = any(
                    isinstance(d, ast.Attribute) and d.attr == "remote" for d in node.decorator_list
                )
                if not already:
                    node.decorator_list.insert(
                        0,
                        ast.Attribute(value=ast.Name(id="ray", ctx=ast.Load()), attr="remote", ctx=ast.Load()),
                    )

    @staticmethod
    def _replace_loop(tree: ast.Module, call_func: str, var_name: str, iter_name: str) -> None:
        """Replace `for x in items: call_func(x)` with
        `ray.get([call_func.remote(x) for x in items])`.
        """
        iter_expr = ast.parse(iter_name, mode="eval").body
        new_stmt = ast.Expr(
            value=ast.Call(
                func=ast.Attribute(value=ast.Name(id="ray", ctx=ast.Load()), attr="get", ctx=ast.Load()),
                args=[
                    ast.ListComp(
                        elt=ast.Call(
                            func=ast.Attribute(
                                value=ast.Name(id=call_func, ctx=ast.Load()),
                                attr="remote",
                                ctx=ast.Load(),
                            ),
                            args=[ast.Name(id=var_name, ctx=ast.Load())],
                            keywords=[],
                        ),
                        generators=[
                            ast.comprehension(
                                target=ast.Name(id=var_name, ctx=ast.Store()),
                                iter=iter_expr,
                                ifs=[],
                                is_async=0,
                            )
                        ],
                    )
                ],
                keywords=[],
            )
        )

        def replace_in(body: list[ast.stmt]) -> None:
            for i, stmt in enumerate(body):
                if (
                    isinstance(stmt, ast.For)
                    and isinstance(stmt.target, ast.Name)
                    and stmt.target.id == var_name
                    and isinstance(stmt.iter, ast.AST)
                    and ast.unparse(stmt.iter) == iter_name
                ):
                    body[i] = new_stmt
                    return

        replace_in(tree.body)
        for node in tree.body:
            if (
                isinstance(node, ast.If)
                and isinstance(node.test, ast.Compare)
                and isinstance(node.test.left, ast.Name)
                and node.test.left.id == "__name__"
            ):
                replace_in(node.body)

    @staticmethod
    def _ensure_ray_import_and_init(tree: ast.Module, workers: int) -> None:
        has_import = any(
            isinstance(n, ast.Import) and any(a.name == "ray" for a in n.names) for n in tree.body
        )
        if not has_import:
            tree.body.insert(0, ast.Import(names=[ast.alias(name="ray", asname=None)]))

        def has_ray_init(body: list[ast.stmt]) -> bool:
            for stmt in body:
                if (
                    isinstance(stmt, ast.Expr)
                    and isinstance(stmt.value, ast.Call)
                    and isinstance(stmt.value.func, ast.Attribute)
                    and isinstance(stmt.value.func.value, ast.Name)
                    and stmt.value.func.value.id == "ray"
                    and stmt.value.func.attr == "init"
                ):
                    return True
            return False

        ray_init_stmt = ast.parse(
            f"ray.init(ignore_reinit_error=True, num_cpus={workers})"
        ).body[0]

        for node in tree.body:
            if (
                isinstance(node, ast.If)
                and isinstance(node.test, ast.Compare)
                and isinstance(node.test.left, ast.Name)
                and node.test.left.id == "__name__"
            ):
                if not has_ray_init(node.body):
                    node.body.insert(0, ray_init_stmt)
                return

        if not has_ray_init(tree.body):
            import_count = sum(1 for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom)))
            tree.body.insert(import_count, ray_init_stmt)
