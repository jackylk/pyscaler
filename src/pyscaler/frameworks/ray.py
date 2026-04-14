"""Ray framework plugin — AST-based conversion for parallel-loop pattern."""
from __future__ import annotations

import ast
import difflib
import textwrap

from .base import ConversionResult, Framework


class RayFramework(Framework):
    name = "ray"

    def supports(self, analysis: dict) -> bool:
        return analysis.get("pattern") in ("parallel_loop", "dataframe_apply")

    def runtime_dependencies(self) -> list[str]:
        return ["ray[default]>=2.9"]

    def convert(self, source: str, analysis: dict, workers: int = 8) -> ConversionResult:
        pattern = analysis.get("pattern")
        tree = ast.parse(source)
        if pattern == "parallel_loop":
            loop = analysis["loop"]
            self._decorate_function(tree, loop["call_func"])
            self._replace_fanout(tree, loop)
            self._ensure_ray_import_and_init(tree, workers)
            summary = {
                "framework": "ray",
                "pattern": "parallel_loop",
                "kind": loop.get("kind", "for_loop"),
                "decorated_function": loop["call_func"],
                "iter": loop["iter_name"],
                "workers": workers,
            }
        elif pattern == "dataframe_apply":
            info = analysis["df_apply"]
            self._rewrite_dataframe_apply(tree, info, workers)
            self._ensure_ray_import_and_init(tree, workers)
            summary = {
                "framework": "ray",
                "pattern": "dataframe_apply",
                "df": info["df_name"],
                "column": info["target_col"],
                "func": info["func_name"],
                "workers": workers,
            }
        else:
            raise NotImplementedError(f"Ray converter can't handle pattern={pattern!r} yet.")

        ast.fix_missing_locations(tree)
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
    def _build_ray_get_comp(call_func: str, var_names: list[str], iter_expr: ast.AST) -> ast.Call:
        """Build `ray.get([call_func.remote(*vars) for <vars> in <iter_expr>])` as AST."""
        # Generator target: single Name or a Tuple of Names (for zip unpack)
        if len(var_names) == 1:
            gen_target: ast.expr = ast.Name(id=var_names[0], ctx=ast.Store())
        else:
            gen_target = ast.Tuple(
                elts=[ast.Name(id=v, ctx=ast.Store()) for v in var_names],
                ctx=ast.Store(),
            )
        return ast.Call(
            func=ast.Attribute(value=ast.Name(id="ray", ctx=ast.Load()), attr="get", ctx=ast.Load()),
            args=[
                ast.ListComp(
                    elt=ast.Call(
                        func=ast.Attribute(
                            value=ast.Name(id=call_func, ctx=ast.Load()),
                            attr="remote",
                            ctx=ast.Load(),
                        ),
                        args=[ast.Name(id=v, ctx=ast.Load()) for v in var_names],
                        keywords=[],
                    ),
                    generators=[ast.comprehension(
                        target=gen_target,
                        iter=iter_expr,
                        ifs=[],
                        is_async=0,
                    )],
                ),
            ],
            keywords=[],
        )

    @classmethod
    def _replace_fanout(cls, tree: ast.Module, loop: dict) -> None:
        """Unified rewrite for for_loop / zip_loop / list_comp / map_call."""
        kind = loop.get("kind", "for_loop")
        call_func = loop["call_func"]
        var_names: list[str] = loop.get("var_names") or [loop.get("var_name")]
        iter_name: str = loop["iter_name"]
        assign_target: str | None = loop.get("assign_target")

        iter_expr = ast.parse(iter_name, mode="eval").body
        ray_get_call = cls._build_ray_get_comp(call_func, var_names, iter_expr)

        # Wrap in Assign or bare Expr depending on pattern
        if assign_target:
            replacement: ast.stmt = ast.Assign(
                targets=[ast.Name(id=assign_target, ctx=ast.Store())],
                value=ray_get_call,
            )
        else:
            replacement = ast.Expr(value=ray_get_call)

        def match(stmt: ast.stmt) -> bool:
            if kind in ("for_loop", "zip_loop") and isinstance(stmt, ast.For):
                return ast.unparse(stmt.iter) == iter_name
            if kind == "list_comp" and isinstance(stmt, ast.Assign):
                if len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name) \
                        and stmt.targets[0].id == assign_target:
                    return isinstance(stmt.value, ast.ListComp)
            if kind == "map_call" and isinstance(stmt, ast.Assign):
                if len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name) \
                        and stmt.targets[0].id == assign_target:
                    v = stmt.value
                    return (isinstance(v, ast.Call) and isinstance(v.func, ast.Name)
                            and v.func.id == "list")
            return False

        def replace_in(body: list[ast.stmt]) -> bool:
            for i, stmt in enumerate(body):
                if match(stmt):
                    body[i] = replacement
                    return True
            return False

        if replace_in(tree.body):
            return
        for node in tree.body:
            if (
                isinstance(node, ast.If)
                and isinstance(node.test, ast.Compare)
                and isinstance(node.test.left, ast.Name)
                and node.test.left.id == "__name__"
            ):
                if replace_in(node.body):
                    return

    @staticmethod
    def _rewrite_dataframe_apply(tree: ast.Module, info: dict, workers: int) -> None:
        """Replace `df[col] = df.apply(func, axis=1)` with a chunked ray.remote version."""
        df = info["df_name"]
        col = info["target_col"]
        func = info["func_name"]

        # Inject a _pyscaler_apply_chunk helper at module top (after imports)
        helper_src = textwrap.dedent(f"""
            @ray.remote
            def _pyscaler_apply_chunk(chunk):
                return chunk.apply({func}, axis=1)
        """).lstrip()
        helper_stmts = ast.parse(helper_src).body

        # Replacement for the original assignment — pandas-native chunking,
        # avoids np.array_split quirks where DataFrame collapses to ndarray.
        replacement_src = textwrap.dedent(f"""
            _chunk_size = max(1, (len({df}) + {workers} - 1) // {workers})
            _chunks = [{df}.iloc[i:i + _chunk_size] for i in range(0, len({df}), _chunk_size)]
            _results = ray.get([_pyscaler_apply_chunk.remote(c) for c in _chunks])
            {df}["{col}"] = pd.concat(_results)
        """).lstrip()
        replacement_stmts = ast.parse(replacement_src).body

        def rewrite(body: list[ast.stmt]) -> bool:
            for i, stmt in enumerate(body):
                if (
                    isinstance(stmt, ast.Assign)
                    and len(stmt.targets) == 1
                    and isinstance(stmt.targets[0], ast.Subscript)
                    and isinstance(stmt.targets[0].value, ast.Name)
                    and stmt.targets[0].value.id == df
                    and isinstance(stmt.value, ast.Call)
                    and isinstance(stmt.value.func, ast.Attribute)
                    and stmt.value.func.attr == "apply"
                ):
                    body[i:i + 1] = replacement_stmts
                    return True
            return False

        # Try module top; fallback to __main__ body
        if not rewrite(tree.body):
            for node in tree.body:
                if (
                    isinstance(node, ast.If)
                    and isinstance(node.test, ast.Compare)
                    and isinstance(node.test.left, ast.Name)
                    and node.test.left.id == "__name__"
                ):
                    rewrite(node.body)
                    break

        # Put helper after imports (before any other stmt)
        import_count = sum(1 for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom)))
        for h in reversed(helper_stmts):
            tree.body.insert(import_count, h)

    @staticmethod
    def _ensure_ray_import_and_init(tree: ast.Module, workers: int, extra_imports: list[str] | None = None) -> None:
        has_import = any(
            isinstance(n, ast.Import) and any(a.name == "ray" for a in n.names) for n in tree.body
        )
        if not has_import:
            tree.body.insert(0, ast.Import(names=[ast.alias(name="ray", asname=None)]))

        for spec in extra_imports or []:
            parts = spec.split(" as ")
            name = parts[0]
            asname = parts[1] if len(parts) == 2 else None
            if not any(
                isinstance(n, ast.Import) and any(a.name == name for a in n.names) for n in tree.body
            ):
                tree.body.insert(0, ast.Import(names=[ast.alias(name=name, asname=asname)]))

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

        # Generated init: auto-detects RAY_ADDRESS (remote cluster) vs local embedded runtime.
        # Works for all three modes: Ray local / Ray local cluster / DBay remote.
        # When connecting to a cluster, upload the driver's CWD to workers so relative
        # paths (./data/input/...) resolve — local mode doesn't need this since workers
        # share the driver's process.
        ray_init_src = textwrap.dedent(f"""
            import os as _os
            _ray_addr = _os.environ.get("RAY_ADDRESS")
            if _ray_addr:
                ray.init(address=_ray_addr, ignore_reinit_error=True,
                         runtime_env={{"working_dir": "."}})
            else:
                ray.init(ignore_reinit_error=True, num_cpus={workers})
        """).lstrip()
        ray_init_stmts = ast.parse(ray_init_src).body

        for node in tree.body:
            if (
                isinstance(node, ast.If)
                and isinstance(node.test, ast.Compare)
                and isinstance(node.test.left, ast.Name)
                and node.test.left.id == "__name__"
            ):
                if not has_ray_init(node.body):
                    node.body[0:0] = ray_init_stmts
                return

        if not has_ray_init(tree.body):
            import_count = sum(1 for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom)))
            tree.body[import_count:import_count] = ray_init_stmts
