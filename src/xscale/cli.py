from __future__ import annotations

import json
import time
from dataclasses import asdict
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.syntax import Syntax

from . import __version__
from .analyzer import analyze_file
from .backends.local import LocalBackend
from .frameworks.registry import available_frameworks, get_framework

app = typer.Typer(
    no_args_is_help=True,
    help="xscale — turn single-machine Python data code into a distributed script.",
)
console = Console()


def _version_cb(value: bool):
    if value:
        console.print(f"xscale {__version__}")
        raise typer.Exit()


@app.callback()
def _main(
    version: bool = typer.Option(
        None, "--version", "-V", callback=_version_cb, is_eager=True, help="Show version and exit."
    ),
):
    pass


def _analysis_to_dict(a) -> dict:
    d = asdict(a)
    d["path"] = str(a.path)
    return d


@app.command()
def analyze(path: Path = typer.Argument(..., help="Python file to analyze")):
    """Static analysis: find bottlenecks, recommend framework, predict speedup."""
    if not path.exists():
        console.print(f"[red]Path not found:[/] {path}")
        raise typer.Exit(1)
    result = analyze_file(path)
    console.print(f"[bold]File:[/] {result.path}")
    console.print(f"[bold]Pattern:[/] {result.pattern}")
    console.print(f"[bold]Recommended framework:[/] {result.recommended_framework}")
    if result.loop:
        console.print(
            f"[bold]Parallel-loop candidate:[/] line {result.loop.line} · "
            f"`{result.loop.call_func}({result.loop.var_name})` over `{result.loop.iter_name}`"
        )
    if result.blockers:
        console.print("[yellow bold]Blockers:[/]")
        for b in result.blockers:
            console.print(f"  • {b}")
    for note in result.notes:
        console.print(f"[dim]• {note}[/]")
    if result.supports_parallel:
        console.print("\n[green]✓ Ready to convert[/] — run `xscale convert`")
    else:
        console.print("\n[yellow]⚠ Not ready to convert as-is — see blockers above[/]")


@app.command()
def convert(
    path: Path = typer.Argument(..., help="Python file to convert"),
    framework: str = typer.Option(
        "ray", "--framework", "-f", help=f"Target framework. Available: {available_frameworks()}"
    ),
    workers: int = typer.Option(8, help="Target worker count"),
    output: Optional[Path] = typer.Option(None, help="Output path (default: <name>_dist.py)"),
):
    """Convert a Python script to a distributed version."""
    if not path.exists():
        console.print(f"[red]Path not found:[/] {path}")
        raise typer.Exit(1)
    analysis = analyze_file(path)
    if not analysis.supports_parallel:
        console.print("[red]Refusing to convert:[/] analyzer reports this code isn't ready.")
        for b in analysis.blockers:
            console.print(f"  • {b}")
        raise typer.Exit(2)

    fw = get_framework(framework)
    result = fw.convert(path.read_text(), _analysis_to_dict(analysis), workers=workers)

    out_path = output or path.with_name(path.stem + "_dist.py")
    out_path.write_text(result.converted)

    console.print(f"\n[bold]Diff:[/]\n")
    console.print(Syntax(result.diff or "(no diff)", "diff", background_color="default"))
    console.print(f"\n[green]✓ Wrote[/] {out_path}")
    console.print(f"[dim]Summary:[/] {json.dumps(result.summary)}")


@app.command()
def verify(
    script: Path = typer.Argument(..., help="Distributed script produced by `convert`"),
    original: Optional[Path] = typer.Option(
        None, help="Original script (defaults to basename minus `_dist`)"
    ),
    input: Path = typer.Option(..., "--input", "-i", help="Input data directory"),
    sample: float = typer.Option(0.2, help="Sample ratio, 0-1"),
):
    """Run original and distributed versions on a sample; compare correctness & speed."""
    if not script.exists():
        console.print(f"[red]Script not found:[/] {script}")
        raise typer.Exit(1)
    if original is None:
        stem = script.stem
        if stem.endswith("_dist"):
            original = script.with_name(stem[: -len("_dist")] + ".py")
        else:
            console.print("[red]Provide --original explicitly[/]")
            raise typer.Exit(1)

    backend = LocalBackend()
    console.print(f"[dim]running original[/] {original}")
    t0 = time.time()
    rid1 = backend.submit(original, str(input), "")
    s1 = backend.status(rid1)
    console.print(f"  state={s1['state']} duration={s1['duration']:.2f}s")

    console.print(f"[dim]running distributed[/] {script}")
    t1 = time.time()
    rid2 = backend.submit(script, str(input), "")
    s2 = backend.status(rid2)
    console.print(f"  state={s2['state']} duration={s2['duration']:.2f}s")

    if s1["state"] != "succeeded" or s2["state"] != "succeeded":
        console.print("[red]One of the runs failed[/]")
        for line in backend.logs(rid1) + backend.logs(rid2):
            console.print(f"  {line}")
        raise typer.Exit(3)

    speedup = s1["duration"] / max(s2["duration"], 0.001)
    console.print(f"\n[bold]Measured speedup:[/] [green]{speedup:.2f}×[/]")
    console.print("[dim](compare output directories separately for correctness)[/]")


@app.command()
def run(
    script: Path = typer.Argument(..., help="Distributed script to execute"),
    backend_name: str = typer.Option("local", "--backend", help="local | ray://... | dbay"),
    input: str = typer.Option(..., "--input", "-i", help="Input path (local dir, s3://, obs://)"),
    output: Optional[str] = typer.Option(None, "--output", "-o"),
):
    """Execute the distributed script on a chosen backend."""
    if backend_name != "local":
        console.print(f"[yellow]Backend `{backend_name}` not implemented yet.[/]")
        raise typer.Exit(4)
    if not script.exists():
        console.print(f"[red]Script not found:[/] {script}")
        raise typer.Exit(1)
    backend = LocalBackend()
    rid = backend.submit(script, input, output or "")
    s = backend.status(rid)
    console.print(f"[bold]State:[/] {s['state']} · [bold]Duration:[/] {s['duration']:.2f}s")
    for line in backend.logs(rid):
        console.print(f"  {line}")
    if s["state"] != "succeeded":
        raise typer.Exit(s["returncode"] or 1)


@app.command()
def frameworks():
    """List available distribution frameworks."""
    for name in available_frameworks():
        console.print(f"  • {name}")


if __name__ == "__main__":
    app()
