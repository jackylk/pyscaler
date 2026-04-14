from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from . import __version__
from .frameworks.registry import available_frameworks

app = typer.Typer(
    no_args_is_help=True,
    help="distify — turn single-machine Python data code into a distributed script.",
)
console = Console()


def _version_cb(value: bool):
    if value:
        console.print(f"distify {__version__}")
        raise typer.Exit()


@app.callback()
def _main(
    version: bool = typer.Option(
        None, "--version", "-V", callback=_version_cb, is_eager=True, help="Show version and exit."
    ),
):
    pass


@app.command()
def analyze(path: Path = typer.Argument(..., help="Python file or repo directory")):
    """Static analysis: find bottlenecks, recommend framework, predict speedup."""
    console.print(f"[dim]analyze[/] {path}")
    console.print("[yellow]not implemented yet[/]")


@app.command()
def convert(
    path: Path = typer.Argument(..., help="Python file to convert"),
    framework: str = typer.Option(
        "ray", "--framework", "-f", help=f"Target framework. Available: {available_frameworks()}"
    ),
    workers: int = typer.Option(8, help="Target worker count"),
    output: Optional[Path] = typer.Option(None, help="Output path (default: <name>_dist.py)"),
    llm_assist: bool = typer.Option(False, "--llm-assist", help="Use LLM to fill template gaps"),
):
    """Convert a Python script to a distributed version."""
    console.print(f"[dim]convert[/] {path} framework={framework} workers={workers}")
    console.print("[yellow]not implemented yet[/]")


@app.command()
def verify(
    script: Path = typer.Argument(..., help="Distributed script produced by `convert`"),
    input: Path = typer.Option(..., "--input", "-i", help="Input data directory"),
    sample: float = typer.Option(0.05, help="Sample ratio, 0-1"),
):
    """Run the original and distributed versions on a sample; compare correctness & speed."""
    console.print(f"[dim]verify[/] {script} input={input} sample={sample}")
    console.print("[yellow]not implemented yet[/]")


@app.command()
def run(
    script: Path = typer.Argument(..., help="Distributed script to execute"),
    backend: str = typer.Option("local", help="local | ray://HOST:PORT | dask://HOST:PORT | dbay"),
    input: str = typer.Option(..., "--input", "-i", help="Input path (local dir, s3://, obs://)"),
    output: Optional[str] = typer.Option(None, "--output", "-o"),
):
    """Execute the distributed script on a chosen backend."""
    console.print(f"[dim]run[/] {script} backend={backend} input={input}")
    console.print("[yellow]not implemented yet[/]")


@app.command()
def frameworks():
    """List available distribution frameworks."""
    for name in available_frameworks():
        console.print(f"  • {name}")


if __name__ == "__main__":
    app()
