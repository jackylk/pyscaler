# distify Design

## Goal

A standalone CLI that converts single-machine Python data processing code
into a **distributed** script, with verified correctness and measured speedup.
Framework-agnostic — Ray today, Dask/Spark tomorrow. Runs on any cluster:
the user's own, or DBay.

## Non-goals

- Not a cluster manager (use `ray up`, `dask-scheduler`, etc.)
- Not a general Python rewriter (targets data-parallel loops / pipelines only)
- Not bound to DBay (DBay is one of several backends)

## Four commands

| Command | Input | Output |
|---|---|---|
| `analyze` | `.py` file or repo | Bottleneck report + recommended framework + predicted speedup |
| `convert` | `.py` file | Distributed version (`*_dist.py`) + unified diff + JSON change summary |
| `verify` | Generated script + input dir | Correctness diff on sample + measured speedup |
| `run` | Generated script + input | Dispatches to a backend (local / remote cluster / DBay) |

Each step's output is the next step's input. Steps are independent — user can
stop at `convert` and take the script elsewhere.

## Two plugin layers

**Frameworks** (how code is transformed) and **backends** (where it runs) are
orthogonal. Keeping them separate means:

- A Ray script can run on `local`, `ray://my-cluster`, or DBay's Ray service
- A Dask script can run on `local`, `dask://my-cluster`, or eventually DBay's Dask service
- New frameworks (Spark, Modin) plug in without touching the execution layer

```
src/distify/
├── frameworks/          # code transformation plugins
│   ├── base.py          # Framework interface
│   ├── registry.py
│   ├── ray.py           # Ray templates
│   └── dask.py          # Dask templates (planned)
└── backends/            # execution plugins
    ├── base.py          # Backend interface
    ├── local.py         # in-process
    ├── ray_cluster.py   # ray://...
    └── dbay.py          # DBay orchestrator API
```

## Analyzer

Static AST analysis, no LLM needed:

- Find top-level loops over collections (files, DataFrames, lists)
- Detect shared mutable state (globals, closures) → parallelism blocker
- Rough compute profile (I/O heavy? CPU heavy? already vectorized?)
- Recommend framework + pattern; or say "not worth it"

## Converter

Template-based, with optional LLM gap-fill (`--llm-assist`). Templates per
framework. For Ray:

1. `for x in items: f(x)` → `ray.get([f.remote(x) for x in items])`
2. `df.apply(f)` → `ray.data.from_pandas(df).map(f).to_pandas()`
3. `for path in files: read+process+write` → `ray.data.read_parquet(files).map_batches(process).write_parquet(out)`

LLM is optional; default is pure template. Output must preserve AST
equivalence on unchanged portions.

## Verifier

1. Random sample N% of input (default 5%)
2. Run original and distributed versions side-by-side
3. Diff outputs — structural (schema/row count) + content (hash/checksum)
4. Report correctness + wall-clock speedup

If speedup < 1.5× or correctness fails → block `run` unless `--force`.

## Backends

- `local` — in-process framework runtime (dev / small data)
- `ray://HOST:PORT` / `dask://HOST:PORT` — user-managed cluster
- `dbay` — HTTP API to DBay orchestrator, auth via `~/.dbay/token.json`

## State storage

Each task gets a workspace dir:

```
.distify/tasks/{task_id}/
├── meta.json              # command, params, commit, timestamps
├── source/                # snapshot of user's code
├── converted/             # generated distributed script + diff
├── verification/          # sample outputs + report
└── runs/{run_id}/         # logs + results per execution
```

Default: `./.distify/`. User can point to OBS/S3 for team sharing.

## LLM

- Analyzer: **no LLM** (AST only — fast, deterministic, offline)
- Converter: LLM only for template gap-fill, opt-in via `--llm-assist`
- Credentials: reads `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` from env,
  or a DBay LLM endpoint if signed into DBay
- Offline mode: template-only, zero network

## Release

- PyPI: `pip install distify`
- Extras: `[ray]`, `[dask]`, `[llm]`, `[dev]`
- `distify` CLI entrypoint registered via `project.scripts`
