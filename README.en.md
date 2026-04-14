# xscale

Turn your single-machine Python data processing code into a distributed script.
Framework-agnostic (Ray, Dask, ...), runs anywhere — your own cluster or DBay.

## Install

```bash
# From PyPI (once published)
pip install xscale

# With framework runtime
pip install "xscale[ray]"        # Ray backend
pip install "xscale[dask]"       # Dask backend (planned)
pip install "xscale[llm]"        # LLM-assisted conversion

# Dev install from source
git clone https://github.com/xscale/xscale
cd xscale
pip install -e ".[ray,dev]"
```

## Quick start

```bash
# 1. Analyze — find bottlenecks, recommend framework, predict speedup
xscale analyze ./process.py

# 2. Convert — write process_dist.py + a unified diff
xscale convert ./process.py --framework ray --workers 8

# 3. Verify — run both versions on a 5% sample, compare correctness & speed
xscale verify ./process_dist.py --input ./data/ --sample 0.05

# 4. Run — execute on a backend of your choice
xscale run ./process_dist.py --backend local             --input ./data/
xscale run ./process_dist.py --backend ray://head:10001  --input ./data/
xscale run ./process_dist.py --backend dbay              --input obs://bucket/data/
```

## Concepts

- **Framework** — how code is transformed (Ray, Dask, Spark, …). Plugin-based.
- **Backend** — where the generated script runs (local, your cluster, DBay).
- Orthogonal: a Ray script can run on any Ray cluster (yours or DBay's).

## Status

Early scaffold. CLI shape finalized; analyzer/converter/verifier implementations coming. See `DESIGN.md`.

## License

Apache-2.0
