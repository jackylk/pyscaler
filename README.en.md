# pyscaler

Turn your single-machine Python data processing code into a distributed script.
Framework-agnostic (Ray, Dask, ...), runs anywhere — your own cluster or DBay.

## Install

```bash
# From PyPI (once published)
pip install pyscaler

# With framework runtime
pip install "pyscaler[ray]"        # Ray backend
pip install "pyscaler[dask]"       # Dask backend (planned)
pip install "pyscaler[llm]"        # LLM-assisted conversion

# Dev install from source
git clone https://github.com/pyscaler/pyscaler
cd pyscaler
pip install -e ".[ray,dev]"
```

## Quick start

```bash
# 1. Analyze — find bottlenecks, recommend framework, predict speedup
pyscaler analyze ./process.py

# 2. Convert — write process_dist.py + a unified diff
pyscaler convert ./process.py --framework ray --workers 8

# 3. Verify — run both versions on a 5% sample, compare correctness & speed
pyscaler verify ./process_dist.py --input ./data/ --sample 0.05

# 4. Run — execute on a backend of your choice
pyscaler run ./process_dist.py --backend local             --input ./data/
pyscaler run ./process_dist.py --backend ray://head:10001  --input ./data/
pyscaler run ./process_dist.py --backend dbay              --input obs://bucket/data/
```

## Concepts

- **Framework** — how code is transformed (Ray, Dask, Spark, …). Plugin-based.
- **Backend** — where the generated script runs (local, your cluster, DBay).
- Orthogonal: a Ray script can run on any Ray cluster (yours or DBay's).

## Status

Early scaffold. CLI shape finalized; analyzer/converter/verifier implementations coming. See `DESIGN.md`.

## License

Apache-2.0
