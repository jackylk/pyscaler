# pyscaler 测试报告

最近一次全量测试：本地 macOS · Python 3.13.5 · Ray 2.49.2 · pandas 已装

## 总览

**19 passed + 1 skipped** · 耗时 ~65s（含 Ray 集群冷启）

```
tests/test_cli_smoke.py ........ 6 passed
tests/test_e2e_ray.py ........... 5 passed
tests/test_e2e_dataframe.py ..... 2 passed
tests/test_e2e_ray_cluster.py ... 2 passed + 1 skipped (DBay real)
tests/test_e2e_dbay.py .......... 3 passed
tests/test_examples_parse.py .... 1 passed
```

> 1 个测试 `test_e2e_against_dbay_remote_cluster` 默认 skip，要真连 DBay 需设：
> `PYSCALER_DBAY_TEST=1 PYSCALER_DBAY_ENDPOINT=... PYSCALER_DBAY_TOKEN=...`
> 手动执行已验证通过（~160s，含 CCI Ray 集群冷启）。

## 覆盖的路径

### Smoke（6）

| 测试 | 验证 |
|---|---|
| `test_version` | `pyscaler --version` 正常退出且输出包含 "pyscaler" |
| `test_help_lists_commands` | 四个命令 + frameworks 都注册到了帮助 |
| `test_frameworks_command_lists_ray_and_aura` | `pyscaler frameworks` 列出 ray 和 aura |
| `test_framework_registry` | 注册表内容正确 |
| `test_get_framework_ray` | 可以拿到 Ray 插件实例 + 其依赖声明 |
| `test_get_framework_unknown_raises` | 未知框架抛 ValueError |

### 端到端 · parallel_loop（5）

| 测试 | 验证 |
|---|---|
| `test_analyzer_detects_parallel_loop` | AST 识别 `for f in files: process_file(f)` |
| `test_analyzer_blocks_on_global_mutation` | 检测到 `counter["k"] += 1` 类的全局共享状态 |
| `test_converter_produces_valid_ray_script` | 生成的脚本可被 `ast.parse` + 包含 `@ray.remote` / `.remote(f)` / `ray.get(` |
| `test_end_to_end_local_run` | 完整链路：原版跑 → 清输出 → 转换 → Ray 版跑 → 输出字节级一致 |
| `test_cli_analyze_and_convert` | 子进程启动 `python -m pyscaler.cli`，真实 CLI 调用 |

### 端到端 · dataframe_apply（2）

| 测试 | 验证 |
|---|---|
| `test_analyzer_detects_dataframe_apply` | 识别 `df["score"] = df.apply(compute_score, axis=1)` |
| `test_end_to_end_dataframe_ray` | 造 50 行 DataFrame → 转换 → 分片并行 apply → 与原版 score 列逐行相等 |

### 示例（1）

| 测试 | 验证 |
|---|---|
| `test_all_examples_parse` | `examples/*.py` 全部语法合法 |

## 不在当前测试覆盖内

- **性能断言**：测试没有硬性断言"加速比 > X×"，只验证正确性。小数据上 Ray 冷启反而比原版慢，这是预期。加速比验证放在 `pyscaler verify` 命令里做，而不是单元测试。
- **非 local backend**：`ray://` 和 `dbay` 还未实现，不覆盖。
- **LLM 辅助转换**：`--llm-assist` 未实现，不覆盖。
- **ast.unparse 格式保留**：当前转换会丢 docstring/注释。记录为已知问题，后续迁到 libcst。

## 复现

```bash
git clone https://github.com/jackylk/pyscaler
cd pyscaler
python3 -m venv .venv
.venv/bin/pip install -e ".[ray,dev]"
.venv/bin/pytest tests/ -v
```

环境需要：Python ≥ 3.10，Ray 已装（通过 `.[ray]` extra），macOS/Linux。
