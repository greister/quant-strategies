# 独立强度因子 - Agent 操作指南

## 多策略并行执行

### 快速开始

执行所有策略并导入 Obsidian：

```bash
# 1. 并行执行所有策略（自动导出结果并复制到 Obsidian）
./scripts/run_all_strategies.sh [YYYY-MM-DD]

# 2. 仅生成报告（不执行策略）
python3 ./scripts/generate_report.py [YYYY-MM-DD]
```

**报告位置**: `30_Research/量化分析/策略执行结果/01-独立强度因子/YYYY-MM-DD_每日选股报告-综合版.md`

### 支持策略

| 策略 | 说明 | 输出表 |
|-----|------|--------|
| 基础版 | 原始独立强度因子 | `independence_score_daily` |
| 尾盘关注型 | 重视收盘前表现 | `independence_score_time_weighted` |
| 早盘关注型 | 重视开盘情绪 | `independence_score_time_weighted` |
| 趋势市 | 早盘权重较高 | `independence_score_time_weighted` |
| 保守型 | 全天均匀加权 | `independence_score_time_weighted` |
| 融资加权版 | 结合融资余额变化 | `independence_score_daily` |

### 并行执行原理

```bash
# 所有策略作为后台任务并行启动
run_strategy_1 & PID1=$!
run_strategy_2 & PID2=$!
run_strategy_3 & PID3=$!
# ...
wait $PID1 $PID2 $PID3
```

执行效率：
- 串行执行：约 30-40 秒
- 并行执行：约 7-10 秒（提升 3-4 倍）

## Obsidian 报告导入

### 报告结构

```markdown
# YYYY-MM-DD 每日选股报告-综合版

## 📈 策略概览
## 🎯 策略说明
## 📊 各策略 Top15
## 🔥 多策略共识股票
## 📝 因子说明
```

### 共识股票识别

系统自动识别在多个策略中均表现优异的股票：

```python
# 收集所有策略 Top10 中的股票
all_symbols = {}
for key, data in strategies.items():
    for item in data[:10]:
        symbol = item['symbol']
        all_symbols[symbol] = [...]

# 找出出现次数 >= 2 的股票
consensus = {k: v for k, v in all_symbols.items() if len(v) >= 2}
```

### Vault 路径配置

**报告存放位置**: `30_Research/量化分析/策略执行结果/01-独立强度因子/`

完整路径: `D:\obsidian\OrbitOS-vault\30_Research\量化分析\策略执行结果\01-独立强度因子\`

如需修改，编辑 `run_all_strategies.sh` 中的 `REPORT_TARGET_DIR` 变量。

## 数据库配置

共享配置位置：`../00.shared/config/database.env`

```bash
# ClickHouse
export CH_HOST=localhost
export CH_PORT=9000
export CH_DB=tdx2db_rust
export CH_USER=default
export CH_PASSWORD=tdx2db

# PostgreSQL
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=quantdb
export PG_USER=postgres
export PG_PASSWORD=postgres
```

## 文件结构

```
.
├── scripts/
│   ├── run_all_strategies.sh          # 并行执行入口
│   ├── generate_report.py             # 报告生成
│   ├── calc_independence_score.sh     # 基础版
│   ├── calc_time_weighted_score.py    # 时间加权版
│   └── calc_independence_score_margin_weighted.py  # 融资加权版
├── sql/
│   ├── create_independence_tables.sql
│   ├── create_time_weighted_tables.sql
│   └── calc_time_weighted_score.sql
└── results/                           # 输出目录
    ├── *.json                         # 各策略原始数据
    └── *_report.md                    # 综合报告
```

## 开发说明

### 添加新策略

1. 创建策略脚本 `scripts/calc_new_strategy.py`
2. 在 `run_all_strategies.sh` 中添加 `run_strategy_new` 函数
3. 在 `generate_report.py` 中添加策略说明和结果展示

### 修改报告模板

编辑 `scripts/generate_report.py` 中的 `generate_report()` 函数。

### 数据源表

- 5分钟K线：`raw_stocks_5min`
- 板块归属：`stock_sectors`
- 融资融券：`margin_trading_detail_combined` (PostgreSQL)

## 常见问题

### Q: 为什么基础版没有数据？
A: 检查 ClickHouse 认证信息是否正确配置。

### Q: 融资加权版被跳过？
A: PostgreSQL 中没有对应日期的融资数据时会自动跳过。

### Q: 如何查看某只股票的详细计算过程？
A: 使用 `sql/queries_independence_score.sql` 中的调试查询。
