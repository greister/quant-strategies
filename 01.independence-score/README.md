# 分时独立强度因子

基于 ClickHouse 5 分钟线数据，计算个股相对板块的"逆势分"。

## 共享资源

- 数据库配置：`../00.shared/config/database.env`
- 工具函数：`../00.shared/utils/`

## 核心逻辑

独立强度因子用于衡量股票在板块下跌时的抗跌能力：

- **板块跌幅 < -0.5%** 时，视为板块下跌区间
- 在该区间内，满足以下任一条件即计 **1 分**：
  - 个股上涨（涨幅 > 0%）
  - 相对板块超额收益 > 1%

全天累加得到独立强度分值，分数越高表示股票在板块下跌时表现越独立、越强韧。

## 快速开始

### 1. 建表

```bash
clickhouse-client --database=tdx2db_rust < sql/create_independence_tables.sql
```

### 2. 计算某日因子

```bash
# 计算今日
./scripts/calc_independence_score.sh

# 计算指定日期
./scripts/calc_independence_score.sh 2025-03-20
```

### 3. 查看结果

```bash
clickhouse-client --database=tdx2db_rust -q "
SELECT * FROM independence_score_daily
WHERE date = '2025-03-20'
ORDER BY score DESC
LIMIT 20
"
```

## 文件结构

```
.
├── sql/
│   ├── create_independence_tables.sql      # 建表脚本（结果表 + 视图）
│   ├── create_time_weighted_tables.sql     # 时间加权表
│   ├── create_advanced_tables.sql          # 高阶因子表
│   ├── calc_independence_score.sql         # 核心计算逻辑（查询版）
│   ├── calc_time_weighted_score.sql        # 时间加权计算
│   ├── queries_independence_score.sql      # 常用查询示例
│   └── backtest_independence_score.sql     # 回测 SQL
├── scripts/
│   ├── run_all_strategies.sh                        # 一键运行全部 8 个策略（并行）
│   ├── calc_independence_score.sh                   # 策略1：基础独立强度因子
│   ├── calc_time_weighted_score.py                  # 策略2~5：时间加权因子（多预设）
│   ├── calc_independence_score_margin_weighted.py   # 策略6：融资余额加权
│   ├── calc_advanced_score.py                       # 策略7：S09/S10/S12 高阶因子
│   ├── calc_weekly_consistency.py                   # 策略8：S11 周频一致性（周五）
│   ├── daily_stock_screening.py                     # 每日全市场扫描 + 生成选股报告
│   ├── backtest_independence_score.py               # 历史回测
│   ├── optimize_backtest.py                         # 回测参数优化
│   ├── combined_factor_demo.py                      # 双因子组合演示
│   ├── market_stats.py                              # 市场统计（日/周/高阶）
│   ├── gen_reports.py                               # 策略执行报告生成
│   ├── visualize_independence_score.py              # 可视化（matplotlib）
│   └── visualize_independence_score_plotly.py       # 可视化（Plotly）
└── docs/
    ├── 完整项目报告.md
    ├── 多因子组合说明.md
    ├── 多因子组合演示报告.md
    └── plans/                              # 设计文档
```

## 参数调整

在 `sql/calc_independence_score.sql` 和 `scripts/calc_independence_score.sh` 中可调整以下阈值：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `sector_return_threshold` | -0.5% | 板块下跌阈值，低于此值视为板块下跌 |
| `stock_return_threshold` | 0% | 个股上涨阈值，高于此值视为个股上涨 |
| `excess_return_threshold` | 1% | 超额收益阈值，高于此值视为显著跑赢板块 |

修改阈值后重新执行计算脚本即可生效。

## 融资余额加权版本

除了基础版本，还支持结合 PostgreSQL 融资融券数据进行加权计算。

### 加权逻辑

- **基础分数**：来自 5 分钟 K 线的独立强度分数
- **融资加权**：融资余额增加的股票获得额外加分
- **加权公式**：`weighted_score = raw_score * (1 + change_rate * weight_factor)`

### 使用方法

```bash
# 安装依赖
pip install psycopg2-binary clickhouse-driver

# 设置环境变量
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=quantdb
export PG_USER=postgres
export PG_PASSWORD=your_password

export CH_HOST=localhost
export CH_PORT=9000
export CH_DB=tdx2db_rust
export CH_USER=default
export CH_PASSWORD=your_password

# 运行融资加权版
./scripts/calc_independence_score_margin_weighted.py 2025-03-20

# 调整加权系数（默认 0.1）
./scripts/calc_independence_score_margin_weighted.py 2025-03-20 --weight-factor 0.2
```

### 输出字段说明

| 字段 | 说明 |
|------|------|
| `score` | 加权后的最终分数 |
| `raw_score` | 基础独立强度分数 |
| `margin_weight` | 融资加权系数（1.0 表示无加权） |
| `contra_count` | 逆势区间数量 |

## 时间加权因子版本

在基础独立强度因子之上，引入时间衰减权重机制，不同时段的逆势表现赋予不同权重。

### 核心逻辑

- **归一化权重**：全天 48 个 5 分钟区间权重之和为 1.0
- **时间导向**：早盘、午盘、尾盘可配置不同权重
- **预设模式**：尾盘关注型、趋势市、震荡市等多种模式

### 预设模式

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| `evening_focus` | 尾盘关注型（默认）| 关注次日开盘预期 |
| `morning_focus` | 早盘关注型 | 把握开盘情绪 |
| `conservative` | 保守型（均匀分布）| 与原始因子等价 |
| `trending_market` | 趋势市 | 早盘权重较高 |
| `ranging_market` | 震荡市 | 尾盘权重较高 |
| `rotating_market` | 轮动市 | 午盘权重较高 |

### 使用方法

```bash
# 初始化（首次使用）
./scripts/calc_time_weighted_score.py --init

# 使用默认预设计算
./scripts/calc_time_weighted_score.py 2025-03-20

# 使用指定预设
./scripts/calc_time_weighted_score.py 2025-03-20 --preset trending_market

# 查看所有预设
./scripts/calc_time_weighted_score.py --list-presets

# 自定义权重（48 个浮点数，逗号分隔）
./scripts/calc_time_weighted_score.py 2025-03-20 \
    --custom-weights "0.02,0.02,..." \
    --custom-name "my_config"
```

### 查看结果

```bash
# 查看某日加权因子排名
clickhouse-client --database=tdx2db_rust -q "
    SELECT * FROM independence_score_time_weighted
    WHERE date = '2025-03-20' AND config_name = 'evening_focus'
    ORDER BY weighted_score DESC
    LIMIT 20
"

# 对比不同配置的选股差异
clickhouse-client --database=tdx2db_rust -q "
    SELECT 
        a.symbol, a.name,
        a.weighted_score as evening_score,
        b.weighted_score as morning_score
    FROM independence_score_time_weighted a
    JOIN independence_score_time_weighted b ON a.symbol = b.symbol AND a.date = b.date
    WHERE a.date = '2025-03-20'
      AND a.config_name = 'evening_focus'
      AND b.config_name = 'morning_focus'
    ORDER BY evening_score DESC
    LIMIT 20
"
```

## 历史回测

验证独立强度因子的历史表现。

### 回测逻辑

1. **信号生成**：选取独立强度分数 >= 阈值的股票作为买入信号
2. **持有期**：买入后持有 N 天
3. **收益计算**：计算持有期内的收益率、最大回撤等指标
4. **统计分析**：胜率、平均收益、夏普比率等

### 使用方法

```bash
# 基础回测（持有 5 天，阈值 3.0）
./scripts/backtest_independence_score.py --start 2025-01-01 --end 2025-03-20

# 调整参数
./scripts/backtest_independence_score.py \
    --start 2025-01-01 \
    --end 2025-03-20 \
    --threshold 5.0 \
    --hold-days 10 \
    --top-n 10
```

### 回测参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--start` | 回测开始日期 | 必填 |
| `--end` | 回测结束日期 | 必填 |
| `--threshold` | 选股阈值（分数 >=） | 3.0 |
| `--hold-days` | 持有天数 | 5 |
| `--top-n` | 每日选股数量（Top N） | 不限 |

### 回测报告输出

- 总交易次数、胜率
- 平均收益率、年化收益率
- 最大单笔收益/亏损
- 平均最大回撤
- 夏普比率
- 分板块表现统计

## 一键运行全部策略

```bash
# 运行今日全部策略（并行执行）
./scripts/run_all_strategies.sh

# 运行指定日期
./scripts/run_all_strategies.sh 2026-04-21
```

执行流程：
1. 并行启动策略 1~6（基础版 + 4 个时间加权 + 融资加权）
2. 等待策略 1 完成后，启动策略 7（S09/S10/S12，依赖 S01 数据）
3. 导出所有结果到 `results/` 目录
4. 生成策略执行报告并复制到 Obsidian Vault
5. 周五额外运行策略 8（S11 周频一致性）+ 周报

---

## 高阶因子策略（S09 / S10 / S11 / S12）

在基础独立强度因子（S01）之上，衍生出 4 个高阶因子：

| 因子 | 名称 | 说明 | 运行方式 |
|------|------|------|----------|
| **S09** | 独立强度 + 融资共振 | S01 分数 + 融资净买入双重确认 | `calc_advanced_score.py --strategy S09` |
| **S10** | 独立强度 + 融券压制 | S01 分数 + 融券余额下降确认 | `calc_advanced_score.py --strategy S10` |
| **S11** | 周频一致性 | 近 5 个交易日 S01 持续高分的股票 | `calc_weekly_consistency.py`（仅周五） |
| **S12** | 独立强度 + 行业偏离 | S01 分数 + 行业内相对偏离度 | `calc_advanced_score.py --strategy S12` |

### 使用方法

```bash
# 运行全部高阶因子
./scripts/calc_advanced_score.py 2026-04-21 --strategy all

# 仅运行 S09
./scripts/calc_advanced_score.py 2026-04-21 --strategy S09

# 周五运行周频一致性
./scripts/calc_weekly_consistency.py 2026-04-21
```

---

## 每日选股报告

全市场扫描，生成包含独立强度、融资融券、分时形态等多维度的个股分析报告。

### 使用方法

```bash
# 生成今日报告
./scripts/daily_stock_screening.py

# 生成指定日期报告
./scripts/daily_stock_screening.py 2026-04-21

# 仅分析指定股票
./scripts/daily_stock_screening.py --symbol sh600519
```

### 报告输出

- 全市场独立强度排名 Top 50
- 融资净买入异动股票
- 分时形态异常（逆势上涨）股票
- 输出到 Obsidian Vault：`30_Research/量化分析/个股分析/`

---

## 双因子组合演示

结合**独立强度因子（S01）**和**动量因子（02）**的综合选股策略演示。

### 核心逻辑

- **S01 独立强度**：衡量板块下跌时的抗跌能力
- **动量因子**：衡量近期价格趋势强度
- **组合方式**：S01 筛选后，按动量排序取 Top N

### 使用方法

```bash
./scripts/combined_factor_demo.py 2026-04-21 --top-n 20
```

---

## 回测参数优化

对独立强度因子的阈值、持有期等参数进行网格搜索，找到最优参数组合。

### 使用方法

```bash
# 参数网格搜索
./scripts/optimize_backtest.py \
    --start 2025-01-01 \
    --end 2025-12-31 \
    --thresholds 2.0,3.0,4.0,5.0 \
    --hold-days 3,5,10 \
    --top-n 10,20

# 输出：各参数组合的胜率、夏普比率、最大回撤对比
```

---

## 市场统计与报告生成

### 市场统计

```bash
# 日度统计
./scripts/market_stats.py 2026-04-21 --mode daily

# 周度统计（周五运行）
./scripts/market_stats.py 2026-04-21 --mode weekly

# 高阶因子统计
./scripts/market_stats.py 2026-04-21 --mode advanced
```

### 报告生成

```bash
# 生成策略执行报告
./scripts/gen_reports.py 2026-04-21
```

---

## 策略汇总

| # | 策略 | 脚本 | 输出表 / 文件 |
|---|------|------|--------------|
| 1 | 基础独立强度 | `calc_independence_score.sh` | `independence_score_daily` |
| 2 | 时间加权-尾盘 | `calc_time_weighted_score.py --preset evening_focus` | `independence_score_time_weighted` |
| 3 | 时间加权-早盘 | `calc_time_weighted_score.py --preset morning_focus` | `independence_score_time_weighted` |
| 4 | 时间加权-趋势市 | `calc_time_weighted_score.py --preset trending_market` | `independence_score_time_weighted` |
| 5 | 时间加权-保守型 | `calc_time_weighted_score.py --preset conservative` | `independence_score_time_weighted` |
| 6 | 融资余额加权 | `calc_independence_score_margin_weighted.py` | `independence_score_margin_weighted` |
| 7 | S09/S10/S12 | `calc_advanced_score.py --strategy all` | `independence_score_advanced` |
| 8 | S11 周频一致性 | `calc_weekly_consistency.py` | `weekly_consistency_screening` |
