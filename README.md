# 分时独立强度因子

基于 ClickHouse 5 分钟线数据，计算个股相对板块的"逆势分"。

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
│   ├── create_independence_tables.sql    # 建表脚本（结果表 + 视图）
│   ├── calc_independence_score.sql       # 核心计算逻辑（查询版）
│   ├── queries_independence_score.sql    # 常用查询示例
│   └── backtest_independence_score.sql   # 回测 SQL
├── scripts/
│   ├── calc_independence_score.sh                 # 批量计算脚本（基础版）
│   ├── calc_independence_score_margin_weighted.py # 融资加权版（CH+PG）
│   └── backtest_independence_score.py             # 历史回测脚本
└── docs/
    └── plans/                            # 设计文档
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
