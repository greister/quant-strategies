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
│   └── queries_independence_score.sql    # 常用查询示例
├── scripts/
│   └── calc_independence_score.sh        # 批量计算脚本
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
