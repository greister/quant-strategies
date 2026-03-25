# 分时独立强度因子策略 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在 ClickHouse 中实现分时独立强度因子计算，识别板块下跌时抗跌的个股。

**Architecture:** 使用 ClickHouse 窗口函数计算板块收益率和个股相对表现，按日聚合生成独立强度分值。

**Tech Stack:** ClickHouse (klickhouse), SQL, Rust (可选集成)

---

## Task 1: 创建独立强度因子计算 SQL

**Files:**
- Create: `sql/calc_independence_score.sql`

**Step 1: 编写核心 SQL**

```sql
-- 分时独立强度因子计算
-- 参数: @trade_date DATE

WITH
-- 1. 计算个股 5 分钟收益率
stock_returns AS (
    SELECT
        symbol,
        toDate(datetime) as date,
        toStartOfInterval(datetime, INTERVAL 5 MINUTE) as time_bucket,
        (close - open) / open as ret
    FROM raw_stocks_5min
    WHERE toDate(datetime) = {trade_date:Date}
      AND open > 0
),

-- 2. 获取股票板块归属
stock_sector_map AS (
    SELECT
        s.symbol,
        sec.name as sector
    FROM stock_sectors s
    JOIN sectors sec ON s.sector_code = sec.code
),

-- 3. 计算板块 5 分钟收益率
sector_returns AS (
    SELECT
        m.sector,
        r.time_bucket,
        avg(r.ret) as sector_ret,
        count() as sector_stock_count
    FROM stock_returns r
    JOIN stock_sector_map m ON r.symbol = m.symbol
    GROUP BY m.sector, r.time_bucket
),

-- 4. 计算个股相对板块表现
relative_performance AS (
    SELECT
        r.symbol,
        r.date,
        r.time_bucket,
        r.ret as stock_ret,
        s.sector,
        s.sector_ret,
        s.sector_stock_count,
        r.ret - s.sector_ret as excess_ret
    FROM stock_returns r
    JOIN stock_sector_map m ON r.symbol = m.symbol
    JOIN sector_returns s ON m.sector = s.sector AND r.time_bucket = s.time_bucket
),

-- 5. 识别逆势区间并计分
contra_intervals AS (
    SELECT
        symbol,
        date,
        sector,
        sector_stock_count,
        countIf(sector_ret < -0.005 AND (stock_ret > 0 OR excess_ret > 0.01)) as contra_count
    FROM relative_performance
    GROUP BY symbol, date, sector, sector_stock_count
)

-- 6. 输出结果
SELECT
    symbol,
    date,
    sector,
    sector_stock_count,
    contra_count as raw_score,
    contra_count as score  -- 基础版本，后续可加融资加权
FROM contra_intervals
WHERE contra_count > 0
ORDER BY contra_count DESC;
```

**Step 2: 验证 SQL 语法**

Run: `clickhouse-client --database=tdx2db_rust < sql/calc_independence_score.sql`
Expected: 无语法错误

**Step 3: Commit**

```bash
git add sql/calc_independence_score.sql
git commit -m "feat: add independence score calculation SQL"
```

---

## Task 2: 创建结果表和视图

**Files:**
- Create: `sql/create_independence_tables.sql`

**Step 1: 编写建表 SQL**

```sql
-- 独立强度因子日表
CREATE TABLE IF NOT EXISTS independence_score_daily (
    symbol String,
    date Date,
    score Float64,
    raw_score Int32,
    margin_weight Float64 DEFAULT 1.0,
    sector String,
    sector_stock_count Int32,
    contra_count Int32 COMMENT '逆势区间数'
) ENGINE = ReplacingMergeTree()
ORDER BY (symbol, date);

-- 每日排名视图
CREATE OR REPLACE VIEW v_independence_leaders AS
SELECT
    date,
    symbol,
    score,
    sector,
    contra_count,
    rank() OVER (PARTITION BY date ORDER BY score DESC) as rank
FROM independence_score_daily
FINAL;
```

**Step 2: 执行建表**

Run: `clickhouse-client --database=tdx2db_rust < sql/create_independence_tables.sql`
Expected: OK

**Step 3: Commit**

```bash
git add sql/create_independence_tables.sql
git commit -m "feat: create independence score tables and views"
```

---

## Task 3: 创建批量计算脚本

**Files:**
- Create: `scripts/calc_independence_score.sh`

**Step 1: 编写脚本**

```bash
#!/bin/bash
# 批量计算独立强度因子

set -e

DB_NAME="${CLICKHOUSE_DB:-tdx2db_rust}"
DATE="${1:-$(date +%Y-%m-%d)}"

echo "Calculating independence score for date: $DATE"

clickhouse-client --database="$DB_NAME" --param_trade_date="$DATE" <<'EOF'
INSERT INTO independence_score_daily
WITH
stock_returns AS (
    SELECT
        symbol,
        toDate(datetime) as date,
        toStartOfInterval(datetime, INTERVAL 5 MINUTE) as time_bucket,
        (close - open) / open as ret
    FROM raw_stocks_5min
    WHERE toDate(datetime) = {trade_date:Date}
      AND open > 0
),
stock_sector_map AS (
    SELECT s.symbol, sec.name as sector
    FROM stock_sectors s
    JOIN sectors sec ON s.sector_code = sec.code
),
sector_returns AS (
    SELECT
        m.sector,
        r.time_bucket,
        avg(r.ret) as sector_ret,
        count() as sector_stock_count
    FROM stock_returns r
    JOIN stock_sector_map m ON r.symbol = m.symbol
    GROUP BY m.sector, r.time_bucket
),
relative_performance AS (
    SELECT
        r.symbol,
        r.date,
        r.time_bucket,
        r.ret as stock_ret,
        s.sector,
        s.sector_ret,
        s.sector_stock_count,
        r.ret - s.sector_ret as excess_ret
    FROM stock_returns r
    JOIN stock_sector_map m ON r.symbol = m.symbol
    JOIN sector_returns s ON m.sector = s.sector AND r.time_bucket = s.time_bucket
)
SELECT
    symbol,
    date,
    contra_count as score,
    contra_count as raw_score,
    1.0 as margin_weight,
    sector,
    sector_stock_count,
    contra_count
FROM (
    SELECT
        symbol,
        date,
        sector,
        sector_stock_count,
        countIf(sector_ret < -0.005 AND (stock_ret > 0 OR excess_ret > 0.01)) as contra_count
    FROM relative_performance
    GROUP BY symbol, date, sector, sector_stock_count
)
WHERE contra_count > 0
ORDER BY contra_count DESC;
EOF

echo "Done. Top 10 scores:"
clickhouse-client --database="$DB_NAME" -q "
    SELECT symbol, score, sector, contra_count
    FROM independence_score_daily
    WHERE date = '$DATE'
    ORDER BY score DESC
    LIMIT 10
"
```

**Step 2: 设置执行权限**

Run: `chmod +x scripts/calc_independence_score.sh`

**Step 3: 测试脚本**

Run: `./scripts/calc_independence_score.sh 2026-03-21`
Expected: 输出计算结果和 Top 10

**Step 4: Commit**

```bash
git add scripts/calc_independence_score.sh
git commit -m "feat: add independence score batch calculation script"
```

---

## Task 4: 创建查询示例

**Files:**
- Create: `sql/queries_independence_score.sql`

**Step 1: 编写常用查询**

```sql
-- 查询 1: 某日独立强度排名
SELECT
    symbol,
    score,
    sector,
    contra_count,
    rank() OVER (ORDER BY score DESC) as rank
FROM independence_score_daily
WHERE date = '2026-03-21'
ORDER BY score DESC
LIMIT 20;

-- 查询 2: 某股票历史独立强度走势
SELECT
    date,
    score,
    contra_count,
    sector
FROM independence_score_daily
WHERE symbol = '000001.SZ'
ORDER BY date DESC
LIMIT 60;

-- 查询 3: 板块内独立强度排名
SELECT
    symbol,
    score,
    contra_count,
    rank() OVER (PARTITION BY sector ORDER BY score DESC) as sector_rank
FROM independence_score_daily
WHERE date = '2026-03-21' AND sector = '银行'
ORDER BY score DESC;

-- 查询 4: 独立强度连续高分股票（近 5 日）
WITH daily_scores AS (
    SELECT
        symbol,
        groupArray(score) as scores,
        avg(score) as avg_score,
        min(score) as min_score
    FROM independence_score_daily
    WHERE date >= today() - 5
    GROUP BY symbol
    HAVING length(scores) >= 3
)
SELECT
    symbol,
    avg_score,
    min_score,
    scores
FROM daily_scores
WHERE min_score > 2
ORDER BY avg_score DESC
LIMIT 20;
```

**Step 2: Commit**

```bash
git add sql/queries_independence_score.sql
git commit -m "docs: add independence score query examples"
```

---

## Task 5: 创建 README 文档

**Files:**
- Create: `README.md`

**Step 1: 编写 README**

```markdown
# 分时独立强度因子

基于 ClickHouse 5 分钟线数据，计算个股相对板块的"逆势分"。

## 核心逻辑

- 板块跌幅 < -0.5% 时，个股涨或相对板块超额 > 1% → 计 1 分
- 全天累加得到独立强度分值

## 快速开始

```bash
# 建表
clickhouse-client --database=tdx2db_rust < sql/create_independence_tables.sql

# 计算某日因子
./scripts/calc_independence_score.sh 2026-03-21

# 查看结果
clickhouse-client --database=tdx2db_rust -q "
    SELECT * FROM v_independence_leaders
    WHERE date = '2026-03-21' LIMIT 10
"
```

## 文件结构

```
sql/
  calc_independence_score.sql      # 核心计算 SQL
  create_independence_tables.sql   # 建表和视图
  queries_independence_score.sql   # 常用查询示例
scripts/
  calc_independence_score.sh       # 批量计算脚本
```

## 参数调整

编辑 `sql/calc_independence_score.sql`：
- `sector_ret < -0.005`：板块跌幅阈值（默认 0.5%）
- `excess_ret > 0.01`：超额收益阈值（默认 1%）
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add project README"
```

---

## 后续扩展（可选）

### Task 6: 融资余额加权版本

**Files:**
- Create: `sql/calc_independence_score_margin_weighted.sql`

**说明：** 从 PostgreSQL 获取融资余额变化率，在应用层 JOIN 后加权。

### Task 7: Rust 集成

**Files:**
- Modify: `tdx2db-rust/src/calc/mod.rs`

**说明：** 将 SQL 逻辑集成到 Rust 工作流系统，支持定时任务。

### Task 8: 历史回测

**Files:**
- Create: `sql/backtest_independence_score.sql`

**说明：** 计算历史每日独立强度因子，与次日收益做相关性分析。
