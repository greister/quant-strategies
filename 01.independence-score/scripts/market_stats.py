#!/usr/bin/env python3
"""
A股5分钟分时统计分析脚本

生成3类Obsidian报告:
  daily     - 单日5分钟涨跌幅统计
  weekly    - 周统计（最近N个交易日）
  advanced  - 高阶因子分析（波动率脉冲/动态Beta/反转效应等）

用法:
  python market_stats.py [日期] [--mode daily|weekly|advanced|all] [--weeks N]

示例:
  python market_stats.py 2026-04-17
  python market_stats.py 2026-04-17 --mode daily
  python market_stats.py 2026-04-17 --mode weekly --weeks 5
  python market_stats.py 2026-04-17 --mode advanced
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ===== 配置 =====
VAULT_DIR = "/mnt/d/obsidian/OrbitOS-vault/30_Research/量化分析/策略执行结果/01-独立强度因子"
A_SHARE_FILTER = "(symbol LIKE 'sh6%' OR symbol LIKE 'sz0%' OR symbol LIKE 'sz3%' OR symbol LIKE 'bj%')"
BOARD_CASE = """CASE
    WHEN symbol LIKE 'sh68%' THEN '科创板'
    WHEN symbol LIKE 'sz3%' THEN '创业板'
    WHEN symbol LIKE 'sh6%' THEN '沪主板'
    WHEN symbol LIKE 'sz0%' THEN '深主板'
    WHEN symbol LIKE 'bj%' THEN '北交所'
END"""

SLOT_CASE = """CASE
    WHEN toHour(datetime) = 9 AND toMinute(datetime) = 35 THEN '09:35(首根)'
    WHEN toHour(datetime) <= 10 AND toMinute(datetime) <= 30 THEN '10:00-10:30'
    WHEN toHour(datetime) = 10 AND toMinute(datetime) > 30 THEN '10:30-11:00'
    WHEN toHour(datetime) = 11 THEN '11:00-11:30'
    WHEN toHour(datetime) = 13 AND toMinute(datetime) <= 30 THEN '13:00-13:30'
    WHEN toHour(datetime) = 13 AND toMinute(datetime) > 30 THEN '13:30-14:00'
    WHEN toHour(datetime) = 14 AND toMinute(datetime) <= 30 THEN '14:00-14:30'
    ELSE '14:30-15:00'
END"""

SLOT_4CASE = """CASE
    WHEN toHour(datetime) <= 10 AND toMinute(datetime) <= 30 THEN 'A.09:30-10:30'
    WHEN toHour(datetime) <= 11 THEN 'B.10:30-11:30'
    WHEN toHour(datetime) <= 14 THEN 'C.13:00-14:00'
    ELSE 'D.14:00-15:00'
END"""


def get_ch_client():
    """连接 ClickHouse"""
    return Client(
        host=os.getenv('CH_HOST', 'localhost'),
        port=int(os.getenv('CH_PORT', '9000')),
        database=os.getenv('CH_DB', 'tdx2db_rust'),
        user=os.getenv('CH_USER', 'default'),
        password=os.getenv('CH_PASSWORD', 'tdx2db'),
    )


def get_latest_date(ch):
    """获取最新交易日期"""
    rows = ch.execute("SELECT max(toDate(datetime)) FROM raw_stocks_5min")
    return rows[0][0].strftime('%Y-%m-%d') if rows else None


def get_trading_dates(ch, end_date, n=5):
    """获取最近N个交易日"""
    rows = ch.execute("""
        SELECT DISTINCT toDate(datetime) as d
        FROM raw_stocks_5min
        WHERE toDate(datetime) <= %(end)s
        ORDER BY d DESC
        LIMIT %(n)s
    """, {'end': end_date, 'n': n})
    dates = sorted([r[0].strftime('%Y-%m-%d') for r in rows])
    return dates


def dedup_base(date_cond, extra_where="AND toMinute(datetime) > 0"):
    """返回去重后的基础子查询。date_cond 为纯 SQL 条件片段，如 "= '2026-04-17'"。"""
    return f"""
    SELECT
        toDate(datetime) as d, symbol, datetime,
        toHour(datetime) as h, toMinute(datetime) as m,
        open, high, low, close, volume, amount,
        row_number() OVER (PARTITION BY symbol, datetime ORDER BY datetime) as rn,
        (close - open) / nullIf(open, 0) * 100 as ret,
        (high - low) / nullIf(open, 0) * 100 as intra_vol,
        {BOARD_CASE} as board
    FROM raw_stocks_5min
    WHERE toDate(datetime) {date_cond}
      AND {A_SHARE_FILTER}
      AND toHour(datetime) >= 9 AND toHour(datetime) <= 15
      {extra_where}
    """


# ================================================================
#  DAILY REPORT
# ================================================================

def daily_overview(ch, date):
    """单日概览"""
    dc = f"= '{date}'"
    rows = ch.execute(f"""
    SELECT
        count(DISTINCT symbol) as a_shares,
        count(*) as bars,
        round(avg(ret), 5) as avg_ret,
        round(median(ret), 4) as med_ret,
        round(countIf(ret < 0) / count(*) * 100, 2) as down_pct,
        round(countIf(ret < -0.2) / count(*) * 100, 2) as below_02_pct,
        round(countIf(ret < -0.5) / count(*) * 100, 2) as below_05_pct,
        round(stddevPop(ret), 4) as vol
    FROM ({dedup_base(dc)}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20
    """)
    return rows[0] if rows else None


def daily_updown(ch, date):
    """日线涨跌分布"""
    dc = f"= '{date}'"
    rows = ch.execute(f"""
    SELECT
        count(*) as total,
        countIf(day_ret > 0.5) as up_big,
        countIf(day_ret > 0 AND day_ret <= 0.5) as up_small,
        countIf(day_ret = 0) as flat,
        countIf(day_ret < 0 AND day_ret >= -0.5) as down_small,
        countIf(day_ret < -0.5) as down_big,
        round(avg(day_ret), 4) as avg_day_ret
    FROM (
        SELECT symbol,
            (argMax(close, datetime) - argMin(open, datetime)) / nullIf(argMin(open, datetime), 0) * 100 as day_ret
        FROM ({dedup_base(dc, '')}) sub
        WHERE rn = 1
        GROUP BY symbol
        HAVING day_ret BETWEEN -30 AND 30
    )
    """)
    return rows[0] if rows else None


def daily_histogram(ch, date):
    """5分钟收益率直方图"""
    dc = f"= '{date}'"
    rows = ch.execute(f"""
    SELECT
        countIf(ret < -2) as n_lt_2,
        countIf(ret >= -2 AND ret < -1) as n_2_1,
        countIf(ret >= -1 AND ret < -0.5) as n_1_05,
        countIf(ret >= -0.5 AND ret < -0.2) as n_05_02,
        countIf(ret >= -0.2 AND ret < 0) as n_02_0,
        countIf(ret >= 0 AND ret < 0.2) as n_0_02,
        countIf(ret >= 0.2 AND ret < 0.5) as n_02_05,
        countIf(ret >= 0.5 AND ret < 1) as n_05_1,
        countIf(ret >= 1 AND ret < 2) as n_1_2,
        countIf(ret >= 2) as n_gt_2
    FROM ({dedup_base(dc)}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20
    """)
    return rows[0] if rows else None


def daily_quantiles(ch, date):
    """分位数"""
    dc = f"= '{date}'"
    rows = ch.execute(f"""
    SELECT
        round(quantile(0.01)(ret), 4),
        round(quantile(0.05)(ret), 4),
        round(quantile(0.10)(ret), 4),
        round(quantile(0.25)(ret), 4),
        round(quantile(0.50)(ret), 4),
        round(quantile(0.75)(ret), 4),
        round(quantile(0.90)(ret), 4),
        round(quantile(0.95)(ret), 4),
        round(quantile(0.99)(ret), 4)
    FROM ({dedup_base(dc)}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20
    """)
    return rows[0] if rows else None


def daily_board_stats(ch, date):
    """按板块统计"""
    dc = f"= '{date}'"
    return ch.execute(f"""
    SELECT board, count(DISTINCT symbol) as stocks,
        round(avg(ret), 5), round(countIf(ret < 0) / count(*) * 100, 2), round(stddevPop(ret), 4)
    FROM ({dedup_base(dc)}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20 AND board IS NOT NULL
    GROUP BY board ORDER BY board
    """)


def daily_slot_stats(ch, date):
    """按时段统计"""
    dc = f"= '{date}'"
    return ch.execute(f"""
    SELECT {SLOT_CASE} as slot, count(*) as bars,
        round(avg(ret), 5), round(countIf(ret < 0) / count(*) * 100, 2)
    FROM ({dedup_base(dc)}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20
    GROUP BY slot ORDER BY slot
    """)


def daily_industry_rank(ch, date, direction='desc', limit=10):
    """行业排名"""
    dc = f"= '{date}'"
    order = 'DESC' if direction == 'desc' else 'ASC'
    return ch.execute(f"""
    SELECT m.industry_name, count(DISTINCT sub.symbol) as stocks,
        round(avg(sub.ret), 5), round(countIf(sub.ret < 0) / count(*) * 100, 2), round(stddevPop(sub.ret), 4)
    FROM ({dedup_base(dc)}) sub
    INNER JOIN stock_industry_mapping m
        ON replaceRegexpOne(sub.symbol, '^(sh|sz|bj)', '') = m.symbol AND m.industry_code LIKE 'T%%'
    WHERE sub.rn = 1 AND sub.ret BETWEEN -20 AND 20
    GROUP BY m.industry_name
    HAVING stocks >= 5
    ORDER BY avg(sub.ret) {order}
    LIMIT {limit}
    """)


def daily_thresholds(ch, date):
    """阈值分析"""
    dc = f"= '{date}'"
    rows = ch.execute(f"""
    SELECT
        round(countIf(ret < -0.1) / count(*) * 100, 2),
        round(countIf(ret < -0.2) / count(*) * 100, 2),
        round(countIf(ret < -0.3) / count(*) * 100, 2),
        round(countIf(ret < -0.5) / count(*) * 100, 2),
        round(countIf(ret < -1.0) / count(*) * 100, 2)
    FROM ({dedup_base(dc)}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20
    """)
    return rows[0] if rows else None


def render_daily(date, overview, updown, histogram, quantiles, boards, slots, ind_strong, ind_weak, thresholds):
    """渲染单日报告 Markdown"""
    o = overview
    u = updown
    h = histogram
    q = quantiles
    t = thresholds

    def pct(v): return f"{v:.2f}%"
    def f5(v): return f"{v:.5f}%"

    total = u[0] if u else 0
    up_pct = f"{(u[1]+u[2])/total*100:.2f}" if total else "?"
    down_pct = f"{(u[4]+u[5])/total*100:.2f}" if total else "?"

    report = f"""---
title: "A股5分钟分时涨跌幅统计"
date: {date}
type: market-statistics
scope: A-share-5min-returns
status: completed
tags: [量化, 市场统计, 5分钟K线, 涨跌幅分布]
---

# A股5分钟分时涨跌幅统计 — {date}

## 数据概览

| 指标 | 值 |
|------|------|
| 统计日期 | {date} |
| A股标的数 | {o[0]:,} |
| 5分钟K线总数 | {o[1]:,} |
| 数据源 | ClickHouse `raw_stocks_5min` |
| A股过滤 | sh6/sz0/sz3/bj 排除ETF/基金/债券 |

## 日线级别涨跌分布

| 方向 | 股票数 | 占比 |
|------|--------|------|
| 上涨(>0.5%) | {u[1]:,} | {(u[1]/total*100):.2f}% |
| 小涨(0~0.5%) | {u[2]:,} | {(u[2]/total*100):.2f}% |
| 平盘 | {u[3]:,} | {(u[3]/total*100):.2f}% |
| 小跌(-0.5~0) | {u[4]:,} | {(u[4]/total*100):.2f}% |
| 下跌(<-0.5%) | {u[5]:,} | {(u[5]/total*100):.2f}% |
| **全市场平均日收益** | **{u[6]:.2f}%** | — |

## 5分钟收益率分布

| 收益率区间 | K线数 | 占比 |
|-----------|-------|------|
| < -2.0% | {h[0]:,} | {h[0]/o[1]*100:.2f}% |
| -2.0% ~ -1.0% | {h[1]:,} | {h[1]/o[1]*100:.2f}% |
| -1.0% ~ -0.5% | {h[2]:,} | {h[2]/o[1]*100:.2f}% |
| -0.5% ~ -0.2% | {h[3]:,} | {h[3]/o[1]*100:.2f}% |
| -0.2% ~ 0.0% | {h[4]:,} | {h[4]/o[1]*100:.2f}% |
| 0.0% ~ +0.2% | {h[5]:,} | {h[5]/o[1]*100:.2f}% |
| +0.2% ~ +0.5% | {h[6]:,} | {h[6]/o[1]*100:.2f}% |
| +0.5% ~ +1.0% | {h[7]:,} | {h[7]/o[1]*100:.2f}% |
| +1.0% ~ +2.0% | {h[8]:,} | {h[8]/o[1]*100:.2f}% |
| > +2.0% | {h[9]:,} | {h[9]/o[1]*100:.2f}% |

### 关键分位数

| 分位 | 5分钟收益率 |
|------|-----------|
| P1 | {q[0]:.4f}% |
| P5 | {q[1]:.4f}% |
| P10 | {q[2]:.4f}% |
| P25 | {q[3]:.4f}% |
| P50 | {q[4]:.4f}% |
| P75 | {q[5]:.4f}% |
| P90 | {q[6]:.4f}% |
| P95 | {q[7]:.4f}% |
| P99 | {q[8]:.4f}% |

> [!tip] 阈值选择依据
> 约 **{t[1]:.2f}%** 的 5 分钟 K 线收益率低于 -0.2%。

## 按板块统计

| 板块 | 股票数 | 5分钟平均 | 下跌占比 | 波动率 |
|------|--------|----------|---------|--------|
"""
    for b in boards:
        report += f"| {b[0]} | {b[1]} | {b[2]:.5f}% | {b[3]:.2f}% | {b[4]:.4f}% |\n"

    report += "\n## 按交易时段统计\n\n| 时段 | K线数 | 平均收益 | 下跌占比 |\n|------|-------|---------|----------|\n"
    for s in slots:
        report += f"| {s[0]} | {s[1]:,} | {s[2]:.5f}% | {s[3]:.2f}% |\n"

    report += "\n## 行业排名\n\n### 最强行业 (Top 10)\n\n| 行业 | 股票数 | 5分钟均值 | 下跌占比 | 波动率 |\n|------|--------|----------|---------|--------|\n"
    for i in ind_strong:
        report += f"| {i[0]} | {i[1]} | {i[2]:.5f}% | {i[3]:.2f}% | {i[4]:.4f}% |\n"

    report += "\n### 最弱行业 (Top 10)\n\n| 行业 | 股票数 | 5分钟均值 | 下跌占比 | 波动率 |\n|------|--------|----------|---------|--------|\n"
    for i in ind_weak:
        report += f"| {i[0]} | {i[1]} | {i[2]:.5f}% | {i[3]:.2f}% | {i[4]:.4f}% |\n"

    report += f"""
## 阈值影响分析

| 阈值 | 触发占比 |
|------|---------|
| < -0.1% | {t[0]:.2f}% |
| < -0.2% | **{t[1]:.2f}%** |
| < -0.3% | {t[2]:.2f}% |
| < -0.5% | {t[3]:.2f}% |
| < -1.0% | {t[4]:.2f}% |

---

*数据源: ClickHouse `tdx2db_rust.raw_stocks_5min` + `stock_industry_mapping`*
"""
    return report


def generate_daily(ch, date):
    """生成单日报告"""
    log.info(f"Generating daily report for {date}")
    o = daily_overview(ch, date)
    if not o or o[1] == 0:
        log.error(f"No data for {date}")
        return None
    u = daily_updown(ch, date)
    h = daily_histogram(ch, date)
    q = daily_quantiles(ch, date)
    b = daily_board_stats(ch, date)
    s = daily_slot_stats(ch, date)
    ind_s = daily_industry_rank(ch, date, 'desc', 10)
    ind_w = daily_industry_rank(ch, date, 'asc', 10)
    t = daily_thresholds(ch, date)

    report = render_daily(date, o, u, h, q, b, s, ind_s, ind_w, t)
    path = os.path.join(VAULT_DIR, f"{date}_A股5分钟分时涨跌幅统计.md")
    with open(path, 'w') as f:
        f.write(report)
    log.info(f"Daily report: {path}")
    return path


# ================================================================
#  WEEKLY REPORT
# ================================================================

def generate_weekly(ch, dates):
    """生成周统计报告"""
    log.info(f"Generating weekly report for {dates[0]} ~ {dates[-1]}")
    start, end = dates[0], dates[-1]
    date_cond = f">= '{start}' AND toDate(datetime) <= '{end}'"
    date_in = tuple(dates)

    # 每日概览
    daily_rows = ch.execute(f"""
    SELECT d, count(DISTINCT symbol) as a_shares, count(*) as bars,
        round(avg(ret), 5), round(median(ret), 4),
        round(countIf(ret < 0) / count(*) * 100, 2),
        round(countIf(ret < -0.2) / count(*) * 100, 2),
        round(countIf(ret < -0.5) / count(*) * 100, 2),
        round(stddevPop(ret), 4)
    FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'")}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20
    GROUP BY d ORDER BY d
    """)

    # 每日涨跌
    updown_rows = ch.execute(f"""
    SELECT d, count(*) as total,
        countIf(day_ret > 0.5), countIf(day_ret > 0 AND day_ret <= 0.5),
        countIf(day_ret = 0),
        countIf(day_ret < 0 AND day_ret >= -0.5), countIf(day_ret < -0.5),
        round(avg(day_ret), 4)
    FROM (
        SELECT toDate(datetime) as d, symbol,
            (argMax(close, datetime) - argMin(open, datetime)) / nullIf(argMin(open, datetime), 0) * 100 as day_ret
        FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'", '')}) sub
        WHERE rn = 1
        GROUP BY d, symbol
        HAVING day_ret BETWEEN -30 AND 30
    ) GROUP BY d ORDER BY d
    """)

    # 每日分位数
    quantile_rows = ch.execute(f"""
    SELECT d,
        round(quantile(0.01)(ret), 4), round(quantile(0.05)(ret), 4),
        round(quantile(0.10)(ret), 4), round(quantile(0.25)(ret), 4),
        round(quantile(0.50)(ret), 4),
        round(quantile(0.75)(ret), 4), round(quantile(0.90)(ret), 4),
        round(quantile(0.95)(ret), 4), round(quantile(0.99)(ret), 4)
    FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'")}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20
    GROUP BY d ORDER BY d
    """)

    # 每日时段统计
    slot_rows = ch.execute(f"""
    SELECT d, {SLOT_CASE} as slot, count(*) as bars,
        round(avg(ret), 5), round(countIf(ret < 0) / count(*) * 100, 2)
    FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'")}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20
    GROUP BY d, slot ORDER BY d, slot
    """)

    # 每日板块统计
    board_rows = ch.execute(f"""
    SELECT d, board, count(DISTINCT symbol) as stocks,
        round(avg(ret), 5), round(countIf(ret < 0) / count(*) * 100, 2), round(stddevPop(ret), 4)
    FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'")}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20 AND board IS NOT NULL
    GROUP BY d, board ORDER BY d, board
    """)

    # 周行业排名
    ind_strong = ch.execute(f"""
    SELECT m.industry_name, count(DISTINCT sub.symbol) as stocks,
        round(avg(sub.ret), 5), round(countIf(sub.ret < 0) / count(*) * 100, 2), round(stddevPop(sub.ret), 4)
    FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'")}) sub
    INNER JOIN stock_industry_mapping m
        ON replaceRegexpOne(sub.symbol, '^(sh|sz|bj)', '') = m.symbol AND m.industry_code LIKE 'T%%'
    WHERE sub.rn = 1 AND sub.ret BETWEEN -20 AND 20
    GROUP BY m.industry_name HAVING stocks >= 5
    ORDER BY avg(sub.ret) DESC LIMIT 10
    """)
    ind_weak = ch.execute(f"""
    SELECT m.industry_name, count(DISTINCT sub.symbol) as stocks,
        round(avg(sub.ret), 5), round(countIf(sub.ret < 0) / count(*) * 100, 2), round(stddevPop(sub.ret), 4)
    FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'")}) sub
    INNER JOIN stock_industry_mapping m
        ON replaceRegexpOne(sub.symbol, '^(sh|sz|bj)', '') = m.symbol AND m.industry_code LIKE 'T%%'
    WHERE sub.rn = 1 AND sub.ret BETWEEN -20 AND 20
    GROUP BY m.industry_name HAVING stocks >= 5
    ORDER BY avg(sub.ret) ASC LIMIT 10
    """)

    # 每日阈值
    thresh_rows = ch.execute(f"""
    SELECT d,
        round(countIf(ret < -0.1) / count(*) * 100, 2),
        round(countIf(ret < -0.2) / count(*) * 100, 2),
        round(countIf(ret < -0.3) / count(*) * 100, 2),
        round(countIf(ret < -0.5) / count(*) * 100, 2),
        round(countIf(ret < -1.0) / count(*) * 100, 2)
    FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'")}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20
    GROUP BY d ORDER BY d
    """)

    # 渲染报告
    isoweek = datetime.strptime(end, '%Y-%m-%d').isocalendar()
    week_label = f"{isoweek[0]}-W{isoweek[1]:02d}"

    report = f"""---
title: "A股5分钟分时涨跌幅周统计 ({week_label})"
date: {end}
period: {start} ~ {end}
type: market-statistics
scope: A-share-5min-returns-weekly
status: completed
tags: [量化, 市场统计, 5分钟K线, 周统计]
---

# A股5分钟分时涨跌幅周统计 — {start} 至 {end}

## 数据范围

| 指标 | 值 |
|------|------|
| 统计周期 | {start} ~ {end} |
| 交易日数 | {len(dates)} |

## 日线级别市场全景

### 每日涨跌分布

| 日期 | 上涨(>0.5%) | 小涨 | 平盘 | 小跌 | 下跌(<-0.5%) | 平均日收益 |
|------|-----------|------|------|------|------------|-----------|
"""
    for r in updown_rows:
        d = r[0].strftime('%Y-%m-%d') if hasattr(r[0], 'strftime') else str(r[0])
        report += f"| {d} | {r[2]:,} | {r[3]:,} | {r[4]:,} | {r[5]:,} | {r[6]:,} | {r[7]:.2f}% |\n"

    report += "\n### 每日5分钟收益率概览\n\n| 日期 | A股数 | K线数 | 平均收益 | 中位数 | 下跌占比 | <-0.2% | <-0.5% | 波动率 |\n|------|--------|-------|---------|--------|---------|--------|--------|--------|\n"
    for r in daily_rows:
        d = r[0].strftime('%Y-%m-%d') if hasattr(r[0], 'strftime') else str(r[0])
        report += f"| {d} | {r[1]:,} | {r[2]:,} | {r[3]:.5f}% | {r[4]:.4f}% | {r[5]:.2f}% | {r[6]:.2f}% | {r[7]:.2f}% | {r[8]:.4f}% |\n"

    report += "\n## 每日分位数对比\n\n| 分位 |"
    for r in daily_rows:
        d = r[0].strftime('%m-%d') if hasattr(r[0], 'strftime') else str(r[0])[-5:]
        report += f" {d} |"
    report += "\n|------|"
    for _ in daily_rows:
        report += "-------|"
    report += "\n"
    for qi, qlabel in enumerate(['P1', 'P5', 'P10', 'P25', 'P50', 'P75', 'P90', 'P95', 'P99']):
        report += f"| {qlabel} |"
        for r in quantile_rows:
            report += f" {r[qi+1]:.4f}% |"
        report += "\n"

    report += "\n## 交易时段分析\n\n| 时段 |"
    for r in daily_rows:
        d = r[0].strftime('%m-%d') if hasattr(r[0], 'strftime') else str(r[0])[-5:]
        report += f" {d} |"
    report += " 周均 |\n|------|"
    for _ in daily_rows:
        report += "-------|"
    report += "------|\n"

    # 按slot聚合
    slots_map = {}
    for r in slot_rows:
        slot = r[1]
        if slot not in slots_map:
            slots_map[slot] = []
        slots_map[slot].append((r[3], r[4]))
    for slot in sorted(slots_map.keys()):
        report += f"| {slot} |"
        vals = slots_map[slot]
        for v in vals:
            report += f" {v[0]:.5f}% |"
        avg_val = sum(v[0] for v in vals) / len(vals) if vals else 0
        report += f" {avg_val:.5f}% |\n"

    report += "\n## 板块对比\n\n| 日期 | 沪主板 | 深主板 | 创业板 | 科创板 | 北交所 |\n|------|--------|--------|--------|--------|--------|\n"
    # 按日期聚合
    board_by_date = {}
    for r in board_rows:
        d = r[0].strftime('%Y-%m-%d') if hasattr(r[0], 'strftime') else str(r[0])
        if d not in board_by_date:
            board_by_date[d] = {}
        board_by_date[d][r[1]] = f"{r[3]:.5f}%"
    for d in dates:
        bd = board_by_date.get(d, {})
        report += f"| {d} | {bd.get('沪主板', '—')} | {bd.get('深主板', '—')} | {bd.get('创业板', '—')} | {bd.get('科创板', '—')} | {bd.get('北交所', '—')} |\n"

    report += "\n## 行业排名\n\n### 周累计最强行业 (Top 10)\n\n| 行业 | 股票数 | 5min均值 | 下跌占比 | 波动率 |\n|------|--------|---------|---------|--------|\n"
    for i in ind_strong:
        report += f"| {i[0]} | {i[1]} | {i[2]:.5f}% | {i[3]:.2f}% | {i[4]:.4f}% |\n"
    report += "\n### 周累计最弱行业 (Top 10)\n\n| 行业 | 股票数 | 5min均值 | 下跌占比 | 波动率 |\n|------|--------|---------|---------|--------|\n"
    for i in ind_weak:
        report += f"| {i[0]} | {i[1]} | {i[2]:.5f}% | {i[3]:.2f}% | {i[4]:.4f}% |\n"

    report += "\n## 阈值影响分析\n\n| 日期 | <-0.1% | <-0.2% | <-0.3% | <-0.5% | <-1.0% |\n|------|--------|--------|--------|--------|--------|\n"
    for r in thresh_rows:
        d = r[0].strftime('%Y-%m-%d') if hasattr(r[0], 'strftime') else str(r[0])
        report += f"| {d} | {r[1]} | **{r[2]}** | {r[3]} | {r[4]} | {r[5]} |\n"

    report += f"\n---\n\n*数据源: ClickHouse `tdx2db_rust.raw_stocks_5min` + `stock_industry_mapping`*\n*关联报告: [[{end}_A股5分钟分时涨跌幅统计]] | [[{end}_独立强度因子策略执行报告]]*\n"

    path = os.path.join(VAULT_DIR, f"{week_label}_A股5分钟分时涨跌幅周统计.md")
    with open(path, 'w') as f:
        f.write(report)
    log.info(f"Weekly report: {path}")
    return path


# ================================================================
#  ADVANCED REPORT
# ================================================================

def generate_advanced(ch, dates):
    """生成高阶因子分析报告"""
    log.info(f"Generating advanced report for {dates[0]} ~ {dates[-1]}")
    start, end = dates[0], dates[-1]
    date_cond = f">= '{start}' AND toDate(datetime) <= '{end}'"

    # 1. 成交额时段分布
    vol_dist = ch.execute(f"""
    SELECT d, {SLOT_4CASE} as slot,
        round(sum(amount) / sum(sum(amount)) OVER (PARTITION BY d) * 100, 2) as amt_pct,
        round(sum(volume) / sum(sum(volume)) OVER (PARTITION BY d) * 100, 2) as vol_pct
    FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'", '')}) sub
    WHERE rn = 1
    GROUP BY d, slot ORDER BY d, slot
    """)

    # 2. 波动率脉冲 (先算板块均值，再计算脉冲)
    board_vol_avg = ch.execute(f"""
    SELECT board, round(avg(intra_vol), 4) as avg_vol
    FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'")}) sub
    WHERE rn = 1 AND intra_vol BETWEEN 0 AND 30 AND board IS NOT NULL
    GROUP BY board
    """)
    vol_spike = []
    for bv in board_vol_avg:
        bd, avg_v = bv[0], bv[1]
        spike_row = ch.execute(f"""
        SELECT board, count(*) as total_bars,
            round(avg(intra_vol), 4) as avg_vol,
            countIf(intra_vol > 3 * {avg_v}) as spike_3x,
            round(countIf(intra_vol > 3 * {avg_v}) / count(*) * 100, 3) as spike_3x_pct,
            countIf(intra_vol > 5 * {avg_v}) as spike_5x,
            round(countIf(intra_vol > 5 * {avg_v}) / count(*) * 100, 3) as spike_5x_pct
        FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'")}) sub
        WHERE rn = 1 AND intra_vol BETWEEN 0 AND 30 AND board = '{bd}'
        GROUP BY board
        """)
        if spike_row:
            vol_spike.append(spike_row[0])

    # 3. 动态Beta (取最弱日和最强日)
    # 先找最弱和最强日
    daily_avg = ch.execute(f"""
    SELECT d, round(avg(ret), 5) as avg_ret
    FROM ({dedup_base(date_cond)}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20
    GROUP BY d ORDER BY avg_ret ASC
    """)
    if len(daily_avg) >= 2:
        weakest = daily_avg[0][0].strftime('%Y-%m-%d') if hasattr(daily_avg[0][0], 'strftime') else str(daily_avg[0][0])
        strongest = daily_avg[-1][0].strftime('%Y-%m-%d') if hasattr(daily_avg[-1][0], 'strftime') else str(daily_avg[-1][0])
    else:
        weakest = strongest = dates[0]

    beta_rows = ch.execute(f"""
    SELECT m.industry_name, count(*) as bars,
        round(
            (sum(ret * sector_ret) - sum(ret) * sum(sector_ret) / count(*)) /
            nullIf(
                sqrt(sum(ret*ret) - sum(ret)*sum(ret)/count(*)) *
                sqrt(sum(sector_ret*sector_ret) - sum(sector_ret)*sum(sector_ret)/count(*)), 0
            ), 4
        ) as corr,
        toDate(datetime) as d
    FROM (
        SELECT symbol, datetime,
            replaceRegexpOne(symbol, '^(sh|sz|bj)', '') as pure_sym,
            row_number() OVER (PARTITION BY symbol, datetime ORDER BY datetime) as rn,
            (close - open) / nullIf(open, 0) * 100 as ret
        FROM raw_stocks_5min
        WHERE toDate(datetime) IN ('{weakest}', '{strongest}')
          AND {A_SHARE_FILTER}
          AND toHour(datetime) >= 9 AND toHour(datetime) <= 15
          AND toMinute(datetime) > 0
    ) s
    INNER JOIN stock_industry_mapping m ON s.pure_sym = m.symbol AND m.industry_code LIKE 'T%%'
    INNER JOIN (
        SELECT toDate(s2.datetime) as d2, s2.datetime as dt2, m2.industry_name as ind2,
            avg((s2.close - s2.open) / nullIf(s2.open, 0) * 100) as sector_ret
        FROM raw_stocks_5min s2
        INNER JOIN stock_industry_mapping m2
            ON replaceRegexpOne(s2.symbol, '^(sh|sz|bj)', '') = m2.symbol AND m2.industry_code LIKE 'T%%'
        WHERE toDate(s2.datetime) IN ('{weakest}', '{strongest}')
          AND {A_SHARE_FILTER.replace('symbol', 's2.symbol')}
          AND toHour(s2.datetime) >= 9 AND toHour(s2.datetime) <= 15 AND toMinute(s2.datetime) > 0
        GROUP BY d2, dt2, m2.industry_name HAVING count(*) >= 5
    ) sec ON toDate(s.datetime) = sec.d2 AND s.datetime = sec.dt2 AND m.industry_name = sec.ind2
    WHERE s.rn = 1 AND s.ret BETWEEN -20 AND 20
    AND m.industry_name IN ('半导体', '元器件', '化学制药', '软件服务', '电气设备', '通信设备', '汽车配件', '专用机械', '互联网', '生物制药')
    GROUP BY m.industry_name, toDate(s.datetime)
    ORDER BY m.industry_name, d
    """)

    # 4. 早盘反转
    reversal_rows = ch.execute(f"""
    SELECT d,
        CASE
            WHEN morning_avg >= 0 THEN 'AM_resistant(>=0)'
            WHEN morning_avg >= -0.05 THEN 'AM_slight_down'
            WHEN morning_avg >= -0.15 THEN 'AM_moderate_down'
            ELSE 'AM_deep_down(<-0.15)'
        END as morning_group,
        count(*) as stocks,
        round(avg(pm_ret), 5) as pm_avg_ret,
        round(countIf(pm_ret > 0) / count(*) * 100, 2) as pm_up_pct
    FROM (
        SELECT m.d, m.symbol, m.morning_avg, a.pm_ret
        FROM (
            SELECT d, symbol, avg(ret) as morning_avg
            FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'")}) sub
            WHERE rn = 1 AND ret BETWEEN -20 AND 20 AND h = 9 AND sub.m > 0
            GROUP BY d, symbol
        ) m
        JOIN (
            SELECT d, symbol, avg(ret) as pm_ret
            FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'")}) sub
            WHERE rn = 1 AND ret BETWEEN -20 AND 20
              AND ((h = 13 AND sub.m > 30) OR (h = 14 AND sub.m <= 30))
            GROUP BY d, symbol
        ) a ON m.d = a.d AND m.symbol = a.symbol
    ) combined
    GROUP BY d, morning_group
    ORDER BY d, morning_group
    """)

    # 5. P99 极端后续
    p99_rows = ch.execute(f"""
    SELECT d, gap,
        count(*) as cnt,
        round(avg(next_ret), 5) as avg_ret,
        round(countIf(next_ret < 0) / count(*) * 100, 2) as down_pct
    FROM (
        SELECT
            toDate(s1.datetime) as d,
            (s2.bar_seq - s1.bar_seq) as gap,
            s2.ret as next_ret
        FROM (
            SELECT symbol, datetime,
                row_number() OVER (PARTITION BY symbol ORDER BY datetime) as bar_seq,
                ret
            FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'")}) sub
            WHERE rn = 1 AND ret BETWEEN -20 AND 20 AND ret > 1.0
        ) s1
        JOIN (
            SELECT symbol, datetime,
                row_number() OVER (PARTITION BY symbol ORDER BY datetime) as bar_seq,
                ret
            FROM ({dedup_base(f">= '{start}' AND toDate(datetime) <= '{end}'")}) sub
            WHERE rn = 1 AND ret BETWEEN -20 AND 20
        ) s2 ON s1.symbol = s2.symbol AND s2.bar_seq > s1.bar_seq AND s2.bar_seq <= s1.bar_seq + 3
    ) GROUP BY d, gap ORDER BY d, gap
    """)

    # 渲染
    isoweek = datetime.strptime(end, '%Y-%m-%d').isocalendar()
    week_label = f"{isoweek[0]}-W{isoweek[1]:02d}"

    report = f"""---
title: "A股5分钟高阶因子分析 ({week_label})"
date: {end}
period: {start} ~ {end}
type: advanced-analysis
scope: A-share-5min-advanced-factors
status: completed
tags: [量化, 高阶因子, 波动率, Beta, 反转]
---

# A股5分钟高阶因子分析 — {start} 至 {end}

## 一、成交额时段分布

| 日期 | 时段 | 成交额占比 | 成交量占比 |
|------|------|----------|----------|
"""
    for r in vol_dist:
        d = r[0].strftime('%m-%d') if hasattr(r[0], 'strftime') else str(r[0])[-5:]
        report += f"| {d} | {r[1]} | {r[2]}% | {r[3]}% |\n"

    report += "\n## 二、波动率脉冲\n\n| 板块 | K线数 | 平均波动率 | 3x脉冲占比 | 5x脉冲占比 |\n|------|--------|----------|----------|----------|\n"
    for r in vol_spike:
        report += f"| {r[0]} | {r[1]:,} | {r[2]:.4f}% | {r[4]:.3f}% | {r[6]:.3f}% |\n"

    report += f"\n## 三、动态Beta\n\n**最弱日: {weakest} | 最强日: {strongest}**\n\n| 行业 | 最弱日 corr | 最强日 corr | 变化 |\n|------|-----------|-----------|------|\n"
    # 按行业聚合
    beta_by_ind = {}
    for r in beta_rows:
        ind = r[0]
        d = r[3].strftime('%Y-%m-%d') if hasattr(r[3], 'strftime') else str(r[3])
        if ind not in beta_by_ind:
            beta_by_ind[ind] = {}
        beta_by_ind[ind][d] = r[2]
    for ind in sorted(beta_by_ind.keys()):
        vals = beta_by_ind[ind]
        w = vals.get(weakest, '—')
        s = vals.get(strongest, '—')
        diff = f"{s - w:+.2f}" if isinstance(w, float) and isinstance(s, float) else '—'
        w_str = f"{w:.4f}" if isinstance(w, float) else w
        s_str = f"{s:.4f}" if isinstance(s, float) else s
        report += f"| {ind} | {w_str} | {s_str} | {diff} |\n"

    report += "\n## 四、时段反转效应\n\n| 日期 | 早盘分组 | 股票数 | 午后均收益 | 午后上涨概率 |\n|------|---------|--------|----------|-------------|\n"
    for r in reversal_rows:
        d = r[0].strftime('%m-%d') if hasattr(r[0], 'strftime') else str(r[0])[-5:]
        report += f"| {d} | {r[1]} | {r[2]:,} | {r[3]:.5f}% | {r[4]:.2f}% |\n"

    report += "\n## 五、P99极端拉升后续走势\n\n| 日期 | 间隔 | 事件数 | 平均收益 | 回撤率 |\n|------|------|--------|---------|--------|\n"
    for r in p99_rows:
        d = r[0].strftime('%m-%d') if hasattr(r[0], 'strftime') else str(r[0])[-5:]
        report += f"| {d} | +{r[1]}根 | {r[2]:,} | {r[3]:.5f}% | {r[4]:.2f}% |\n"

    report += f"""
---

*数据源: ClickHouse `tdx2db_rust.raw_stocks_5min` + `stock_industry_mapping`*
*基础统计: [[{week_label}_A股5分钟分时涨跌幅周统计]]*
*策略报告: [[{end}_独立强度因子策略执行报告]]*
"""

    path = os.path.join(VAULT_DIR, f"{week_label}_A股5分钟高阶因子分析.md")
    with open(path, 'w') as f:
        f.write(report)
    log.info(f"Advanced report: {path}")
    return path


# ================================================================
#  MAIN
# ================================================================

def main():
    parser = argparse.ArgumentParser(description='A股5分钟分时统计分析')
    parser.add_argument('date', nargs='?', help='目标日期 (YYYY-MM-DD)，默认最新交易日')
    parser.add_argument('--mode', choices=['daily', 'weekly', 'advanced', 'all'], default='all',
                        help='报告模式 (default: all)')
    parser.add_argument('--weeks', type=int, default=5, help='周统计天数 (default: 5)')
    args = parser.parse_args()

    # 加载环境变量
    env_file = Path(__file__).resolve().parent.parent.parent / '00.shared' / 'config' / 'database.env'
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.strip() and not line.startswith('#') and '=' in line:
                k, v = line.strip().split('=', 1)
                os.environ.setdefault(k, v)

    ch = get_ch_client()
    date = args.date or get_latest_date(ch)
    if not date:
        log.error("Cannot determine trading date")
        sys.exit(1)

    log.info(f"Target date: {date}, mode: {args.mode}")

    if args.mode in ('daily', 'all'):
        generate_daily(ch, date)

    if args.mode in ('weekly', 'advanced', 'all'):
        dates = get_trading_dates(ch, date, args.weeks)
        log.info(f"Trading dates: {dates}")
        if args.mode in ('weekly', 'all'):
            generate_weekly(ch, dates)
        if args.mode in ('advanced', 'all'):
            generate_advanced(ch, dates)

    log.info("Done.")


if __name__ == '__main__':
    main()
