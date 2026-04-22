#!/usr/bin/env python3
"""
独立强度因子策略报告生成器

从 ClickHouse 读取策略执行结果，生成 Obsidian 报告。

用法:
  python gen_reports.py [日期]

示例:
  python gen_reports.py 2026-04-17
"""

import os
import sys
import argparse
import logging
from pathlib import Path

from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

VAULT = "/mnt/d/obsidian/OrbitOS-vault/30_Research/量化分析/策略执行结果/01-独立强度因子"

PROFILES = {
    "conservative":    {"num": "S05", "name": "保守均衡", "desc": "全天均匀分布，与原始因子等价",
                        "weights": [("9:30-10:30", 0.02083, "25.0%", "等权"), ("10:30-11:30", 0.02083, "25.0%", "等权"), ("13:00-14:00", 0.02083, "25.0%", "等权"), ("14:00-15:00", 0.02083, "25.0%", "等权")]},
    "evening_focus":   {"num": "S02", "name": "尾盘聚焦", "desc": "尾盘权重更高，适合关注次日预期",
                        "weights": [("9:30-10:30", 0.016, "19.2%", "低"), ("10:30-11:30", 0.018, "21.6%", "较低"), ("13:00-14:00", 0.022, "26.4%", "中等"), ("14:00-15:00", 0.027, "32.4%", "最高")]},
    "morning_focus":   {"num": "S03", "name": "早盘聚焦", "desc": "早盘权重更高，适合把握开盘情绪",
                        "weights": [("9:30-10:30", 0.030, "36.0%", "最高"), ("10:30-11:30", 0.025, "30.0%", "次高"), ("13:00-14:00", 0.017, "20.4%", "较低"), ("14:00-15:00", 0.011, "13.2%", "低")]},
    "trending_market": {"num": "S04", "name": "趋势市", "desc": "早盘权重较高，适合把握趋势启动",
                        "weights": [("9:30-10:30", 0.024, "28.8%", "最高"), ("10:30-11:30", 0.022, "26.4%", "次高"), ("13:00-14:00", 0.020, "24.0%", "中等"), ("14:00-15:00", 0.017, "20.4%", "低")]},
    "ranging_market":  {"num": "S06", "name": "震荡市", "desc": "尾盘权重高，博弈次日反转",
                        "weights": [("9:30-10:30", 0.015, "18.0%", "低"), ("10:30-11:30", 0.017, "20.4%", "较低"), ("13:00-14:00", 0.020, "24.0%", "中等"), ("14:00-15:00", 0.030, "36.0%", "最高")]},
    "rotating_market": {"num": "S07", "name": "轮动市", "desc": "午盘权重高，捕捉资金切换",
                        "weights": [("9:30-10:30", 0.018, "21.6%", "较低"), ("10:30-11:30", 0.018, "21.6%", "较低"), ("13:00-14:00", 0.024, "28.8%", "最高"), ("14:00-15:00", 0.023, "27.6%", "次高")]},
}


def get_ch_client():
    return Client(
        host=os.getenv('CH_HOST', 'localhost'),
        port=int(os.getenv('CH_PORT', '9000')),
        database=os.getenv('CH_DB', 'tdx2db_rust'),
        user=os.getenv('CH_USER', 'default'),
        password=os.getenv('CH_PASSWORD', 'tdx2db'),
    )


def stock_table(rows, limit=20):
    lines = ["| # | 代码 | 名称 | 行业 | 得分 | 加权分 | 逆势次数 |",
             "|---|------|------|------|------|--------|---------|"]
    for i, r in enumerate(rows[:limit], 1):
        lines.append(f"| {i} | {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]:.3f} | {r[5]} |")
    return '\n'.join(lines)


def s01_table(rows, limit=30):
    lines = ["| # | 代码 | 行业 | 得分 | 逆势次数 |",
             "|---|------|------|------|---------|"]
    for i, r in enumerate(rows[:limit], 1):
        lines.append(f"| {i} | {r[0]} | {r[1]} | {r[2]} | {r[3]} |")
    return '\n'.join(lines)


def get_tw_quantiles(ch, date, preset):
    """从数据库获取时间加权策略的分位数"""
    row = ch.execute("""
        SELECT count() as total,
            quantile(0.50)(raw_score) as p50,
            quantile(0.75)(raw_score) as p75,
            quantile(0.90)(raw_score) as p90,
            quantile(0.95)(raw_score) as p95,
            quantile(0.99)(raw_score) as p99,
            max(raw_score) as max_score
        FROM independence_score_time_weighted
        WHERE date = %(d)s AND config_name = %(p)s
    """, {'d': date, 'p': preset})
    if row and row[0][0] > 0:
        r = row[0]
        return {"total": r[0], "p50": int(r[1]), "p75": int(r[2]), "p90": int(r[3]),
                "p95": int(r[4]), "p99": int(r[5]), "max": int(r[6])}
    return None


def get_tw_top20(ch, date, preset):
    """获取时间加权策略 Top 20"""
    return ch.execute("""
        SELECT symbol, name, sector, raw_score, weighted_score, contra_count
        FROM independence_score_time_weighted
        WHERE date = %(d)s AND config_name = %(p)s
        ORDER BY weighted_score DESC
        LIMIT 20
    """, {'d': date, 'p': preset})


def get_s01_top30(ch, date):
    """获取 S01 Top 30"""
    return ch.execute("""
        SELECT symbol, sector, score, contra_count
        FROM independence_score_daily
        WHERE date = %(d)s
        ORDER BY score DESC
        LIMIT 30
    """, {'d': date})


def get_s01_stats(ch, date):
    """S01 基础统计"""
    row = ch.execute("""
        SELECT count() as total, max(score) as max_score, round(avg(score), 2) as avg_score
        FROM independence_score_daily
        WHERE date = %(d)s
    """, {'d': date})
    return row[0] if row else None


def generate_s01(ch, date):
    """生成 S01 报告"""
    stats = get_s01_stats(ch, date)
    if not stats or stats[0] == 0:
        log.warning(f"S01: no data for {date}")
        return
    top30 = get_s01_top30(ch, date)

    report = f"""---
title: "S01 基础独立强度因子"
date: {date}
type: strategy-report
strategy: independence-score
variant: basic
status: completed
tags: [量化, 独立强度, S01]
---

# S01 基础独立强度因子 — {date}

## 策略逻辑

**逆势条件**:
- 板块 5 分钟收益率 < **-0.2%** 时触发
- 个股收益率 > 板块收益率（跑赢板块即可，不要求绝对正收益）

全天 48 个 5 分钟区间累计得分。

**脚本**: `scripts/calc_independence_score.sh`

## 执行结果

| 指标 | 值 |
|------|------|
| 入选股票 | {stats[0]:,} |
| 最高得分 | {stats[1]} |
| 平均得分 | {stats[2]} |

## Top 30 入选股票

{s01_table(top30, 30)}

---

*数据源: `tdx2db_rust.independence_score_daily`*
"""
    path = os.path.join(VAULT, f"{date}_S01_基础独立强度因子.md")
    with open(path, 'w') as f:
        f.write(report)
    log.info(f"S01: {path}")


def generate_tw_reports(ch, date):
    """生成时间加权策略报告"""
    all_quantiles = {}

    for preset, profile in PROFILES.items():
        q = get_tw_quantiles(ch, date, preset)
        if not q:
            log.warning(f"{preset}: no data for {date}")
            continue
        all_quantiles[preset] = q

        top20 = get_tw_top20(ch, date, preset)
        num = profile['num']
        name = profile['name']

        wt = "| 时段 | 单区间权重 | 合计 | 特征 |\n|------|-----------|------|------|"
        for ts, w, pct, feat in profile['weights']:
            wt += f"\n| {ts} | {w:.4f} | {pct} | {feat} |"

        report = f"""---
title: "{num} {name} ({preset})"
date: {date}
type: strategy-report
strategy: independence-score
variant: {preset}
status: completed
tags: [量化, 独立强度, {num}, 时间加权]
---

# {num} {name} ({preset}) — {date}

## 策略逻辑

**核心思想**: {profile['desc']}。

**逆势条件**:
- 板块 5 分钟收益率 < **-0.2%**
- 个股收益率 > 板块收益率（跑赢板块即可）

**时间权重分配**:

{wt}

## 执行结果

| 指标 | 值 |
|------|------|
| 入选股票 | {q['total']:,} |
| 最高得分 | {q['max']} |
| 中位数 (P50) | {q['p50']} |
| P75 | {q['p75']} |
| P90 | {q['p90']} |
| P95 | {q['p95']} |
| P99 | {q['p99']} |

> [!tip] 如何筛选
> 建议以 **P95 得分** ({q['p95']}) 作为筛选阈值，约 {int(q['total']*0.05)} 只股票进入候选池。

## Top 20

{stock_table(top20, 20)}

## 行业板块对比

"""
        sectors_in_top = {}
        for r in top20:
            s = r[2]  # sector
            if s not in sectors_in_top:
                sectors_in_top[s] = []
            sectors_in_top[s].append(r)

        for sector, stocks in sectors_in_top.items():
            cnt = len(stocks)
            best = stocks[0]
            report += f"### {sector}\n"
            report += f"- Top 20 中有 {cnt} 只，最高分: {best[1]} ({best[0]}) = {best[3]}分\n\n"

        report += f"""## 与其他策略对比

| 策略 | 入选数 | P50 | P95 | 最高 |
|------|--------|-----|-----|------|
"""
        for p2, prof2 in PROFILES.items():
            if p2 in all_quantiles:
                q2 = all_quantiles[p2]
                report += f"| {prof2['num']} {prof2['name']} | {q2['total']:,} | {q2['p50']} | {q2['p95']} | {q2['max']} |\n"

        report += f"""
---

*数据源: `tdx2db_rust.independence_score_time_weighted` (config_name='{preset}')*
"""
        fname = f"{date}_{num}_{name}.md"
        with open(os.path.join(VAULT, fname), 'w') as f:
            f.write(report)
        log.info(f"{num}: {fname} ({q['total']} stocks)")

    return all_quantiles


def generate_s08(date):
    """生成 S08 报告（跳过状态）"""
    report = f"""---
title: "S08 融资余额加权"
date: {date}
type: strategy-report
strategy: independence-score
variant: margin_weighted
status: skipped
tags: [量化, 独立强度, S08, 融资加权]
---

# S08 融资余额加权 — {date}

## 执行状态: 跳过

> [!danger] 无法执行
> PostgreSQL `margin_trading_detail_combined` 表不存在，且脚本引用了已删除的 `stock_sectors` 表。

## 待修复
- [ ] 创建 `margin_trading_detail_combined` 表
- [ ] 更新 SQL: `stock_sectors` → `stock_industry_mapping` + 格式转换
- [ ] 阈值已同步更新为 -0.2%

---

*脚本: `scripts/calc_independence_score_margin_weighted.py`*
"""
    path = os.path.join(VAULT, f"{date}_S08_融资余额加权.md")
    with open(path, 'w') as f:
        f.write(report)
    log.info(f"S08: {path}")


# ================================================================
#  S09-S12 高阶因子报告
# ================================================================

ADVANCED_PROFILES = {
    "S09": {"name": "黄金时段爆发检测", "desc": "10:30-11:30 P99拉升 + 30分钟不回落 + 13:30-14:00维持 + 融资趋势确认",
            "scoring": [("P99触发 (>1.4%)", "+1分"), ("持久性 (6根K线不回落)", "+2分"), ("放量确认 (>2x平均)", "+1分"), ("早盘抗跌 (ret>=0)", "+1分"), ("13:30-14:00维持", "+2分"), ("融资趋势确认", "+1分")]},
    "S10": {"name": "早盘抗跌+午后修复", "desc": "时段反转效应选股 + 融券平仓信号(空头回补检测)",
            "scoring": [("早盘分组", "10-40分"), ("修复幅度", "最高30分"), ("午后收益", "最高20分"), ("S01 Top30%", "+10分"), ("空头回补信号", "+15分"), ("空头回补比率>1", "+10分")]},
    "S12": {"name": "量能确认+两融验证", "desc": "成交额时段异常 + stock_margin_ranking趋势 + 融券平仓",
            "scoring": [("成交额Z-Score", "最高30分"), ("融资趋势INCREASING", "+20分"), ("融券趋势DECREASING", "+20分"), ("百分位Top10%", "+10分"), ("空头平仓>新增", "+20分")]},
    "S13": {"name": "三步联评工作流", "desc": "S09量能筛选 → S11周频强度 → 两融定案，三维共振选股",
            "scoring": [("S09量能入选", "+30分"), ("S11周频入选", "+30分"), ("两融共振(融资升+融券降)", "+40分")]},
}


def get_advanced_stats(ch, date, strategy):
    """获取高阶策略统计"""
    row = ch.execute(f"""
        SELECT count() as total,
            quantile(0.50)(score) as p50,
            quantile(0.75)(score) as p75,
            quantile(0.90)(score) as p90,
            quantile(0.95)(score) as p95,
            quantile(0.99)(score) as p99,
            max(score) as max_score,
            round(avg(score), 1) as avg_score
        FROM independence_score_advanced
        WHERE date = %(d)s AND strategy = %(s)s
    """, {'d': date, 's': strategy})
    if row and row[0][0] > 0:
        r = row[0]
        return {"total": r[0], "p50": round(r[1], 1), "p75": round(r[2], 1), "p90": round(r[3], 1),
                "p95": round(r[4], 1), "p99": round(r[5], 1), "max": round(r[6], 1), "avg": round(r[7], 1)}
    return None


def get_advanced_top20(ch, date, strategy):
    """获取高阶策略 Top 20"""
    return ch.execute(f"""
        SELECT symbol, name, sector, score, rank
        FROM independence_score_advanced
        WHERE date = %(d)s AND strategy = %(s)s
        ORDER BY score DESC
        LIMIT 20
    """, {'d': date, 's': strategy})


def generate_advanced_report(ch, date, strategy):
    """生成单个高阶策略报告"""
    profile = ADVANCED_PROFILES.get(strategy)
    if not profile:
        return None

    stats = get_advanced_stats(ch, date, strategy)
    if not stats:
        log.warning(f"{strategy}: no data for {date}")
        return None

    top20 = get_advanced_top20(ch, date, strategy)

    scoring_table = "| 评分维度 | 分值 |\n|---------|------|\n"
    for dim, val in profile['scoring']:
        scoring_table += f"| {dim} | {val} |\n"

    top_table = "| # | 代码 | 名称 | 行业 | 得分 | 排名 |\n|---|------|------|------|------|------|\n"
    for i, r in enumerate(top20, 1):
        top_table += f"| {i} | {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} |\n"

    report = f"""---
title: "{strategy} {profile['name']}"
date: {date}
type: strategy-report
strategy: independence-score
variant: {strategy.lower()}
status: completed
tags: [量化, 独立强度, {strategy}, 高阶因子]
---

# {strategy} {profile['name']} — {date}

## 策略逻辑

**核心思想**: {profile['desc']}。

**评分维度**:

{scoring_table}

## 执行结果

| 指标 | 值 |
|------|------|
| 入选股票 | {stats['total']:,} |
| 最高得分 | {stats['max']} |
| 平均得分 | {stats['avg']} |
| 中位数 (P50) | {stats['p50']} |
| P75 | {stats['p75']} |
| P90 | {stats['p90']} |
| P95 | {stats['p95']} |
| P99 | {stats['p99']} |

> [!tip] 如何筛选
> 建议以 **P95 得分** ({stats['p95']}) 作为筛选阈值，约 {int(stats['total']*0.05)} 只股票进入候选池。

## Top 20

{top_table}

## 行业板块对比

"""
    sectors_in_top = {}
    for r in top20:
        s = r[2]
        if s not in sectors_in_top:
            sectors_in_top[s] = []
        sectors_in_top[s].append(r)

    for sector, stocks in sectors_in_top.items():
        cnt = len(stocks)
        best = stocks[0]
        report += f"### {sector}\n"
        report += f"- Top 20 中有 {cnt} 只，最高分: {best[1]} ({best[0]}) = {best[3]}分\n\n"

    report += f"""
---

*数据源: `tdx2db_rust.independence_score_advanced` (strategy='{strategy}')*
"""
    fname = f"{date}_{strategy}_{profile['name']}.md"
    path = os.path.join(VAULT, fname)
    with open(path, 'w') as f:
        f.write(report)
    log.info(f"{strategy}: {fname} ({stats['total']} stocks)")
    return stats


def generate_weekly_report(ch, end_date):
    """生成 S11 周频一致性报告"""
    # 检查是否有数据
    count_row = ch.execute("""
        SELECT count() FROM independence_score_weekly WHERE week_end = %(d)s
    """, {'d': end_date})
    if not count_row or count_row[0][0] == 0:
        log.warning(f"S11: no weekly data for week ending {end_date}")
        return

    total = count_row[0][0]
    # 获取统计
    rows = ch.execute("""
        SELECT any(week_start) as ws, any(week_end) as we,
            max(consistency_score) as max_score, round(avg(consistency_score), 1) as avg_score
        FROM independence_score_weekly
        WHERE week_end = %(d)s
    """, {'d': end_date})
    week_start, week_end, max_score, avg_score = rows[0]

    top = ch.execute(f"""
        SELECT symbol, name, sector, appear_days, avg_rank, avg_score, score_cv, consistency_score
        FROM independence_score_weekly
        WHERE week_end = %(d)s
        ORDER BY consistency_score DESC
        LIMIT 20
    """, {'d': end_date})

    top_table = "| # | 代码 | 名称 | 行业 | 入选天数 | 平均排名 | 平均得分 | CV | 综合分 |\n|---|------|------|------|---------|---------|---------|-----|-------|\n"
    for i, r in enumerate(top, 1):
        top_table += f"| {i} | {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5]} | {r[6]} | {r[7]} |\n"

    report = f"""---
title: "S11 周频一致性筛选"
date: {end_date}
type: strategy-report
strategy: independence-score
variant: weekly_consistency
status: completed
tags: [量化, 独立强度, S11, 周频]
---

# S11 周频一致性筛选 — {week_start} ~ {week_end}

## 策略逻辑

**核心思想**: 一周内多次进入 S01 Top 20 名单的个股，剔除"一日游"随机波动。

**评分维度**:

| 评分维度 | 分值 |
|---------|------|
| 入选天数 | 10分/天 (最高50分) |
| 平均排名 | 最高20分 |
| 得分稳定性 (CV) | 最高15分 |
| 行业集中度 | 最高15分 |

## 执行结果

| 指标 | 值 |
|------|------|
| 周范围 | {week_start} ~ {week_end} |
| 入选股票 | {total} |
| 最高综合分 | {max_score} |
| 平均综合分 | {avg_score} |

## Top 20

{top_table}

---

*数据源: `tdx2db_rust.independence_score_weekly`*
"""
    fname = f"{end_date}_S11_周频一致性筛选.md"
    path = os.path.join(VAULT, fname)
    with open(path, 'w') as f:
        f.write(report)
    log.info(f"S11: {fname} ({total} stocks)")


def generate_summary(ch, date, all_quantiles):
    """生成总报告"""
    s01_stats = get_s01_stats(ch, date)

    # 行业分布
    sector_dist = ch.execute("""
        SELECT sector, count() as cnt, round(avg(raw_score), 2) as avg_score, max(raw_score) as max_score
        FROM independence_score_time_weighted
        WHERE date = %(d)s AND config_name = 'conservative'
        GROUP BY sector
        ORDER BY cnt DESC
        LIMIT 15
    """, {'d': date})

    report = f"""---
title: 独立强度因子策略执行总报告
date: {date}
type: strategy-report
strategy: independence-score
status: completed
tags: [量化, 独立强度, 总报告]
---

# 独立强度因子策略执行总报告 — {date}

## 参数说明

| 参数 | 值 | 说明 |
|------|------|------|
| 板块跌幅阈值 | **-0.2%** | 板块 5 分钟收益率低于此值触发逆势检测 |
| 个股逆势条件 | **个股收益 > 板块收益** | 跑赢板块即可，不要求绝对正收益 |

## 概览

| 指标 | 值 |
|------|------|
| 执行日期 | {date} |
| 运行策略 | 7/8 |

## 策略执行结果汇总

| 编号 | 策略 | 入选数 | P50 | P90 | P95 | P99 | 最高 |
|------|------|--------|-----|-----|-----|-----|------|
"""
    if s01_stats:
        report += f"| S01 | 基础版 | {s01_stats[0]:,} | — | — | — | — | **{s01_stats[1]}** |\n"
    for preset, prof in PROFILES.items():
        if preset in all_quantiles:
            q = all_quantiles[preset]
            report += f"| {prof['num']} | {prof['name']} | {q['total']:,} | {q['p50']} | {q['p90']} | {q['p95']} | {q['p99']} | {q['max']} |\n"
    report += "| S08 | 融资加权 | — | — | — | — | — | 跳过 |\n"

    report += "\n## 行业板块分布 (Top 15)\n\n| 行业 | 入选股票 | 平均得分 | 最高得分 |\n|------|---------|---------|---------|\n"
    for s in sector_dist:
        report += f"| {s[0]} | {s[1]} | {s[2]} | {s[3]} |\n"

    report += f"""
## 各策略详细报告

| 报告 | 描述 |
|------|------|
| [[{date}_S01_基础独立强度因子]] | 等权逆势得分 |
"""
    for preset, prof in PROFILES.items():
        report += f"| [[{date}_{prof['num']}_{prof['name']}]] | {prof['desc']} |\n"
    report += f"| [[{date}_S08_融资余额加权]] | 依赖融资数据（跳过） |\n"
    for s, prof in ADVANCED_PROFILES.items():
        report += f"| [[{date}_{s}_{prof['name']}]] | {prof['desc']} |\n"
    report += f"| [[{date}_S11_周频一致性筛选]] | 周频多次入选过滤 |\n"

    report += f"""
---

*阈值: 板块跌 < -0.2%, 个股收益 > 板块收益 | 数据库: ClickHouse tdx2db_rust*
"""
    path = os.path.join(VAULT, f"{date}_独立强度因子策略执行报告.md")
    with open(path, 'w') as f:
        f.write(report)
    log.info(f"Summary: {path}")


def main():
    parser = argparse.ArgumentParser(description='生成独立强度因子策略报告')
    parser.add_argument('date', nargs='?', help='目标日期 (YYYY-MM-DD)')
    args = parser.parse_args()

    # 加载环境变量
    env_file = Path(__file__).resolve().parent.parent.parent / '00.shared' / 'config' / 'database.env'
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.strip() and not line.startswith('#') and '=' in line:
                k, v = line.strip().split('=', 1)
                os.environ.setdefault(k, v)

    ch = get_ch_client()

    # 确定日期
    if args.date:
        date = args.date
    else:
        row = ch.execute("SELECT max(toDate(datetime)) FROM raw_stocks_5min")
        date = row[0][0].strftime('%Y-%m-%d') if row else None
        if not date:
            log.error("Cannot determine trading date")
            sys.exit(1)

    log.info(f"Generating reports for {date}")

    # S01
    generate_s01(ch, date)

    # S02-S07
    all_quantiles = generate_tw_reports(ch, date)

    # S08
    generate_s08(date)

    # S09-S13 高阶因子报告
    for strategy in ['S09', 'S10', 'S12', 'S13']:
        generate_advanced_report(ch, date, strategy)

    # S11 周频报告
    generate_weekly_report(ch, date)

    # Summary
    generate_summary(ch, date, all_quantiles)

    log.info("All reports generated.")


if __name__ == '__main__':
    main()
