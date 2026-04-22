#!/usr/bin/env python3
"""
S11 — 周频一致性筛选

逻辑: 一周内 >= 3 天进入基础策略 Top N 名单的个股，剔除"一日游"随机波动。

评分维度:
1. 入选天数: 一周内在 S01 Top 20 中出现的次数
2. 排名均值: 入选日排名的平均值
3. 得分稳定性: 入选日原始得分的变异系数 (CV = std/mean)
4. 行业集中度: 所属行业在周 Top 20 中的总占比

用法:
  python calc_weekly_consistency.py [结束日期] [--top 20] [--min-days 2]
  python calc_weekly_consistency.py 2026-04-17
"""

import os
import re
import sys
import json
import argparse
import logging
import statistics
from pathlib import Path
from datetime import date, timedelta
from collections import defaultdict

import psycopg2
from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent / '00.shared' / 'config'


def load_env():
    env_file = BASE_DIR / 'database.env'
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.strip() and not line.startswith('#') and '=' in line:
                k, v = line.strip().split('=', 1)
                os.environ.setdefault(k, v)


def get_ch():
    return Client(
        host=os.getenv('CH_HOST', 'localhost'),
        port=int(os.getenv('CH_PORT', '9000')),
        database=os.getenv('CH_DB', 'tdx2db_rust'),
        user=os.getenv('CH_USER', 'default'),
        password=os.getenv('CH_PASSWORD', 'tdx2db'),
    )


def get_trading_days(ch, end_date, n=5):
    """获取结束日期之前的 n 个交易日"""
    rows = ch.execute(f"""
    SELECT DISTINCT toDate(datetime) as d
    FROM raw_stocks_5min
    WHERE toDate(datetime) <= '{end_date}'
    ORDER BY d DESC
    LIMIT {n}
    """)
    return [r[0] for r in reversed(rows)]


def get_stock_info(ch, symbols):
    """批量获取股票名称和行业"""
    if not symbols:
        return {}
    sym_list = ','.join([f"'{s}'" for s in symbols[:200]])
    rows = ch.execute(f"""
    SELECT s.symbol,
        COALESCE(g.name, '') as name,
        COALESCE(mi.industry_name, '') as sector
    FROM (
        SELECT arrayJoin([{sym_list}]) as symbol
    ) s
    LEFT JOIN (SELECT symbol, name FROM gtja_stock_names) g
        ON s.symbol = g.symbol
    LEFT JOIN (
        SELECT symbol, industry_name
        FROM stock_industry_mapping
        WHERE industry_code LIKE 'T%%'
        LIMIT 1 BY symbol
    ) mi ON replaceRegexpOne(s.symbol, '^(sh|sz|bj)', '') = mi.symbol
    """, settings={'allow_experimental_analyzer': 0})
    return {r[0]: (r[1], r[2]) for r in rows}


def calc_s11(ch, end_date, top_n=20, min_days=2):
    """S11: 周频一致性筛选"""
    log.info(f"S11: 周频一致性筛选 (end={end_date}, top={top_n}, min_days={min_days})")

    # Step 1: 获取最近5个交易日
    trading_days = get_trading_days(ch, end_date, 5)
    if len(trading_days) < 3:
        log.warning(f"交易日不足3天: {trading_days}")
        return []
    week_start = trading_days[0]
    week_end = trading_days[-1]
    log.info(f"周范围: {week_start} ~ {week_end} ({len(trading_days)} 天)")

    # Step 2: 获取每日S01 Top N
    daily_data = {}  # {symbol: [{date, rank, score}, ...]}
    day_list = ','.join([f"'{d}'" for d in trading_days])

    rows = ch.execute(f"""
    SELECT date, symbol, score, rn
    FROM independence_score_daily
    WHERE date IN ({day_list})
    ORDER BY date, rn
    """)

    # 按日期分组，取每天Top N
    by_date = defaultdict(list)
    for r in rows:
        by_date[r[0]].append({'symbol': r[1], 'score': r[2], 'rank': r[3]})

    for d, items in by_date.items():
        items.sort(key=lambda x: (x['rank'] if x['rank'] else 999999, -x['score']))
        for item in items[:top_n]:
            sym = item['symbol']
            if sym not in daily_data:
                daily_data[sym] = []
            daily_data[sym].append({
                'date': str(d), 'rank': item['rank'], 'score': item['score']
            })

    log.info(f"S11: 共 {len(daily_data)} 只股票出现在 Top {top_n} 中")

    # Step 3: 筛选 >= min_days 的股票
    consistent = {}
    for sym, appearances in daily_data.items():
        if len(appearances) >= min_days:
            consistent[sym] = appearances

    log.info(f"S11: {len(consistent)} 只股票 >= {min_days} 天入选")

    if not consistent:
        return []

    # Step 4: 计算行业集中度
    info = get_stock_info(ch, list(consistent.keys()))
    sector_counts = defaultdict(int)
    for sym in consistent:
        _, sector = info.get(sym, ('?', '?'))
        sector_counts[sector] += 1

    # Step 5: 计算综合评分
    results = []
    for sym, appearances in consistent.items():
        name, sector = info.get(sym, ('?', '?'))
        appear_days = len(appearances)
        scores = [a['score'] for a in appearances]
        ranks = [a['rank'] for a in appearances]

        avg_rank = statistics.mean(ranks) if ranks else 0
        avg_score = statistics.mean(scores) if scores else 0

        # 变异系数
        if len(scores) > 1 and avg_score > 0:
            score_cv = statistics.stdev(scores) / avg_score
        else:
            score_cv = 0

        # 行业集中度
        sector_pct = sector_counts.get(sector, 0) / len(consistent) * 100

        # 综合评分: 入选天数为主，稳定性为辅
        # 天数: 5天=50, 4天=40, 3天=30, 2天=20
        day_score = appear_days * 10
        # 排名: 平均排名越小越好，归一化到0-20
        rank_score = max(0, 20 - avg_rank * 0.5)
        # 稳定性: CV越小越好，归一化到0-15
        stability_score = max(0, 15 * (1 - score_cv))
        # 行业集中度: 越高越好，归一化到0-15
        sector_score = min(sector_pct / 5, 15)

        consistency_score = day_score + rank_score + stability_score + sector_score

        metrics = {
            "appear_days": appear_days,
            "dates": [a['date'] for a in appearances],
            "avg_rank": round(avg_rank, 1),
            "avg_score": round(avg_score, 2),
            "score_cv": round(score_cv, 3),
            "sector_pct": round(sector_pct, 1),
        }
        results.append({
            'symbol': sym, 'name': name, 'sector': sector,
            'appear_days': appear_days,
            'avg_rank': round(avg_rank, 1),
            'avg_score': round(avg_score, 2),
            'score_cv': round(score_cv, 3),
            'consistency_score': round(consistency_score, 1),
            'raw_metrics': json.dumps(metrics, ensure_ascii=False),
        })

    results.sort(key=lambda x: x['consistency_score'], reverse=True)
    return results, week_start, week_end


def save_results(ch, results, week_start, week_end):
    """保存结果到ClickHouse"""
    if not results:
        log.info("S11: 无结果，跳过保存")
        return

    for i, r in enumerate(results, 1):
        r['rank'] = i

    data = [
        (week_start, week_end, r['symbol'], r['name'], r['sector'],
         r['appear_days'], r['avg_rank'], r['avg_score'],
         r['score_cv'], r['consistency_score'])
        for r in results
    ]
    ch.execute(f"ALTER TABLE independence_score_weekly DELETE WHERE week_end = '{week_end}'")
    ch.execute(
        "INSERT INTO independence_score_weekly "
        "(week_start, week_end, symbol, name, sector, appear_days, avg_rank, avg_score, score_cv, consistency_score) VALUES",
        data
    )
    log.info(f"S11: 保存 {len(data)} 条结果 (week {week_start} ~ {week_end})")


def main():
    parser = argparse.ArgumentParser(description='S11 周频一致性筛选')
    parser.add_argument('date', nargs='?', help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--top', type=int, default=20, help='每日 Top N')
    parser.add_argument('--min-days', type=int, default=2, help='最少入选天数')
    args = parser.parse_args()

    load_env()
    ch = get_ch()

    if args.date:
        end_date = args.date
    else:
        row = ch.execute("SELECT max(toDate(datetime)) FROM raw_stocks_5min")
        end_date = row[0][0].strftime('%Y-%m-%d') if row else None
        if not end_date:
            log.error("Cannot determine trading date")
            sys.exit(1)

    result = calc_s11(ch, end_date, top_n=args.top, min_days=args.min_days)
    if not result:
        log.info("S11: 无结果")
        return

    results, week_start, week_end = result
    save_results(ch, results, week_start, week_end)

    log.info(f"S11 Top 5:")
    for r in results[:5]:
        log.info(f"  {r['symbol']} {r['name']} | days={r['appear_days']} score={r['consistency_score']}")

    log.info("Done.")


if __name__ == '__main__':
    main()
