#!/usr/bin/env python3
"""
刷新行业两融聚合数据

步骤:
1. 从CH导出行业映射 → 更新PG margin.stock_industry_map
2. 从PG stock_margin_ranking JOIN industry_map → 重新聚合 margin.industry_margin_summary

用法:
  python refresh_industry_margin.py
"""

import os
import sys
import logging
from pathlib import Path

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


def get_pg():
    return psycopg2.connect(
        host=os.getenv('PG_HOST', 'localhost'),
        port=int(os.getenv('PG_PORT', '5432')),
        dbname=os.getenv('PG_DB', 'quantdb'),
        user=os.getenv('PG_USER', 'postgres'),
        password=os.getenv('PG_PASSWORD', 'postgres'),
    )


def refresh_industry_map(ch, pg):
    """Step 1: 刷新行业映射表"""
    log.info("刷新行业映射表...")
    rows = ch.execute("""
    SELECT replaceRegexpOne(symbol, '^(sh|sz|bj)', '') as ts_code, industry_name
    FROM stock_industry_mapping
    WHERE industry_code LIKE 'T%%'
    LIMIT 1 BY ts_code
    """, settings={'allow_experimental_analyzer': 0})

    cur = pg.cursor()
    cur.execute('TRUNCATE margin.stock_industry_map')
    for r in rows:
        cur.execute('INSERT INTO margin.stock_industry_map (ts_code, industry_name) VALUES (%s, %s)',
                     [r[0], r[1]])
    pg.commit()
    cur.close()
    log.info(f"行业映射: 插入 {len(rows)} 条")


def refresh_industry_summary(pg):
    """Step 2: 重新聚合行业两融数据"""
    log.info("重新聚合行业两融数据...")
    cur = pg.cursor()
    cur.execute('TRUNCATE margin.industry_margin_summary')

    cur.execute("""
    INSERT INTO margin.industry_margin_summary
    SELECT
        r.trade_date,
        m.industry_name,
        count(*) as stock_count,
        round(avg(r.margin_percentile), 2) as avg_pctile,
        sum(CASE WHEN r.margin_trend = 'INCREASING' AND r.short_trend = 'DECREASING' THEN 1 ELSE 0 END) as bullish_count,
        sum(CASE WHEN r.margin_trend = 'DECREASING' AND r.short_trend = 'INCREASING' THEN 1 ELSE 0 END) as bearish_count,
        sum(CASE WHEN r.margin_trend = 'INCREASING' AND r.short_trend <> 'DECREASING' THEN 1 ELSE 0 END) as margin_up_count,
        sum(CASE WHEN r.short_trend = 'DECREASING' AND r.margin_trend <> 'INCREASING' THEN 1 ELSE 0 END) as short_down_count,
        sum(CASE WHEN r.activity_level IN ('HIGH_ACTIVE', 'TOP_50') THEN 1 ELSE 0 END) as high_active_count,
        round(avg(r.margin_buy_amount)) as avg_margin_buy,
        CASE
            WHEN sum(CASE WHEN r.margin_trend = 'INCREASING' AND r.short_trend = 'DECREASING' THEN 1 ELSE 0 END)::float >= count(*)::float * 0.3 THEN 'BULLISH'
            WHEN sum(CASE WHEN r.margin_trend = 'DECREASING' AND r.short_trend = 'INCREASING' THEN 1 ELSE 0 END)::float >= count(*)::float * 0.3 THEN 'BEARISH'
            WHEN sum(CASE WHEN r.margin_trend = 'INCREASING' THEN 1 ELSE 0 END)::float >= count(*)::float * 0.5 THEN 'MARGIN_UP'
            WHEN sum(CASE WHEN r.short_trend = 'DECREASING' THEN 1 ELSE 0 END)::float >= count(*)::float * 0.5 THEN 'SHORT_DOWN'
            ELSE 'NEUTRAL'
        END as dominant_signal
    FROM margin.stock_margin_ranking r
    JOIN margin.stock_industry_map m ON r.ts_code = m.ts_code
    WHERE m.industry_name IS NOT NULL
    GROUP BY r.trade_date, m.industry_name
    """)
    pg.commit()

    cur.execute('SELECT count(*), max(trade_date) FROM margin.industry_margin_summary')
    r = cur.fetchone()
    cur.close()
    log.info(f"行业两融聚合: {r[0]} 行, 最新日期 {r[1]}")


def main():
    load_env()
    ch = get_ch()
    pg = get_pg()

    refresh_industry_map(ch, pg)
    refresh_industry_summary(pg)

    pg.close()
    log.info("Done.")


if __name__ == '__main__':
    main()
