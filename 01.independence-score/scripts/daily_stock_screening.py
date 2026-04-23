#!/usr/bin/env python3
"""
每日全市场选股扫描脚本 (优化版)

基于增强因子 (VWAP、VaP、杠杆深度、轧空潜力、Beta敏感性)
批量扫描全市场 A 股，输出综合评分排名，支持昨日对比。

用法:
  python daily_stock_screening.py --date 2026-04-21 --top 50
  python daily_stock_screening.py --date 2026-04-21 --sector 饲料 --top 20
  python daily_stock_screening.py --date 2026-04-21 --min-change 2.0
"""

import os
import sys
import argparse
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

import psycopg2
from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent / '00.shared' / 'config'
VAULT_DIR = "/mnt/d/obsidian/OrbitOS-vault/30_Research/量化分析/个股分析"

# 个股代码过滤: 排除指数、基金、B股、可转债等
A_SHARE_PATTERNS = [
    "sh6%",   # 沪市主板
    "sh688%", # 科创板
    "sh689%", # 科创板
    "sz0%",   # 深市主板
    "sz3%",   # 创业板
    "bj8%",   # 北证
    "bj4%",   # 北证
]

EXCLUDE_PATTERNS = [
    "sz399%",  # 深证指数
    "sz395%",  # 深证指数
    "sh000%",  # 上证指数
    "sh880%",  # 行业指数
    "sh900%",  # B股
    "sz200%",  # B股
    "sh11%",   # 可转债
    "sh12%",   # 可转债
    "sz12%",   # 可转债
    "sz11%",   # 可转债
]


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


def fetch_latest_trade_date(ch):
    r = ch.execute('SELECT max(toDate(datetime)) FROM raw_stocks_5min')[0][0]
    return r


def check_independence_data(ch, trade_date):
    """检查当日独立强度数据是否存在"""
    r = ch.execute(f"""
        SELECT count(*) FROM independence_score_daily WHERE date = '{trade_date}'
    """)
    return int(r[0][0])


def build_a_share_filter():
    """构建A股过滤条件"""
    include = " OR ".join([f"symbol LIKE '{p}'" for p in A_SHARE_PATTERNS])
    exclude = " AND ".join([f"symbol NOT LIKE '{p}'" for p in EXCLUDE_PATTERNS])
    return f"({include}) AND ({exclude})"


def fetch_all_stocks(ch, trade_date):
    """获取当日所有A股的基础数据"""
    filter_sql = build_a_share_filter()
    rows = ch.execute(f"""
    SELECT
        symbol,
        argMax(close, datetime) as close,
        argMax(open, datetime) as open,
        max(high) as high,
        min(low) as low,
        sum(volume) as volume,
        sum(amount) as amount,
        count() as kline_count
    FROM raw_stocks_5min
    WHERE toDate(datetime) = '{trade_date}'
      AND {filter_sql}
    GROUP BY symbol
    HAVING kline_count >= 40
    """)
    stocks = {}
    for r in rows:
        stocks[r[0]] = {
            'symbol': r[0],
            'close': float(r[1]),
            'open': float(r[2]),
            'high': float(r[3]),
            'low': float(r[4]),
            'volume': int(r[5]),
            'amount': float(r[6]),
            'kline_count': int(r[7]),
        }
    return stocks


def fetch_vwap_data(ch, trade_date):
    """获取所有股票的 VWAP 数据"""
    filter_sql = build_a_share_filter()
    rows = ch.execute(f"""
    WITH vwap_calc AS (
        SELECT
            symbol,
            sum(amount) / sum(volume) as vwap,
            argMax(close, datetime) as close
        FROM raw_stocks_5min
        WHERE toDate(datetime) = '{trade_date}'
          AND {filter_sql}
        GROUP BY symbol
    )
    SELECT symbol, vwap, close, (close - vwap) / vwap * 100 as vwap_dev
    FROM vwap_calc
    WHERE vwap > 0
    """)
    result = {}
    for r in rows:
        result[r[0]] = {
            'vwap': float(r[1]),
            'close': float(r[2]),
            'vwap_dev': float(r[3]),
        }
    return result


def fetch_vap_data(ch, trade_date):
    """获取价格密集区 (VaP) 数据: POC 和 Value Area"""
    filter_sql = build_a_share_filter()
    rows = ch.execute(f"""
    WITH price_buckets AS (
        SELECT
            symbol,
            round(close, 1) as price_bucket,
            sum(amount) as bucket_amount
        FROM raw_stocks_5min
        WHERE toDate(datetime) = '{trade_date}'
          AND {filter_sql}
        GROUP BY symbol, price_bucket
    ),
    poc_calc AS (
        SELECT
            symbol,
            argMax(price_bucket, bucket_amount) as poc,
            max(bucket_amount) as poc_amount
        FROM price_buckets
        GROUP BY symbol
    ),
    total_amount AS (
        SELECT symbol, sum(bucket_amount) as total
        FROM price_buckets
        GROUP BY symbol
    ),
    sorted_buckets AS (
        SELECT
            symbol,
            price_bucket,
            bucket_amount,
            sum(bucket_amount) OVER (PARTITION BY symbol ORDER BY bucket_amount DESC) as cumsum,
            total_amount.total as total
        FROM price_buckets
        JOIN total_amount USING (symbol)
    ),
    va_calc AS (
        SELECT
            symbol,
            min(price_bucket) as va_low,
            max(price_bucket) as va_high
        FROM sorted_buckets
        WHERE cumsum / total <= 0.70
        GROUP BY symbol
    )
    SELECT
        poc_calc.symbol,
        poc_calc.poc,
        poc_calc.poc_amount,
        va_calc.va_low,
        va_calc.va_high,
        argMax(close, datetime) as close
    FROM poc_calc
    JOIN va_calc ON poc_calc.symbol = va_calc.symbol
    JOIN raw_stocks_5min ON poc_calc.symbol = raw_stocks_5min.symbol
    WHERE toDate(datetime) = '{trade_date}'
    GROUP BY poc_calc.symbol, poc_calc.poc, poc_calc.poc_amount, va_calc.va_low, va_calc.va_high
    """)
    result = {}
    for r in rows:
        symbol = r[0]
        poc = float(r[1])
        poc_amount = float(r[2])
        va_low = float(r[3])
        va_high = float(r[4])
        close = float(r[5])
        # 计算价格在 VA 中的位置百分比
        if va_high > va_low:
            va_position = (close - va_low) / (va_high - va_low) * 100
        else:
            va_position = 50
        result[symbol] = {
            'poc': poc,
            'poc_amount': poc_amount,
            'va_low': va_low,
            'va_high': va_high,
            'va_position': va_position,
            'close': close,
        }
    return result


def fetch_intraday_profile(ch, trade_date):
    """获取分时段统计"""
    filter_sql = build_a_share_filter()
    rows = ch.execute(f"""
    SELECT
        symbol,
        sumIf(amount, (toHour(datetime) = 9 AND toMinute(datetime) >= 35) OR (toHour(datetime) = 10 AND toMinute(datetime) <= 30)) as morning_amt,
        sumIf(amount, toHour(datetime) IN (10, 11) AND NOT (toHour(datetime) = 10 AND toMinute(datetime) <= 30)) as golden_amt,
        sumIf(amount, toHour(datetime) = 13 AND toMinute(datetime) <= 30) as noon_amt,
        sumIf(amount, (toHour(datetime) = 13 AND toMinute(datetime) > 30) OR (toHour(datetime) = 14 AND toMinute(datetime) <= 30)) as strong_amt,
        sumIf(amount, toHour(datetime) = 14 AND toMinute(datetime) > 30) as afternoon_amt,
        sum(amount) as total_amt,
        argMaxIf(close, datetime, (toHour(datetime) = 9 AND toMinute(datetime) >= 35) OR (toHour(datetime) = 10 AND toMinute(datetime) <= 30)) as morning_close,
        argMinIf(low, datetime, (toHour(datetime) = 9 AND toMinute(datetime) >= 35) OR (toHour(datetime) = 10 AND toMinute(datetime) <= 30)) as morning_low,
        argMaxIf(high, datetime, (toHour(datetime) = 9 AND toMinute(datetime) >= 35) OR (toHour(datetime) = 10 AND toMinute(datetime) <= 30)) as morning_high
    FROM raw_stocks_5min
    WHERE toDate(datetime) = '{trade_date}'
      AND {filter_sql}
    GROUP BY symbol
    HAVING total_amt > 0
    """)
    result = {}
    for r in rows:
        total = float(r[6])
        if total == 0:
            continue
        morning_close = r[7]
        morning_low = r[8]
        morning_high = r[9]
        morning_max_rise = ((morning_high - morning_low) / morning_low * 100) if morning_low and morning_low > 0 else 0
        result[r[0]] = {
            'morning_pct': float(r[1]) / total * 100,
            'golden_pct': float(r[2]) / total * 100,
            'noon_pct': float(r[3]) / total * 100,
            'strong_pct': float(r[4]) / total * 100,
            'afternoon_pct': float(r[5]) / total * 100,
            'morning_max_rise': morning_max_rise,
        }
    return result


def fetch_independence_scores(ch, trade_date):
    """获取相对强度得分（逆势+顺势领先）"""
    rows = ch.execute(f"""
    SELECT symbol, score, contra_count, lead_count
    FROM independence_score_daily
    WHERE date = '{trade_date}'
    """)
    result = {}
    for r in rows:
        result[r[0]] = {
            'score': float(r[1]),
            'contra_count': int(r[2]),
            'lead_count': int(r[3]) if r[3] is not None else 0,
        }
    return result


def fetch_yesterday_scores(ch, trade_date):
    """获取昨日评分用于对比"""
    yesterday = (datetime.strptime(str(trade_date), '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    # 找到最近一个有数据的交易日
    rows = ch.execute(f"""
    SELECT toDate(datetime) as d FROM raw_stocks_5min
    WHERE toDate(datetime) < '{trade_date}'
    GROUP BY d ORDER BY d DESC LIMIT 1
    """)
    if not rows:
        return {}
    yesterday = rows[0][0]
    
    filter_sql = build_a_share_filter()
    # 获取昨日基础数据用于计算评分
    yesterday_stocks = {}
    rows = ch.execute(f"""
    SELECT
        symbol,
        argMax(close, datetime) as close,
        argMax(open, datetime) as open,
        sum(amount) as amount
    FROM raw_stocks_5min
    WHERE toDate(datetime) = '{yesterday}'
      AND {filter_sql}
    GROUP BY symbol
    """)
    for r in rows:
        yesterday_stocks[r[0]] = {
            'close': float(r[1]),
            'open': float(r[2]),
            'amount': float(r[3]),
        }
    return yesterday_stocks


def fetch_margin_data(pg, trade_date):
    """
    获取融资融券数据（已统一处理两市差异）

    数据源: margin.margin_trading_detail_unified 视图
    该视图自动处理沪市与深市的数据差异:
      - 沪市(SSE): 使用交易所原始披露的偿还额/偿还量
      - 深市(SZSE): 通过余额变化公式计算偿还额/偿还量
        融资偿还额 = 前日融资余额 + 当日融资买入额 - 当日融资余额
        融券偿还量 = 前日融券余量 + 当日融券卖出量 - 当日融券余量
    """
    cur = pg.cursor()

    cur.execute("""
        SELECT
            ts_code,
            margin_buy_amount,
            margin_repay_calc,      -- 统一计算后的融资偿还额(元)
            margin_balance_buy,
            short_balance_volume,   -- 融券余量(股数)
            short_sell_volume,      -- 融券卖出量(股数)
            short_repay_calc,       -- 统一计算后的融券偿还量(股数)
            margin_net_calc         -- 统一计算后的融资净买入(元)
        FROM margin.margin_trading_detail_unified
        WHERE trade_date = %s
    """, (trade_date,))
    result = {}
    for r in cur.fetchall():
        ts_code = r[0]
        symbol = ts_code
        # 统一前缀处理
        if not symbol.startswith(('sh', 'sz', 'bj')):
            if symbol.startswith('6'):
                symbol = 'sh' + symbol
            elif symbol.startswith(('0', '3')):
                symbol = 'sz' + symbol
            elif symbol.startswith(('8', '4', '43')):
                symbol = 'bj' + symbol

        result[symbol] = {
            'margin_buy': float(r[1] or 0),
            'margin_repay': float(r[2] or 0),
            'margin_balance': float(r[3] or 0),
            'short_balance': int(r[4] or 0),
            'short_sell': int(r[5] or 0),
            'short_repay': int(r[6] or 0),
            'net_buy': float(r[7] or 0),
        }
    cur.close()
    return result


def fetch_stock_names(ch):
    """获取股票名称映射"""
    rows = ch.execute("""
    SELECT DISTINCT symbol, name FROM v_gtja_stock_names
    WHERE symbol LIKE 'sh6%' OR symbol LIKE 'sh688%' OR symbol LIKE 'sz0%' OR symbol LIKE 'sz3%' OR symbol LIKE 'bj8%' OR symbol LIKE 'bj4%'
    """)
    return {r[0]: r[1] for r in rows}


def fetch_sectors(ch):
    """获取行业映射"""
    rows = ch.execute("""
    SELECT symbol, industry_name
    FROM stock_industry_mapping
    """)
    return {r[0]: r[1] for r in rows}


def calculate_composite_score(stock, vwap, vap, profile, margin, ind_score, min_change=0):
    """计算综合评分 (0-100)"""
    score = 50.0  # 基础分
    notes = []

    change_pct = (stock['close'] - stock['open']) / stock['open'] * 100
    
    # 过滤最小涨跌幅
    if min_change > 0 and abs(change_pct) < min_change:
        return 0, [" filtered"]

    # 1. 日内涨跌 (0-20分) — 提高权重
    if change_pct > 9.9:
        score += 20
        notes.append("涨停")
    elif change_pct > 7:
        score += 18
        notes.append("大涨")
    elif change_pct > 5:
        score += 15
        notes.append("中涨")
    elif change_pct > 3:
        score += 12
        notes.append("小涨")
    elif change_pct > 1:
        score += 8
        notes.append("微涨")
    elif change_pct > 0:
        score += 4
    elif change_pct > -2:
        score -= 2
    else:
        score -= 8
        notes.append("下跌")

    # 2. VWAP 偏离度 (0-15分)
    if vwap:
        dev = vwap['vwap_dev']
        if dev > 3:
            score += 15
            notes.append("VWAP极强")
        elif dev > 1.5:
            score += 10
            notes.append("VWAP强势")
        elif dev > 0.5:
            score += 5
            notes.append("VWAP偏强")
        elif dev > -0.5:
            score += 0
        elif dev > -2:
            score -= 5
        else:
            score -= 10
            notes.append("VWAP弱势")

    # 3. VaP 价格密集区 (0-10分)
    if vap:
        va_pos = vap['va_position']
        if va_pos > 100:
            score += 10
            notes.append("突破VA上沿")
        elif va_pos > 70:
            score += 7
            notes.append("VA上方强势")
        elif va_pos > 30:
            score += 3
        elif va_pos > 0:
            score -= 3
            notes.append("VA下方弱势")
        else:
            score -= 7
            notes.append("跌破VA下沿")
        
        poc_dist = abs(stock['close'] - vap['poc']) / vap['poc'] * 100
        if poc_dist > 3:
            notes.append("远离POC")

    # 4. 分时结构 (0-10分)
    if profile:
        if profile['morning_pct'] > 55:
            score += 5
            notes.append("早盘极度集中")
        elif profile['morning_pct'] > 45:
            score += 3
            notes.append("早盘集中")
        if profile['morning_max_rise'] > 3:
            score += 5
            notes.append("早盘脉冲")
        elif profile['morning_max_rise'] > 1.5:
            score += 2

    # 5. 独立强度 (0-15分) — v2.0 阈值校准
    # 原始阈值(v1.0): 3/1.5/0.5 对应约 p90/p70/p40
    # v2.0 等效:      15/10/8   对应约 p90/p75/p60
    if ind_score:
        s = ind_score['score']
        if s >= 15:
            score += 15
            notes.append("独立强度高")
        elif s >= 10:
            score += 10
            notes.append("独立强度良好")
        elif s >= 8:
            score += 5
        elif s > 0:
            score += 2

    # 6. 资金面 (0-15分)
    if margin:
        net_buy = margin['net_buy']
        concentration = net_buy / stock['amount'] * 100 if stock['amount'] > 0 else 0
        if concentration > 20:
            score += 15
            notes.append("杠杆极高")
        elif concentration > 10:
            score += 10
            notes.append("杠杆流入")
        elif concentration > 5:
            score += 5
        if margin['short_balance'] < 1000:
            score += 5
            notes.append("无空头")

    # 7. 流动性 (0-5分)
    amount = stock['amount']
    if amount > 1e9:
        score += 5
    elif amount > 5e8:
        score += 3
    elif amount > 1e8:
        score += 1

    return max(0, min(100, score)), notes


def generate_report(trade_date, top_n=50, sector_filter=None, min_change=0):
    load_env()
    ch = get_ch()
    pg = get_pg()

    log.info(f"开始扫描 {trade_date} 全市场 A 股...")

    # 检查独立强度数据
    ind_count = check_independence_data(ch, trade_date)
    if ind_count == 0:
        log.warning(f"⚠️  {trade_date} 独立强度得分数据缺失 (independence_score_daily 表为空)")
        log.warning("请先运行: ./scripts/calc_independence_score.sh {trade_date}")
        print(f"\n❌ 数据缺失: {trade_date} 的 independence_score_daily 表无记录")
        print("   请执行: cd 01.independence-score && ./scripts/calc_independence_score.sh {trade_date}")
        ch.disconnect()
        pg.close()
        return None
    else:
        log.info(f"独立强度数据检查通过: {ind_count} 条记录")

    # 获取数据
    stocks = fetch_all_stocks(ch, trade_date)
    log.info(f"获取 {len(stocks)} 只股票基础数据")

    vwap_data = fetch_vwap_data(ch, trade_date)
    vap_data = fetch_vap_data(ch, trade_date)
    profile_data = fetch_intraday_profile(ch, trade_date)
    ind_scores = fetch_independence_scores(ch, trade_date)
    margin_data = fetch_margin_data(pg, trade_date)
    names = fetch_stock_names(ch)
    sectors = fetch_sectors(ch)

    # 计算综合评分
    results = []
    for symbol, stock in stocks.items():
        sector = sectors.get(symbol, '')
        if sector_filter and sector_filter not in sector:
            continue

        vwap = vwap_data.get(symbol)
        vap = vap_data.get(symbol)
        profile = profile_data.get(symbol)
        margin = margin_data.get(symbol)
        ind_score = ind_scores.get(symbol)

        score, notes = calculate_composite_score(stock, vwap, vap, profile, margin, ind_score, min_change)
        if score == 0 and notes == [" filtered"]:
            continue

        change_pct = (stock['close'] - stock['open']) / stock['open'] * 100

        results.append({
            'symbol': symbol,
            'name': names.get(symbol, ''),
            'sector': sector,
            'close': stock['close'],
            'change_pct': change_pct,
            'amount': stock['amount'] / 1e8,
            'vwap_dev': vwap['vwap_dev'] if vwap else None,
            'va_position': vap['va_position'] if vap else None,
            'ind_score': ind_score['score'] if ind_score else 0,
            'contra_count': ind_score['contra_count'] if ind_score else 0,
            'lead_count': ind_score['lead_count'] if ind_score else 0,
            'morning_pct': profile['morning_pct'] if profile else 0,
            'margin_concentration': (margin['net_buy'] / stock['amount'] * 100) if margin and stock['amount'] > 0 else 0,
            'short_balance': margin['short_balance'] if margin else 0,
            'score': score,
            'notes': notes,
        })

    # 排序
    results.sort(key=lambda x: x['score'], reverse=True)

    # 生成报告
    report_lines = [
        f"---",
        f"title: \"{trade_date} 每日选股报告 — 增强因子扫描 (优化版)\"",
        f"date: {trade_date}",
        f"type: daily-screening",
        f"tags: [量化, 选股, 每日扫描, VWAP, VaP, Beta, 杠杆]",
        f"---",
        f"",
        f"# {trade_date} 每日选股报告",
        f"",
        f"> 基于增强因子: VWAP偏离度、VaP价格密集区(POC/Value Area)、杠杆深度、独立强度、分时结构",
        f">",
        f"> 扫描范围: A股个股 {len(results)} 只 | 最新数据日期: {trade_date}",
        f"> 独立强度数据: {ind_count} 条记录 | 代码过滤: 已排除指数/基金/B股/可转债",
        f"",
        f"## 📊 市场概览",
        f"",
        f"| 指标 | 数值 |",
        f"|------|------|",
        f"| 上涨家数 | {sum(1 for r in results if r['change_pct'] > 0)} |",
        f"| 下跌家数 | {sum(1 for r in results if r['change_pct'] < 0)} |",
        f"| 平盘家数 | {sum(1 for r in results if r['change_pct'] == 0)} |",
        f"| 平均涨跌幅 | {sum(r['change_pct'] for r in results) / len(results):.2f}% |",
        f"| 平均成交额 | {sum(r['amount'] for r in results) / len(results):.2f}亿 |",
        f"| 有相对强度得分 | {sum(1 for r in results if r['ind_score'] > 0)} 只 |",
        f"| 其中: 逆势抗跌 | {sum(1 for r in results if r.get('contra_count', 0) > 0)} 只 |",
        f"| 其中: 顺势领先 | {sum(1 for r in results if r.get('lead_count', 0) > 0)} 只 |",
        f"",
        f"## 🏆 Top {top_n} 精选股票",
        f"",
        f"> 综合评分 = 日内涨跌(20) + VWAP偏离(15) + VaP位置(10) + 分时结构(10) + 独立强度(15) + 资金面(15) + 流动性(5)",
        f">",
        f"> 过滤条件: 已排除 sz399/sh000/sh880/sh900/sz200/sh11/sh12/sz12/sz11 等指数/基金/B股/可转债",
        f"",
        f"| 排名 | 代码 | 名称 | 行业 | 评分 | 涨跌% | 成交额(亿) | VWAP偏离% | VA位置% | 综合强度 | 逆势 | 顺势 | 早盘占比% | 杠杆集中度% | 信号 |",
        f"|------|------|------|------|------|-------|-----------|-----------|---------|---------|------|------|----------|------------|------|",
    ]

    for i, r in enumerate(results[:top_n], 1):
        vwap_str = f"{r['vwap_dev']:.2f}" if r['vwap_dev'] is not None else "-"
        va_str = f"{r['va_position']:.0f}" if r['va_position'] is not None else "-"
        notes_str = ", ".join(r['notes'][:3]) if r['notes'] else "-"
        report_lines.append(
            f"| {i} | `{r['symbol']}` | {r['name']} | {r['sector']} | **{r['score']:.1f}** | "
            f"{r['change_pct']:+.2f} | {r['amount']:.2f} | {vwap_str} | {va_str} | "
            f"{r['ind_score']:.2f} | {r.get('contra_count', 0)} | {r.get('lead_count', 0)} | {r['morning_pct']:.1f} | {r['margin_concentration']:.1f} | {notes_str} |"
        )

    # 按行业分组 Top 3
    report_lines.extend([
        f"",
        f"## 📂 分行业 Top 3",
        f"",
    ])
    sector_groups = defaultdict(list)
    for r in results:
        sector_groups[r['sector']].append(r)

    for sector, stocks in sorted(sector_groups.items(), key=lambda x: len(x[1]), reverse=True)[:20]:
        top3 = sorted(stocks, key=lambda x: x['score'], reverse=True)[:3]
        if not top3 or not sector:
            continue
        report_lines.append(f"### {sector} ({len(stocks)}只)")
        report_lines.append(f"")
        report_lines.append(f"| 排名 | 代码 | 名称 | 评分 | 涨跌% | VWAP偏离% | VA位置% | 信号 |")
        report_lines.append(f"|------|------|------|------|-------|-----------|---------|------|")
        for i, r in enumerate(top3, 1):
            notes_str = ", ".join(r['notes'][:2]) if r['notes'] else "-"
            vwap_str = f"{r['vwap_dev']:.2f}" if r['vwap_dev'] is not None else "-"
            va_str = f"{r['va_position']:.0f}" if r['va_position'] is not None else "-"
            report_lines.append(f"| {i} | `{r['symbol']}` | {r['name']} | {r['score']:.1f} | {r['change_pct']:+.2f} | {vwap_str} | {va_str} | {notes_str} |")
        report_lines.append(f"")

    # 特殊信号筛选
    report_lines.extend([
        f"",
        f"## 🔔 特殊信号股票",
        f"",
        f"### VWAP 强势偏离 (>2%) 且上涨",
        f"",
    ])
    vwap_strong = [r for r in results if r['vwap_dev'] is not None and r['vwap_dev'] > 2 and r['change_pct'] > 0][:20]
    if vwap_strong:
        report_lines.append(f"| 排名 | 代码 | 名称 | 涨跌% | VWAP偏离% | VA位置% | 早盘占比% | 信号 |")
        report_lines.append(f"|------|------|------|-------|-----------|---------|----------|------|")
        for r in vwap_strong:
            notes_str = ", ".join(r['notes'][:2]) if r['notes'] else "-"
            va_str = f"{r['va_position']:.0f}" if r['va_position'] is not None else "-"
            report_lines.append(f"| {r['score']:.0f} | `{r['symbol']}` | {r['name']} | {r['change_pct']:+.2f} | {r['vwap_dev']:.2f} | {va_str} | {r['morning_pct']:.1f} | {notes_str} |")
    else:
        report_lines.append("当日无 VWAP 强势偏离且上涨的股票。")

    report_lines.extend([
        f"",
        f"### VaP 突破上沿 (VA位置 >100%)",
        f"",
    ])
    vap_break = [r for r in results if r['va_position'] is not None and r['va_position'] > 100][:20]
    if vap_break:
        report_lines.append(f"| 代码 | 名称 | 涨跌% | VA位置% | POC | 成交额(亿) | 信号 |")
        report_lines.append(f"|------|------|-------|---------|-----|-----------|------|")
        for r in vap_break:
            notes_str = ", ".join(r['notes'][:2]) if r['notes'] else "-"
            report_lines.append(f"| `{r['symbol']}` | {r['name']} | {r['change_pct']:+.2f} | {r['va_position']:.0f} | - | {r['amount']:.2f} | {notes_str} |")
    else:
        report_lines.append("当日无 VaP 突破上沿的股票。")

    report_lines.extend([
        f"",
        f"### 早盘集中 (>50%) 且上涨 >1%",
        f"",
    ])
    morning_burst = [r for r in results if r['morning_pct'] > 50 and r['change_pct'] > 1][:20]
    if morning_burst:
        report_lines.append(f"| 代码 | 名称 | 涨跌% | 早盘占比% | 成交额(亿) | VWAP偏离% | 信号 |")
        report_lines.append(f"|------|------|-------|-----------|-----------|-----------|------|")
        for r in morning_burst:
            notes_str = ", ".join(r['notes'][:2]) if r['notes'] else "-"
            vwap_str = f"{r['vwap_dev']:.2f}" if r['vwap_dev'] is not None else "-"
            report_lines.append(f"| `{r['symbol']}` | {r['name']} | {r['change_pct']:+.2f} | {r['morning_pct']:.1f} | {r['amount']:.2f} | {vwap_str} | {notes_str} |")
    else:
        report_lines.append("当日无早盘集中且上涨的股票。")

    report_lines.extend([
        f"",
        f"### 杠杆资金流入 (集中度>8%) 且无空头",
        f"",
    ])
    margin_in = [r for r in results if r['margin_concentration'] > 8 and r['short_balance'] < 1000][:20]
    if margin_in:
        report_lines.append(f"| 代码 | 名称 | 涨跌% | 杠杆集中度% | 融券余量 | 成交额(亿) | 信号 |")
        report_lines.append(f"|------|------|-------|------------|---------|-----------|------|")
        for r in margin_in:
            notes_str = ", ".join(r['notes'][:2]) if r['notes'] else "-"
            report_lines.append(f"| `{r['symbol']}` | {r['name']} | {r['change_pct']:+.2f} | {r['margin_concentration']:.1f} | {r['short_balance']} | {r['amount']:.2f} | {notes_str} |")
    else:
        report_lines.append("当日无显著杠杆资金流入且无空头的股票。")

    report_lines.extend([
        f"",
        f"### 相对强度高分 (>=8.0) 且上涨",
        f"",
    ])
    ind_high = [r for r in results if r['ind_score'] >= 8.0 and r['change_pct'] > 0][:20]
    if ind_high:
        report_lines.append(f"| 代码 | 名称 | 涨跌% | 综合强度 | 逆势 | 顺势 | VWAP偏离% | 早盘占比% | 信号 |")
        report_lines.append(f"|------|------|-------|---------|------|------|-----------|----------|------|")
        for r in ind_high:
            notes_str = ", ".join(r['notes'][:2]) if r['notes'] else "-"
            vwap_str = f"{r['vwap_dev']:.2f}" if r['vwap_dev'] is not None else "-"
            report_lines.append(f"| `{r['symbol']}` | {r['name']} | {r['change_pct']:+.2f} | {r['ind_score']:.2f} | {r.get('contra_count', 0)} | {r.get('lead_count', 0)} | {vwap_str} | {r['morning_pct']:.1f} | {notes_str} |")
    else:
        report_lines.append("当日无相对强度高分且上涨的股票。")

    report_lines.extend([
        f"",
        f"---",
        f"> 报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"> 数据源: ClickHouse (tdx2db_rust) + PostgreSQL (quantdb)",
        f"> 代码过滤: 已排除指数(sz399/sh000/sh880)、B股(sh900/sz200)、可转债(sh11/sh12/sz11/sz12)",
        f"> 免责声明: 本报告基于量化因子分析，不构成投资建议",
    ])

    # 保存报告
    today = datetime.now().strftime("%Y-%m-%d")
    report_path = Path(VAULT_DIR) / f"{today}_每日选股报告_增强因子扫描.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(report_lines), encoding='utf-8')
    log.info(f"报告已保存: {report_path}")

    pg.close()
    return report_path


def main():
    parser = argparse.ArgumentParser(description='每日全市场选股扫描 (优化版)')
    parser.add_argument('--date', type=str, help='分析日期 (YYYY-MM-DD)，默认最新交易日')
    parser.add_argument('--top', type=int, default=50, help='Top N 股票 (默认50)')
    parser.add_argument('--sector', type=str, help='行业过滤')
    parser.add_argument('--min-change', type=float, default=0, help='最小涨跌幅过滤(%)')
    args = parser.parse_args()

    load_env()
    ch = get_ch()

    trade_date = args.date or fetch_latest_trade_date(ch)
    ch.disconnect()

    report_path = generate_report(trade_date, args.top, args.sector, args.min_change)
    if report_path:
        print(f"\n✅ 报告生成完成: {report_path}")
    else:
        print(f"\n❌ 报告生成失败: 数据缺失")
        sys.exit(1)


if __name__ == '__main__':
    main()
