#!/usr/bin/env python3
"""
个股多维度深度分析脚本

基于5分钟分时数据，从6个维度对个股进行深度分析：
  1. 分时形态 — 当日逐笔5min全览 + 关键时段统计
  2. 多日对比 — 近N日分时走势叠加 (逐K线)
  3. 行业对比 — 同行业横向排名 Top10
  4. 量能异动 — 成交额异常检测 (全48根K线)
  5. 两融分析 — 20日融资融券趋势 + 市场背景
  6. 综合评分 — 多维度评级

用法:
  python analyze_stock.py sh600418 --date 2026-04-17
  python analyze_stock.py sh600418 --days 10
  python analyze_stock.py sh600418 sh600703 sh688001 --date 2026-04-17
"""

import os
import re
import sys
import json
import argparse
import logging
import statistics
from pathlib import Path
from datetime import datetime, date as date_cls
from collections import defaultdict

import psycopg2
from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent / '00.shared' / 'config'
VAULT_DIR = "/mnt/d/obsidian/OrbitOS-vault/30_Research/量化分析/个股分析"

A_SHARE_FILTER = "(symbol LIKE 'sh6%' OR symbol LIKE 'sz0%' OR symbol LIKE 'sz3%' OR symbol LIKE 'bj%')"

SLOTS = {
    '早盘(9:35-10:30)':  lambda h, m: (h == 9 and m >= 35) or (h == 10 and m <= 30),
    '黄金(10:30-11:30)': lambda h, m: (h == 10 and m > 30) or h == 11,
    '午盘(13:00-13:30)': lambda h, m: h == 13 and m <= 30,
    '最强(13:30-14:00)': lambda h, m: (h == 13 and m > 30) or (h == 14 and m <= 30),
    '尾盘(14:00-15:00)': lambda h, m: (h == 14 and m > 30) or h == 15,
}


# ================================================================
#  基础设施
# ================================================================

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

def symbol_to_tscode(symbol):
    return re.sub(r'^(sh|sz|bj)', '', symbol)

def get_stock_info(ch, symbol):
    """获取单只股票的名称和行业"""
    rows = ch.execute(f"""
    SELECT s.symbol,
        COALESCE(g.name, '') as name,
        COALESCE(mi.industry_name, '') as sector
    FROM (SELECT '{symbol}' as symbol) s
    LEFT JOIN (SELECT symbol, name FROM gtja_stock_names) g
        ON s.symbol = g.symbol
    LEFT JOIN (
        SELECT symbol, industry_name
        FROM stock_industry_mapping
        WHERE industry_code LIKE 'T%%'
        LIMIT 1 BY symbol
    ) mi ON replaceRegexpOne(s.symbol, '^(sh|sz|bj)', '') = mi.symbol
    """, settings={'allow_experimental_analyzer': 0})
    if rows:
        return rows[0][1] or '?', rows[0][2] or '?'
    return '?', '?'

def get_latest_date(ch):
    row = ch.execute("SELECT max(toDate(datetime)) FROM raw_stocks_5min")
    return row[0][0].strftime('%Y-%m-%d') if row else None

def get_trading_days(ch, end_date, n=5):
    rows = ch.execute(f"""
    SELECT DISTINCT toDate(datetime) as d
    FROM raw_stocks_5min
    WHERE toDate(datetime) <= '{end_date}'
    ORDER BY d DESC
    LIMIT {n}
    """)
    return [str(r[0]) for r in reversed(rows)]

def fetch_5min(ch, symbol, date):
    rows = ch.execute(f"""
    SELECT datetime, open, high, low, close, volume, amount,
        (close - open) / nullIf(open, 0) * 100 as ret
    FROM raw_stocks_5min
    WHERE symbol = '{symbol}' AND toDate(datetime) = '{date}'
    ORDER BY datetime
    """)
    return [{'datetime': r[0], 'open': r[1], 'high': r[2], 'low': r[3],
             'close': r[4], 'volume': r[5], 'amount': r[6], 'ret': r[7]}
            for r in rows]

def fetch_5min_multi_day(ch, symbol, dates):
    day_list = ','.join([f"'{d}'" for d in dates])
    rows = ch.execute(f"""
    SELECT toDate(datetime) as d, datetime, open, high, low, close, volume, amount,
        (close - open) / nullIf(open, 0) * 100 as ret
    FROM raw_stocks_5min
    WHERE symbol = '{symbol}' AND toDate(datetime) IN ({day_list})
    ORDER BY datetime
    """)
    by_day = defaultdict(list)
    for r in rows:
        by_day[str(r[0])].append({
            'datetime': r[1], 'open': r[2], 'high': r[3], 'low': r[4],
            'close': r[5], 'volume': r[6], 'amount': r[7], 'ret': r[8],
        })
    return dict(by_day)

def _pearson(x, y):
    n = len(x)
    if n < 2:
        return 0
    mx = statistics.mean(x)
    my = statistics.mean(y)
    cov = sum((a - mx) * (b - my) for a, b in zip(x, y))
    sx = sum((a - mx) ** 2 for a in x) ** 0.5
    sy = sum((b - my) ** 2 for b in y) ** 0.5
    if sx == 0 or sy == 0:
        return 0
    return cov / (sx * sy)


# ================================================================
#  额外数据获取
# ================================================================

def fetch_daily(ch, symbol, n=20):
    """获取近N日日线数据 (去重: LIMIT 1 BY date)"""
    rows = ch.execute(f"""
    SELECT date, open, high, low, close, amount
    FROM raw_stocks_daily
    WHERE symbol = '{symbol}'
    ORDER BY date DESC
    LIMIT {n}
    SETTINGS allow_experimental_analyzer = 0
    """, settings={'allow_experimental_analyzer': 0})
    # 日线表可能有重复行 (相同date出现多次)，去重
    seen = set()
    deduped = []
    for r in reversed(rows):
        d = str(r[0])
        if d not in seen:
            seen.add(d)
            # amount字段单位是"手"(volume/100), 成交额 = amount * 100 * 均价
            deduped.append({'date': d, 'open': r[1], 'high': r[2], 'low': r[3],
                           'close': r[4], 'vol_lots': r[5]})
    return deduped

def fetch_concepts(ch, symbol):
    """获取概念标签"""
    rows = ch.execute(f"""
    SELECT block_code FROM raw_tdx_blocks_member
    WHERE stock_symbol = '{symbol}'
      AND block_code NOT LIKE 'T%%' AND block_code NOT LIKE 'X%%'
    ORDER BY block_code
    """)
    return [r[0] for r in rows if r[0]]

def fetch_s01_scores(ch, symbol):
    """获取S01独立强度得分"""
    rows = ch.execute(f"""
    SELECT date, score, contra_count, sector
    FROM independence_score_daily
    WHERE symbol = '{symbol}'
    ORDER BY date DESC
    LIMIT 10
    """)
    return [{'date': str(r[0]), 'score': round(r[1], 2), 'contra_count': r[2], 'sector': r[3]}
            for r in rows]

def fetch_market_background(pg, date):
    """获取市场级两融背景 (按日期聚合SH+SZ)"""
    cur = pg.cursor()
    cur.execute("""
    SELECT trade_date,
        array_agg(margin_trend ORDER BY exchange) as m_trends,
        array_agg(short_trend ORDER BY exchange) as s_trends,
        avg(margin_strength_pct) as avg_m_str,
        avg(short_strength_pct) as avg_s_str
    FROM margin.margin_trend_analysis
    WHERE trade_date <= %s
    GROUP BY trade_date
    ORDER BY trade_date DESC
    LIMIT 3
    """, [date])
    rows = cur.fetchall()
    cur.close()
    result = []
    for r in rows:
        m_strs = r[1] if r[1] else []
        s_strs = r[2] if r[2] else []
        # 取主要趋势 (SH优先)
        m_trend = m_strs[0] if m_strs else '?'
        s_trend = s_strs[0] if s_strs else '?'
        result.append({
            'date': str(r[0]),
            'm_trend': m_trend, 's_trend': s_trend,
            'm_str': round(float(r[3]), 2) if r[3] else None,
            's_str': round(float(r[4]), 2) if r[4] else None,
        })
    return result

def fetch_industry_margin(pg, sector, date, days=5):
    """获取行业级两融概况"""
    if sector == '?':
        return None
    cur = pg.cursor()
    cur.execute("""
    SELECT trade_date, stock_count, avg_pctile, bullish_count, bearish_count,
           margin_up_count, short_down_count, high_active_count,
           avg_margin_buy, dominant_signal
    FROM margin.industry_margin_summary
    WHERE industry_name = %s AND trade_date <= %s
    ORDER BY trade_date DESC
    LIMIT %s
    """, [sector, date, days])
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return None
    result = []
    for r in rows:
        result.append({
            'date': str(r[0]), 'stock_count': r[1], 'avg_pctile': float(r[2] or 0),
            'bullish': r[3], 'bearish': r[4], 'margin_up': r[5], 'short_down': r[6],
            'high_active': r[7], 'avg_margin_buy': r[8] or 0, 'signal': r[9] or 'NEUTRAL',
        })
    return result


# ================================================================
#  分析函数 (与旧版相同，不改动逻辑)
# ================================================================

def analyze_intraday_profile(bars):
    if not bars:
        return None
    open_price = bars[0]['open']
    close_price = bars[-1]['close']
    day_ret = (close_price - open_price) / open_price * 100 if open_price else 0
    total_amount = sum(b['amount'] for b in bars)
    total_volume = sum(b['volume'] for b in bars)
    avg_amount = total_amount / len(bars) if bars else 0
    slot_stats = {}
    for name in SLOTS:
        slot_bars = [b for b in bars if SLOTS[name](b['datetime'].hour, b['datetime'].minute)]
        if not slot_bars:
            continue
        s_open = slot_bars[0]['open']
        s_close = slot_bars[-1]['close']
        s_ret = (s_close - s_open) / s_open * 100 if s_open else 0
        s_amount = sum(b['amount'] for b in slot_bars)
        s_high = max(b['high'] for b in slot_bars)
        s_low = min(b['low'] for b in slot_bars)
        s_amplitude = (s_high - s_low) / s_low * 100 if s_low else 0
        max_surge = max(b['ret'] for b in slot_bars) if slot_bars else 0
        max_drop = min(b['ret'] for b in slot_bars) if slot_bars else 0
        s_amount_ratio = s_amount / avg_amount if avg_amount else 0
        slot_stats[name] = {
            'ret': round(s_ret, 3), 'amount': s_amount,
            'amount_ratio': round(s_amount_ratio, 2),
            'amplitude': round(s_amplitude, 3),
            'max_surge': round(max_surge, 3), 'max_drop': round(max_drop, 3),
            'bar_count': len(slot_bars),
            'amount_pct': round(s_amount / total_amount * 100, 1) if total_amount else 0,
        }
    # ── VWAP 计算 ──
    vwap_bars = []
    cum_amount = 0
    cum_volume = 0
    for b in bars:
        cum_amount += b['amount']
        cum_volume += b['volume']
        vwap = cum_amount / cum_volume if cum_volume else b['close']
        deviation = (b['close'] - vwap) / vwap * 100 if vwap else 0
        vwap_bars.append({
            'time': b['datetime'].strftime('%H:%M'),
            'close': b['close'], 'vwap': round(vwap, 2),
            'deviation': round(deviation, 3),
            'amount': b['amount'], 'volume': b['volume'],
        })
    final_vwap = vwap_bars[-1]['vwap'] if vwap_bars else 0
    final_deviation = vwap_bars[-1]['deviation'] if vwap_bars else 0

    # ── Volume at Price 分布 ──
    # 根据价格水平自动确定 bucket 大小
    price_range = max(b['high'] for b in bars) - min(b['low'] for b in bars)
    if price_range <= 3:
        bucket_size = 0.1
    elif price_range <= 10:
        bucket_size = 0.2
    elif price_range <= 30:
        bucket_size = 0.5
    else:
        bucket_size = 1.0
    vap = defaultdict(lambda: {'volume': 0, 'amount': 0, 'bar_count': 0})
    for b in bars:
        # 将该K线的成交按 (open, high, low, close) 的范围均匀分布到价格区间
        low_p = b['low']
        high_p = b['high']
        if high_p == low_p:
            # 无振幅，全部归入一个桶
            bucket_key = round(low_p // bucket_size * bucket_size, 2)
            vap[bucket_key]['volume'] += b['volume']
            vap[bucket_key]['amount'] += b['amount']
            vap[bucket_key]['bar_count'] += 1
        else:
            # 用均匀分布近似 (简化: 按 OHLC 四个价位分配)
            points = [b['open'], b['high'], b['low'], b['close']]
            vol_per_point = b['volume'] / len(points)
            amt_per_point = b['amount'] / len(points)
            for p in points:
                bucket_key = round(p // bucket_size * bucket_size, 2)
                vap[bucket_key]['volume'] += vol_per_point
                vap[bucket_key]['amount'] += amt_per_point
                vap[bucket_key]['bar_count'] += 1

    # 转换为排序列表，找 POC (Point of Control)
    vap_list = []
    for price_bucket, data in sorted(vap.items(), reverse=True):
        vap_list.append({
            'price': price_bucket,
            'volume': int(data['volume']),
            'amount': int(data['amount']),
            'bar_count': data['bar_count'],
        })
    poc = max(vap_list, key=lambda x: x['amount']) if vap_list else None
    total_vap_amount = sum(v['amount'] for v in vap_list)

    # Value Area: 包含 70% 成交额的价格区间
    vap_sorted_by_amt = sorted(vap_list, key=lambda x: x['amount'], reverse=True)
    va_amount = 0
    va_prices = []
    for v in vap_sorted_by_amt:
        va_amount += v['amount']
        va_prices.append(v['price'])
        if va_amount >= total_vap_amount * 0.7:
            break
    va_high = max(va_prices) if va_prices else 0
    va_low = min(va_prices) if va_prices else 0

    # 当前价在 Value Area 中的位置
    if va_high > va_low:
        close_price = bars[-1]['close']
        va_position = (close_price - va_low) / (va_high - va_low) * 100
    else:
        va_position = 50.0

    return {
        'day_ret': round(day_ret, 3), 'total_amount': total_amount,
        'total_volume': total_volume, 'bar_count': len(bars), 'slots': slot_stats,
        # VWAP
        'vwap_bars': vwap_bars, 'final_vwap': final_vwap,
        'final_deviation': round(final_deviation, 3),
        # Volume at Price
        'vap': vap_list, 'poc': poc,
        'va_high': va_high, 'va_low': va_low,
        'va_position': round(va_position, 1),
        'bucket_size': bucket_size, 'price_range': round(price_range, 2),
    }

def analyze_historical_comparison(ch, symbol, date, days=5):
    trading_days = get_trading_days(ch, date, days)
    if not trading_days:
        return None
    multi_data = fetch_5min_multi_day(ch, symbol, trading_days)
    if not multi_data:
        return None
    daily_stats = {}
    for d in trading_days:
        day_bars = multi_data.get(d, [])
        if not day_bars:
            continue
        d_open = day_bars[0]['open']
        d_close = day_bars[-1]['close']
        d_ret = (d_close - d_open) / d_open * 100 if d_open else 0
        d_amount = sum(b['amount'] for b in day_bars)
        d_high = max(b['high'] for b in day_bars)
        d_low = min(b['low'] for b in day_bars)
        d_amplitude = (d_high - d_low) / d_low * 100 if d_low else 0
        gold_bars = [b for b in day_bars if SLOTS['黄金(10:30-11:30)'](b['datetime'].hour, b['datetime'].minute)]
        gold_amount = sum(b['amount'] for b in gold_bars)
        gold_pct = gold_amount / d_amount * 100 if d_amount else 0
        strong_bars = [b for b in day_bars if SLOTS['最强(13:30-14:00)'](b['datetime'].hour, b['datetime'].minute)]
        strong_open = strong_bars[0]['open'] if strong_bars else 0
        strong_close = strong_bars[-1]['close'] if strong_bars else 0
        strong_ret = (strong_close - strong_open) / strong_open * 100 if strong_open else 0
        daily_stats[d] = {
            'ret': round(d_ret, 3), 'amount': d_amount,
            'gold_pct': round(gold_pct, 1), 'strong_ret': round(strong_ret, 3),
            'amplitude': round(d_amplitude, 3), 'high': d_high, 'low': d_low,
        }
    corr = None
    today_key = date if date in multi_data else trading_days[-1]
    today_bars = multi_data.get(today_key, [])
    if len(trading_days) >= 2 and today_bars:
        prev_key = trading_days[-2] if today_key == trading_days[-1] else trading_days[trading_days.index(today_key) - 1]
        prev_bars = multi_data.get(prev_key, [])
        if len(today_bars) > 2 and len(prev_bars) > 2:
            min_len = min(len(today_bars), len(prev_bars))
            today_cum = []
            prev_cum = []
            base_t = today_bars[0]['open']
            base_p = prev_bars[0]['open']
            for i in range(min_len):
                today_cum.append((today_bars[i]['close'] - base_t) / base_t * 100 if base_t else 0)
                prev_cum.append((prev_bars[i]['close'] - base_p) / base_p * 100 if base_p else 0)
            if len(today_cum) > 2:
                corr = round(_pearson(today_cum, prev_cum), 3)
    return {
        'trading_days': trading_days, 'daily_stats': daily_stats,
        'correlation': corr, 'multi_data': multi_data,
    }

def analyze_sector_comparison(ch, symbol, date, sector):
    if sector == '?':
        return None
    rows = ch.execute(f"""
    SELECT r.symbol, toDate(r.datetime) as d, r.datetime,
        r.open, r.high, r.low, r.close, r.volume, r.amount
    FROM raw_stocks_5min r
    ANY LEFT JOIN (
        SELECT symbol, industry_name
        FROM stock_industry_mapping
        WHERE industry_code LIKE 'T%%'
        LIMIT 1 BY symbol
    ) m ON replaceRegexpOne(r.symbol, '^(sh|sz|bj)', '') = m.symbol
    WHERE m.industry_name = '{sector}'
      AND toDate(r.datetime) = '{date}'
      AND {A_SHARE_FILTER}
    ORDER BY r.symbol, r.datetime
    """, settings={'allow_experimental_analyzer': 0})
    if not rows:
        return None
    by_sym = defaultdict(list)
    for r in rows:
        by_sym[r[0]].append({
            'datetime': r[2],
            'open': r[3], 'high': r[4], 'low': r[5],
            'close': r[6], 'volume': r[7], 'amount': r[8],
        })
    if symbol not in by_sym:
        return None
    stock_metrics = {}
    for sym, bars in by_sym.items():
        if not bars:
            continue
        s_open = bars[0]['open']
        s_close = bars[-1]['close']
        s_ret = (s_close - s_open) / s_open * 100 if s_open else 0
        s_amount = sum(b['amount'] for b in bars)
        s_high = max(b['high'] for b in bars)
        s_low = min(b['low'] for b in bars)
        s_amplitude = (s_high - s_low) / s_low * 100 if s_low else 0
        stock_metrics[sym] = {'ret': s_ret, 'amount': s_amount, 'amplitude': s_amplitude}
    by_ret = sorted(stock_metrics.items(), key=lambda x: x[1]['ret'], reverse=True)
    by_amount = sorted(stock_metrics.items(), key=lambda x: x[1]['amount'], reverse=True)
    ret_rank = next((i + 1 for i, (s, _) in enumerate(by_ret) if s == symbol), len(by_ret))
    amt_rank = next((i + 1 for i, (s, _) in enumerate(by_amount) if s == symbol), len(by_amount))
    total_peers = len(stock_metrics)
    max_bars = max(len(v) for v in by_sym.values())
    ind_avg_ret = []
    target_bars = by_sym.get(symbol, [])
    for i in range(min(max_bars, len(target_bars))):
        peer_closes = []
        for sym, bars in by_sym.items():
            if i < len(bars):
                peer_closes.append(bars[i]['close'])
        if peer_closes:
            ind_avg_ret.append(statistics.mean(peer_closes))
    alpha_bars = []
    for i, b in enumerate(target_bars):
        if i < len(ind_avg_ret) and i > 0:
            stock_pct = (b['close'] - target_bars[0]['open']) / target_bars[0]['open'] * 100 if target_bars[0]['open'] else 0
            ind_pct = (ind_avg_ret[i] - ind_avg_ret[0]) / ind_avg_ret[0] * 100 if ind_avg_ret[0] else 0
            alpha_bars.append(stock_pct - ind_pct)
    avg_alpha = statistics.mean(alpha_bars) if alpha_bars else 0
    final_alpha = alpha_bars[-1] if alpha_bars else 0

    # 行业各时段平均成交额 (等权均值)
    slot_ind_avg = {}
    for slot_name, slot_fn in SLOTS.items():
        peer_amounts = []
        for sym, bars in by_sym.items():
            s_amt = sum(b['amount'] for b in bars if slot_fn(b['datetime'].hour, b['datetime'].minute))
            if s_amt > 0:
                peer_amounts.append(s_amt)
        slot_ind_avg[slot_name] = statistics.mean(peer_amounts) if peer_amounts else 0

    return {
        'sector': sector, 'total_peers': total_peers,
        'ret_rank': ret_rank, 'amt_rank': amt_rank,
        'avg_alpha': round(avg_alpha, 3), 'final_alpha': round(final_alpha, 3),
        'top10_by_ret': [(s, round(m['ret'], 2), round(m['amplitude'], 2)) for s, m in by_ret[:10]],
        'slot_ind_avg': slot_ind_avg,
    }

def analyze_volume_anomaly(ch, symbol, date, multi_data):
    today_bars = multi_data.get(date, [])
    if not today_bars:
        return None
    mkt_stats = ch.execute(f"""
    SELECT quantile(0.50)(amount) as p50, quantile(0.95)(amount) as p95
    FROM raw_stocks_5min
    WHERE toDate(datetime) = '{date}' AND {A_SHARE_FILTER}
    """)
    mkt_p50 = mkt_stats[0][0] if mkt_stats else 0
    mkt_p95 = mkt_stats[0][1] if mkt_stats else 0
    all_dates = sorted(multi_data.keys())
    prev_dates = [d for d in all_dates if d < date]
    hist_slot_amounts = defaultdict(list)
    for d in prev_dates:
        for b in multi_data[d]:
            slot_key = f"{b['datetime'].hour}:{b['datetime'].minute:02d}"
            hist_slot_amounts[slot_key].append(b['amount'])
    anomaly_bars = []
    total_amount = sum(b['amount'] for b in today_bars)
    for b in today_bars:
        slot_key = f"{b['datetime'].hour}:{b['datetime'].minute:02d}"
        hist_avg = statistics.mean(hist_slot_amounts[slot_key]) if slot_key in hist_slot_amounts else b['amount']
        volume_ratio = b['amount'] / hist_avg if hist_avg else 1.0
        is_anomaly = b['amount'] > mkt_p95 if mkt_p95 else False
        amount_pct = b['amount'] / total_amount * 100 if total_amount else 0
        anomaly_bars.append({
            'time': b['datetime'].strftime('%H:%M'),
            'open': b['open'], 'high': b['high'], 'low': b['low'], 'close': b['close'],
            'amount': b['amount'], 'volume': b['volume'],
            'volume_ratio': round(volume_ratio, 2),
            'is_anomaly': is_anomaly,
            'amount_pct': round(amount_pct, 1),
            'ret': round(b.get('ret', 0), 3),
        })
    slot_amounts = {}
    slot_volume_comparison = []  # 时段级量能对比
    for name in SLOTS:
        slot_bars = [b for b in today_bars if SLOTS[name](b['datetime'].hour, b['datetime'].minute)]
        s_amount = sum(b['amount'] for b in slot_bars)
        slot_amounts[name] = s_amount

        # 历史同时段成交额
        hist_slot_amt = []
        for d in prev_dates:
            d_bars = multi_data.get(d, [])
            d_slot_bars = [b for b in d_bars if SLOTS[name](b['datetime'].hour, b['datetime'].minute)]
            if d_slot_bars:
                hist_slot_amt.append(sum(b['amount'] for b in d_slot_bars))
        hist_avg = statistics.mean(hist_slot_amt) if hist_slot_amt else s_amount
        hist_min = min(hist_slot_amt) if hist_slot_amt else s_amount
        hist_max = max(hist_slot_amt) if hist_slot_amt else s_amount
        vr = s_amount / hist_avg if hist_avg else 1.0

        # 判断放量/缩量
        if vr >= 2.0:
            vol_label = '放量'
        elif vr >= 1.3:
            vol_label = '温和放量'
        elif vr >= 0.7:
            vol_label = '正常'
        elif vr >= 0.4:
            vol_label = '缩量'
        else:
            vol_label = '明显缩量'

        slot_volume_comparison.append({
            'slot': name, 'amount': s_amount,
            'hist_avg': hist_avg, 'hist_min': hist_min, 'hist_max': hist_max,
            'hist_days': len(hist_slot_amt), 'vol_ratio': round(vr, 2),
            'vol_label': vol_label, 'amount_pct': round(s_amount / total_amount * 100, 1) if total_amount else 0,
        })

    peak_slot = max(slot_amounts, key=slot_amounts.get) if slot_amounts else '?'
    surge_bars = [b for b in anomaly_bars if b['volume_ratio'] >= 3.0]

    # 全日量能对比
    hist_daily_totals = []
    for d in prev_dates:
        d_bars = multi_data.get(d, [])
        if d_bars:
            hist_daily_totals.append(sum(b['amount'] for b in d_bars))
    daily_hist_avg = statistics.mean(hist_daily_totals) if hist_daily_totals else total_amount
    daily_vol_ratio = total_amount / daily_hist_avg if daily_hist_avg else 1.0

    return {
        'bars': anomaly_bars, 'mkt_p50': round(mkt_p50, 0), 'mkt_p95': round(mkt_p95, 0),
        'peak_slot': peak_slot, 'surge_count': len(surge_bars),
        'surge_times': [b['time'] for b in surge_bars],
        'slot_comparison': slot_volume_comparison,
        'daily_hist_avg': daily_hist_avg, 'daily_vol_ratio': round(daily_vol_ratio, 2),
        'hist_daily_totals': hist_daily_totals,
    }


def analyze_beta(ch, symbol, date, sector, days=5):
    """分析个股与行业的Beta敏感性"""
    if sector == '?':
        return None

    trading_days = get_trading_days(ch, date, days)
    if len(trading_days) < 2:
        return None

    # 获取个股5分钟数据
    multi_data = fetch_5min_multi_day(ch, symbol, trading_days)

    # 获取行业5分钟数据 (等权平均)
    day_list = ','.join([f"'{d}'" for d in trading_days])
    rows = ch.execute(f"""
    SELECT toDate(r.datetime) as d, r.datetime,
        r.close, r.amount
    FROM raw_stocks_5min r
    ANY LEFT JOIN (
        SELECT symbol, industry_name
        FROM stock_industry_mapping
        WHERE industry_code LIKE 'T%%'
        LIMIT 1 BY symbol
    ) m ON replaceRegexpOne(r.symbol, '^(sh|sz|bj)', '') = m.symbol
    WHERE m.industry_name = '{sector}'
      AND toDate(r.datetime) IN ({day_list})
      AND {A_SHARE_FILTER}
    ORDER BY r.datetime
    """, settings={'allow_experimental_analyzer': 0})

    if not rows:
        return None

    # 按日+时间构建行业平均收益序列
    # 先按(datetime)聚合所有行业股票的close
    from collections import defaultdict as _dd
    time_closes = _dd(list)  # (date_str, time_str) -> [close1, close2, ...]
    time_amounts = _dd(float)
    for r in rows:
        d_str = str(r[0])
        t_str = r[1].strftime('%H:%M')
        time_closes[(d_str, t_str)].append(r[2])
        time_amounts[(d_str, t_str)] += r[3]

    # 计算行业等权平均价格序列
    ind_avg_prices = {}  # (date, time) -> avg_close
    for key, closes in time_closes.items():
        ind_avg_prices[key] = statistics.mean(closes)

    # 逐日计算5分钟收益率，然后计算Beta
    daily_betas = {}
    slot_betas = {}

    for d in trading_days:
        stock_bars = multi_data.get(d, [])
        if len(stock_bars) < 3:
            continue

        # 构建收益率序列 (5min bar-to-bar return)
        stock_rets = []
        ind_rets = []
        for i in range(1, len(stock_bars)):
            t_key = (d, stock_bars[i]['datetime'].strftime('%H:%M'))
            prev_t_key = (d, stock_bars[i - 1]['datetime'].strftime('%H:%M'))
            ind_p = ind_avg_prices.get(t_key)
            ind_pp = ind_avg_prices.get(prev_t_key)
            if ind_p and ind_pp and ind_pp > 0 and stock_bars[i - 1]['close'] > 0:
                s_ret = (stock_bars[i]['close'] - stock_bars[i - 1]['close']) / stock_bars[i - 1]['close']
                i_ret = (ind_p - ind_pp) / ind_pp
                stock_rets.append(s_ret)
                ind_rets.append(i_ret)

        if len(stock_rets) < 5:
            continue

        # 全日Beta = cov(stock, ind) / var(ind)
        beta = _calc_beta(stock_rets, ind_rets)
        daily_betas[d] = round(beta, 3)

        # 分时段Beta
        for slot_name, slot_fn in SLOTS.items():
            s_rets_slot = []
            i_rets_slot = []
            for i in range(1, len(stock_bars)):
                h, m = stock_bars[i]['datetime'].hour, stock_bars[i]['datetime'].minute
                if slot_fn(h, m):
                    t_key = (d, stock_bars[i]['datetime'].strftime('%H:%M'))
                    prev_t_key = (d, stock_bars[i - 1]['datetime'].strftime('%H:%M'))
                    ind_p = ind_avg_prices.get(t_key)
                    ind_pp = ind_avg_prices.get(prev_t_key)
                    if ind_p and ind_pp and ind_pp > 0 and stock_bars[i - 1]['close'] > 0:
                        s_ret = (stock_bars[i]['close'] - stock_bars[i - 1]['close']) / stock_bars[i - 1]['close']
                        i_ret = (ind_p - ind_pp) / ind_pp
                        s_rets_slot.append(s_ret)
                        i_rets_slot.append(i_ret)
            if len(s_rets_slot) >= 3:
                slot_b = _calc_beta(s_rets_slot, i_rets_slot)
                if slot_name not in slot_betas:
                    slot_betas[slot_name] = []
                slot_betas[slot_name].append(round(slot_b, 3))

    if not daily_betas:
        return None

    # 平均Beta
    avg_beta = statistics.mean(daily_betas.values())
    # 分时段平均Beta
    avg_slot_betas = {}
    for slot_name, betas in slot_betas.items():
        avg_slot_betas[slot_name] = round(statistics.mean(betas), 3)

    # 相关性
    all_s_rets = []
    all_i_rets = []
    for d in trading_days:
        stock_bars = multi_data.get(d, [])
        for i in range(1, len(stock_bars)):
            t_key = (d, stock_bars[i]['datetime'].strftime('%H:%M'))
            prev_t_key = (d, stock_bars[i - 1]['datetime'].strftime('%H:%M'))
            ind_p = ind_avg_prices.get(t_key)
            ind_pp = ind_avg_prices.get(prev_t_key)
            if ind_p and ind_pp and ind_pp > 0 and stock_bars[i - 1]['close'] > 0:
                all_s_rets.append((stock_bars[i]['close'] - stock_bars[i - 1]['close']) / stock_bars[i - 1]['close'])
                all_i_rets.append((ind_p - ind_pp) / ind_pp)

    corr = round(_pearson(all_s_rets, all_i_rets), 3) if len(all_s_rets) > 5 else None

    return {
        'avg_beta': round(avg_beta, 3),
        'daily_betas': daily_betas,
        'slot_betas': avg_slot_betas,
        'correlation': corr,
        'sector': sector,
        'days': len(daily_betas),
    }


def _calc_beta(stock_rets, market_rets):
    """计算 Beta = cov(stock, market) / var(market)"""
    n = len(stock_rets)
    if n < 2:
        return 1.0
    ms = statistics.mean(stock_rets)
    mm = statistics.mean(market_rets)
    cov = sum((s - ms) * (m - mm) for s, m in zip(stock_rets, market_rets)) / (n - 1)
    var_m = sum((m - mm) ** 2 for m in market_rets) / (n - 1)
    if var_m == 0:
        return 1.0
    return cov / var_m


def analyze_margin(pg, symbol, date, days=20, daily_amounts=None):
    ts = symbol_to_tscode(symbol)
    cur = pg.cursor()
    cur.execute("""
    SELECT
        trade_date,
        margin_buy_amount,
        margin_repay_calc,      -- 统一计算后的融资偿还额(元)
        short_sell_volume,
        short_repay_calc,       -- 统一计算后的融券偿还量(股数)
        short_balance_volume,
        margin_balance_buy,
        margin_net_calc         -- 统一计算后的融资净买入(元)
    FROM margin.margin_trading_detail_unified
    WHERE ts_code = %s AND trade_date <= %s
    ORDER BY trade_date DESC
    LIMIT %s
    """, [ts, date, days])
    detail_rows = cur.fetchall()
    cur.execute("""
    SELECT trade_date, margin_trend, short_trend,
        margin_percentile, activity_level, daily_exchange_rank
    FROM margin.stock_margin_ranking
    WHERE ts_code = %s AND trade_date <= %s
    ORDER BY trade_date DESC
    LIMIT %s
    """, [ts, date, days])
    ranking_rows = cur.fetchall()
    cur.close()
    if not detail_rows and not ranking_rows:
        return None
    detail = []
    for r in detail_rows:
        margin_buy = r[1] or 0
        margin_repay = r[2] or 0
        short_sell = r[3] or 0
        short_repay = r[4] or 0
        short_bal = r[5] or 0
        margin_balance = r[6] or 0
        margin_net = r[7] or 0
        # 杠杆集中度: 融资买入额 / 当日总成交额
        d = str(r[0])
        daily_amt = daily_amounts.get(d, 0) if daily_amounts else 0
        leverage_ratio = margin_buy / daily_amt * 100 if daily_amt else 0
        detail.append({
            'date': d, 'margin_buy': margin_buy, 'margin_repay': margin_repay,
            'margin_net': margin_net,
            'short_sell': short_sell, 'short_repay': short_repay,
            'short_net': short_sell - short_repay, 'short_bal': short_bal,
            'margin_balance': margin_balance,
            'leverage_ratio': round(leverage_ratio, 2),
            'daily_amount': daily_amt,
        })
    ranking = []
    for r in ranking_rows:
        ranking.append({
            'date': str(r[0]), 'margin_trend': r[1] or '?', 'short_trend': r[2] or '?',
            'margin_pctile': float(r[3] or 100), 'activity': r[4] or '?', 'rank': r[5] or 0,
        })
    latest_ranking = ranking[0] if ranking else {}
    margin_trend = latest_ranking.get('margin_trend', '?')
    short_trend = latest_ranking.get('short_trend', '?')
    signal = 'NEUTRAL'
    if margin_trend == 'INCREASING' and short_trend == 'DECREASING':
        signal = 'BULLISH (融资升+融券降)'
    elif margin_trend == 'INCREASING':
        signal = 'MARGIN_UP (融资升)'
    elif short_trend == 'DECREASING':
        signal = 'SHORT_DOWN (融券降)'
    elif margin_trend == 'DECREASING' and short_trend == 'INCREASING':
        signal = 'BEARISH (融资降+融券升)'

    # ── 杠杆深度分析 ──
    leverage_analysis = None
    if detail:
        latest = detail[0]
        # 1) 杠杆集中度 (融资买入占当日成交额的比例)
        lev_ratio = latest['leverage_ratio']
        # 2) 融资余额趋势 (近5日)
        balances = [d['margin_balance'] for d in detail[:5] if d.get('margin_balance')]
        balance_trend = 'INCREASING' if len(balances) >= 2 and balances[0] > balances[-1] else \
                        'DECREASING' if len(balances) >= 2 and balances[0] < balances[-1] else 'STABLE'
        # 3) 融券回补动机
        short_bals = [d['short_bal'] for d in detail[:5]]
        short_sells = [d['short_sell'] for d in detail[:5]]
        short_repays = [d['short_repay'] for d in detail[:5]]
        total_short_sell = sum(short_sells)
        total_short_repay = sum(short_repays)
        # 融券余额变化率
        short_bal_change = 0
        if len(short_bals) >= 2 and short_bals[-1] > 0:
            short_bal_change = (short_bals[0] - short_bals[-1]) / short_bals[-1] * 100
        # 融券回补比 = 融券偿还 / 融券卖出 (越大说明空头越在回补)
        short_cover_ratio = total_short_repay / total_short_sell * 100 if total_short_sell > 0 else 0
        # 空头回补信号
        short_cover_signal = 'COVERING' if short_cover_ratio > 150 else \
                             'ACTIVE' if total_short_sell > 0 and short_cover_ratio < 50 else \
                             'NEUTRAL'
        # 4) 融资净买入趋势
        net_buys = [d['margin_net'] for d in detail[:5]]
        consecutive_net_buy = 0
        for nb in net_buys:
            if nb > 0:
                consecutive_net_buy += 1
            else:
                break
        # 5) 融资余额占比 (融资余额 / 当日成交额，衡量杠杆存量对流动性的比例)
        balance_to_flow = latest['margin_balance'] / latest['daily_amount'] if latest['daily_amount'] else 0

        leverage_analysis = {
            'leverage_ratio': lev_ratio,
            'balance_trend': balance_trend,
            'margin_balance': latest['margin_balance'],
            'short_cover_ratio': round(short_cover_ratio, 1),
            'short_cover_signal': short_cover_signal,
            'short_bal_change': round(short_bal_change, 2),
            'short_bal_latest': latest['short_bal'],
            'consecutive_net_buy': consecutive_net_buy,
            'balance_to_flow': round(balance_to_flow, 2),
            # 近5日集中度趋势
            'leverage_trend': [d['leverage_ratio'] for d in detail[:5]],
            # 轧空潜力分析
            'short_interest_ratio': 0,
            'days_to_cover': 0,
            'squeeze_score': 0,
            'squeeze_signal': 'NONE',
        }

        # ── 轧空潜力 (Short Squeeze) ──
        short_bal_latest = latest['short_bal']
        if short_bal_latest > 0 and daily_amounts:
            # Short Interest Ratio = 融券余额(股) / 近5日日均成交量(股)
            recent_daily_vols = []
            for d in detail[:5]:
                dv = daily_amounts.get(d['date'], 0)
                if dv > 0:
                    # 近似成交量(股) = 成交额 / 当日均价 (用5min数据无法直接获取成交量)
                    # 更精确: 近5日融券日均卖出量
                    recent_daily_vols.append(dv)
            # 用近5日平均融券卖出量作为 "日回补能力" 的替代
            avg_short_sell = statistics.mean([d['short_sell'] for d in detail[:5]]) if detail else 0
            # Days to Cover = 融券余额 / 日均融券卖出量 (如果空头想全部平仓需要多少天)
            days_to_cover = short_bal_latest / avg_short_sell if avg_short_sell > 0 else 0
            # Short Interest Ratio = 融券余额(股) / 用融资余额作为市值代理的比例
            # 简化: 直接用融券余额的绝对值和历史变化率
            leverage_analysis['short_interest_ratio'] = round(
                short_bal_latest / statistics.mean([d['short_bal'] for d in detail[:5]]) * 100
                if len(detail) >= 5 and statistics.mean([d['short_bal'] for d in detail[:5]]) > 0 else 0, 1)
            leverage_analysis['days_to_cover'] = round(days_to_cover, 1)

            # Squeeze Score (0-100):
            # - 融券余额高且增长 (空头累积)
            # - 融券回补比低 (空头还没开始平仓)
            # - 价格在VWAP上方 (有上涨趋势触发轧空)
            sq = 0
            # 融券余额增长 (+0~30分)
            if short_bal_change > 50:
                sq += 30
            elif short_bal_change > 20:
                sq += 20
            elif short_bal_change > 0:
                sq += 10
            # 融券余额绝对量 (用days_to_cover衡量) (+0~30分)
            if days_to_cover > 30:
                sq += 30
            elif days_to_cover > 15:
                sq += 20
            elif days_to_cover > 5:
                sq += 10
            # 融券回补比低 = 还没回补 (+0~20分)
            if short_cover_ratio < 30:
                sq += 20
            elif short_cover_ratio < 60:
                sq += 10
            # 融券活动存在 (+0~20分)
            if total_short_sell > 0:
                sq += 10
            if short_bal_latest > 100000:
                sq += 10

            leverage_analysis['squeeze_score'] = sq
            if sq >= 70:
                leverage_analysis['squeeze_signal'] = 'HIGH_RISK'
            elif sq >= 40:
                leverage_analysis['squeeze_signal'] = 'MODERATE'
            elif sq >= 20:
                leverage_analysis['squeeze_signal'] = 'LOW'
            else:
                leverage_analysis['squeeze_signal'] = 'NONE'

    return {'detail': detail, 'ranking': ranking, 'signal': signal,
            'leverage': leverage_analysis}

def analyze_composite(intraday, historical, sector, volume, margin):
    scores = {}
    s1 = 5.0
    if intraday:
        day_ret = intraday['day_ret']
        if day_ret > 3: s1 = 9
        elif day_ret > 1: s1 = 7
        elif day_ret > 0: s1 = 6
        elif day_ret > -1: s1 = 4
        elif day_ret > -3: s1 = 2
        else: s1 = 1
        strong = intraday['slots'].get('最强(13:30-14:00)', {})
        if strong.get('ret', 0) > 0: s1 = min(10, s1 + 1)
        # VWAP 偏离度加分
        vwap_dev = intraday.get('final_deviation', 0)
        if vwap_dev > 1.0: s1 = min(10, s1 + 1)
        elif vwap_dev < -1.0: s1 = max(1, s1 - 1)
    scores['分时形态'] = s1

    s2 = 5.0
    if historical and historical['correlation'] is not None:
        corr = historical['correlation']
        if corr > 0.8: s2 = 8
        elif corr > 0.5: s2 = 7
        elif corr > 0.2: s2 = 6
        elif corr > -0.2: s2 = 5
        else: s2 = 3
    scores['历史一致性'] = s2

    s3 = 5.0
    if sector:
        total = sector['total_peers']
        rank = sector['ret_rank']
        pct = rank / total if total else 0.5
        if pct <= 0.1: s3 = 9
        elif pct <= 0.25: s3 = 7
        elif pct <= 0.5: s3 = 6
        elif pct <= 0.75: s3 = 4
        else: s3 = 2
        if sector['final_alpha'] > 1: s3 = min(10, s3 + 1)
    scores['行业相对强度'] = s3

    s4 = 5.0
    if volume:
        if volume['surge_count'] >= 3: s4 = 7
        elif volume['surge_count'] >= 1: s4 = 6
        if volume['peak_slot'] == '黄金(10:30-11:30)': s4 = min(10, s4 + 1)
        if volume['peak_slot'] == '尾盘(14:00-15:00)': s4 = max(1, s4 - 1)
    scores['量能健康度'] = s4

    s5 = 5.0
    if margin:
        sig = margin['signal']
        if 'BULLISH' in sig: s5 = 9
        elif 'MARGIN_UP' in sig: s5 = 7
        elif 'SHORT_DOWN' in sig: s5 = 7
        elif 'BEARISH' in sig: s5 = 2
        if margin['detail']:
            net_buys = [d['margin_net'] for d in margin['detail'][:3]]
            positive_nets = sum(1 for n in net_buys if n > 0)
            if positive_nets >= 2: s5 = min(10, s5 + 1)
            elif positive_nets == 0: s5 = max(1, s5 - 1)
    scores['资金面'] = s5

    total = sum(scores.values())
    avg = total / len(scores)
    if avg >= 8: grade = 'S'
    elif avg >= 7: grade = 'A'
    elif avg >= 6: grade = 'B'
    elif avg >= 4: grade = 'C'
    else: grade = 'D'

    findings = _generate_findings(scores, intraday, sector, margin)
    judgment = _generate_judgment(scores, intraday, historical, sector, volume, margin)

    return {
        'scores': scores, 'total': round(total, 1),
        'average': round(avg, 1), 'grade': grade,
        'findings': findings, 'judgment': judgment,
    }

def _generate_findings(scores, intraday, sector, margin):
    findings = []
    best = max(scores, key=scores.get)
    worst = min(scores, key=scores.get)
    if scores[best] >= 8:
        findings.append(f"优势维度: {best} ({scores[best]}分)")
    if scores[worst] <= 3:
        findings.append(f"弱势维度: {worst} ({scores[worst]}分)")
    if intraday:
        slots = intraday.get('slots', {})
        strong = slots.get('最强(13:30-14:00)', {})
        if strong.get('ret', 0) > 0.5:
            findings.append(f"最强时段(13:30-14:00)上涨 {strong['ret']}%，符合强势特征")
        elif strong.get('ret', 0) < -0.5:
            findings.append(f"最强时段(13:30-14:00)下跌 {strong['ret']}%，午后走弱")
    if sector:
        if sector['ret_rank'] <= 3:
            findings.append(f"行业内涨幅排名 {sector['ret_rank']}/{sector['total_peers']}，领先")
        elif sector['ret_rank'] > sector['total_peers'] * 0.7:
            findings.append(f"行业内涨幅排名 {sector['ret_rank']}/{sector['total_peers']}，落后")
    if margin:
        sig = margin['signal']
        if 'BULLISH' in sig:
            findings.append("两融共振: 融资升+融券降，资金面看多")
        elif 'BEARISH' in sig:
            findings.append("两融背离: 融资降+融券升，注意风险")
    return findings


def _generate_judgment(scores, intraday, historical, sector, volume, margin):
    """生成每个维度的具体分析文字"""
    rows = []  # [(维度, 得分, 要点)]

    # ── 分时形态 ──
    s1 = scores.get('分时形态', 5)
    j1 = ''
    if intraday:
        day_ret = intraday['day_ret']
        slots = intraday['slots']
        # 找最强时段
        best_slot = max(slots, key=lambda k: slots[k]['amount_pct']) if slots else ''
        best_pct = slots[best_slot]['amount_pct'] if best_slot else 0
        j1 = f"当日涨 {day_ret:+.3f}%，{best_slot}占成交额{best_pct}%"
        # 各时段涨跌简述
        parts = []
        for sn in SLOTS:
            ss = slots.get(sn)
            if ss:
                short_name = sn.split('(')[0]
                parts.append(f"{short_name}{ss['ret']:+.2f}%")
        if parts:
            j1 += '，' + '→'.join(parts)
        # VWAP 信息
        vwap_dev = intraday.get('final_deviation', 0)
        vwap_price = intraday.get('final_vwap', 0)
        if vwap_price:
            j1 += f"，VWAP={vwap_price:.2f}(偏离{vwap_dev:+.3f}%)"
    rows.append(('分时形态', s1, j1 or '无数据'))

    # ── 历史一致性 ──
    s2 = scores.get('历史一致性', 5)
    j2 = ''
    if historical:
        corr = historical['correlation']
        corr_text = f"相关系数{corr:.3f}" if corr is not None else "无相关数据"
        ds = historical['daily_stats']
        trading_days = historical['trading_days']
        if trading_days and len(trading_days) >= 2:
            rets = [ds.get(d, {}).get('ret', 0) for d in trading_days]
            up_days = sum(1 for r in rets if r > 0)
            j2 = f"{corr_text}，{len(trading_days)}日中{up_days}日上涨"
            # 成交额趋势
            amounts = [ds.get(d, {}).get('amount', 0) for d in trading_days]
            if amounts[-1] > amounts[0] * 1.2:
                j2 += "，成交额递增"
            elif amounts[-1] < amounts[0] * 0.6:
                j2 += "，成交额递减"
        else:
            j2 = corr_text
    rows.append(('历史一致性', s2, j2 or '无数据'))

    # ── 行业相对强度 ──
    s3 = scores.get('行业相对强度', 5)
    j3 = ''
    if sector:
        j3 = f"{sector['sector']}行业{sector['total_peers']}只中排第{sector['ret_rank']}名"
        if sector['final_alpha'] > 0:
            j3 += f"，Alpha +{sector['final_alpha']:.2f}%"
        else:
            j3 += f"，Alpha {sector['final_alpha']:.2f}%"
    rows.append(('行业相对强度', s3, j3 or '无数据'))

    # ── 量能健康度 ──
    s4 = scores.get('量能健康度', 5)
    j4 = ''
    if volume:
        surge = volume['surge_count']
        if surge > 0:
            j4 = f"{surge}根放量K线(>3x量比)，集中在{volume['peak_slot']}"
        else:
            j4 = f"无放量K线(>3x量比)，成交额集中在{volume['peak_slot']}"
        bars = volume.get('bars', [])
        if bars:
            max_bar = max(bars, key=lambda b: b['amount'])
            j4 += f"，最大成交{max_bar['time']}({format_amount(max_bar['amount'])})"
    rows.append(('量能健康度', s4, j4 or '无数据'))

    # ── 资金面 ──
    s5 = scores.get('资金面', 5)
    j5 = ''
    if margin:
        sig = margin['signal']
        detail = margin['detail']
        j5 = f"信号: {sig}"
        if detail:
            latest = detail[0]
            j5 += f"，当日融资净买入{format_net_amount(latest['margin_net'])}"
            # 近3日净买趋势
            nets = [d['margin_net'] for d in detail[:3]]
            pos = sum(1 for n in nets if n > 0)
            if pos >= 2:
                j5 += "，近期融资持续净流入"
            elif pos == 0:
                j5 += "，近期融资持续净流出"
            # 融券
            short_total = sum(d['short_sell'] for d in detail[:5])
            if short_total == 0:
                j5 += "，无融券交易"
        # 杠杆深度
        lev = margin.get('leverage')
        if lev:
            if lev['leverage_ratio'] > 0:
                j5 += f"，杠杆集中度{lev['leverage_ratio']:.1f}%"
            if lev['short_cover_signal'] == 'COVERING':
                j5 += "，空头积极回补"
            elif lev['short_cover_signal'] == 'ACTIVE':
                j5 += "，空头活跃建仓"
    rows.append(('资金面', s5, j5 or '无数据'))

    return rows


def _generate_notes(intraday, historical, sector, volume, margin, daily_rows):
    """生成'值得注意'的要点列表"""
    notes = []

    # VWAP / VaP 信号
    if intraday:
        vwap_dev = intraday.get('final_deviation', 0)
        poc = intraday.get('poc')
        va_position = intraday.get('va_position', 50)
        if vwap_dev > 2.0:
            notes.append(f"收盘价大幅高于VWAP (+{vwap_dev:.2f}%)，日内强势明显")
        elif vwap_dev < -2.0:
            notes.append(f"收盘价大幅低于VWAP ({vwap_dev:.2f}%)，日内弱势明显")
        if poc:
            close_price = intraday.get('vwap_bars', [{}])[-1].get('close', 0) if intraday.get('vwap_bars') else 0
            if close_price and poc['price'] > 0:
                poc_dev = (close_price - poc['price']) / poc['price'] * 100
                if poc_dev > 1.5:
                    notes.append(f"价格在POC({poc['price']:.2f})上方{poc_dev:.1f}%，远离公允价值，可能面临回撤")
                elif poc_dev < -1.5:
                    notes.append(f"价格在POC({poc['price']:.2f})下方{abs(poc_dev):.1f}%，远离公允价值，关注是否反弹")
        if va_position > 90:
            notes.append(f"价格位于Value Area顶部({va_position:.0f}%)，突破VA上边界关注趋势加速")
        elif va_position < 10:
            notes.append(f"价格位于Value Area底部({va_position:.0f}%)，跌破VA下边界注意下行风险")

    # 日线级别的异动
    if daily_rows and len(daily_rows) >= 3:
        # 找大涨/大跌日
        prev_close = None
        big_moves = []
        for dr in daily_rows:
            if prev_close and prev_close > 0:
                chg = (dr['close'] - prev_close) / prev_close * 100
                if abs(chg) >= 5:
                    big_moves.append((dr['date'], chg))
            prev_close = dr['close']
        for d, c in big_moves:
            if c > 0:
                notes.append(f"{d} 曾出现 +{c:.1f}% 的大涨")
            else:
                notes.append(f"{d} 曾出现 {c:.1f}% 的大跌")

        # 成交额趋势
        vols = [dr['vol_lots'] for dr in daily_rows if dr.get('vol_lots')]
        if len(vols) >= 3:
            recent = vols[-3:]
            older = vols[:-3]
            if older:
                avg_recent = statistics.mean(recent)
                avg_older = statistics.mean(older)
                if avg_recent > avg_older * 1.5:
                    notes.append(f"近期成交量({avg_recent:.0f}万手)显著高于前期({avg_older:.0f}万手)")
                elif avg_recent < avg_older * 0.5:
                    notes.append(f"近期成交量({avg_recent:.0f}万手)显著低于前期({avg_older:.0f}万手)，缩量明显")

    # 两融特殊信号
    if margin and margin['detail']:
        detail = margin['detail']
        # 连续净买入
        nets = [d['margin_net'] for d in detail[:5]]
        if all(n > 0 for n in nets):
            notes.append(f"连续{len(nets)}日融资净买入，杠杆资金持续看多")
        elif all(n < 0 for n in nets[:3]):
            notes.append("连续3日融资净卖出，杠杆资金撤离")

        # 融券活动
        short_sells = [d['short_sell'] for d in detail[:5]]
        if sum(short_sells) == 0:
            notes.append("近5日无融券交易，说明空头不活跃")

    # 杠杆深度信号
    if margin and margin.get('leverage'):
        lev = margin['leverage']
        if lev['leverage_ratio'] > 25:
            notes.append(f"杠杆集中度高达{lev['leverage_ratio']:.1f}%，杠杆资金主导交易，波动性风险加大")
        if lev['short_cover_signal'] == 'COVERING':
            notes.append(f"融券回补比{lev['short_cover_ratio']:.0f}%，空头积极回补，可能是轧空信号")
        elif lev['short_cover_signal'] == 'ACTIVE':
            notes.append(f"融券回补比仅{lev['short_cover_ratio']:.0f}%，空头活跃建仓，注意下行压力")
        if lev['balance_to_flow'] > 10:
            notes.append(f"融资余额是当日成交额的{lev['balance_to_flow']:.1f}倍，杠杆存量压力大，关注去杠杆风险")
        if lev.get('squeeze_score', 0) >= 70:
            notes.append(f"轧空概率高(Squeeze Score {lev['squeeze_score']}/100)，融券余额{lev['short_bal_latest']:,}股，"
                         f"需{lev.get('days_to_cover', 0):.0f}日才能平仓，若价格上涨可能触发空头被迫回补")
        elif lev.get('squeeze_score', 0) >= 40:
            notes.append(f"有一定轧空潜力(Squeeze Score {lev['squeeze_score']}/100)，关注价格突破时空头回补的助推效应")

    # 多日分时特征
    if historical and historical['daily_stats']:
        ds = historical['daily_stats']
        trading_days = historical['trading_days']
        if len(trading_days) >= 2:
            # 检查最强时段维持情况
            strong_rets = []
            for d in trading_days:
                stat = ds.get(d, {})
                gold_pct = stat.get('gold_pct', 0)
                strong_ret = stat.get('strong_ret', 0)
                strong_rets.append(strong_ret)
            pos_strong = sum(1 for r in strong_rets if r > 0)
            if pos_strong == len(strong_rets) and len(strong_rets) >= 3:
                notes.append("最强时段(13:30-14:00)连续多日维持正收益，属于\"最强时段维持涨幅\"类型")
            elif pos_strong == 0 and len(strong_rets) >= 3:
                notes.append("最强时段(13:30-14:00)连续多日下跌，午后持续走弱")

    return notes


# ================================================================
#  报告生成 (增强版)
# ================================================================

def format_amount(val):
    if abs(val) >= 1e8:
        return f"{val / 1e8:.2f}亿"
    elif abs(val) >= 1e4:
        return f"{val / 1e4:.0f}万"
    else:
        return f"{val:.0f}"

def format_net_amount(val):
    """格式化可能为负数的净额"""
    s = format_amount(abs(val))
    return f"-{s}" if val < 0 else s

def generate_report(symbol, name, sector, date,
                    daily_rows, concepts, s01_scores, mkt_bg, ind_margin,
                    intraday, historical, sector_comp, volume, margin_result, composite,
                    beta_result=None):
    lines = []

    # ─── frontmatter ───
    lines.append('---')
    lines.append(f'title: "个股分析: {name}({symbol})"')
    lines.append(f'date: {date}')
    lines.append('type: stock-analysis')
    lines.append(f'tags: [量化, 个股分析, {sector}]')
    lines.append('---')
    lines.append('')

    # ─── 标题 ───
    lines.append(f'# {name}({symbol}) — {date} 个股分析')
    lines.append('')

    # ─── 基本信息 ───
    lines.append('## 基本信息')
    lines.append('')
    lines.append('| 项目 | 值 |')
    lines.append('|------|------|')
    lines.append(f'| 代码 | `{symbol}` |')
    lines.append(f'| 名称 | {name} |')
    lines.append(f'| 行业 | {sector} |')
    lines.append(f'| 分析日期 | {date} |')
    if concepts:
        lines.append(f'| 概念标签 | {", ".join(concepts[:10])} |')
    if composite:
        lines.append(f'| **综合评级** | **{composite["grade"]}** (均分 {composite["average"]}) |')
    lines.append('')

    # ─── 日线行情 ───
    if daily_rows and len(daily_rows) >= 2:
        lines.append('### 近期日线行情')
        lines.append('')
        lines.append('| 日期 | 开盘 | 最高 | 最低 | 收盘 | 涨跌% | 振幅% | 成交量(万手) |')
        lines.append('|------|------|------|------|------|-------|-------|-------------|')
        prev_close = None
        for dr in daily_rows:
            chg = ''
            if prev_close and prev_close > 0:
                chg_val = (dr['close'] - prev_close) / prev_close * 100
                chg = f'{chg_val:+.2f}'
            amp = (dr['high'] - dr['low']) / dr['low'] * 100 if dr['low'] else 0
            vol_wan = dr['vol_lots'] / 10000 if dr['vol_lots'] else 0
            marker = ' **<<' if dr['date'] == date else ''
            lines.append(f"| {dr['date']}{marker} | {dr['open']:.2f} | {dr['high']:.2f} | {dr['low']:.2f} | {dr['close']:.2f} | {chg} | {amp:.2f} | {vol_wan:.1f} |")
            prev_close = dr['close']
        lines.append('')

    # ─── S01得分 ───
    if s01_scores:
        lines.append('### S01 独立强度得分')
        lines.append('')
        lines.append('| 日期 | 得分 | 逆势次数 | 行业 |')
        lines.append('|------|------|---------|------|')
        for s in s01_scores:
            lines.append(f"| {s['date']} | {s['score']} | {s['contra_count']} | {s['sector']} |")
        lines.append('')

    # ─── 维度1: 分时形态 + 逐笔全览 ───
    if intraday:
        lines.append('## 1. 分时形态')
        lines.append('')
        lines.append(f'日内涨跌幅: **{intraday["day_ret"]}%** | '
                     f'总成交额: {format_amount(intraday["total_amount"])} | '
                     f'总成交量: {intraday["total_volume"]:,}')
        lines.append('')

        lines.append('### 时段统计')
        lines.append('')
        lines.append('> [!info] 时段定义: 早盘(9:35-10:30) | 黄金(10:30-11:30) | 午盘(13:00-13:30) | 最强(13:30-14:00) | 尾盘(14:00-15:00)')
        lines.append('>')
        lines.append('> 量比 = 时段成交额 / 全日每根K线平均成交额，反映该时段相对全天平均的成交集中度')
        lines.append('')
        lines.append('| 时段 | 涨跌% | 量比 | 振幅% | 最大拉升% | 最大回撤% | 成交额占比 |')
        lines.append('|------|-------|------|-------|---------|---------|-----------|')
        for slot_name in SLOTS:
            s = intraday['slots'].get(slot_name)
            if s:
                lines.append(f'| {slot_name} | {s["ret"]:+.3f} | {s["amount_ratio"]:.1f}x | '
                             f'{s["amplitude"]:.3f} | {s["max_surge"]:+.3f} | {s["max_drop"]:+.3f} | '
                             f'{s["amount_pct"]}% |')
        lines.append('')

    # ─── VWAP 分析 ───
    if intraday and intraday.get('vwap_bars'):
        lines.append('### VWAP 偏离度')
        lines.append('')
        lines.append('> [!info] VWAP (Volume Weighted Average Price) = 累计成交额 / 累计成交量，反映市场持仓均价')
        lines.append('>')
        lines.append('> - 偏离度 = (收盘价 - VWAP) / VWAP × 100%，正值表示多数持仓者浮盈，负值表示浮亏')
        lines.append('> - 机构常以 VWAP 作为执行基准，价格在 VWAP 上方通常视为强势')
        lines.append('> - Value Area (VA): 包含 70% 成交额的价格区间，POC (Point of Control) 为最大成交额的价格位')
        lines.append('')
        vwap = intraday
        dev_label = '强势(价格>均线上方)' if vwap['final_deviation'] > 0.5 else \
                    '偏强' if vwap['final_deviation'] > 0 else \
                    '偏弱' if vwap['final_deviation'] > -0.5 else '弱势(价格<均线下方)'
        lines.append(f'收盘价: **{volume["bars"][-1]["close"]:.2f}** | '
                     f'VWAP: **{vwap["final_vwap"]:.2f}** | '
                     f'偏离度: **{vwap["final_deviation"]:+.3f}%** ({dev_label})')
        lines.append('')
        poc = vwap.get('poc')
        if poc:
            lines.append(f'POC (最大成交价): **{poc["price"]:.2f}** (成交额 {format_amount(poc["amount"])}) | '
                         f'Value Area: {vwap["va_low"]:.2f} ~ {vwap["va_high"]:.2f} | '
                         f'价格位于VA {vwap["va_position"]:.0f}%位置')
            lines.append('')
            va_pos_label = '上方(强势)' if vwap['va_position'] > 70 else \
                           '中上部' if vwap['va_position'] > 50 else \
                           '中下部' if vwap['va_position'] > 30 else '下方(弱势)'
            lines.append(f'VA 位置判定: **{va_pos_label}**')
            lines.append('')

        # VWAP 逐笔追踪 (关键时间点)
        lines.append('#### VWAP 逐笔追踪 (关键时间点)')
        lines.append('')
        key_times_vwap = set()
        for t in ['09:40', '10:00', '10:30', '11:00', '11:30', '13:00', '13:30', '14:00', '14:30', '15:00']:
            key_times_vwap.add(t)
        lines.append('| 时间 | 收盘价 | VWAP | 偏离度% |')
        lines.append('|------|--------|------|---------|')
        for vb in vwap['vwap_bars']:
            if vb['time'] in key_times_vwap:
                lines.append(f"| {vb['time']} | {vb['close']:.2f} | {vb['vwap']:.2f} | {vb['deviation']:+.3f} |")
        lines.append('')

    # ─── Volume at Price 分布 ───
    if intraday and intraday.get('vap'):
        lines.append('### 价格密集区 (Volume at Price)')
        lines.append('')
        vap = intraday
        lines.append('> [!info] 将当日5分钟K线的成交按价格区间聚合，显示资金在哪个价位最活跃')
        lines.append('>')
        lines.append(f'> 价格区间粒度: {vap["bucket_size"]}元 | 日内价格区间: {vap["price_range"]:.2f}元')
        lines.append('> POC (Point of Control): 成交额最大的价格位，即当日"公允价格"')
        lines.append('> Value Area: 包含 70% 成交额的价格区间，价格突破 VA 边界常意味着趋势加速')
        lines.append('')

        poc = vap.get('poc')
        va_low = vap['va_low']
        va_high = vap['va_high']
        max_amount = max(v['amount'] for v in vap['vap']) if vap['vap'] else 1

        lines.append('| 价格 | 成交额 | 占比 | 分布 | VA |')
        lines.append('|------|--------|------|------|-----|')
        for v in vap['vap']:
            pct = v['amount'] / max_amount * 100 if max_amount else 0
            bar_len = int(pct / 5)
            bar = '█' * bar_len + '░' * (20 - bar_len)
            is_va = ' **VA**' if va_low <= v['price'] <= va_high else ''
            is_poc = ' POC' if poc and abs(v['price'] - poc['price']) < 0.001 else ''
            marker = is_poc if is_poc else is_va
            total_pct = v['amount'] / sum(vv['amount'] for vv in vap['vap']) * 100 if vap['vap'] else 0
            lines.append(f"| {v['price']:.2f} | {format_amount(v['amount'])} | {total_pct:.1f}% | {bar} |{marker} |")
        lines.append('')

    # ─── 维度1补充: 当日逐笔5min K线 ───
    if volume and volume['bars']:
        lines.append('### 当日逐笔5分钟K线')
        lines.append('')
        lines.append('> [!info] 量比 = 该K线成交额 / 近N日同时段均值，>3x 标记为异常')
        lines.append('')
        lines.append('| 时间 | 开 | 高 | 低 | 收 | 涨跌% | 成交额 | 量比 | 占比% |')
        lines.append('|------|-----|-----|-----|-----|-------|--------|------|------|')
        for b in volume['bars']:
            flag = ' **异常**' if b['is_anomaly'] else ''
            lines.append(f"| {b['time']} | {b['open']:.2f} | {b['high']:.2f} | {b['low']:.2f} | {b['close']:.2f} | "
                         f"{b['ret']:+.3f} | {format_amount(b['amount'])} | {b['volume_ratio']:.1f}x | {b['amount_pct']} |{flag}")
        lines.append('')

    # ─── 维度2: 多日对比 ───
    if historical and historical['daily_stats']:
        lines.append('## 2. 多日分时对比')
        lines.append('')
        if historical['correlation'] is not None:
            corr_desc = '强正相关' if historical['correlation'] > 0.5 else '弱相关' if historical['correlation'] > -0.2 else '负相关'
            lines.append(f'与前一交易日分时相关系数: **{historical["correlation"]:.3f}** ({corr_desc})')
            lines.append('')

        lines.append('### 每日概览')
        lines.append('')
        lines.append('| 日期 | 涨跌% | 振幅% | 成交额 | 黄金时段占比 | 最强时段涨跌% |')
        lines.append('|------|-------|-------|--------|-------------|-------------|')
        for d in historical['trading_days']:
            ds = historical['daily_stats'].get(d)
            if ds:
                marker = ' **<<' if d == date else ''
                lines.append(f'| {d}{marker} | {ds["ret"]:+.3f} | {ds["amplitude"]:.2f} | '
                             f'{format_amount(ds["amount"])} | {ds["gold_pct"]}% | {ds["strong_ret"]:+.3f} |')
        lines.append('')

        # 逐日分时收益率对比 (每天取关键时间点的累计收益)
        multi_data = historical.get('multi_data', {})
        if multi_data:
            lines.append('### 分时累计收益率对比 (关键时间点)')
            lines.append('')
            lines.append('> [!info] 以每日开盘价为基准的累计收益率(%)，* 表示该时间点无数据取最后一根K线')
            lines.append('')
            key_times = ['09:40', '10:00', '10:30', '11:00', '11:30', '13:00', '13:30', '14:00', '14:30', '15:00']
            header = '| 时间 |'
            for d in historical['trading_days']:
                short_d = d[5:]  # MM-DD
                header += f' {short_d} |'
            lines.append(header)
            sep = '|------|'
            for _ in historical['trading_days']:
                sep += '-------|'
            lines.append(sep)

            for t_str in key_times:
                row = f'| {t_str} |'
                for d in historical['trading_days']:
                    day_bars = multi_data.get(d, [])
                    if not day_bars:
                        row += ' - |'
                        continue
                    base = day_bars[0]['open']
                    # 找到 >= 该时间的第一根K线
                    target_h, target_m = int(t_str[:2]), int(t_str[3:])
                    cum_ret = None
                    for b in day_bars:
                        bh, bm = b['datetime'].hour, b['datetime'].minute
                        if bh > target_h or (bh == target_h and bm >= target_m):
                            cum_ret = (b['close'] - base) / base * 100 if base else 0
                            break
                    if cum_ret is not None:
                        row += f' {cum_ret:+.2f} |'
                    else:
                        # 用最后一根K线
                        cum_ret = (day_bars[-1]['close'] - base) / base * 100 if base else 0
                        row += f' {cum_ret:+.2f}* |'
                lines.append(row)
            lines.append('')

    # ─── 维度3: 行业对比 ───
    if sector_comp:
        lines.append('## 3. 行业对比')
        lines.append('')
        lines.append(f'行业: **{sector_comp["sector"]}** | '
                     f'行业内共 {sector_comp["total_peers"]} 只 | '
                     f'涨幅排名: **{sector_comp["ret_rank"]}/{sector_comp["total_peers"]}** | '
                     f'成交额排名: {sector_comp["amt_rank"]}/{sector_comp["total_peers"]}')
        lines.append('')
        lines.append(f'平均Alpha: **{sector_comp["avg_alpha"]:+.3f}%** | '
                     f'最终Alpha: **{sector_comp["final_alpha"]:+.3f}%**')
        lines.append('')
        lines.append('> [!info] Alpha = 个股累计收益率 - 行业等权平均累计收益率，正值表示跑赢行业')
        lines.append('')

        lines.append('### 行业涨幅排名 Top 10')
        lines.append('')
        lines.append('| # | 代码 | 涨跌% | 振幅% |')
        lines.append('|---|------|-------|-------|')
        for i, (sym, ret, amp) in enumerate(sector_comp['top10_by_ret'], 1):
            marker = ' **<目标>**' if sym == symbol else ''
            lines.append(f'| {i} | `{sym}` | {ret:+.2f} | {amp:.2f} |{marker}')
        lines.append('')

    # ─── Beta敏感性 ───
    if beta_result:
        lines.append('### Beta敏感性 (vs 行业)')
        lines.append('')
        beta = beta_result
        beta_label = '高Beta(进攻型)' if beta['avg_beta'] > 1.5 else \
                     '偏高Beta' if beta['avg_beta'] > 1.0 else \
                     '低Beta(防御型)' if beta['avg_beta'] < 0.5 else \
                     '偏低Beta' if beta['avg_beta'] < 0.8 else '同步型'
        lines.append(f'平均5分钟Beta: **{beta["avg_beta"]:.3f}** ({beta_label}) | '
                     f'基于 {beta["days"]} 日5分钟数据')
        if beta.get('correlation') is not None:
            corr_desc = '强正相关' if beta['correlation'] > 0.6 else \
                        '中等相关' if beta['correlation'] > 0.3 else \
                        '弱相关' if beta['correlation'] > 0 else '负相关'
            lines.append(f' | 与行业相关性: **{beta["correlation"]:.3f}** ({corr_desc})')
        lines.append('')
        lines.append('> [!info] Beta = cov(个股5min收益, 行业5min收益) / var(行业5min收益)')
        lines.append('>')
        lines.append('> - Beta > 1.5: 个股波动远大于行业 (进攻型，涨跌都更剧烈)')
        lines.append('> - Beta ≈ 1.0: 个股与行业同步')
        lines.append('> - Beta < 0.5: 个股波动远小于行业 (防御型，抗跌但也可能抗涨)')
        lines.append('> - 分时段Beta差异可揭示该股在哪个时段最敏感于行业波动')
        lines.append('')

        # 分时段Beta
        if beta.get('slot_betas'):
            lines.append('#### 分时段Beta')
            lines.append('')
            lines.append('| 时段 | Beta | 判定 |')
            lines.append('|------|------|------|')
            for slot_name in SLOTS:
                sb = beta['slot_betas'].get(slot_name)
                if sb is not None:
                    sb_label = '高敏感' if sb > 1.5 else '偏高' if sb > 1.0 else \
                               '低敏感' if sb < 0.5 else '同步'
                    lines.append(f'| {slot_name} | {sb:.3f} | {sb_label} |')
            lines.append('')

        # 逐日Beta
        if beta.get('daily_betas') and len(beta['daily_betas']) > 1:
            lines.append('#### 逐日Beta变化')
            lines.append('')
            lines.append('| 日期 | Beta | 偏离均值 |')
            lines.append('|------|------|---------|')
            avg_b = beta['avg_beta']
            for d in sorted(beta['daily_betas'].keys()):
                b_val = beta['daily_betas'][d]
                marker = ' **<<' if d == date else ''
                dev = b_val - avg_b
                lines.append(f'| {d}{marker} | {b_val:.3f} | {dev:+.3f} |')
            lines.append('')

    # ─── 维度4: 量能异动 ───
    if volume:
        lines.append('## 4. 量能异动')
        lines.append('')
        lines.append(f'全市场成交额 P50: {format_amount(volume["mkt_p50"])} | '
                     f'P95: {format_amount(volume["mkt_p95"])}')
        lines.append('')

        # 全日量能对比
        daily_label = '放量' if volume['daily_vol_ratio'] >= 1.3 else '缩量' if volume['daily_vol_ratio'] < 0.7 else '正常'
        lines.append(f'当日成交额: {format_amount(volume["bars"] and sum(b["amount"] for b in volume["bars"]))} | '
                     f'历史均额: {format_amount(volume["daily_hist_avg"])} | '
                     f'量比: **{volume["daily_vol_ratio"]:.2f}x** ({daily_label})')
        lines.append('')

        # 重点时段量能对比
        if volume.get('slot_comparison'):
            lines.append('### 重点时段量能对比')
            lines.append('')
            lines.append('> [!info] 列说明')
            lines.append('>')
            lines.append('> - **历史均值**: 近N日同时段该股自身成交额均值')
            lines.append('> - **量比**: 当日/历史均值，衡量相对自身历史的放量程度')
            lines.append('> - **行业均值**: 同行业所有股票该时段成交额等权均值')
            lines.append('> - **行业量比**: 当日/行业均值，衡量相对行业的资金关注度')
            lines.append('>')
            lines.append('> 量比判定: ≥2.0x 放量 | ≥1.3x 温和放量 | ≥0.7x 正常 | ≥0.4x 缩量 | <0.4x 明显缩量')
            lines.append('')
            # 判断是否有行业数据
            has_sector = bool(sector_comp and sector_comp.get('slot_ind_avg'))
            if has_sector:
                lines.append('| 时段 | 当日成交额 | 历史均值 | 量比 | 行业均值 | 行业量比 | 判定 | 占比% |')
                lines.append('|------|-----------|---------|------|---------|---------|------|------|')
                for sc in volume['slot_comparison']:
                    ind_avg = sector_comp['slot_ind_avg'].get(sc['slot'], 0)
                    ind_vr = sc['amount'] / ind_avg if ind_avg else 0
                    lines.append(f"| {sc['slot']} | {format_amount(sc['amount'])} | "
                                 f"{format_amount(sc['hist_avg'])} | {sc['vol_ratio']:.2f}x | "
                                 f"{format_amount(ind_avg)} | {ind_vr:.2f}x | "
                                 f"{sc['vol_label']} | {sc['amount_pct']} |")
            else:
                lines.append('| 时段 | 当日成交额 | 历史均值 | 历史最低 | 历史最高 | 量比 | 判定 | 占比% |')
                lines.append('|------|-----------|---------|---------|---------|------|------|------|')
                for sc in volume['slot_comparison']:
                    lines.append(f"| {sc['slot']} | {format_amount(sc['amount'])} | "
                                 f"{format_amount(sc['hist_avg'])} | {format_amount(sc['hist_min'])} | "
                                 f"{format_amount(sc['hist_max'])} | {sc['vol_ratio']:.2f}x | "
                                 f"{sc['vol_label']} | {sc['amount_pct']} |")
            lines.append('')

        lines.append(f'放量K线(>3x量比): **{volume["surge_count"]}根** '
                     f'{", ".join(volume["surge_times"]) if volume["surge_times"] else "无"}')
        lines.append('')
        lines.append(f'成交额最集中时段: **{volume["peak_slot"]}**')
        lines.append('')

        # 多日成交额趋势
        if volume.get('hist_daily_totals'):
            lines.append('### 多日成交额趋势')
            lines.append('')
            lines.append('| 日期 | 成交额 | 量比 |')
            lines.append('|------|--------|------|')
            hist_dates = sorted(multi_data.keys()) if multi_data else []
            all_totals = []
            for d in hist_dates:
                d_bars = multi_data.get(d, [])
                all_totals.append((d, sum(b['amount'] for b in d_bars) if d_bars else 0))
            if all_totals:
                grand_avg = statistics.mean([t for _, t in all_totals])
                for d, amt in all_totals:
                    marker = ' **<<' if d == date else ''
                    vr = amt / grand_avg if grand_avg else 1.0
                    lines.append(f'| {d}{marker} | {format_amount(amt)} | {vr:.2f}x |')
            lines.append('')

    # ─── 维度5: 两融分析 ───
    if margin_result:
        lines.append('## 5. 两融分析')
        lines.append('')
        lines.append(f'**两融信号: {margin_result["signal"]}**')
        lines.append('')
        lines.append('> [!info] 信号含义')
        lines.append('>')
        lines.append('> - **BULLISH**: 融资升+融券降，投机性资金看多，做多意愿强')
        lines.append('> - **MARGIN_UP**: 融资升，杠杆资金流入但空头未减')
        lines.append('> - **SHORT_DOWN**: 融券降，空头回补但融资未增')
        lines.append('> - **BEARISH**: 融资降+融券升，投机性资金看空，注意风险')
        lines.append('>')

        if margin_result['ranking']:
            r = margin_result['ranking'][0]
            lines.append('### 当前状态')
            lines.append('')
            lines.append('| 项目 | 值 |')
            lines.append('|------|------|')
            lines.append(f'| 融资趋势 | {r["margin_trend"]} |')
            lines.append(f'| 融券趋势 | {r["short_trend"]} |')
            lines.append(f'| 融资百分位 | Top {r["margin_pctile"]:.0f}% |')
            lines.append(f'| 活跃度 | {r["activity"]} |')
            lines.append(f'| 市场排名 | {r["rank"]} |')
            lines.append('')

            # 趋势变化表
            if len(margin_result['ranking']) > 1:
                lines.append('### 两融趋势变化')
                lines.append('')
                lines.append('| 日期 | 融资趋势 | 融券趋势 | 百分位 | 活跃度 | 排名 |')
                lines.append('|------|---------|---------|--------|--------|------|')
                for rk in margin_result['ranking'][:10]:
                    lines.append(f"| {rk['date']} | {rk['margin_trend']} | {rk['short_trend']} | "
                                 f"Top {rk['margin_pctile']:.0f}% | {rk['activity']} | {rk['rank']} |")
                lines.append('')

        if margin_result['detail']:
            lines.append('### 两融资金明细 (近10日)')
            lines.append('')
            lines.append('| 日期 | 融资买入 | 融资偿还 | 融资净买 | 融券卖出 | 融券偿还 | 融券净增 | 融券余额 |')
            lines.append('|------|---------|---------|---------|---------|---------|---------|---------|')
            for d in margin_result['detail'][:10]:
                lines.append(f'| {d["date"]} | {format_amount(d["margin_buy"])} | '
                             f'{format_amount(d["margin_repay"])} | '
                             f'{format_net_amount(d["margin_net"])} | '
                             f'{d["short_sell"]:,} | {d["short_repay"]:,} | '
                             f'{d["short_net"]:,} | {d["short_bal"]:,} |')
            lines.append('')

    # ─── 杠杆深度分析 ───
    if margin_result and margin_result.get('leverage'):
        lev = margin_result['leverage']
        lines.append('### 杠杆深度分析')
        lines.append('')
        lines.append('> [!info] 杠杆分析指标说明')
        lines.append('>')
        lines.append('> - **杠杆集中度** = 融资买入额 / 当日总成交额 × 100%，反映杠杆资金在当日交易中的占比')
        lines.append('> - **融资余额趋势** = 近5日融资余额的变化方向 (INCREASING/DECREASING/STABLE)')
        lines.append('> - **融券回补比** = 近5日融券偿还量 / 融券卖出量 × 100%，>150% 说明空头在积极回补')
        lines.append('> - **余额/成交额** = 融资余额 / 当日成交额，衡量杠杆存量对日流动性的压力倍数')
        lines.append('> - **空头回补信号**: COVERING=空头积极回补(利好) | ACTIVE=空头活跃建仓(利空) | NEUTRAL=中性')
        lines.append('')

        lines.append('#### 当前杠杆指标')
        lines.append('')
        lines.append('| 指标 | 值 | 判定 |')
        lines.append('|------|------|------|')
        # 杠杆集中度
        lev_label = '极高' if lev['leverage_ratio'] > 25 else \
                    '偏高' if lev['leverage_ratio'] > 15 else \
                    '正常' if lev['leverage_ratio'] > 5 else \
                    '偏低' if lev['leverage_ratio'] > 0 else '无数据'
        lines.append(f"| 杠杆集中度 | {lev['leverage_ratio']:.2f}% | {lev_label} |")
        # 融资余额
        lines.append(f"| 融资余额 | {format_amount(lev['margin_balance'])} | {lev['balance_trend']} |")
        # 余额/成交额
        flow_label = '高压力' if lev['balance_to_flow'] > 10 else \
                     '中等' if lev['balance_to_flow'] > 3 else '低压力'
        lines.append(f"| 余额/成交额 | {lev['balance_to_flow']:.1f}x | {flow_label} |")
        # 融券
        lines.append(f"| 融券余额 | {lev['short_bal_latest']:,}股 | 5日变化 {lev['short_bal_change']:+.1f}% |")
        lines.append(f"| 融券回补比 | {lev['short_cover_ratio']:.1f}% | {lev['short_cover_signal']} |")
        lines.append(f"| 连续净买入 | {lev['consecutive_net_buy']}日 | - |")
        lines.append('')

        # 近5日杠杆集中度趋势
        if lev.get('leverage_trend') and len(lev['leverage_trend']) > 1:
            lines.append('#### 近5日杠杆集中度趋势')
            lines.append('')
            lines.append('| 日期 | 集中度% | 融资净买 | 融券余额 | 融券变化 |')
            lines.append('|------|--------|---------|---------|---------|')
            detail = margin_result['detail']
            for i, d in enumerate(detail[:5]):
                short_bal = d['short_bal']
                prev_bal = detail[i + 1]['short_bal'] if i + 1 < len(detail) else short_bal
                short_chg = (short_bal - prev_bal) / prev_bal * 100 if prev_bal else 0
                lines.append(f"| {d['date']} | {d['leverage_ratio']:.2f}% | "
                             f"{format_net_amount(d['margin_net'])} | "
                             f"{short_bal:,} | {short_chg:+.1f}% |")
            lines.append('')

        # ── 轧空潜力 ──
        if lev.get('squeeze_score', 0) > 0:
            lines.append('#### 轧空潜力 (Short Squeeze)')
            lines.append('')
            lines.append('> [!info] 轧空(Short Squeeze)发生在空头被迫买入平仓时，推高股价形成正反馈')
            lines.append('>')
            lines.append('> - **SIR (Short Interest Ratio)** = 当前融券余额 / 近5日均值，>120% 表示空头在加速建仓')
            lines.append('> - **Days to Cover** = 融券余额 / 日均融券卖出量，空头全部平仓所需的预估天数')
            lines.append('> - **Squeeze Score** (0~100) 综合考量: 融券余额增长(0~30) + 回补天数(0~30) + 低回补比(0~20) + 融券活跃度(0~20)')
            lines.append('> - 判定: ≥70 HIGH_RISK (高概率轧空) | ≥40 MODERATE | ≥20 LOW | <20 NONE')
            lines.append('')
            sq = lev['squeeze_score']
            sq_label = lev['squeeze_signal']
            sq_bar = '█' * (sq // 5) + '░' * (20 - sq // 5)
            lines.append(f'**Squeeze Score: {sq}/100** {sq_bar} — {sq_label}')
            lines.append('')
            lines.append('| 指标 | 值 | 含义 |')
            lines.append('|------|------|------|')
            sir = lev.get('short_interest_ratio', 0)
            sir_label = '空头加速建仓' if sir > 120 else '正常' if sir > 80 else '空头减仓'
            lines.append(f"| SIR | {sir:.1f}% | {sir_label} |")
            dtc = lev.get('days_to_cover', 0)
            dtc_label = '极难平仓' if dtc > 30 else '较难平仓' if dtc > 15 else '容易平仓' if dtc > 0 else '无融券'
            lines.append(f"| Days to Cover | {dtc:.1f}日 | {dtc_label} |")
            lines.append(f"| 融券余额 | {lev['short_bal_latest']:,}股 | 5日变化 {lev['short_bal_change']:+.1f}% |")
            lines.append(f"| 融券回补比 | {lev['short_cover_ratio']:.1f}% | {'尚未回补' if lev['short_cover_ratio'] < 50 else '已开始回补'} |")
            lines.append('')

    # ─── 市场背景 ───
    if mkt_bg:
        lines.append('### 市场两融背景')
        lines.append('')
        lines.append('| 日期 | 融资趋势 | 融券趋势 | 融资强度% | 融券强度% |')
        lines.append('|------|---------|---------|---------|---------|')
        for mb in mkt_bg:
            lines.append(f"| {mb['date']} | {mb['m_trend']} | {mb['s_trend']} | "
                         f"{mb['m_str'] or '-'} | {mb['s_str'] or '-'} |")
        lines.append('')

    # ─── 行业两融对比 ───
    if ind_margin:
        lines.append('### 行业两融概况')
        lines.append('')
        latest = ind_margin[0]
        lines.append(f'{sector}行业共 {latest["stock_count"]} 只两融标的，'
                     f'行业信号: **{latest["signal"]}**')
        lines.append('')
        lines.append('> [!info] 看多/看空指行业内融资升+融券降(BULLISH) / 融资降+融券升(BEARISH)的股票数，'
                     '高活跃指HIGH_ACTIVE或TOP_50级别，行业信号由看多占比>30%决定')
        lines.append('')
        lines.append('| 日期 | 行业信号 | 看多 | 看空 | 高活跃 | 平均百分位 | 平均融资买入 |')
        lines.append('|------|---------|------|------|--------|-----------|-------------|')
        for im in ind_margin[:5]:
            lines.append(f"| {im['date']} | {im['signal']} | {im['bullish']} | {im['bearish']} | "
                         f"{im['high_active']} | Top {im['avg_pctile']:.0f}% | "
                         f"{format_amount(im['avg_margin_buy'])} |")
        lines.append('')

        # 个股 vs 行业对比
        if margin_result and margin_result.get('ranking'):
            r = margin_result['ranking'][0]
            lines.append('#### 个股 vs 行业')
            lines.append('')
            lines.append('| 项目 | 个股 | 行业均值 |')
            lines.append('|------|------|---------|')
            lines.append(f"| 融资百分位 | Top {r['margin_pctile']:.0f}% | Top {latest['avg_pctile']:.0f}% |")
            lines.append(f"| 市场排名 | {r['rank']} | - |")
            lines.append(f"| 活跃度 | {r['activity']} | 高活跃 {latest['high_active']}/{latest['stock_count']}只 |")
            # 信号对比
            stock_sig = margin_result['signal'].split('(')[0].strip()
            lines.append(f"| 两融信号 | {stock_sig} | {latest['signal']} |")
            lines.append('')

    # ─── 维度6: 综合评价 ───
    if composite:
        lines.append('## 6. 综合评价')
        lines.append('')
        lines.append(f'### 评级: **{composite["grade"]}** (总分 {composite["total"]}/50)')
        lines.append('')
        lines.append('| 维度 | 得分 | 评价 |')
        lines.append('|------|------|------|')
        eval_map = {range(9, 11): '优秀', range(7, 9): '良好', range(5, 7): '中性', range(3, 5): '偏弱'}
        for dim, score in composite['scores'].items():
            ev = '较弱'
            for rng, label in eval_map.items():
                if int(score) in rng:
                    ev = label
                    break
            bar = '█' * int(score) + '░' * (10 - int(score))
            lines.append(f'| {dim} | {score:.1f}/10 {bar} | {ev} |')
        lines.append('')

        if composite['findings']:
            lines.append('### 关键发现')
            lines.append('')
            for f in composite['findings']:
                lines.append(f'- {f}')
            lines.append('')

        # ─── 综合研判 ───
        if composite.get('judgment'):
            lines.append('### 综合研判')
            lines.append('')
            lines.append('> [!note] 量化分析总结')
            lines.append('')
            lines.append('| 维度 | 得分 | 要点 |')
            lines.append('|------|------|------|')
            for dim, score, text in composite['judgment']:
                lines.append(f'| {dim} | {score:.0f}/10 | {text} |')
            lines.append('')

            # 值得注意
            notes = _generate_notes(intraday, historical, sector, volume, margin_result, daily_rows)
            if notes:
                lines.append('**值得注意：**')
                for n in notes:
                    lines.append(f'- {n}')
                lines.append('')

    return '\n'.join(lines)


# ================================================================
#  主流程
# ================================================================

def analyze_one(ch, pg, symbol, date, days=5):
    name, sector = get_stock_info(ch, symbol)
    log.info(f"分析 {symbol} {name} ({sector}) 日期={date}")

    # 额外数据
    daily_rows = fetch_daily(ch, symbol, 20)
    concepts = fetch_concepts(ch, symbol)
    s01_scores = fetch_s01_scores(ch, symbol)
    mkt_bg = fetch_market_background(pg, date)
    ind_margin = fetch_industry_margin(pg, sector, date)

    # 维度1
    today_bars = fetch_5min(ch, symbol, date)
    intraday = analyze_intraday_profile(today_bars)

    # 维度2
    trading_days = get_trading_days(ch, date, days)
    multi_data = fetch_5min_multi_day(ch, symbol, trading_days)
    historical = analyze_historical_comparison(ch, symbol, date, days)

    # 维度3
    sector_comp = analyze_sector_comparison(ch, symbol, date, sector)

    # Beta敏感性
    beta_result = analyze_beta(ch, symbol, date, sector, days)

    # 维度4
    volume = analyze_volume_anomaly(ch, symbol, date, multi_data)

    # 维度5
    # 计算每日成交额 (从5min数据), 供杠杆分析使用
    daily_amounts = {}
    for d, d_bars in multi_data.items():
        daily_amounts[d] = sum(b['amount'] for b in d_bars)
    # 也用5min数据获取更多历史日 (获取近20交易日)
    all_td = get_trading_days(ch, date, 20)
    if len(all_td) > len(trading_days):
        extra_dates = [d for d in all_td if d not in multi_data]
        if extra_dates:
            extra_data = fetch_5min_multi_day(ch, symbol, extra_dates)
            for d, d_bars in extra_data.items():
                daily_amounts[d] = sum(b['amount'] for b in d_bars)
    margin_result = analyze_margin(pg, symbol, date, 20, daily_amounts)

    # 维度6
    composite = analyze_composite(intraday, historical, sector_comp, volume, margin_result)

    # 生成报告
    report = generate_report(symbol, name, sector, date,
                             daily_rows, concepts, s01_scores, mkt_bg, ind_margin,
                             intraday, historical, sector_comp, volume, margin_result, composite,
                             beta_result)

    # 保存
    output_dir = Path(VAULT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_file = output_dir / f"{symbol}_{name}_{date}_个股分析.md"
    report_file.write_text(report, encoding='utf-8')
    log.info(f"报告已保存: {report_file}")
    return report_file


def main():
    parser = argparse.ArgumentParser(description='个股多维度深度分析')
    parser.add_argument('symbols', nargs='+', help='股票代码 (如 sh600418)')
    parser.add_argument('--date', help='分析日期 (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, default=5, help='多日对比天数 (默认5)')
    args = parser.parse_args()

    load_env()
    ch = get_ch()
    pg = get_pg()

    date = args.date or get_latest_date(ch)
    if not date:
        log.error("无法确定交易日期")
        sys.exit(1)

    log.info(f"分析日期: {date}, 对比天数: {args.days}")

    for symbol in args.symbols:
        try:
            analyze_one(ch, pg, symbol, date, args.days)
        except Exception as e:
            log.error(f"分析 {symbol} 失败: {e}")
            import traceback
            traceback.print_exc()

    pg.close()
    log.info("Done.")


if __name__ == '__main__':
    main()
