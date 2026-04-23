#!/usr/bin/env python3
"""
高阶因子策略计算脚本 (S09/S10/S12)

S09 — 黄金时段爆发检测: 10:30-11:30 P99拉升 + 30分钟不回落 + 放量 + 早盘抗跌
S10 — 早盘抗跌+午后修复: 时段反转效应选股
S12 — 量能确认+两融验证: 成交额异常 + 融资/融券变化

用法:
  python calc_advanced_score.py [日期] --strategy S09|S10|S12|all
  python calc_advanced_score.py 2026-04-17 --strategy S09
"""

import os
import re
import sys
import json
import argparse
import logging
from pathlib import Path

import psycopg2
from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent / '00.shared' / 'config'

A_SHARE_FILTER = "(symbol LIKE 'sh6%' OR symbol LIKE 'sz0%' OR symbol LIKE 'sz3%' OR symbol LIKE 'bj%')"


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
    """CH symbol 'sz002828' → PG ts_code '002828'"""
    return re.sub(r'^(sh|sz|bj)', '', symbol)


def dedup_base(date_cond):
    """去重基础子查询"""
    return f"""
    SELECT symbol, datetime, toHour(datetime) as h, toMinute(datetime) as min5,
        open, high, low, close, volume, amount,
        row_number() OVER (PARTITION BY symbol, datetime ORDER BY datetime) as rn,
        (close - open) / nullIf(open, 0) * 100 as ret
    FROM raw_stocks_5min
    WHERE toDate(datetime) {date_cond}
      AND {A_SHARE_FILTER}
      AND toHour(datetime) >= 9 AND toHour(datetime) <= 15
      AND toMinute(datetime) > 0
    """


# ================================================================
#  S09 — 黄金时段爆发检测
# ================================================================

def calc_s09(ch, date):
    """S09: 黄金时段爆发检测 — Python后处理 + 两融趋势确认"""
    dc = f"= '{date}'"
    log.info("S09: 黄金时段爆发检测 (增强版)")

    # Step 1: 获取全量5min数据，按symbol分组
    raw = ch.execute(f"""
    SELECT symbol, datetime, h, min5, open, high, low, close, volume, amount, ret
    FROM ({dedup_base(dc)}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20
    ORDER BY symbol, datetime
    """)

    # 按symbol分组
    from collections import defaultdict
    stocks = defaultdict(list)
    for r in raw:
        stocks[r[0]].append({
            'datetime': r[1], 'h': r[2], 'min5': r[3],
            'open': r[4], 'high': r[5], 'low': r[6], 'close': r[7],
            'volume': r[8], 'amount': r[9], 'ret': r[10],
        })

    # 全市场10:30-11:30平均成交额
    all_1030_amounts = []
    for bars in stocks.values():
        for b in bars:
            if (b['h'] == 10 and b['min5'] > 30) or b['h'] == 11:
                all_1030_amounts.append(b['amount'])
    import statistics
    avg_1030_amount = statistics.mean(all_1030_amounts) if all_1030_amounts else 0

    # Step 2: 从PG获取融资趋势
    pg = get_pg()
    cur = pg.cursor()
    cur.execute("""
    SELECT ts_code, margin_trend, short_trend
    FROM margin.stock_margin_ranking
    WHERE trade_date = (SELECT MAX(trade_date) FROM margin.stock_margin_ranking WHERE trade_date <= %s)
    """, [date])
    margin_trends = {r[0]: {'margin_trend': r[1], 'short_trend': r[2]} for r in cur.fetchall()}
    cur.close()
    pg.close()

    results = []
    for sym, bars in stocks.items():
        # 找10:30-11:30中ret > 1.4%的K线
        spikes = []
        for i, b in enumerate(bars):
            if ((b['h'] == 10 and b['min5'] > 30) or b['h'] == 11) and b['ret'] > 1.4:
                spikes.append((i, b))

        if not spikes:
            continue

        # 取最强的spike
        best_spike_idx, best_spike = max(spikes, key=lambda x: x[1]['ret'])
        mid_price = (best_spike['high'] + best_spike['low']) / 2

        # *** 否决条件: spike发生在14:00之后 → 跳过 ***
        if best_spike['h'] >= 14:
            continue

        # 检查后续6根K线 (持久性)
        hold_count = 0
        for j in range(best_spike_idx + 1, min(best_spike_idx + 7, len(bars))):
            if bars[j]['close'] > mid_price:
                hold_count += 1

        # 早盘收益 (09:35-10:00)
        am_bars = [b for b in bars if b['h'] == 9 and b['min5'] >= 35]
        am_ret = statistics.mean([b['ret'] for b in am_bars]) if am_bars else 0

        # *** 新增: 13:30-14:00 最强时段维持 ***
        pm_strong_bars = [b for b in bars if (b['h'] == 13 and b['min5'] > 30) or (b['h'] == 14 and b['min5'] <= 30)]
        pm_strong_hold = False
        if pm_strong_bars:
            pm_above_mid = sum(1 for b in pm_strong_bars if b['close'] > mid_price)
            pm_strong_hold = pm_above_mid >= len(pm_strong_bars) * 0.6  # 60%以上K线高于中轴

        # *** 新增: 融资趋势确认 ***
        ts = symbol_to_tscode(sym)
        trend_info = margin_trends.get(ts, {})
        margin_trend = trend_info.get('margin_trend', '')
        short_trend = trend_info.get('short_trend', '')
        margin_confirmed = margin_trend == 'INCREASING' and short_trend != 'INCREASING'

        # 评分 (0-10)
        score = 0
        score += 1  # P99触发 (已过滤)
        score += 2 if hold_count >= 6 else (1 if hold_count >= 4 else 0)
        score += 1 if best_spike['amount'] > avg_1030_amount * 2 else 0
        score += 1 if am_ret >= 0 else 0
        score += 2 if pm_strong_hold else 0  # 13:30-14:00维持
        score += 1 if margin_confirmed else 0  # 融资趋势确认

        if score < 3:
            continue

        metrics = {
            "spike_time": str(best_spike['datetime']),
            "spike_ret": round(best_spike['ret'], 4),
            "spike_amount": best_spike['amount'],
            "avg_1030_amount": round(avg_1030_amount, 0),
            "am_ret": round(am_ret, 5),
            "hold_count": hold_count,
            "mid_price": round(mid_price, 2),
            "pm_strong_hold": pm_strong_hold,
            "margin_trend": margin_trend,
            "short_trend": short_trend,
        }
        results.append((sym, score, metrics))

    # 获取名称和行业
    if not results:
        return []
    info = get_stock_info(ch, [r[0] for r in results])
    return [
        {'symbol': sym, 'name': info.get(sym, ('?', '?'))[0],
         'sector': info.get(sym, ('?', '?'))[1],
         'score': score, 'raw_metrics': json.dumps(metrics, ensure_ascii=False)}
        for sym, score, metrics in results
    ]


# ================================================================
#  S10 — 早盘抗跌+午后修复
# ================================================================

def calc_s10(ch, date):
    """S10: 早盘抗跌+午后修复 + 融券平仓信号"""
    dc = f"= '{date}'"
    log.info("S10: 早盘抗跌+午后修复 (增强版)")
    import statistics
    from collections import defaultdict

    # Step 1: 获取全量5min数据
    raw = ch.execute(f"""
    SELECT symbol, datetime, h, min5, open, high, low, close, volume, amount, ret
    FROM ({dedup_base(dc)}) sub
    WHERE rn = 1 AND ret BETWEEN -20 AND 20
    ORDER BY symbol, datetime
    """)
    log.info(f"S10: 获取 {len(raw)} 条5min数据")

    # 按symbol分组
    stocks = defaultdict(list)
    for r in raw:
        stocks[r[0]].append({
            'datetime': r[1], 'h': r[2], 'min5': r[3],
            'open': r[4], 'high': r[5], 'low': r[6], 'close': r[7],
            'volume': r[8], 'amount': r[9], 'ret': r[10],
        })

    # 全市场平均收益
    all_rets = [b['ret'] for bars in stocks.values() for b in bars]
    mkt_avg = statistics.mean(all_rets) if all_rets else 0
    is_weak = mkt_avg < -0.02

    # Step 2: 获取S01得分
    s01_rows = ch.execute(f"""
    SELECT symbol, score, sector FROM independence_score_daily WHERE date = '{date}'
    """)
    s01_data = {r[0]: {'score': r[1], 'sector': r[2]} for r in s01_rows}
    s01_scores = [r[1] for r in s01_rows if r[1]]
    s01_th = sorted(s01_scores)[int(len(s01_scores) * 0.7)] if len(s01_scores) > 3 else 0

    # Step 3: 计算每只股票的早盘和午后表现
    results = []
    for sym, bars in stocks.items():
        # 早盘: 09:35-10:00 (h=9, min5>=35)
        am_bars = [b for b in bars if b['h'] == 9 and b['min5'] >= 35]
        # 午后: 13:30-14:00 (h=13, min5>30) + (h=14, min5<=30)
        pm_bars = [b for b in bars if (b['h'] == 13 and b['min5'] > 30) or (b['h'] == 14 and b['min5'] <= 30)]

        if len(am_bars) < 3 or len(pm_bars) < 3:
            continue

        am_avg = statistics.mean([b['ret'] for b in am_bars])
        pm_avg = statistics.mean([b['ret'] for b in pm_bars])

        # 午后必须正收益
        if pm_avg <= 0:
            continue

        # 早盘分组
        if am_avg >= 0:
            am_group = 'resistant'
        elif am_avg >= -0.05:
            am_group = 'slight_down'
        elif am_avg >= -0.15:
            am_group = 'moderate_down'
        else:
            am_group = 'deep_down'

        # 评分
        repair_ratio = pm_avg / abs(am_avg) if am_avg != 0 else 0
        score = 0
        group_score = {'resistant': 40, 'slight_down': 30, 'moderate_down': 20, 'deep_down': 10}
        score += group_score.get(am_group, 0)
        score += min(repair_ratio * 30, 30)
        score += min(pm_avg * 500, 20)

        s01_info = s01_data.get(sym, {})
        s01_score = s01_info.get('score')
        sector = s01_info.get('sector', '?')
        if s01_score and s01_th > 0 and s01_score >= s01_th:
            score += 10

        metrics = {
            "am_avg": round(am_avg, 5),
            "pm_avg": round(pm_avg, 5),
            "am_group": am_group,
            "repair_ratio": round(repair_ratio, 4),
            "s01_score": round(s01_score, 2) if s01_score else None,
            "market_avg": round(mkt_avg, 5),
            "is_weak_day": is_weak,
        }
        results.append((sym, score, metrics, sector))

    if not results:
        return []

    # Step 4: 从PG获取融券平仓数据 (分市场)
    pg = get_pg()
    cur = pg.cursor()

    # 批量获取当日融券平仓数据
    syms_ts = {r[0]: symbol_to_tscode(r[0]) for r in results}
    ts_codes = list(syms_ts.values())
    placeholders = ','.join(['%s'] * len(ts_codes))

    cur.execute(f"""
    SELECT ts_code, short_repay, short_sell_volume, margin_buy_amount
    FROM margin.margin_trading_detail_combined
    WHERE trade_date = %s AND ts_code IN ({placeholders})
    """, [date] + ts_codes)
    margin_data = {}
    for r in cur.fetchall():
        margin_data[r[0]] = {
            'short_repay': r[1] or 0,
            'short_sell': r[2] or 0,
            'margin_buy': r[3] or 0,
        }

    cur.close()
    pg.close()

    # 批量获取名称 (分批，每批200)
    all_syms = [r[0] for r in results]
    info = {}
    for i in range(0, len(all_syms), 200):
        batch = all_syms[i:i+200]
        info.update(get_stock_info(ch, batch))
    output = []
    for sym, score, metrics, sector in results:
        name, sec = info.get(sym, ('?', sector or '?'))
        ts = syms_ts[sym]
        md = margin_data.get(ts, {})

        short_repay = md.get('short_repay', 0)
        short_sell = md.get('short_sell', 0)

        # 空头回补信号: short_repay > short_sell_volume
        if short_repay > 0 and short_sell > 0 and short_repay > short_sell:
            score += 15  # 空头回补加分
            metrics["short_cover_signal"] = True
        else:
            metrics["short_cover_signal"] = False

        # 空头回补比率
        if short_sell > 0:
            cover_ratio = short_repay / short_sell
            if cover_ratio > 1:
                score += 10  # 平仓 > 新增
            metrics["short_cover_ratio"] = round(cover_ratio, 2)
        else:
            metrics["short_cover_ratio"] = None

        metrics["short_repay"] = short_repay
        metrics["short_sell"] = short_sell

        output.append({
            'symbol': sym, 'name': name, 'sector': sec or sector or '?',
            'score': round(score, 1), 'raw_metrics': json.dumps(metrics, ensure_ascii=False),
        })

    output.sort(key=lambda x: x['score'], reverse=True)
    return output


# ================================================================
#  S12 — 量能确认+两融验证
# ================================================================

def calc_s12(ch, date):
    """S12: 量能确认+两融验证 — 利用stock_margin_ranking预计算排名"""
    dc = f"= '{date}'"
    log.info("S12: 量能确认+两融验证 (增强版)")

    # 1. 从ClickHouse获取个股10:30-11:30成交额占比
    amount_rows = ch.execute(f"""
    SELECT symbol, amount_1030, total_amount,
        round(amount_1030 / nullIf(total_amount, 0) * 100, 2) as pct_1030
    FROM (
        SELECT symbol,
            sum(CASE WHEN (h = 10 AND min5 > 30) OR h = 11 THEN amount ELSE 0 END) as amount_1030,
            sum(amount) as total_amount
        FROM ({dedup_base(dc)}) sub
        WHERE rn = 1
        GROUP BY symbol
        HAVING total_amount > 0
    )
    ORDER BY pct_1030 DESC
    """)

    if not amount_rows:
        return []

    import statistics
    pcts = [r[3] for r in amount_rows if r[3] is not None]
    if not pcts:
        return []
    avg_pct = statistics.mean(pcts)
    std_pct = statistics.stdev(pcts) if len(pcts) > 1 else 1

    # 筛选成交额异常的股票
    threshold = avg_pct + 1.5 * std_pct
    abnormal = {r[0]: r[3] for r in amount_rows if r[3] and r[3] > threshold}

    if not abnormal:
        log.info("S12: 无成交额异常股票，放宽到 avg + 1σ")
        threshold = avg_pct + 1 * std_pct
        abnormal = {r[0]: r[3] for r in amount_rows if r[3] and r[3] > threshold}

    if not abnormal:
        log.info("S12: 仍无异常股票")
        return []

    # 2. 从PG获取stock_margin_ranking预计算数据
    pg = get_pg()
    cur = pg.cursor()
    symbols_ts = {sym: symbol_to_tscode(sym) for sym in abnormal}
    ts_codes = list(symbols_ts.values())
    placeholders = ','.join(['%s'] * len(ts_codes))

    # 利用预计算排名
    cur.execute(f"""
    SELECT ts_code, name, exchange, margin_trend, short_trend,
        margin_percentile, short_change, activity_level,
        margin_buy_amount, short_sell_volume, short_balance_volume
    FROM margin.stock_margin_ranking
    WHERE trade_date = (SELECT MAX(trade_date) FROM margin.stock_margin_ranking WHERE trade_date <= %s) AND ts_code IN ({placeholders})
    """, [date] + ts_codes)
    ranking_data = {}
    for r in cur.fetchall():
        ranking_data[r[0]] = {
            'name': r[1], 'exchange': r[2],
            'margin_trend': r[3], 'short_trend': r[4],
            'margin_pctile': float(r[5] or 0),
            'short_change': int(r[6] or 0),
            'activity': r[7],
            'buy_amt': int(r[8] or 0),
            'short_sell': int(r[9] or 0),
            'short_bal': int(r[10] or 0),
        }

    # 分市场获取 short_repay (融券平仓)
    cur.execute(f"""
    SELECT ts_code, short_repay, margin_repay
    FROM margin.margin_trading_detail_combined
    WHERE trade_date = (SELECT MAX(trade_date) FROM margin.margin_trading_detail_combined WHERE trade_date <= %s) AND ts_code IN ({placeholders})
    """, [date] + ts_codes)
    repay_data = {r[0]: {'short_repay': r[1] or 0, 'margin_repay': r[2] or 0} for r in cur.fetchall()}

    cur.close()
    pg.close()

    # 3. 综合评分
    results = []
    for sym, pct_1030 in abnormal.items():
        ts = symbols_ts[sym]
        rank = ranking_data.get(ts, {})
        repay = repay_data.get(ts, {})
        name = rank.get('name', '?')

        if not rank:
            continue

        margin_trend = rank.get('margin_trend', '')
        short_trend = rank.get('short_trend', '')

        # *** 剔除诱多: 融资下降 + 融券上升 ***
        if margin_trend == 'DECREASING' and short_trend == 'INCREASING':
            continue

        score = 0
        metrics = {
            "pct_1030": pct_1030,
            "exchange": rank.get('exchange', '?'),
        }

        # 成交额异常加分 (最高30)
        score += min((pct_1030 - avg_pct) / std_pct * 10, 30) if std_pct > 0 else 0
        metrics["amount_zscore"] = round((pct_1030 - avg_pct) / std_pct, 2) if std_pct > 0 else 0

        # 融资趋势 INCREASING (+20)
        if margin_trend == 'INCREASING':
            score += 20
        metrics["margin_trend"] = margin_trend

        # 融券趋势 DECREASING (+20, 空方退潮)
        if short_trend == 'DECREASING':
            score += 20
        metrics["short_trend"] = short_trend

        # 百分位 Top 10% (+10)
        margin_pctile = rank.get('margin_pctile', 100)
        if margin_pctile < 10:
            score += 10
        metrics["margin_percentile"] = margin_pctile

        # 空头平仓 > 新增卖空 (+20)
        short_repay = repay.get('short_repay', 0)
        short_sell = rank.get('short_sell', 0)
        if short_repay > 0 and short_sell > 0 and short_repay > short_sell:
            score += 20
            metrics["short_cover"] = True
        else:
            metrics["short_cover"] = False
        metrics["short_repay"] = short_repay
        metrics["short_sell"] = short_sell

        metrics["activity_level"] = rank.get('activity', '?')
        metrics["short_change"] = rank.get('short_change', 0)

        # 获取行业
        results.append({
            'symbol': sym, 'name': name, 'sector': '?',
            'score': round(score, 1), 'raw_metrics': json.dumps(metrics, ensure_ascii=False),
        })

    # 批量获取名称和行业
    if results:
        info = get_stock_info(ch, [r['symbol'] for r in results])
        for r in results:
            ch_name, sector = info.get(r['symbol'], (r['name'], '?'))
            if ch_name:
                r['name'] = ch_name
            r['sector'] = sector if sector else '?'

    results.sort(key=lambda x: x['score'], reverse=True)
    return results


# ================================================================
#  辅助函数
# ================================================================

def get_stock_info(ch, symbols):
    """批量获取股票名称和行业"""
    if not symbols:
        return {}
    # gtja_stock_names.symbol 带 sh/sz/bj 前缀，直接匹配
    # stock_industry_mapping.symbol 不带前缀，需要去前缀匹配
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


# ================================================================
#  S13 — 三步联评工作流
# ================================================================

def calc_s13(ch, date):
    """S13: 三步联评 — 量能筛选 → 强度过滤 → 两融定案"""
    log.info("S13: 三步联评工作流")
    import statistics

    # === 第一步: 量能筛选 (从S09取14:00前的spike标的) ===
    s09_rows = ch.execute(f"""
    SELECT symbol, score, raw_metrics
    FROM independence_score_advanced
    WHERE date = '{date}' AND strategy = 'S09'
    """)
    # 解析metrics，过滤spike_time < 14:00的 (S09已过滤，但二次确认)
    s09_candidates = {}
    for r in s09_rows:
        try:
            m = json.loads(r[2])
            spike_time = m.get('spike_time', '')
            # S09已过滤14:00后的，这里取score>=4的作为强信号
            if r[1] >= 4:
                s09_candidates[r[0]] = {'score': r[1], 'metrics': m}
        except (json.JSONDecodeError, TypeError):
            continue

    log.info(f"S13 第一步: S09量能筛选 {len(s09_candidates)} 只 (score>=4)")
    if not s09_candidates:
        log.info("S13: 第一步无候选")
        return []

    # === 第二步: 强度过滤 (S11周频一致性) ===
    s11_rows = ch.execute("""
    SELECT symbol, appear_days, consistency_score
    FROM independence_score_weekly
    WHERE week_end = %(d)s
    """, {'d': date})
    s11_set = {r[0]: {'appear_days': r[1], 'consistency': r[2]} for r in s11_rows}

    step2_pass = {}
    step2_nomatch = {}
    for sym, info in s09_candidates.items():
        if sym in s11_set:
            step2_pass[sym] = {**info, **s11_set[sym]}
        else:
            step2_nomatch[sym] = info

    log.info(f"S13 第二步: S11强度过滤 {len(step2_pass)} 只匹配, {len(step2_nomatch)} 只无周频数据")

    # === 第三步: 两融定案 ===
    pg = get_pg()
    cur = pg.cursor()

    # 获取所有候选的两融趋势
    all_candidates = {**step2_pass, **step2_nomatch}
    syms_ts = {sym: symbol_to_tscode(sym) for sym in all_candidates}
    ts_codes = list(syms_ts.values())
    if not ts_codes:
        cur.close()
        pg.close()
        return []

    placeholders = ','.join(['%s'] * len(ts_codes))

    cur.execute(f"""
    SELECT ts_code, name, margin_trend, short_trend, margin_percentile,
        margin_buy_amount, short_sell_volume, short_change, activity_level
    FROM margin.stock_margin_ranking
    WHERE trade_date = %s AND ts_code IN ({placeholders})
    """, [date] + ts_codes)
    ranking = {}
    for r in cur.fetchall():
        ranking[r[0]] = {
            'name': r[1], 'margin_trend': r[2], 'short_trend': r[3],
            'margin_pctile': float(r[4] or 0),
            'buy_amt': int(r[5] or 0), 'short_sell': int(r[6] or 0),
            'short_change': int(r[7] or 0), 'activity': r[8],
        }

    cur.close()
    pg.close()

    # 综合评分
    results = []
    for sym, info in all_candidates.items():
        ts = syms_ts[sym]
        rank = ranking.get(ts, {})

        score = 0
        metrics = {}

        # 第一步加分: S09入选 (+30)
        s09_score = info.get('score', 0)
        score += 30
        metrics["s09_score"] = s09_score
        metrics["spike_ret"] = info.get('metrics', {}).get('spike_ret')
        metrics["pm_strong_hold"] = info.get('metrics', {}).get('pm_strong_hold')

        # 第二步加分: S11周频入选 (+30)
        if sym in s11_set:
            score += 30
            metrics["s11_appear_days"] = s11_set[sym]['appear_days']
            metrics["s11_consistency"] = s11_set[sym]['consistency']
        else:
            metrics["s11_appear_days"] = 0

        # 第三步加分: 两融共振 (+40)
        margin_trend = rank.get('margin_trend', '')
        short_trend = rank.get('short_trend', '')

        if margin_trend == 'INCREASING' and short_trend == 'DECREASING':
            score += 40  # 完美共振
            metrics["margin_resonance"] = "BULLISH"
        elif margin_trend == 'INCREASING':
            score += 20
            metrics["margin_resonance"] = "MARGIN_UP"
        elif short_trend == 'DECREASING':
            score += 15
            metrics["margin_resonance"] = "SHORT_DOWN"
        else:
            metrics["margin_resonance"] = "NEUTRAL"

        # 诱多剔除
        if margin_trend == 'DECREASING' and short_trend == 'INCREASING':
            score -= 20  # 降分而非完全剔除
            metrics["margin_resonance"] = "TRAP"

        metrics["margin_trend"] = margin_trend
        metrics["short_trend"] = short_trend
        metrics["margin_pctile"] = rank.get('margin_pctile', 0)
        metrics["activity_level"] = rank.get('activity', '?')

        # 获取名称和行业
        name = rank.get('name', '?')
        results.append({
            'symbol': sym, 'name': name, 'sector': '?',
            'score': round(score, 1),
            'raw_metrics': json.dumps(metrics, ensure_ascii=False),
        })

    # 批量获取名称和行业
    if results:
        info = get_stock_info(ch, [r['symbol'] for r in results])
        for r in results:
            ch_name, sector = info.get(r['symbol'], (r['name'], '?'))
            if ch_name:
                r['name'] = ch_name
            r['sector'] = sector if sector else '?'

    results.sort(key=lambda x: x['score'], reverse=True)
    log.info(f"S13: {len(results)} 只通过三步联评, 最高分: {results[0]['score'] if results else 0}")
    return results


def save_results(ch, date, strategy, results):
    """保存结果到ClickHouse"""
    if not results:
        log.info(f"{strategy}: 无结果，跳过保存")
        return

    from datetime import date as date_cls
    if isinstance(date, str):
        date = date_cls(*[int(x) for x in date.split('-')])

    # 添加排名
    for i, r in enumerate(results, 1):
        r['rank'] = i

    ch.execute(f"ALTER TABLE independence_score_advanced DELETE WHERE date = '{date}' AND strategy = '{strategy}'")

    data = [
        (date, r['symbol'], r['name'], r['sector'], strategy,
         r['score'], r['raw_metrics'], r.get('rank', 0))
        for r in results
    ]
    ch.execute(
        "INSERT INTO independence_score_advanced (date, symbol, name, sector, strategy, score, raw_metrics, rank) VALUES",
        data
    )
    log.info(f"{strategy}: 保存 {len(data)} 条结果")


# ================================================================
#  主流程
# ================================================================

def main():
    parser = argparse.ArgumentParser(description='高阶因子策略计算')
    parser.add_argument('date', nargs='?', help='目标日期 (YYYY-MM-DD)')
    parser.add_argument('--strategy', default='all', choices=['S09', 'S10', 'S12', 'S13', 'all'],
                        help='策略选择')
    args = parser.parse_args()

    load_env()
    ch = get_ch()

    if args.date:
        date = args.date
    else:
        row = ch.execute("SELECT max(toDate(datetime)) FROM raw_stocks_5min")
        date = row[0][0].strftime('%Y-%m-%d') if row else None
        if not date:
            log.error("Cannot determine trading date")
            sys.exit(1)

    log.info(f"Target date: {date}, strategy: {args.strategy}")

    strategies = ['S09', 'S10', 'S12', 'S13'] if args.strategy == 'all' else [args.strategy]

    for s in strategies:
        if s == 'S09':
            results = calc_s09(ch, date)
        elif s == 'S10':
            results = calc_s10(ch, date)
        elif s == 'S12':
            results = calc_s12(ch, date)
        elif s == 'S13':
            results = calc_s13(ch, date)
        else:
            continue
        save_results(ch, date, s, results)
        if results:
            log.info(f"{s} Top 3: {[(r['symbol'], r['name'], r['score']) for r in results[:3]]}")

    log.info("Done.")


if __name__ == '__main__':
    main()
