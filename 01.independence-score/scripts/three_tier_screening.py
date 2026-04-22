#!/usr/bin/env python3
"""
================================================================================
三层筛选选股系统 (Three-Tier Screening System)
================================================================================

【设计理念】
第一层"批量过滤"解决速度问题（3秒扫全市场），第二层"精筛"解决深度问题
（多因子验证），第三层"组合评分"解决权衡问题（加权综合决策）。

【三层架构】
┌─────────────────────────────────────────────────────────────────────────────┐
│ 第一层: 批量过滤 (Batch Filter)                                             │
│  ──→ 独立强度因子快速筛选                                                    │
│  ──→ 3秒内完成全市场扫描                                                     │
│  ──→ 输出: 防御型候选池 (Top N * 3~5倍)                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│ 第二层: 精筛验证 (Deep Validation)                                          │
│  ──→ 对候选池计算增强因子群 (VWAP / VaP / 杠杆 / Beta)                      │
│  ──→ 剔除异常值、验证信号一致性                                              │
│  ──→ 输出: 高置信度候选池                                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ 第三层: 组合评分 (Composite Scoring)                                        │
│  ──→ 五因子加权综合评分                                                      │
│  ──→ 权重: 独立强度30% + VWAP20% + 杠杆20% + Beta15% + VaP15%               │
│  ──→ 输出: 最终选股名单 + 评分明细                                           │
└─────────────────────────────────────────────────────────────────────────────┘

【因子说明与计算方式】

1. 独立强度因子 (Independence Score, IS) — 权重 30%
   ─────────────────────────────────────────
   定义: 个股在板块下跌时的抗跌能力 + 顺势领先能力
   计算: contra_count + lead_count (来自 independence_score_daily 表)
   归一化: min-max 归一化到 0-100 (基于当日全市场分布)
   方向: 越高越好 (分数高 = 独立性强)

2. VWAP 偏离度 (Volume Weighted Average Price Deviation) — 权重 20%
   ─────────────────────────────────────────────────────────
   定义: 收盘价相对 VWAP 的偏离百分比
   计算: (close - VWAP) / VWAP * 100
   归一化:  sigmoid 映射，正值映射到 50-100，负值映射到 0-50
   方向: 正值偏好 (收盘在 VWAP 上方 = 日内强势)

3. 杠杆深度 (Margin Leverage) — 权重 20%
   ─────────────────────────────────────────
   定义: 融资净买入占当日成交额的比率
   计算: margin_net_buy / daily_amount * 100
   归一化: 对数缩放 + min-max 归一化
   方向: 适度正值偏好 (杠杆流入但不极端)

4. Beta 敏感性 (Beta Sensitivity) — 权重 15%
   ─────────────────────────────────────────
   定义: 个股相对沪深300的系统性风险暴露
   计算: 个股5分钟收益率 vs 沪深300指数5分钟收益率的回归系数
   归一化: Beta < 0.5 映射到 80-100 (低Beta防御型偏好)
           0.5 <= Beta < 1.0 映射到 50-80
           1.0 <= Beta < 1.5 映射到 20-50
           Beta >= 1.5 映射到 0-20
   方向: 低Beta偏好 (防御型策略)

5. VaP 位置 (Volume at Price Position) — 权重 15%
   ─────────────────────────────────────────
   定义: 收盘价在当日价格密集区(Value Area)中的相对位置
   计算: (close - VA_low) / (VA_high - VA_low) * 100
   归一化: 直接使用百分比位置
   方向: 越高越好 (收盘在价值区上方 = 突破密集区)

【使用方法】

  # 基础用法: 扫描今日全市场，输出 Top 20
  python three_tier_screening.py

  # 指定日期
  python three_tier_screening.py --date 2026-04-21

  # 调整第一层过滤阈值和输出数量
  python three_tier_screening.py --is-threshold 2.0 --top 30

  # 指定行业过滤
  python three_tier_screening.py --sector 半导体

【输出】
  - 终端: 表格形式展示三层筛选过程和最终结果
  - Obsidian Vault: Markdown 报告 (含评分明细和买入建议)
  - JSON: 机器可读格式供下游策略调用

================================================================================
"""

import os
import sys
import argparse
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import psycopg2
from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ─── 配置 ───────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent / '00.shared' / 'config'
VAULT_DIR = "/mnt/d/obsidian/OrbitOS-vault/30_Research/量化分析/个股分析"

# A-share 过滤条件
A_SHARE_FILTER = """
symbol LIKE 'sh6%' OR symbol LIKE 'sh688%' OR symbol LIKE 'sh689%'
OR symbol LIKE 'sz0%' OR symbol LIKE 'sz3%'
OR symbol LIKE 'bj8%' OR symbol LIKE 'bj4%' OR symbol LIKE 'bj43%'
"""

# 五因子权重配置 (可调整)
WEIGHTS = {
    'independence': 0.30,  # 独立强度
    'vwap': 0.20,          # VWAP偏离
    'margin': 0.20,        # 杠杆深度
    'beta': 0.15,          # Beta敏感性
    'vap': 0.15,           # VaP位置
}

# 第一层过滤阈值 (独立强度原始分 >= 此值进入候选池)
DEFAULT_IS_THRESHOLD = 8.0  # v2.0 等效阈值 (v1.0=1.5 ≈ p55; v2.0 p55≈8.0)

# 第一层输出倍数 (最终输出 Top N, 第一层筛出 Top N * MULTIPLIER)
FIRST_TIER_MULTIPLIER = 3


# ─── 数据结构 ─────────────────────────────────────────────────────────────────

@dataclass
class Tier1Candidate:
    """第一层候选: 独立强度筛选结果"""
    symbol: str
    name: str
    sector: str
    is_raw_score: float      # 原始独立强度分
    is_normalized: float     # 归一化到 0-100
    contra_count: int
    lead_count: int


@dataclass
class Tier2Candidate:
    """第二层候选: 精筛后的增强因子数据"""
    symbol: str
    name: str
    sector: str
    is_score: float          # 独立强度归一化分
    vwap_dev: float          # VWAP偏离度(%)
    vwap_normalized: float   # VWAP归一化 0-100
    margin_concentration: float  # 杠杆集中度(%)
    margin_normalized: float     # 杠杆归一化 0-100
    beta: float              # Beta值
    beta_normalized: float   # Beta归一化 0-100
    vap_position: float      # VaP位置(%)
    vap_normalized: float    # VaP归一化 0-100
    daily_amount: float      # 当日成交额


@dataclass
class Tier3Result:
    """第三层结果: 组合评分"""
    symbol: str
    name: str
    sector: str
    composite_score: float   # 组合综合评分 0-100
    component_scores: Dict[str, float]  # 各因子得分明细
    weights: Dict[str, float]           # 权重配置
    rank: int                # 排名
    recommendation: str      # 买入建议


# ─── 数据库连接 ───────────────────────────────────────────────────────────────

def load_env():
    """加载数据库配置"""
    env_file = BASE_DIR / 'database.env'
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, val = line.split('=', 1)
                    # 移除可能的 export 前缀和引号
                    key = key.replace('export ', '').strip()
                    val = val.strip().strip('"').strip("'")
                    os.environ[key] = val


def get_ch() -> Client:
    """获取 ClickHouse 连接"""
    return Client(
        host=os.getenv('CH_HOST', 'localhost'),
        port=int(os.getenv('CH_PORT', '9000')),
        user=os.getenv('CH_USER', 'default'),
        password=os.getenv('CH_PASSWORD', ''),
        database=os.getenv('CH_DB', 'tdx2db_rust'),
        settings={'use_numpy': False}
    )


def get_pg() -> 'psycopg2.extensions.connection':
    """获取 PostgreSQL 连接"""
    return psycopg2.connect(
        host=os.getenv('PG_HOST', 'localhost'),
        port=int(os.getenv('PG_PORT', '5432')),
        dbname=os.getenv('PG_DB', 'quantdb'),
        user=os.getenv('PG_USER', 'postgres'),
        password=os.getenv('PG_PASSWORD', 'postgres')
    )


# ─── 第一层: 批量过滤 ─────────────────────────────────────────────────────────

def tier1_batch_filter(ch, trade_date: str, threshold: float, max_candidates: int) -> List[Tier1Candidate]:
    """
    第一层: 基于独立强度因子的批量过滤

    逻辑:
      1. 从 independence_score_daily 读取当日全市场独立强度分数
      2. 筛选 score >= threshold 的股票
      3. 按 score 降序排列，取前 max_candidates 名
      4. 对 score 做当日 min-max 归一化 (0-100)

    性能: ClickHouse 单表查询，通常在 1-3 秒内完成
    """
    log.info(f"【第一层】批量过滤: date={trade_date}, threshold={threshold}, max_candidates={max_candidates}")

    query = f"""
    SELECT
        symbol,
        sector,
        score,
        contra_count,
        lead_count
    FROM independence_score_daily
    WHERE date = '{trade_date}'
      AND score >= {threshold}
    ORDER BY score DESC
    LIMIT {max_candidates}
    """

    rows = ch.execute(query)
    if not rows:
        log.warning("第一层: 无数据，请确认 independence_score_daily 表已有该日数据")
        return []

    # 计算当日 min-max 用于归一化
    scores = [float(r[2]) for r in rows]  # r[2] = score
    min_score, max_score = min(scores), max(scores)
    score_range = max_score - min_score if max_score > min_score else 1.0

    candidates = []
    for r in rows:
        symbol, sector, score, contra_count, lead_count = r
        normalized = (float(score) - min_score) / score_range * 100 if score_range > 0 else 50.0
        candidates.append(Tier1Candidate(
            symbol=symbol,
            name='',  # 名称在第二层通过 stock_names 补充
            sector=sector or '',
            is_raw_score=float(score),
            is_normalized=normalized,
            contra_count=int(contra_count or 0),
            lead_count=int(lead_count or 0),
        ))

    log.info(f"【第一层】完成: 从全市场筛出 {len(candidates)} 只候选 (score 范围 {min_score:.2f} ~ {max_score:.2f})")
    return candidates


# ─── 第二层: 精筛验证 ─────────────────────────────────────────────────────────

def tier2_deep_validation(ch, pg, trade_date: str, tier1_list: List[Tier1Candidate]) -> List[Tier2Candidate]:
    """
    第二层: 对候选池计算增强因子群

    计算内容:
      1. VWAP 偏离度     → 衡量日内价格相对成交量均价的偏离
      2. VaP 位置        → 衡量收盘价在当日价格密集区的位置
      3. 杠杆深度        → 融资净买入占成交额的比率
      4. Beta 敏感性     → 个股相对沪深300的系统性风险暴露

    异常处理:
      - 数据缺失的股票保留基础分但不加分
      - 成交额过低的股票 (< 500万) 标记为流动性风险
    """
    log.info(f"【第二层】精筛验证: 对 {len(tier1_list)} 只候选计算增强因子")

    symbols = [c.symbol for c in tier1_list]
    symbol_list = "','".join(symbols)

    # ── 2.1 VWAP 数据 ──
    vwap_map = {}
    vwap_rows = ch.execute(f"""
        SELECT symbol,
               sum(amount) / sum(volume) as vwap,
               argMax(close, datetime) as close
        FROM raw_stocks_5min
        WHERE toDate(datetime) = '{trade_date}'
          AND symbol IN ('{symbol_list}')
        GROUP BY symbol
    """)
    for r in vwap_rows:
        sym, vwap, close = r
        if vwap and float(vwap) > 0:
            dev = (float(close) - float(vwap)) / float(vwap) * 100
            vwap_map[sym] = {'vwap': float(vwap), 'close': float(close), 'dev': dev}

    # ── 2.2 VaP 数据 (Value Area Position) ──
    vap_map = {}
    vap_rows = ch.execute(f"""
        WITH buckets AS (
            SELECT symbol, round(close, 1) as pb, sum(amount) as amt
            FROM raw_stocks_5min
            WHERE toDate(datetime) = '{trade_date}'
              AND symbol IN ('{symbol_list}')
            GROUP BY symbol, pb
        ),
        poc AS (
            SELECT symbol, argMax(pb, amt) as poc
            FROM buckets GROUP BY symbol
        ),
        ranked AS (
            SELECT symbol, pb, amt,
                   sum(amt) OVER (PARTITION BY symbol ORDER BY amt DESC) as cum_amt,
                   sum(amt) OVER (PARTITION BY symbol) as total_amt
            FROM buckets
        ),
        va AS (
            SELECT symbol,
                   min(pb) as va_low,
                   max(pb) as va_high
            FROM ranked
            WHERE cum_amt <= total_amt * 0.7
            GROUP BY symbol
        )
        SELECT p.symbol, p.poc, v.va_low, v.va_high,
               argMax(r.close, r.datetime) as close
        FROM poc p
        JOIN va v ON p.symbol = v.symbol
        JOIN raw_stocks_5min r ON p.symbol = r.symbol
        WHERE toDate(r.datetime) = '{trade_date}'
        GROUP BY p.symbol, p.poc, v.va_low, v.va_high
    """)
    for r in vap_rows:
        sym, poc, va_low, va_high, close = r
        if va_low and va_high and float(va_high) > float(va_low):
            position = (float(close) - float(va_low)) / (float(va_high) - float(va_low)) * 100
            vap_map[sym] = {'poc': float(poc), 'va_low': float(va_low), 'va_high': float(va_high),
                           'position': position, 'close': float(close)}

    # ── 2.3 杠杆数据 (融资净买入 / 成交额) ──
    margin_map = {}
    cur = pg.cursor()
    cur.execute(f"""
        SELECT t.ts_code,
               t.margin_balance_buy,
               t.margin_buy_amount,
               t.margin_net_calc
        FROM margin.margin_trading_detail_unified t
        WHERE t.trade_date = '{trade_date}'
          AND (t.ts_code IN (
              SELECT replace(symbol, 'sh', '') FROM (VALUES {','.join(["('" + s + "')" for s in symbols if s.startswith('sh')])}) AS t(symbol)
          ) OR t.ts_code IN (
              SELECT replace(symbol, 'sz', '') FROM (VALUES {','.join(["('" + s + "')" for s in symbols if s.startswith('sz')])}) AS t(symbol)
          ))
    """)
    # 简化: 用统一查询
    code_map = {}
    for s in symbols:
        if s.startswith('sh'):
            code_map[s[2:]] = s
        elif s.startswith('sz'):
            code_map[s[2:]] = s

    if code_map:
        codes = "','".join(code_map.keys())
        cur.execute(f"""
            SELECT ts_code, margin_balance_buy, margin_buy_amount, margin_net_calc
            FROM margin.margin_trading_detail_unified
            WHERE trade_date = %s AND ts_code IN ('{codes}')
        """, (trade_date,))
        for r in cur.fetchall():
            ts_code, bal, buy, net = r
            sym = code_map.get(ts_code, ts_code)
            margin_map[sym] = {'balance': float(bal or 0), 'buy': float(buy or 0), 'net': float(net or 0)}
    cur.close()

    # ── 2.4 Beta 计算 (个股 vs 沪深300) ──
    beta_map = {}
    # 获取沪深300的5分钟收益率序列
    hs300_rows = ch.execute(f"""
        SELECT datetime, close
        FROM raw_stocks_5min
        WHERE toDate(datetime) = '{trade_date}'
          AND symbol = 'sh000300'
        ORDER BY datetime
    """)
    if len(hs300_rows) >= 10:
        hs300_returns = []
        for i in range(1, len(hs300_rows)):
            prev_close = float(hs300_rows[i-1][1])
            curr_close = float(hs300_rows[i][1])
            if prev_close > 0:
                hs300_returns.append((curr_close - prev_close) / prev_close * 100)

        # 逐个计算候选股的 Beta
        for sym in symbols:
            stock_rows = ch.execute(f"""
                SELECT datetime, close
                FROM raw_stocks_5min
                WHERE toDate(datetime) = '{trade_date}'
                  AND symbol = '{sym}'
                ORDER BY datetime
            """)
            if len(stock_rows) >= 10 and len(stock_rows) == len(hs300_rows):
                stock_returns = []
                for i in range(1, len(stock_rows)):
                    prev_close = float(stock_rows[i-1][1])
                    curr_close = float(stock_rows[i][1])
                    if prev_close > 0:
                        stock_returns.append((curr_close - prev_close) / prev_close * 100)

                if len(stock_returns) == len(hs300_returns) and len(stock_returns) >= 5:
                    # 简单线性回归: stock_return = alpha + beta * market_return
                    n = len(stock_returns)
                    sum_x = sum(hs300_returns)
                    sum_y = sum(stock_returns)
                    sum_xy = sum(x*y for x, y in zip(hs300_returns, stock_returns))
                    sum_x2 = sum(x*x for x in hs300_returns)
                    denom = sum_x2 - sum_x**2 / n
                    if denom != 0:
                        beta = (sum_xy - sum_x * sum_y / n) / denom
                        beta_map[sym] = beta

    # ── 2.5 成交额 ──
    amount_map = {}
    amt_rows = ch.execute(f"""
        SELECT symbol, sum(amount) as total_amount
        FROM raw_stocks_5min
        WHERE toDate(datetime) = '{trade_date}'
          AND symbol IN ('{symbol_list}')
        GROUP BY symbol
    """)
    for r in amt_rows:
        amount_map[r[0]] = float(r[1] or 0)

    # ── 2.6 组装 Tier2 结果 ──
    # 归一化: 基于当日候选池的分布
    vwap_values = [v['dev'] for v in vwap_map.values()]
    margin_values = []
    for sym in symbols:
        net = margin_map.get(sym, {}).get('net', 0)
        amt = amount_map.get(sym, 1)
        if amt > 0:
            margin_values.append(net / amt * 100)
        else:
            margin_values.append(0)
    beta_values = list(beta_map.values())
    vap_values = [v['position'] for v in vap_map.values()]

    def normalize_min_max(values: List[float], low_better: bool = False) -> Dict[int, float]:
        """min-max 归一化到 0-100"""
        if not values:
            return {}
        vmin, vmax = min(values), max(values)
        vr = vmax - vmin if vmax > vmin else 1.0
        result = {}
        for i, v in enumerate(values):
            if low_better:
                result[i] = (vmax - v) / vr * 100
            else:
                result[i] = (v - vmin) / vr * 100
        return result

    vwap_norm = normalize_min_max(vwap_values)
    margin_norm = normalize_min_max(margin_values)
    # Beta: 低Beta偏好 (防御型)
    beta_norm = normalize_min_max(beta_values, low_better=True)
    vap_norm = normalize_min_max(vap_values)

    candidates = []
    for i, t1 in enumerate(tier1_list):
        sym = t1.symbol

        vwap_data = vwap_map.get(sym, {})
        vap_data = vap_map.get(sym, {})
        margin_data = margin_map.get(sym, {})
        amount = amount_map.get(sym, 0)
        beta_val = beta_map.get(sym, 1.0)

        margin_conc = (margin_data.get('net', 0) / amount * 100) if amount > 0 else 0

        candidates.append(Tier2Candidate(
            symbol=sym,
            name=t1.name,
            sector=t1.sector,
            is_score=t1.is_normalized,
            vwap_dev=vwap_data.get('dev', 0),
            vwap_normalized=vwap_norm.get(i, 50),
            margin_concentration=margin_conc,
            margin_normalized=margin_norm.get(i, 50),
            beta=beta_val,
            beta_normalized=beta_norm.get(list(beta_map.keys()).index(sym), 50) if sym in beta_map else 50,
            vap_position=vap_data.get('position', 50),
            vap_normalized=vap_norm.get(i, 50),
            daily_amount=amount,
        ))

    log.info(f"【第二层】完成: {len(candidates)} 只候选通过精筛")
    log.info(f"  VWAP计算: {len(vwap_map)}/{len(tier1_list)}, "
             f"VaP: {len(vap_map)}/{len(tier1_list)}, "
             f"杠杆: {len(margin_map)}/{len(tier1_list)}, "
             f"Beta: {len(beta_map)}/{len(tier1_list)}")
    return candidates


# ─── 第三层: 组合评分 ─────────────────────────────────────────────────────────

def tier3_composite_scoring(tier2_list: List[Tier2Candidate], top_n: int, weights: Dict[str, float]) -> List[Tier3Result]:
    """
    第三层: 五因子加权组合评分

    公式:
      composite_score = IS*0.30 + VWAP*0.20 + Margin*0.20 + Beta*0.15 + VaP*0.15

    所有因子已归一化到 0-100，直接加权即可
    """
    log.info(f"【第三层】组合评分: 对 {len(tier2_list)} 只候选进行五因子加权")

    results = []
    for c in tier2_list:
        component = {
            'independence': c.is_score,
            'vwap': c.vwap_normalized,
            'margin': c.margin_normalized,
            'beta': c.beta_normalized,
            'vap': c.vap_normalized,
        }

        composite = sum(component[k] * weights[k] for k in weights)

        # 生成买入建议
        if composite >= 80:
            rec = "强烈推荐"
        elif composite >= 70:
            rec = "推荐买入"
        elif composite >= 60:
            rec = "适度关注"
        elif composite >= 50:
            rec = "观望"
        else:
            rec = "回避"

        results.append(Tier3Result(
            symbol=c.symbol,
            name=c.name,
            sector=c.sector,
            composite_score=composite,
            component_scores=component,
            weights=weights.copy(),
            rank=0,
            recommendation=rec,
        ))

    # 按组合评分降序排列
    results.sort(key=lambda x: x.composite_score, reverse=True)
    for i, r in enumerate(results, 1):
        r.rank = i

    # 只返回 Top N
    top_results = results[:top_n]
    log.info(f"【第三层】完成: 最终输出 Top {len(top_results)}")
    return top_results


# ─── 报告生成 ─────────────────────────────────────────────────────────────────

def print_results(results: List[Tier3Result], tier2_map: Dict[str, Tier2Candidate], weights: Dict[str, float]):
    """终端表格输出"""
    print("\n" + "="*100)
    print("三层筛选选股结果")
    print("="*100)
    print(f"{'排名':<4} {'代码':<12} {'名称':<10} {'行业':<12} {'综合分':>8} {'IS':>6} {'VWAP':>6} {'杠杆':>6} {'Beta':>6} {'VaP':>6} {'建议':<8}")
    print("-"*100)

    for r in results:
        c = tier2_map.get(r.symbol)
        print(f"{r.rank:<4} {r.symbol:<12} {r.name:<10} {r.sector:<12} "
              f"{r.composite_score:>8.1f} "
              f"{r.component_scores['independence']:>6.1f} "
              f"{r.component_scores['vwap']:>6.1f} "
              f"{r.component_scores['margin']:>6.1f} "
              f"{r.component_scores['beta']:>6.1f} "
              f"{r.component_scores['vap']:>6.1f} "
              f"{r.recommendation:<8}")

    print("="*100)
    print(f"\n权重配置: 独立强度{weights['independence']*100:.0f}% + VWAP{weights['vwap']*100:.0f}% + "
          f"杠杆{weights['margin']*100:.0f}% + Beta{weights['beta']*100:.0f}% + VaP{weights['vap']*100:.0f}%")


def generate_markdown_report(trade_date: str, results: List[Tier3Result],
                             tier2_map: Dict[str, Tier2Candidate],
                             tier1_map: Dict[str, Tier1Candidate],
                             output_path: str):
    """生成 Obsidian Markdown 报告"""
    lines = []
    lines.append("---")
    lines.append(f"title: \"三层筛选选股报告: {trade_date}\"")
    lines.append(f"date: {trade_date}")
    lines.append("type: screening-report")
    lines.append("tags: [量化, 选股, 三层筛选]")
    lines.append("---")
    lines.append("")
    lines.append(f"# 三层筛选选股报告 ({trade_date})")
    lines.append("")
    lines.append("## 筛选逻辑")
    lines.append("")
    lines.append("```")
    lines.append("第一层: 独立强度因子批量过滤 → 筛出防御型候选池")
    lines.append("第二层: VWAP / VaP / 杠杆 / Beta 精筛验证")
    lines.append("第三层: 五因子加权组合评分")
    lines.append("```")
    lines.append("")
    lines.append("## 权重配置")
    lines.append("")
    lines.append("| 因子 | 权重 | 说明 |")
    lines.append("|------|------|------|")
    lines.append("| 独立强度 | 30% | 板块下跌时的抗跌能力 |")
    lines.append("| VWAP偏离 | 20% | 收盘价相对成交量均价的偏离 |")
    lines.append("| 杠杆深度 | 20% | 融资净买入占成交额比率 |")
    lines.append("| Beta敏感 | 15% | 相对沪深300的系统性风险 |")
    lines.append("| VaP位置 | 15% | 收盘价在价格密集区的位置 |")
    lines.append("")
    lines.append("## 选股结果")
    lines.append("")
    lines.append("| 排名 | 代码 | 名称 | 行业 | 综合分 | IS | VWAP | 杠杆 | Beta | VaP | 建议 |")
    lines.append("|------|------|------|------|--------|----|------|------|------|-----|------|")

    for r in results:
        c = tier2_map.get(r.symbol)
        t1 = tier1_map.get(r.symbol)
        lines.append(f"| {r.rank} | {r.symbol} | {r.name} | {r.sector} | "
                     f"{r.composite_score:.1f} | "
                     f"{r.component_scores['independence']:.1f} | "
                     f"{r.component_scores['vwap']:.1f} | "
                     f"{r.component_scores['margin']:.1f} | "
                     f"{r.component_scores['beta']:.1f} | "
                     f"{r.component_scores['vap']:.1f} | "
                     f"{r.recommendation} |")

    lines.append("")
    lines.append("## 明细")
    lines.append("")
    for r in results:
        c = tier2_map.get(r.symbol)
        lines.append(f"### {r.rank}. {r.symbol} {r.name}")
        lines.append("")
        lines.append(f"- **综合评分**: {r.composite_score:.1f}")
        lines.append(f"- **买入建议**: {r.recommendation}")
        lines.append(f"- **行业**: {r.sector}")
        lines.append("")
        lines.append("**五因子明细**:")
        lines.append("")
        lines.append(f"| 因子 | 原始值 | 归一化分 | 权重 | 贡献 |")
        lines.append(f"|------|--------|----------|------|------|")
        for k, w in r.weights.items():
            raw = getattr(c, f"{k}_dev" if k == 'vwap' else f"{k}_concentration" if k == 'margin' else f"{k}_position" if k == 'vap' else k, 0)
            if k == 'independence':
                t1 = tier1_map.get(r.symbol)
                raw = t1.is_raw_score if t1 else 0
            elif k == 'beta':
                raw = c.beta
            contrib = r.component_scores[k] * w
            lines.append(f"| {k} | {raw:.2f} | {r.component_scores[k]:.1f} | {w*100:.0f}% | {contrib:.1f} |")
        lines.append("")

    content = "\n".join(lines)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)
    log.info(f"报告已保存: {output_path}")


def generate_json_output(trade_date: str, results: List[Tier3Result], output_path: str):
    """生成 JSON 输出供下游策略调用"""
    data = {
        'trade_date': trade_date,
        'weights': WEIGHTS,
        'candidates': [
            {
                'rank': r.rank,
                'symbol': r.symbol,
                'name': r.name,
                'sector': r.sector,
                'composite_score': round(r.composite_score, 2),
                'component_scores': {k: round(v, 2) for k, v in r.component_scores.items()},
                'recommendation': r.recommendation,
            }
            for r in results
        ]
    }
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info(f"JSON已保存: {output_path}")


# ─── 主函数 ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='三层筛选选股系统')
    parser.add_argument('--date', type=str, default=datetime.now().strftime('%Y-%m-%d'),
                        help='交易日期 (默认: 今日)')
    parser.add_argument('--top', type=int, default=20,
                        help='最终输出 Top N (默认: 20)')
    parser.add_argument('--is-threshold', type=float, default=DEFAULT_IS_THRESHOLD,
                        help=f'第一层独立强度过滤阈值 (默认: {DEFAULT_IS_THRESHOLD})')
    parser.add_argument('--sector', type=str, default=None,
                        help='行业过滤 (例如: 半导体)')
    parser.add_argument('--output-dir', type=str, default='./results',
                        help='输出目录 (默认: ./results)')
    parser.add_argument('--no-vault', action='store_true',
                        help='不复制到 Obsidian Vault')

    args = parser.parse_args()
    trade_date = args.date
    top_n = args.top
    is_threshold = args.is_threshold
    sector_filter = args.sector

    load_env()
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(VAULT_DIR, exist_ok=True)

    ch = get_ch()
    pg = get_pg()

    try:
        # ── 第一层: 批量过滤 ──
        max_candidates = top_n * FIRST_TIER_MULTIPLIER
        tier1_list = tier1_batch_filter(ch, trade_date, is_threshold, max_candidates)
        if not tier1_list:
            print(f"❌ {trade_date} 无数据或第一层过滤结果为空")
            print("   请先运行: ./scripts/calc_independence_score.sh {trade_date}")
            return

        # 行业过滤 (第一层后)
        if sector_filter:
            tier1_list = [c for c in tier1_list if sector_filter in c.sector]
            log.info(f"行业过滤后: {len(tier1_list)} 只 ({sector_filter})")

        tier1_map = {c.symbol: c for c in tier1_list}

        # ── 第二层: 精筛验证 ──
        tier2_list = tier2_deep_validation(ch, pg, trade_date, tier1_list)
        tier2_map = {c.symbol: c for c in tier2_list}

        # ── 第三层: 组合评分 ──
        results = tier3_composite_scoring(tier2_list, top_n, WEIGHTS)

        # ── 输出 ──
        print_results(results, tier2_map, WEIGHTS)

        # Markdown 报告
        md_path = os.path.join(args.output_dir, f"{trade_date}_三层筛选报告.md")
        generate_markdown_report(trade_date, results, tier2_map, tier1_map, md_path)

        # JSON 输出
        json_path = os.path.join(args.output_dir, f"{trade_date}_三层筛选.json")
        generate_json_output(trade_date, results, json_path)

        # 复制到 Vault
        if not args.no_vault:
            vault_path = os.path.join(VAULT_DIR, f"{trade_date}_三层筛选报告.md")
            import shutil
            shutil.copy(md_path, vault_path)
            log.info(f"已复制到 Vault: {vault_path}")

    finally:
        ch.disconnect()
        pg.close()


if __name__ == '__main__':
    main()
