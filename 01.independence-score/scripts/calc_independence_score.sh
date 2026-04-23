#!/bin/bash
set -e

# ==============================================================================
# S01 — 基础独立强度因子 (v2.0)
# ==============================================================================
# 升级内容:
#   1. 成交量加权: 高成交量区间的逆势/顺势表现获得更高权重
#   2. 超额收益强度: 超额收益的大小被量化，而不仅仅是0/1计数
#   3. 板块同涨时的绝对强势: 大幅跑赢板块的个股获得额外加分
#
# 得分公式 (量级与 v1.0 一致):
#   contra_count  = Σ(区间成交额权重 × 逆势标志) × 48
#   lead_count    = Σ(区间成交额权重 × 顺势领先标志) × 48
#   excess_strength = Σ(超额收益% × 区间成交额权重 × 有效标志) / 10
#   score         = contra_count + lead_count + excess_strength
#
# 说明:
#   ×48 是因为全天有48个5分钟区间，权重之和为1.0，放大48倍后
#   等效于"按成交额加权的逆势/顺势区间数量"，量级与原始计数一致。
#
# 数据源: stock_industry_mapping (通达信T级), raw_stocks_5min
# ==============================================================================

DB_NAME="${CLICKHOUSE_DB:-tdx2db_rust}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-tdx2db}"
DATE="${1:-$(date +%Y-%m-%d)}"

echo "S01 独立强度因子 v2.0 | 日期: $DATE"
echo "升级点: 成交量加权 + 超额收益强度 + 板块同涨绝对强势"
echo "量级: 与 v1.0 一致 (score 范围 0~48+)"
echo ""

clickhouse-client --user="$CH_USER" --password="$CH_PASSWORD" --database="$DB_NAME" --param_trade_date="$DATE" -q "
DELETE FROM independence_score_daily WHERE date = {trade_date:Date};
INSERT INTO independence_score_daily
(symbol, date, score, raw_score, margin_weight, sector, sector_stock_count,
 contra_count, independence_ratio, avg_contra_return, max_excess_return,
 total_intervals, rn, lead_count, lead_ratio, avg_lead_return, max_lead_excess)
WITH
-- ── 1. 行业映射 ────────────────────────────────────────────────────────────
stock_sector_mapping AS (
    SELECT
        concat(
            multiIf(
                substring(symbol, 1, 2) IN ('00', '30', '15'), 'sz',
                substring(symbol, 1, 2) IN ('60', '68', '51'), 'sh',
                substring(symbol, 1, 2) IN ('82', '83', '87', '43', '92'), 'bj',
                'sz'
            ),
            symbol
        ) as symbol,
        industry_name as sector_code
    FROM (
        SELECT symbol, industry_code, industry_name,
            row_number() OVER (PARTITION BY symbol ORDER BY length(industry_code) DESC, industry_code) as rn
        FROM stock_industry_mapping
        WHERE industry_code LIKE 'T%'
          AND industry_code != 'T00'
          AND industry_name != ''
    )
    WHERE rn = 1
),

-- ── 2. 个股5分钟收益率 + 成交量 ─────────────────────────────────────────────
stock_returns AS (
    SELECT
        symbol,
        datetime,
        close,
        prev_close,
        high,
        low,
        volume,
        amount,
        (close - prev_close) / prev_close * 100 as stock_return
    FROM (
        SELECT
            symbol,
            datetime,
            close,
            high,
            low,
            volume,
            amount,
            lagInFrame(close) OVER (
                PARTITION BY symbol, toDate(datetime)
                ORDER BY datetime
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            ) as prev_close
        FROM raw_stocks_5min
        WHERE toDate(datetime) = {trade_date:Date}
    )
    WHERE prev_close > 0
      AND high != low  -- 排除一字板K线（涨停/跌停/停牌）
),

-- ── 3. 计算每只股票的当日总成交额 ──────────────────────────────────────────
stock_daily_amount AS (
    SELECT
        symbol,
        sum(amount) as daily_amount
    FROM stock_returns
    WHERE stock_return IS NOT NULL
    GROUP BY symbol
    HAVING daily_amount > 0
),

-- ── 4. 带成交额权重的个股数据 ──────────────────────────────────────────────
stock_weighted AS (
    SELECT
        sr.symbol,
        sr.datetime,
        sr.stock_return,
        sr.amount,
        sa.daily_amount,
        sr.amount / sa.daily_amount as amount_weight
    FROM stock_returns sr
    INNER JOIN stock_daily_amount sa ON sr.symbol = sa.symbol
    WHERE sr.stock_return IS NOT NULL
),

-- ── 5. 个股+板块归属 ───────────────────────────────────────────────────────
stock_with_sector AS (
    SELECT
        sw.symbol,
        sw.datetime,
        sw.stock_return,
        sw.amount,
        sw.daily_amount,
        sw.amount_weight,
        ss.sector_code
    FROM stock_weighted sw
    INNER JOIN stock_sector_mapping ss ON sw.symbol = ss.symbol
),

-- ── 6. 计算板块5分钟收益率（板块内股票成交额加权平均）───────────────────────
sector_returns AS (
    SELECT
        sector_code,
        datetime,
        sum(stock_return * amount) / sum(amount) as sector_return,
        count() as sector_stock_count
    FROM stock_with_sector
    GROUP BY sector_code, datetime
    HAVING count() >= 3
),

-- ── 7. 合并个股和板块，计算超额收益 + 逆势/顺势标志 ────────────────────────
combined_data AS (
    SELECT
        sws.symbol,
        sws.sector_code,
        sws.datetime,
        sws.stock_return,
        sws.amount_weight,
        sr.sector_return,
        sr.sector_stock_count,
        sws.stock_return - sr.sector_return as excess_return,
        -- 逆势标志: 板块下跌(<-0.2%) 且 个股跑赢板块
        sr.sector_return < -0.2 AND sws.stock_return > sr.sector_return as is_contra_move,
        -- 顺势领先标志: 板块上涨(>0.2%) 且 个股跑赢板块
        sr.sector_return > 0.2 AND sws.stock_return > sr.sector_return as is_lead_move,
        -- 有效区间标志: 板块有明确趋势(涨/跌) 且 个股跑赢板块
        (sr.sector_return < -0.2 OR sr.sector_return > 0.2) AND sws.stock_return > sr.sector_return as is_valid_move
    FROM stock_with_sector sws
    INNER JOIN sector_returns sr
        ON sws.sector_code = sr.sector_code AND sws.datetime = sr.datetime
),

-- ── 8. 统计每个股票的加权得分 ──────────────────────────────────────────────
independence_score AS (
    SELECT
        symbol,
        sector_code,
        sector_stock_count,
        -- 原始逆势/顺势计数 (兼容 v1.0)
        countIf(is_contra_move) as raw_contra,
        countIf(is_lead_move) as raw_lead,
        -- 成交量加权的等效逆势计数 (×48 让量级与原始计数一致)
        round(sumIf(amount_weight, is_contra_move) * 48, 4) as contra_count,
        round(sumIf(amount_weight, is_lead_move) * 48, 4) as lead_count,
        -- 超额收益强度奖励
        round(sumIf(excess_return * amount_weight, is_valid_move) / 10, 4) as excess_strength,
        count(*) as total_intervals,
        round(countIf(is_contra_move) * 100.0 / count(*), 2) as contra_ratio,
        round(countIf(is_lead_move) * 100.0 / count(*), 2) as lead_ratio,
        avgIf(stock_return, is_contra_move) as avg_contra_return,
        avgIf(stock_return, is_lead_move) as avg_lead_return,
        maxIf(excess_return, is_contra_move) as max_contra_excess,
        maxIf(excess_return, is_lead_move) as max_lead_excess
    FROM combined_data
    GROUP BY symbol, sector_code, sector_stock_count
)

SELECT
    symbol,
    {trade_date:Date} as date,
    -- 综合得分 = 加权逆势 + 超额收益强度 (v2.1: 去掉 lead_count，只处理下跌时的抗跌表现)
    round(contra_count + excess_strength, 4) as score,
    round(contra_count + excess_strength, 4) as raw_score,
    1.0 as margin_weight,
    sector_code as sector,
    sector_stock_count,
    contra_count,
    contra_ratio as independence_ratio,
    avg_contra_return,
    max_contra_excess as max_excess_return,
    total_intervals,
    row_number() OVER (ORDER BY (contra_count + lead_count + excess_strength) DESC) as rn,
    lead_count,
    lead_ratio,
    avg_lead_return,
    max_lead_excess
FROM independence_score
WHERE (contra_count + lead_count + excess_strength) > 0
ORDER BY score DESC, contra_count DESC
"

echo "✅ 计算完成. Top 10 得分:"

# 查询并显示当日 Top 10 结果
clickhouse-client --user="$CH_USER" --password="$CH_PASSWORD" --database="$DB_NAME" --param_trade_date="$DATE" -q "
SELECT
    date,
    symbol,
    sector,
    score,
    contra_count,
    lead_count,
    max_excess_return
FROM independence_score_daily
WHERE date = {trade_date:Date}
ORDER BY score DESC
LIMIT 10
FORMAT PrettyCompact
"
