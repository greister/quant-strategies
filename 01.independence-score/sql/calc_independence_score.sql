-- ==============================================================================
-- S01 v2.0 — 分时独立强度因子计算 (查询版)
-- ==============================================================================
-- 升级内容:
--   1. 成交量加权: 高成交量区间的逆势/顺势表现获得更高权重
--   2. 超额收益强度: 超额收益大小被量化，而不仅仅是0/1计数
--   3. 板块同涨绝对强势: 大幅跑赢板块的个股获得额外加分
--
-- 得分公式:
--   contra_count  = Σ(区间成交额权重 × 逆势标志) × 48
--   lead_count    = Σ(区间成交额权重 × 顺势领先标志) × 48
--   excess_strength = Σ(超额收益% × 区间成交额权重 × 有效标志) / 10
--   score         = contra_count + lead_count + excess_strength
--
-- ×48 原因: 全天48个5分钟区间，权重之和为1.0，放大48倍后
-- 等效于"按成交额加权的逆势/顺势区间数量"，量级与原始计数一致。
-- ==============================================================================

WITH
-- ── 1. 个股5分钟收益率 + 成交量 ──
stock_returns AS (
    SELECT
        symbol,
        datetime,
        close,
        prev_close,
        volume,
        amount,
        (close - prev_close) / prev_close * 100 as stock_return
    FROM (
        SELECT
            symbol,
            datetime,
            close,
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
),

-- ── 2. 计算每只股票的当日总成交额 ──
stock_daily_amount AS (
    SELECT symbol, sum(amount) as daily_amount
    FROM stock_returns
    WHERE stock_return IS NOT NULL
    GROUP BY symbol
    HAVING daily_amount > 0
),

-- ── 3. 带成交额权重的个股数据 ──
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

-- ── 4. 个股+板块归属 ──
stock_with_sector AS (
    SELECT
        sw.symbol,
        sw.datetime,
        sw.stock_return,
        sw.amount_weight,
        ss.sector_code
    FROM stock_weighted sw
    INNER JOIN stock_sectors ss ON sw.symbol = ss.symbol
),

-- ── 5. 板块5分钟收益率（成交额加权平均）──
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

-- ── 6. 合并个股和板块，计算超额收益 + 标志 ──
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
        sr.sector_return < -0.2 AND sws.stock_return > sr.sector_return as is_contra_move,
        sr.sector_return > 0.2 AND sws.stock_return > sr.sector_return as is_lead_move,
        (sr.sector_return < -0.2 OR sr.sector_return > 0.2) AND sws.stock_return > sr.sector_return as is_valid_move
    FROM stock_with_sector sws
    INNER JOIN sector_returns sr
        ON sws.sector_code = sr.sector_code AND sws.datetime = sr.datetime
),

-- ── 7. 统计加权得分 ──
independence_score AS (
    SELECT
        symbol,
        sector_code,
        sector_stock_count,
        countIf(is_contra_move) as raw_contra,
        countIf(is_lead_move) as raw_lead,
        round(sumIf(amount_weight, is_contra_move) * 48, 4) as contra_count,
        round(sumIf(amount_weight, is_lead_move) * 48, 4) as lead_count,
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
    {trade_date:Date} as trade_date,
    symbol,
    sector_code,
    round(contra_count + lead_count + excess_strength, 4) as score,
    total_intervals,
    contra_ratio,
    round(avg_contra_return, 4) as avg_contra_return,
    round(max_contra_excess, 4) as max_excess_return,
    contra_count,
    lead_count,
    lead_ratio,
    round(avg_lead_return, 4) as avg_lead_return,
    round(max_lead_excess, 4) as max_lead_excess
FROM independence_score
WHERE (contra_count + lead_count + excess_strength) > 0
ORDER BY score DESC, contra_count DESC
