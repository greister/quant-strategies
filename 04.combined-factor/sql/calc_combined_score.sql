-- 双因子组合得分计算
-- 参数: trade_date (Date), weight_ind (Float32), weight_mom (Float32)

-- ============================================================
-- 计算双因子组合得分
-- ============================================================

INSERT INTO combined_factor_daily (
    date, symbol, sector,
    independence_score, momentum_score,
    independence_rank_pct, momentum_rank_pct,
    combined_score,
    weight_ind, weight_mom,
    rank, rank_pct
)
WITH
-- 获取独立强度分数及排名
ind_scores AS (
    SELECT 
        symbol,
        sector,
        score as independence_score,
        rank() OVER (ORDER BY score DESC) as ind_rank,
        count() OVER () as ind_total
    FROM independence_score_daily
    WHERE date = {trade_date:Date}
),
-- 获取动量分数及排名
mom_scores AS (
    SELECT 
        symbol,
        momentum_score,
        rank() OVER (ORDER BY momentum_score DESC) as mom_rank,
        count() OVER () as mom_total
    FROM momentum_factor_daily
    WHERE date = {trade_date:Date}
),
-- 计算排名分位和综合得分
combined_calc AS (
    SELECT 
        i.symbol,
        i.sector,
        i.independence_score,
        m.momentum_score,
        i.ind_rank::Float32 / i.ind_total as independence_rank_pct,
        m.mom_rank::Float32 / m.mom_total as momentum_rank_pct,
        -- 综合得分计算
        (
            (1.0 - i.ind_rank::Float32 / i.ind_total) * {weight_ind:Float32} +
            (1.0 - m.mom_rank::Float32 / m.mom_total) * {weight_mom:Float32}
        ) * 100 as combined_score,
        {weight_ind:Float32} as weight_ind,
        {weight_mom:Float32} as weight_mom
    FROM ind_scores i
    INNER JOIN mom_scores m ON i.symbol = m.symbol
    WHERE i.independence_score IS NOT NULL
      AND m.momentum_score IS NOT NULL
)
SELECT 
    {trade_date:Date} as date,
    symbol,
    sector,
    independence_score,
    momentum_score,
    independence_rank_pct,
    momentum_rank_pct,
    combined_score,
    weight_ind,
    weight_mom,
    rank() OVER (ORDER BY combined_score DESC) as rank,
    rank() OVER (ORDER BY combined_score DESC) / count() OVER () as rank_pct
FROM combined_calc
ORDER BY combined_score DESC;
