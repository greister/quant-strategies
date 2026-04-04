-- 动量因子策略 - 常用查询示例

-- 1. 查询某日动量因子 TOP 20
SELECT 
    symbol,
    name,
    sector,
    momentum_score,
    return_20d,
    rank
FROM momentum_factor_daily
WHERE date = '2026-03-20'
ORDER BY momentum_score DESC
LIMIT 20;

-- 2. 查询某股票的动量历史
SELECT 
    date,
    momentum_score,
    return_20d,
    rank
FROM momentum_factor_daily
WHERE symbol = 'sz300001'
ORDER BY date DESC
LIMIT 30;

-- 3. 按行业统计动量因子
SELECT 
    sector,
    count() as stock_count,
    avg(momentum_score) as avg_momentum,
    max(momentum_score) as max_momentum,
    argMax(name, momentum_score) as leader_name
FROM momentum_factor_daily
WHERE date = '2026-03-20'
GROUP BY sector
ORDER BY avg_momentum DESC;

-- 4. 查询动量因子排名前10%的股票
SELECT 
    symbol,
    name,
    sector,
    momentum_score,
    rank_pct
FROM momentum_factor_daily
WHERE date = '2026-03-20'
  AND rank_pct <= 0.1
ORDER BY momentum_score DESC;

-- 5. 动量因子与收益的相关性（回测用）
SELECT 
    m.symbol,
    m.momentum_score,
    m.return_20d as past_return,
    f.return_5d as future_return
FROM momentum_factor_daily m
JOIN (
    SELECT symbol, return_5d
    FROM future_returns  -- 假设有未来收益表
    WHERE date = '2026-03-20'
) f ON m.symbol = f.symbol
WHERE m.date = '2026-03-20'
ORDER BY m.momentum_score DESC;
