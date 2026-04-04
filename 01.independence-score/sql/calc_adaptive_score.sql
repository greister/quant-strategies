-- 自适应独立强度因子计算
-- 根据市场波动动态调整阈值，保持信号数量稳定
-- 参数: {trade_date:Date}

WITH
-- 计算个股5分钟收益率
stock_returns AS (
    SELECT
        symbol,
        datetime,
        close,
        prev_close,
        (close - prev_close) / prev_close * 100 as stock_return
    FROM (
        SELECT
            symbol,
            datetime,
            close,
            lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as prev_close
        FROM raw_stocks_5min
        WHERE toDate(datetime) = {trade_date:Date}
    )
),

-- 过滤有效数据
filtered AS (
    SELECT *
    FROM stock_returns
    WHERE stock_return IS NOT NULL
      AND abs(stock_return) < 50
      AND prev_close > 0
),

-- 关联行业
with_sector AS (
    SELECT 
        f.symbol,
        f.datetime,
        f.stock_return,
        m.industry_code as sector_code
    FROM filtered f
    JOIN stock_industry_mapping m ON f.symbol = m.symbol
),

-- 计算行业收益
sector_returns AS (
    SELECT
        sector_code,
        datetime,
        avg(stock_return) as sector_return
    FROM with_sector
    GROUP BY sector_code, datetime
),

-- 计算市场波动水平
market_stats AS (
    SELECT
        countIf(abs(sector_return) < 50 AND sector_return < -0.3) as volatile_intervals,
        countIf(abs(sector_return) < 50) as total_intervals,
        countIf(abs(sector_return) < 50 AND sector_return < -0.3) * 100.0 / nullIf(countIf(abs(sector_return) < 50), 0) as volatility_pct
    FROM sector_returns
),

-- 根据波动水平选择阈值
dynamic_threshold AS (
    SELECT
        CASE 
            WHEN volatility_pct > 15 THEN -0.5   -- 高波动日：严格阈值
            WHEN volatility_pct > 8 THEN -0.3    -- 中波动日：中等阈值
            WHEN volatility_pct > 3 THEN -0.1    -- 低波动日：宽松阈值
            ELSE 0.0                              -- 极低波动日：不计算
        END as sector_threshold,
        CASE 
            WHEN volatility_pct > 15 THEN 0.0    -- 高波动：要求上涨
            WHEN volatility_pct > 8 THEN -0.3    -- 中波动：允许微跌
            WHEN volatility_pct > 3 THEN 0.0     -- 低波动：要求上涨
            ELSE 0.0
        END as stock_threshold,
        CASE 
            WHEN volatility_pct > 15 THEN 1.0    -- 高波动：超额1%
            WHEN volatility_pct > 8 THEN 0.5     -- 中波动：超额0.5%
            WHEN volatility_pct > 3 THEN 0.3     -- 低波动：超额0.3%
            ELSE 0.0
        END as excess_threshold,
        volatility_pct
    FROM market_stats
),

-- 合并数据并计算分数
combined AS (
    SELECT 
        s.symbol,
        s.sector_code,
        s.datetime,
        s.stock_return,
        r.sector_return,
        s.stock_return - r.sector_return as excess_return,
        -- 使用动态阈值判断逆势
        CASE 
            WHEN r.sector_return < (SELECT sector_threshold FROM dynamic_threshold)
                 AND (s.stock_return > (SELECT stock_threshold FROM dynamic_threshold)
                      OR s.stock_return - r.sector_return > (SELECT excess_threshold FROM dynamic_threshold))
            THEN 1 
            ELSE 0 
        END as is_contra,
        (SELECT volatility_pct FROM dynamic_threshold) as volatility_pct,
        (SELECT sector_threshold FROM dynamic_threshold) as used_threshold
    FROM with_sector s
    JOIN sector_returns r ON s.sector_code = r.sector_code AND s.datetime = r.datetime
    WHERE abs(r.sector_return) < 50
),

-- 汇总分数
final_scores AS (
    SELECT
        symbol,
        sector_code,
        sum(is_contra) as independence_score,
        count(*) as total_intervals,
        round(sum(is_contra) * 100.0 / count(*), 2) as independence_ratio,
        avgIf(stock_return, is_contra = 1) as avg_contra_return,
        maxIf(excess_return, is_contra = 1) as max_excess_return,
        any(volatility_pct) as market_volatility,
        any(used_threshold) as sector_threshold_used
    FROM combined
    GROUP BY symbol, sector_code
    HAVING sum(is_contra) > 0
)

SELECT
    {trade_date:Date} as trade_date,
    symbol,
    sector_code,
    independence_score,
    total_intervals,
    independence_ratio,
    round(avg_contra_return, 4) as avg_contra_return,
    round(max_excess_return, 4) as max_excess_return,
    round(market_volatility, 2) as market_volatility_pct,
    sector_threshold_used
FROM final_scores
ORDER BY independence_score DESC, independence_ratio DESC
