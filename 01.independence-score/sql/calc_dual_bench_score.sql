-- 双基准独立强度因子计算
-- 同时计算相对于行业板块和中证300的独立强度
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

-- 提取中证300指数收益
csi300_returns AS (
    SELECT
        datetime,
        (close - lagInFrame(close) OVER (ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) / lagInFrame(close) OVER (ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) * 100 as csi300_return
    FROM raw_stocks_5min
    WHERE toDate(datetime) = {trade_date:Date}
      AND symbol = 'sh000300'
),

-- 关联行业并计算行业收益
industry_data AS (
    SELECT 
        s.symbol,
        s.datetime,
        s.stock_return,
        m.industry_code,
        avg(s.stock_return) OVER (PARTITION BY m.industry_code, s.datetime) as industry_return
    FROM filtered s
    JOIN stock_industry_mapping m ON s.symbol = m.symbol
),

-- 去重行业数据
industry_returns AS (
    SELECT DISTINCT
        industry_code,
        datetime,
        industry_return
    FROM industry_data
),

-- 合并个股、行业和中证300数据
combined_data AS (
    SELECT 
        s.symbol,
        s.industry_code,
        s.datetime,
        s.stock_return,
        ind.industry_return,
        c.csi300_return,
        s.stock_return - ind.industry_return as excess_sector,
        s.stock_return - c.csi300_return as excess_csi300,
        CASE WHEN ind.industry_return < -0.3 AND (s.stock_return > -0.3 OR s.stock_return - ind.industry_return > 0.5) THEN 1 ELSE 0 END as is_sector_contra,
        CASE WHEN c.csi300_return < -0.3 AND (s.stock_return > -0.3 OR s.stock_return - c.csi300_return > 0.5) THEN 1 ELSE 0 END as is_csi300_contra
    FROM industry_data s
    JOIN industry_returns ind ON s.industry_code = ind.industry_code AND s.datetime = ind.datetime
    JOIN csi300_returns c ON s.datetime = c.datetime
    WHERE abs(ind.industry_return) < 50
      AND abs(c.csi300_return) < 50
)

SELECT
    {trade_date:Date} as trade_date,
    symbol,
    industry_code as sector_code,
    sum(is_sector_contra) as sector_score,
    sum(is_csi300_contra) as csi300_score,
    round(sum(is_sector_contra) * 0.6 + sum(is_csi300_contra) * 0.4, 2) as dual_score,
    count(*) as total_intervals,
    round(sum(is_sector_contra) * 100.0 / count(*), 2) as sector_independence_ratio,
    round(sum(is_csi300_contra) * 100.0 / count(*), 2) as csi300_independence_ratio,
    round(avgIf(stock_return, is_sector_contra = 1), 4) as avg_sector_contra_return,
    round(avgIf(stock_return, is_csi300_contra = 1), 4) as avg_csi300_contra_return,
    round(maxIf(excess_sector, is_sector_contra = 1), 4) as max_sector_excess,
    round(maxIf(excess_csi300, is_csi300_contra = 1), 4) as max_csi300_excess
FROM combined_data
GROUP BY symbol, industry_code
HAVING sum(is_sector_contra) > 0 OR sum(is_csi300_contra) > 0
ORDER BY dual_score DESC, sector_score DESC, csi300_score DESC
