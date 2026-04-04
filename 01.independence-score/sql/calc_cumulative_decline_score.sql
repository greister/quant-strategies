-- 累积跌幅独立强度因子计算
-- 解决"温水煮青蛙"缓跌行情下的算法失效问题
-- 参数: {trade_date:Date}

WITH
-- 计算个股5分钟收益率
stock_returns AS (
    SELECT
        symbol,
        datetime,
        close,
        lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as prev_close,
        (close - lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) / lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) * 100 as stock_return
    FROM raw_stocks_5min
    WHERE toDate(datetime) = {trade_date:Date}
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
        (close - lagInFrame(close) OVER (ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) / lagInFrame(close) OVER (ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) * 100 as csi300_return,
        close as csi300_close
    FROM raw_stocks_5min
    WHERE toDate(datetime) = {trade_date:Date}
      AND symbol = 'sh000300'
),

-- 计算中证300累积跌幅（使用滑动窗口）
csi300_cumulative AS (
    SELECT
        datetime,
        csi300_return,
        -- 当前区间相对于N个区间前的累积收益
        (csi300_close - lagInFrame(csi300_close) OVER (ORDER BY datetime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) / lagInFrame(csi300_close) OVER (ORDER BY datetime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) * 100 as cum_3interval_return,
        (csi300_close - lagInFrame(csi300_close) OVER (ORDER BY datetime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) / lagInFrame(csi300_close) OVER (ORDER BY datetime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) * 100 as cum_5interval_return
    FROM csi300_returns
),

-- 关联行业
with_industry AS (
    SELECT 
        f.symbol,
        f.datetime,
        f.stock_return,
        m.industry_code
    FROM filtered f
    JOIN stock_industry_mapping m ON f.symbol = m.symbol
),

-- 计算行业收益
industry_returns AS (
    SELECT
        industry_code,
        datetime,
        avg(stock_return) as industry_return
    FROM with_industry
    GROUP BY industry_code, datetime
),

-- 合并数据并计算独立强度（包含累积跌幅触发）
combined_data AS (
    SELECT 
        s.symbol,
        s.industry_code,
        s.datetime,
        s.stock_return,
        ind.industry_return,
        c.csi300_return,
        c.cum_3interval_return,
        c.cum_5interval_return,
        s.stock_return - ind.industry_return as excess_industry,
        s.stock_return - c.csi300_return as excess_csi300,
        -- 行业独立强度（原逻辑）
        CASE WHEN ind.industry_return < -0.3 AND (s.stock_return > -0.3 OR s.stock_return - ind.industry_return > 0.5) THEN 1 ELSE 0 END as is_industry_contra,
        -- 中证300独立强度（原逻辑 - 单区间急跌）
        CASE WHEN c.csi300_return < -0.3 AND (s.stock_return > -0.3 OR s.stock_return - c.csi300_return > 0.5) THEN 1 ELSE 0 END as is_csi300_contra_immediate,
        -- 中证300独立强度（新增 - 累积跌幅缓跌）
        CASE 
            WHEN (c.cum_3interval_return < -0.4 OR c.cum_5interval_return < -0.6) 
                 AND (s.stock_return > -0.3 OR s.stock_return - c.csi300_return > 0.3) 
            THEN 1 
            ELSE 0 
        END as is_csi300_contra_cumulative,
        -- 综合中证300独立强度（急跌或缓跌）
        CASE 
            WHEN c.csi300_return < -0.3 AND (s.stock_return > -0.3 OR s.stock_return - c.csi300_return > 0.5) THEN 1
            WHEN (c.cum_3interval_return < -0.4 OR c.cum_5interval_return < -0.6) AND (s.stock_return > -0.3 OR s.stock_return - c.csi300_return > 0.3) THEN 1
            ELSE 0 
        END as is_csi300_contra
    FROM with_industry s
    JOIN industry_returns ind ON s.industry_code = ind.industry_code AND s.datetime = ind.datetime
    JOIN csi300_cumulative c ON s.datetime = c.datetime
    WHERE abs(ind.industry_return) < 50
      AND abs(c.csi300_return) < 50
)

SELECT
    {trade_date:Date} as trade_date,
    symbol,
    industry_code as sector_code,
    sum(is_industry_contra) as industry_score,
    sum(is_csi300_contra_immediate) as csi300_immediate_score,
    sum(is_csi300_contra_cumulative) as csi300_cumulative_score,
    sum(is_csi300_contra) as csi300_total_score,
    round(sum(is_industry_contra) * 0.6 + sum(is_csi300_contra) * 0.4, 2) as dual_score,
    count(*) as total_intervals,
    round(sum(is_industry_contra) * 100.0 / count(*), 2) as industry_independence_ratio,
    round(sum(is_csi300_contra) * 100.0 / count(*), 2) as csi300_independence_ratio
FROM combined_data
GROUP BY symbol, industry_code
HAVING sum(is_industry_contra) > 0 OR sum(is_csi300_contra) > 0
ORDER BY dual_score DESC, industry_score DESC, csi300_total_score DESC
