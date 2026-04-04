-- 低贝塔混合策略 - 核心计算SQL
-- 参数: trade_date (Date)

-- ============================================================
-- STEP 1: 日线低贝塔预筛选
-- ============================================================

-- 清理旧数据（同一天）
DELETE FROM low_beta_pool_daily WHERE date = {trade_date:Date};

-- 计算20日贝塔和抗跌次数
INSERT INTO low_beta_pool_daily (
    date, symbol, name, sector, 
    beta, beta_500, anti_fall_days, 
    avg_return_20d, volatility_20d
)
WITH 
-- 获取股票信息和日收益
stock_daily AS (
    SELECT 
        d.symbol,
        d.date,
        (d.close - d.open) / d.open * 100 AS stock_return,
        s.industry_name AS sector
    FROM raw_stocks_daily d
    JOIN (
        SELECT s.symbol, i.block_name AS industry_name
        FROM v_stock_industry_mapping s
        LEFT JOIN v_gtja_industry_list i ON substring(s.industry_code, 3) = i.block_code
        WHERE i.block_name != ''
    ) s ON d.symbol = s.symbol
    WHERE d.date BETWEEN {trade_date:Date} - INTERVAL 20 DAY AND {trade_date:Date}
),

-- 获取中证500指数收益（从日线表中提取 sh000905）
index_daily AS (
    SELECT 
        date,
        (close - open) / open * 100 AS index_return
    FROM raw_stocks_daily
    WHERE symbol = 'sh000905'
      AND date BETWEEN {trade_date:Date} - INTERVAL 20 DAY AND {trade_date:Date}
),

-- 计算贝塔（简化版：使用协方差/方差）
beta_calc AS (
    SELECT 
        s.symbol,
        s.sector,
        -- 计算对中证500的贝塔
        covarSamp(s.stock_return, i.index_return) / 
            nullIf(varSamp(i.index_return), 0) AS beta_500,
        -- 统计指标
        count() AS trading_days,
        avg(s.stock_return) AS avg_return,
        stddevSamp(s.stock_return) AS volatility,
        -- 抗跌次数：指数下跌日，个股相对收益>0
        countIf(i.index_return < 0 AND s.stock_return > i.index_return) AS anti_fall_days
    FROM stock_daily s
    JOIN index_daily i ON s.date = i.date
    GROUP BY s.symbol, s.sector
    HAVING count() >= 15  -- 至少15个交易日数据
)

SELECT 
    {trade_date:Date} AS date,
    symbol,
    '' AS name,  -- 后续通过JOIN更新
    sector,
    toFloat32(beta_500) AS beta,
    toFloat32(beta_500) AS beta_500,
    toUInt8(anti_fall_days) AS anti_fall_days,
    toFloat32(avg_return) AS avg_return_20d,
    toFloat32(volatility) AS volatility_20d
FROM beta_calc
WHERE beta_500 < 0.8        -- 低贝塔：对中证500贝塔 < 0.8
  AND anti_fall_days >= 8   -- 抗跌次数 >= 8次
  AND beta_500 > 0          -- 排除负贝塔（异常值）
ORDER BY beta_500 ASC, anti_fall_days DESC;

-- 更新股票名称
ALTER TABLE low_beta_pool_daily
    UPDATE name = g.name
    FROM v_gtja_stock_names g
    WHERE low_beta_pool_daily.symbol = g.symbol
      AND low_beta_pool_daily.date = {trade_date:Date};

-- ============================================================
-- STEP 2: 5分钟相对强度计分（仅在低贝塔池内）
-- ============================================================

-- 清理旧数据
DELETE FROM low_beta_hybrid_daily 
WHERE date = {trade_date:Date} 
  AND config_name = 'evening_focus';

-- 计算独立强度分（复用原策略逻辑，但只计算低贝塔池内的股票）
INSERT INTO low_beta_hybrid_daily (
    date, symbol, name, sector,
    beta, anti_fall_days,
    raw_score, weighted_score, config_name,
    contra_count, avg_contra_return,
    hybrid_score
)
WITH
-- 获取低贝塔池
low_beta_pool AS (
    SELECT symbol, beta, anti_fall_days
    FROM low_beta_pool_daily
    WHERE date = {trade_date:Date}
),

-- 获取股票信息
stock_info AS (
    SELECT 
        s.symbol,
        g.name AS stock_name,
        i.block_name AS industry_name
    FROM low_beta_pool l
    JOIN v_stock_industry_mapping s ON l.symbol = s.symbol
    LEFT JOIN v_gtja_stock_names g ON s.symbol = g.symbol
    LEFT JOIN v_gtja_industry_list i ON substring(s.industry_code, 3) = i.block_code
    WHERE i.block_name != ''
),

-- 计算个股5分钟收益
stock_returns AS (
    SELECT
        r.symbol,
        s.stock_name AS name,
        s.industry_name AS sector,
        r.datetime,
        (r.close - r.open) / r.open AS return_5min,
        multiIf(
            toHour(r.datetime) < 12,
            ((toHour(r.datetime) - 9) * 60 + (toMinute(r.datetime) - 30)) / 5,
            24 + ((toHour(r.datetime) - 13) * 60 + toMinute(r.datetime)) / 5
        ) AS interval_idx
    FROM raw_stocks_5min r
    JOIN stock_info s ON r.symbol = s.symbol
    WHERE toDate(r.datetime) = {trade_date:Date}
      AND ((toHour(r.datetime) = 9 AND toMinute(r.datetime) >= 30) 
           OR toHour(r.datetime) IN (10, 13, 14)
           OR (toHour(r.datetime) = 11 AND toMinute(r.datetime) <= 30))
),

-- 计算板块收益
sector_returns AS (
    SELECT
        sector,
        datetime,
        avg(return_5min) AS sector_return,
        multiIf(
            toHour(datetime) < 12,
            ((toHour(datetime) - 9) * 60 + (toMinute(datetime) - 30)) / 5,
            24 + ((toHour(datetime) - 13) * 60 + toMinute(datetime)) / 5
        ) AS interval_idx
    FROM stock_returns
    GROUP BY sector, datetime, interval_idx
),

-- 识别逆势区间
contra_intervals AS (
    SELECT
        r.symbol,
        r.name,
        r.sector,
        r.return_5min,
        sec.sector_return,
        multiIf(
            sec.sector_return < -0.005 AND (r.return_5min > 0 OR r.return_5min - sec.sector_return > 0.01),
            1, 0
        ) AS is_contra
    FROM stock_returns r
    JOIN sector_returns sec ON r.sector = sec.sector AND r.datetime = sec.datetime
),

-- 汇总独立强度分
independence_scores AS (
    SELECT
        symbol,
        any(name) AS name,
        any(sector) AS sector,
        toFloat32(sum(is_contra)) AS raw_score,
        countIf(is_contra = 1) AS contra_count,
        avgIf(return_5min, is_contra = 1) AS avg_contra_return
    FROM contra_intervals
    GROUP BY symbol
    HAVING raw_score > 0
),

-- 获取低贝塔信息
final_calc AS (
    SELECT 
        i.symbol,
        i.name,
        i.sector,
        l.beta,
        l.anti_fall_days,
        i.raw_score,
        toFloat32(i.raw_score * 0.018) AS weighted_score,  -- evening_focus简化权重
        'evening_focus' AS config_name,
        i.contra_count,
        i.avg_contra_return,
        -- 综合得分 = 低贝塔得分 + 相对强度得分
        -- 低贝塔得分：(1 - beta) * 50，beta越小得分越高
        -- 相对强度得分：raw_score * 10
        toFloat32((1 - l.beta) * 50 + i.raw_score * 10) AS hybrid_score
    FROM independence_scores i
    JOIN low_beta_pool l ON i.symbol = l.symbol
)

SELECT 
    {trade_date:Date} AS date,
    symbol,
    name,
    sector,
    beta,
    anti_fall_days,
    raw_score,
    weighted_score,
    config_name,
    contra_count,
    avg_contra_return,
    hybrid_score
FROM final_calc
ORDER BY hybrid_score DESC;

-- 更新排名信息
ALTER TABLE low_beta_hybrid_daily
    UPDATE 
        rank = r.rank,
        rank_pct = r.rank_pct
    FROM (
        SELECT 
            symbol,
            row_number() OVER (ORDER BY hybrid_score DESC) as rank,
            row_number() OVER (ORDER BY hybrid_score DESC) / count() OVER () as rank_pct
        FROM low_beta_hybrid_daily
        WHERE date = {trade_date:Date}
          AND config_name = 'evening_focus'
    ) r
    WHERE low_beta_hybrid_daily.symbol = r.symbol
      AND low_beta_hybrid_daily.date = {trade_date:Date}
      AND low_beta_hybrid_daily.config_name = 'evening_focus';
