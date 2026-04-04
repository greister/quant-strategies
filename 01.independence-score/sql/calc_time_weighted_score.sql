-- 分时独立强度因子 - 时间加权版本
-- 计算股票在板块下跌时的逆势表现得分（带时间权重）
-- 参数: trade_date (Date), config_name (String)

WITH
-- 获取指定配置的权重
config AS (
    SELECT weights
    FROM score_weight_configs
    WHERE config_name = {config_name:String}
),

-- 获取股票名称和行业映射
stock_info AS (
    SELECT 
        s.symbol,
        g.name AS stock_name,
        i.block_name AS industry_name
    FROM stock_industry_mapping s
    LEFT JOIN gtja_stock_names g ON s.symbol = g.symbol
    LEFT JOIN gtja_industry_list i ON substring(s.industry_code, 3) = i.block_code
    WHERE i.block_name != ''
),

-- 计算各股票各区间收益
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
           OR toHour(r.datetime) = 10 
           OR (toHour(r.datetime) = 11 AND toMinute(r.datetime) <= 30)
           OR toHour(r.datetime) = 13 
           OR toHour(r.datetime) = 14)
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
        s.symbol,
        s.name,
        s.sector,
        s.datetime,
        s.interval_idx,
        s.return_5min,
        sec.sector_return,
        multiIf(
            sec.sector_return < -0.005 AND (s.return_5min > 0 OR s.return_5min - sec.sector_return > 0.01),
            1, 0
        ) AS is_contra,
        (SELECT weights[toUInt8(s.interval_idx) + 1] FROM config) AS weight
    FROM stock_returns s
    JOIN sector_returns sec ON s.sector = sec.sector AND s.datetime = sec.datetime
),

-- 按股票汇总
final_scores AS (
    SELECT
        symbol,
        name,
        sector,
        sum(is_contra) AS raw_score,
        sum(is_contra * weight) AS weighted_score,
        groupArray((toUInt8(interval_idx), toUInt8(is_contra), toFloat32(weight))) AS contra_details,
        countIf(is_contra = 1) AS contra_count
    FROM contra_intervals
    GROUP BY symbol, name, sector
)

-- 插入结果表
INSERT INTO independence_score_time_weighted (
    date, symbol, name, sector,
    raw_score, weighted_score,
    config_name, contra_count, contra_details
)
SELECT
    {trade_date:Date} AS date,
    symbol,
    name,
    sector,
    toFloat32(raw_score) AS raw_score,
    toFloat32(weighted_score) AS weighted_score,
    {config_name:String} AS config_name,
    contra_count,
    contra_details
FROM final_scores
WHERE raw_score > 0
