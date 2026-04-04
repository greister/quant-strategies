-- 简化版独立强度因子计算（不使用 name 和 sector）
-- 参数: trade_date (Date)

WITH
-- 计算各股票各区间收益
stock_returns AS (
    SELECT
        symbol,
        datetime,
        (close - open) / open AS return_5min,
        -- 计算区间序号 (0-47)
        multiIf(
            toHour(datetime) < 12,
            ((toHour(datetime) - 9) * 60 + (toMinute(datetime) - 30)) / 5,
            24 + ((toHour(datetime) - 13) * 60 + toMinute(datetime)) / 5
        ) AS interval_idx
    FROM raw_stocks_5min
    WHERE toDate(datetime) = {trade_date:Date}
      AND ((toHour(datetime) = 9 AND toMinute(datetime) >= 30) 
           OR toHour(datetime) = 10 
           OR (toHour(datetime) = 11 AND toMinute(datetime) <= 30)
           OR toHour(datetime) = 13 
           OR toHour(datetime) = 14)
),

-- 计算板块收益（按 symbol 前两位分组，如 sh, sz, bj）
sector_returns AS (
    SELECT
        substring(symbol, 1, 2) AS market,
        datetime,
        avg(return_5min) AS sector_return,
        multiIf(
            toHour(datetime) < 12,
            ((toHour(datetime) - 9) * 60 + (toMinute(datetime) - 30)) / 5,
            24 + ((toHour(datetime) - 13) * 60 + toMinute(datetime)) / 5
        ) AS interval_idx
    FROM stock_returns
    GROUP BY market, datetime, interval_idx
),

-- 识别逆势区间
contra_intervals AS (
    SELECT
        s.symbol,
        s.datetime,
        s.interval_idx,
        s.return_5min,
        sec.sector_return,
        -- 是否满足逆势条件
        multiIf(
            sec.sector_return < -0.005 AND (s.return_5min > 0 OR s.return_5min - sec.sector_return > 0.01),
            1,
            0
        ) AS is_contra
    FROM stock_returns s
    JOIN sector_returns sec 
        ON substring(s.symbol, 1, 2) = sec.market 
        AND s.datetime = sec.datetime
),

-- 按股票汇总
final_scores AS (
    SELECT
        symbol,
        -- 原始分数：逆势区间计数
        sum(is_contra) AS raw_score,
        -- 逆势区间数量
        countIf(is_contra = 1) AS contra_count,
        -- 平均逆势收益
        avgIf(return_5min, is_contra = 1) AS avg_contra_return,
        -- 总区间数
        count() AS total_intervals
    FROM contra_intervals
    GROUP BY symbol
)

SELECT
    {trade_date:Date} AS date,
    symbol,
    toFloat32(raw_score) AS raw_score,
    contra_count,
    avg_contra_return,
    total_intervals
FROM final_scores
WHERE raw_score > 0
ORDER BY raw_score DESC
