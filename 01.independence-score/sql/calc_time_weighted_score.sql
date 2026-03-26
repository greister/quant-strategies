-- 分时独立强度因子 - 时间加权版本
-- 计算股票在板块下跌时的逆势表现得分（带时间权重）
-- 参数: trade_date (Date), config_name (String)
--
-- 阈值说明:
--   sector_return_threshold: -0.5%  - 板块下跌阈值，低于此值视为板块下跌
--   stock_return_threshold:  0%     - 个股上涨阈值，高于此值视为个股上涨
--   excess_return_threshold: 1%     - 超额收益阈值，高于此值视为显著跑赢板块

WITH
-- 获取指定配置的权重
config AS (
    SELECT weights
    FROM score_weight_configs
    WHERE config_name = {config_name:String}
),

-- 计算各股票各区间收益
stock_returns AS (
    SELECT
        symbol,
        name,
        sector,
        datetime,
        -- 计算 5 分钟收益
        (close - open) / open AS return_5min,
        -- 计算区间序号 (0-47)，正确处理午休时间
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

-- 识别逆势区间（复用原始因子逻辑）
contra_intervals AS (
    SELECT
        s.symbol,
        s.name,
        s.sector,
        s.datetime,
        s.interval_idx,
        s.return_5min,
        sec.sector_return,
        -- 是否满足逆势条件
        multiIf(
            sec.sector_return < -0.005 AND (s.return_5min > 0 OR s.return_5min - sec.sector_return > 0.01),
            1,
            0
        ) AS is_contra,
        -- 获取对应权重（ClickHouse 数组索引从 1 开始）
        -- 注意：scalar subquery 在大数据量时可能有性能问题，可考虑优化为 JOIN
        (SELECT weights[s.interval_idx + 1] FROM config) AS weight
    FROM stock_returns s
    JOIN sector_returns sec ON s.sector = sec.sector AND s.datetime = sec.datetime
),

-- 按股票汇总
final_scores AS (
    SELECT
        code,
        name,
        sector,
        -- 原始分数：逆势区间计数
        sum(is_contra) AS raw_score,
        -- 加权分数：逆势区间权重之和
        sum(is_contra * weight) AS weighted_score,
        -- 逆势区间详情
        groupArray((toUInt8(interval_idx), toUInt8(is_contra), toFloat32(weight))) AS contra_details,
        countIf(is_contra = 1) AS contra_count
    FROM contra_intervals
    GROUP BY code, name, sector
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
