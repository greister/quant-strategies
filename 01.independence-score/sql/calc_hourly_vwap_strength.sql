-- 小时VWAP强势因子计算
-- 统计全天有多少个5分钟区间满足：当前价 > 过去1小时VWAP（成交量加权均价）
-- VWAP = 过去12个区间的总成交额 / 总成交量
-- 参数: {trade_date:Date}

WITH
-- 计算个股5分钟数据，包含小时VWAP（过去12个区间=1小时）
stock_hourly_vwap AS (
    SELECT
        symbol,
        datetime,
        close,
        volume,
        amount,
        -- 计算过去1小时（12个5分钟区间）的VWAP
        -- VWAP = 滚动12个区间的总成交额 / 总成交量
        sum(amount) OVER (PARTITION BY symbol ORDER BY datetime ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) as hourly_amount,
        sum(volume) OVER (PARTITION BY symbol ORDER BY datetime ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) as hourly_volume,
        -- 区间序号
        row_number() OVER (PARTITION BY symbol ORDER BY datetime) as interval_idx
    FROM raw_stocks_5min
    WHERE toDate(datetime) = {trade_date:Date}
),

-- 过滤有足够历史数据且成交量>0的区间
valid_intervals AS (
    SELECT 
        symbol,
        datetime,
        close,
        volume,
        amount,
        -- 计算VWAP（避免除0）
        CASE 
            WHEN hourly_volume > 0 THEN hourly_amount / hourly_volume 
            ELSE NULL 
        END as hourly_vwap
    FROM stock_hourly_vwap
    WHERE interval_idx > 1  -- 至少有1个历史区间
      AND hourly_volume > 0
),

-- 过滤至少有12个区间VWAP的数据（确保是完整1小时）
full_hour_intervals AS (
    SELECT *
    FROM valid_intervals
    WHERE hourly_vwap IS NOT NULL
),

-- 判断强势条件：当前价 > 过去1小时VWAP
strength_judgment AS (
    SELECT
        symbol,
        datetime,
        close,
        volume,
        amount,
        hourly_vwap,
        -- 强势判断：当前价在VWAP上方
        CASE WHEN close > hourly_vwap THEN 1 ELSE 0 END as above_vwap,
        -- 强势程度：当前价高于VWAP的幅度
        (close - hourly_vwap) / hourly_vwap * 100 as strength_ratio,
        -- VWAP偏离度：VWAP与收盘价的偏离
        abs(close - hourly_vwap) / hourly_vwap * 100 as vwap_deviation
    FROM full_hour_intervals
),

-- 汇总每个股票的强势统计
hourly_vwap_strength AS (
    SELECT
        symbol,
        count(*) as valid_intervals,  -- 有效区间数
        sum(above_vwap) as above_vwap_count,  -- 在VWAP上方的区间数
        round(sum(above_vwap) * 100.0 / count(*), 2) as above_vwap_ratio,  -- 强势区间占比
        round(avg(strength_ratio), 4) as avg_strength_ratio,  -- 平均强势幅度
        round(max(strength_ratio), 4) as max_strength_ratio,  -- 最大强势幅度
        round(min(strength_ratio), 4) as min_strength_ratio,  -- 最小强势幅度（可能为负）
        round(avg(vwap_deviation), 4) as avg_vwap_deviation,  -- 平均偏离度
        -- 计算成交量加权的强势得分
        round(sum(above_vwap * volume) * 100.0 / sum(volume), 2) as volume_weighted_strength,
        -- 综合强势分数
        round(sum(above_vwap) * 100.0 / count(*) * (1 + sum(above_vwap * amount) / sum(amount)), 2) as composite_score
    FROM strength_judgment
    GROUP BY symbol
    HAVING count(*) >= 30  -- 至少要有30个有效区间（约3小时数据）
)

SELECT
    {trade_date:Date} as trade_date,
    symbol,
    valid_intervals,
    above_vwap_count as hourly_vwap_strength_score,
    above_vwap_ratio as strength_ratio_pct,
    round(avg_strength_ratio, 2) as avg_above_pct,
    round(max_strength_ratio, 2) as max_above_pct,
    round(min_strength_ratio, 2) as min_above_pct,
    round(avg_vwap_deviation, 2) as avg_vwap_deviation_pct,
    volume_weighted_strength,
    composite_score as hourly_vwap_composite_score
FROM hourly_vwap_strength
ORDER BY composite_score DESC, above_vwap_count DESC, above_vwap_ratio DESC
