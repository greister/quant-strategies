-- 小时均线强势因子计算
-- 统计全天有多少个5分钟区间满足：当前价 > 过去1小时均价
-- 参数: {trade_date:Date}

WITH
-- 计算个股5分钟数据，包含小时均线（过去12个区间=1小时）
stock_hourly_ma AS (
    SELECT
        symbol,
        datetime,
        close,
        open,
        high,
        low,
        -- 计算过去1小时（12个5分钟区间）的均价
        avg(close) OVER (PARTITION BY symbol ORDER BY datetime ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) as hourly_ma,
        -- 计算过去1小时的最高价和最低价
        max(high) OVER (PARTITION BY symbol ORDER BY datetime ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) as hourly_high,
        min(low) OVER (PARTITION BY symbol ORDER BY datetime ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) as hourly_low,
        -- 区间序号，用于过滤前12个没有足够历史的区间
        row_number() OVER (PARTITION BY symbol ORDER BY datetime) as interval_idx
    FROM raw_stocks_5min
    WHERE toDate(datetime) = {trade_date:Date}
),

-- 过滤有足够历史数据的区间（至少1小时）
valid_intervals AS (
    SELECT *
    FROM stock_hourly_ma
    WHERE interval_idx > 12  -- 确保有12个区间的历史数据
      AND hourly_ma IS NOT NULL
      AND hourly_ma > 0
),

-- 判断强势条件：当前价 > 过去1小时均价
strength_judgment AS (
    SELECT
        symbol,
        datetime,
        close,
        hourly_ma,
        -- 强势判断：当前价在小时均线上方
        CASE WHEN close > hourly_ma THEN 1 ELSE 0 END as above_ma,
        -- 强势程度：当前价高于均线的幅度
        (close - hourly_ma) / hourly_ma * 100 as strength_ratio,
        -- 是否突破小时新高
        CASE WHEN close > hourly_high THEN 1 ELSE 0 END as break_high,
        -- 是否保持在小时区间上半部分
        CASE WHEN close > (hourly_high + hourly_low) / 2 THEN 1 ELSE 0 END as upper_half
    FROM valid_intervals
),

-- 汇总每个股票的强势统计
hourly_ma_strength AS (
    SELECT
        symbol,
        count(*) as valid_intervals,  -- 有效区间数（有1小时均线的）
        sum(above_ma) as above_ma_count,  -- 在均线上方的区间数
        round(sum(above_ma) * 100.0 / count(*), 2) as above_ma_ratio,  -- 强势区间占比
        round(avg(strength_ratio), 4) as avg_strength_ratio,  -- 平均强势幅度
        round(max(strength_ratio), 4) as max_strength_ratio,  -- 最大强势幅度
        sum(break_high) as break_high_count,  -- 突破小时新高次数
        sum(upper_half) as upper_half_count,  -- 在小时区间上半部分的次数
        round(sum(upper_half) * 100.0 / count(*), 2) as upper_half_ratio,
        -- 综合强势分数：均线上方占比 + 突破新高次数×2
        round(sum(above_ma) * 100.0 / count(*) + sum(break_high) * 2, 2) as composite_score
    FROM strength_judgment
    GROUP BY symbol
    HAVING count(*) >= 30  -- 至少要有30个有效区间（约3小时数据）
)

SELECT
    {trade_date:Date} as trade_date,
    symbol,
    valid_intervals,
    above_ma_count as hourly_ma_strength_score,
    above_ma_ratio as strength_ratio_pct,
    round(avg_strength_ratio, 2) as avg_above_pct,
    round(max_strength_ratio, 2) as max_above_pct,
    break_high_count,
    upper_half_count,
    upper_half_ratio as upper_half_pct,
    composite_score as hourly_ma_composite_score
FROM hourly_ma_strength
ORDER BY composite_score DESC, above_ma_count DESC, above_ma_ratio DESC
