-- 低贝塔混合策略 - 建表脚本
-- 日内低贝塔抗跌 + 相对强度混合策略

-- 1. 低贝塔股票池（日线预筛选结果）
CREATE TABLE IF NOT EXISTS low_beta_pool_daily (
    date Date,
    symbol String,
    name String,
    sector String,
    
    -- 贝塔值
    beta Float32,
    beta_500 Float32,  -- 对中证500的贝塔
    
    -- 统计指标
    anti_fall_days UInt8,  -- 抗跌次数（过去20日）
    avg_return_20d Float32,  -- 20日平均收益
    volatility_20d Float32,  -- 20日波动率
    
    -- 元数据
    calculated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, symbol)
TTL date + INTERVAL 2 YEAR;

-- 2. 混合策略结果表（低贝塔 + 5分钟相对强度）
CREATE TABLE IF NOT EXISTS low_beta_hybrid_daily (
    date Date,
    symbol String,
    name String,
    sector String,
    
    -- 低贝塔筛选信息
    beta Float32,
    anti_fall_days UInt8,
    
    -- 独立强度得分（5分钟计算）
    raw_score Float32,
    weighted_score Float32,
    config_name String,
    
    -- 逆势区间统计
    contra_count UInt16,
    avg_contra_return Float32,
    
    -- 排名信息
    rank UInt32,
    rank_pct Float32,
    
    -- 综合评分
    hybrid_score Float32,  -- 综合得分 = 低贝塔得分 + 相对强度得分
    
    -- 元数据
    calculated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, symbol, config_name)
TTL date + INTERVAL 2 YEAR;

-- 3. 视图：获取某日低贝塔混合策略TOP标的
CREATE OR REPLACE VIEW v_low_beta_hybrid_leaders AS
SELECT 
    date,
    symbol,
    name,
    sector,
    beta,
    anti_fall_days,
    raw_score,
    weighted_score,
    hybrid_score,
    rank,
    rank_pct
FROM low_beta_hybrid_daily FINAL
WHERE date = (SELECT max(date) FROM low_beta_hybrid_daily)
  AND config_name = 'evening_focus'
ORDER BY hybrid_score DESC;

-- 4. 视图：按行业统计
CREATE OR REPLACE VIEW v_low_beta_hybrid_by_sector AS
SELECT 
    date,
    sector,
    count() as stock_count,
    avg(beta) as avg_beta,
    avg(raw_score) as avg_score,
    max(raw_score) as max_score,
    argMax(symbol, hybrid_score) as leader_symbol
FROM low_beta_hybrid_daily FINAL
WHERE config_name = 'evening_focus'
GROUP BY date, sector
ORDER BY avg_score DESC;

-- 5. 中证500指数日收益表（用于计算贝塔）
CREATE TABLE IF NOT EXISTS index_daily_returns (
    date Date,
    index_code String,  -- '000905' 中证500
    close Float64,
    open Float64,
    high Float64,
    low Float64,
    return_pct Float32,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (index_code, date)
TTL date + INTERVAL 3 YEAR;
