-- 双因子组合策略 - 建表脚本
-- 创建结果表和视图

-- ============================================================
-- 主结果表：存储每日双因子组合计算结果
-- ============================================================

CREATE TABLE IF NOT EXISTS combined_factor_daily (
    date Date,
    symbol String,
    sector String,
    
    -- 原始因子分数
    independence_score Float32,
    momentum_score Float32,
    
    -- 排名分位（0-1，越小越好）
    independence_rank_pct Float32,
    momentum_rank_pct Float32,
    
    -- 综合得分
    combined_score Float32,
    
    -- 权重配置
    weight_ind Float32 COMMENT '独立强度权重',
    weight_mom Float32 COMMENT '动量权重',
    
    -- 排名信息
    rank UInt32,
    rank_pct Float32,
    
    -- 元数据
    calculated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, symbol)
TTL date + INTERVAL 2 YEAR;

-- ============================================================
-- 视图：获取某日双因子组合排名
-- ============================================================

CREATE OR REPLACE VIEW v_combined_factor_leaders AS
SELECT 
    date,
    symbol,
    sector,
    independence_score,
    momentum_score,
    combined_score,
    weight_ind,
    weight_mom,
    rank,
    rank_pct
FROM combined_factor_daily FINAL
WHERE date = (SELECT max(date) FROM combined_factor_daily)
ORDER BY combined_score DESC;

-- ============================================================
-- 视图：按板块统计双因子表现
-- ============================================================

CREATE OR REPLACE VIEW v_combined_factor_by_sector AS
SELECT 
    date,
    sector,
    count() as stock_count,
    avg(combined_score) as avg_combined_score,
    avg(independence_score) as avg_independence,
    avg(momentum_score) as avg_momentum,
    argMax(symbol, combined_score) as leader_symbol,
    max(combined_score) as max_combined_score
FROM combined_factor_daily FINAL
GROUP BY date, sector
ORDER BY avg_combined_score DESC;

-- ============================================================
-- 视图：双因子分布统计
-- ============================================================

CREATE OR REPLACE VIEW v_combined_factor_stats AS
SELECT 
    date,
    count() as total_stocks,
    avg(combined_score) as avg_score,
    stddevSamp(combined_score) as std_score,
    quantile(0.9)(combined_score) as score_90th,
    quantile(0.75)(combined_score) as score_75th,
    quantile(0.5)(combined_score) as score_median,
    quantile(0.25)(combined_score) as score_25th
FROM combined_factor_daily FINAL
GROUP BY date
ORDER BY date DESC;
