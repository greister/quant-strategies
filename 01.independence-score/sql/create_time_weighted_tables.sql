-- 配置表：存储各种权重配置方案
CREATE TABLE IF NOT EXISTS score_weight_configs (
    config_name String,
    config_type Enum('time_based' = 1, 'risk_based' = 2, 'market_style' = 3, 'combined' = 4),
    granularity Enum('interval' = 1, 'hour_block' = 2),
    -- 归一化权重数组，索引对应 5 分钟区间序号 (0-47)
    weights Array(Float32),
    description String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    is_default UInt8 DEFAULT 0,
    
    PRIMARY KEY config_name
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY config_name;

-- 结果表：存储时间加权因子计算结果
CREATE TABLE IF NOT EXISTS independence_score_time_weighted (
    date Date,
    code String,
    name String,
    sector String,
    
    -- 原始分数
    raw_score Float32,
    
    -- 时间加权分数（核心结果）
    weighted_score Float32,
    
    -- 使用的配置
    config_name String,
    
    -- 逆势区间数量
    contra_count UInt16,
    
    -- 各区间详情（可选，用于详细分析）
    -- 存储格式: [(interval_idx, is_contra, weight), ...]
    contra_details Array(Tuple(UInt8, UInt8, Float32)),
    
    -- 元数据
    calculated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, code)
TTL date + INTERVAL 2 YEAR;

-- 便捷查询视图
CREATE OR REPLACE VIEW v_independence_time_weighted_leaders AS
SELECT 
    date,
    code,
    name,
    sector,
    raw_score,
    weighted_score,
    config_name,
    contra_count,
    -- 计算权重调整幅度（避免除零）
    CASE 
        WHEN raw_score > 0 THEN (weighted_score - raw_score) / raw_score 
        ELSE 0 
    END AS weight_adjustment_rate
FROM independence_score_time_weighted
WHERE date = (SELECT max(date) FROM independence_score_time_weighted);
