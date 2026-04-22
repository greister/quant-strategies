-- S09-S12 高阶因子策略表结构

-- 通用结果表（S09/S10/S12 单日结果）
CREATE TABLE IF NOT EXISTS independence_score_advanced (
    date Date,
    symbol String,
    name String,
    sector String,
    strategy String,          -- 'S09', 'S10', 'S12'
    score Float32,             -- 标准化得分
    raw_metrics String,        -- JSON: 各维度原始值
    rank UInt16 DEFAULT 0,     -- 当日排名
    calculated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, strategy, symbol)
TTL date + INTERVAL 2 YEAR;

-- S11 周频一致性结果表
CREATE TABLE IF NOT EXISTS independence_score_weekly (
    week_start Date,
    week_end Date,
    symbol String,
    name String,
    sector String,
    appear_days UInt8,         -- 入选天数
    avg_rank Float32,          -- 平均排名
    avg_score Float32,         -- 平均得分
    score_cv Float32,          -- 得分变异系数
    consistency_score Float32, -- 综合得分
    calculated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(week_start)
ORDER BY (week_end, symbol)
TTL week_end + INTERVAL 2 YEAR;
