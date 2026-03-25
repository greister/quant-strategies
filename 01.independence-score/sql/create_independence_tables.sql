-- 创建独立性评分日表
CREATE TABLE IF NOT EXISTS independence_score_daily
(
    symbol String,
    date Date,
    score Float64,
    raw_score Int32,
    margin_weight Float64 DEFAULT 1.0,
    sector String,
    sector_stock_count Int32,
    contra_count Int32 COMMENT '逆势区间数'
)
ENGINE = ReplacingMergeTree()
ORDER BY (symbol, date);

-- 创建独立性领导者视图
CREATE OR REPLACE VIEW v_independence_leaders AS
SELECT
    date,
    symbol,
    score,
    sector,
    contra_count,
    rank() OVER (PARTITION BY date ORDER BY score DESC) AS rank
FROM independence_score_daily
FINAL;
