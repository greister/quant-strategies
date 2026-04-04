-- 动量因子策略 - 建表脚本
-- 创建结果表和视图

-- 结果表：存储每日动量因子计算结果
CREATE TABLE IF NOT EXISTS momentum_factor_daily (
    date Date,
    symbol String,
    name String,
    sector String,
    
    -- 动量得分（核心字段）
    momentum_score Float32,
    
    -- 原始价格数据
    price_current Float32,
    price_20d_ago Float32,
    
    -- 收益率
    return_1d Float32,
    return_5d Float32,
    return_10d Float32,
    return_20d Float32,
    
    -- 排名信息
    rank UInt32,
    rank_pct Float32,
    
    -- 元数据
    calculated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, symbol)
TTL date + INTERVAL 2 YEAR;

-- 视图：获取某日动量因子排名
CREATE OR REPLACE VIEW v_momentum_factor_leaders AS
SELECT 
    date,
    symbol,
    name,
    sector,
    momentum_score,
    return_20d,
    rank,
    rank_pct
FROM momentum_factor_daily FINAL
WHERE date = (SELECT max(date) FROM momentum_factor_daily)
ORDER BY momentum_score DESC;

-- 视图：按行业统计动量因子
CREATE OR REPLACE VIEW v_momentum_by_sector AS
SELECT 
    date,
    sector,
    count() as stock_count,
    avg(momentum_score) as avg_momentum,
    max(momentum_score) as max_momentum,
    argMax(symbol, momentum_score) as leader_symbol
FROM momentum_factor_daily FINAL
GROUP BY date, sector
ORDER BY avg_momentum DESC;

-- ============================================================
-- 低贝塔 + 相对强度混合策略表
-- ============================================================

-- 结果表：存储低贝塔抗跌 + 相对强度策略计算结果
CREATE TABLE IF NOT EXISTS low_beta_rs_factor_daily (
    date Date,
    symbol String,
    name String,
    sector String,
    
    -- 核心指标
    beta Float32,                       -- Beta值（防御性）
    relative_strength Float32,          -- 相对强度（进攻性）
    composite_score Float32,            -- 综合得分 (0-100)
    
    -- 收益数据
    return_1d Float32,                  -- 当日收益
    return_20d Float32,                 -- 20日收益
    ma20_deviation Float32,             -- 20日均线偏离
    
    -- 成交量数据
    volume_ratio Float32,               -- 成交量比率(相对于20日均量)
    intraday_range Float32,             -- 日内振幅
    
    -- 分项得分
    beta_score UInt8,                   -- 贝塔得分 (0-40)
    rs_score UInt8,                     -- 相对强度得分 (0-40)
    volume_score UInt8,                 -- 成交量得分 (0-20)
    
    -- 策略标签
    strategy_tag String,                -- 策略分类标签
    intraday_signal String,             -- 日内交易信号
    
    -- 排名信息
    rank UInt32,
    rank_pct Float32,
    
    -- 元数据
    calculated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, symbol)
TTL date + INTERVAL 2 YEAR;

-- 视图：获取某日低贝塔+RS策略排名
CREATE OR REPLACE VIEW v_low_beta_rs_leaders AS
SELECT 
    date,
    symbol,
    name,
    sector,
    beta,
    relative_strength,
    composite_score,
    return_1d,
    volume_ratio,
    strategy_tag,
    intraday_signal,
    rank,
    rank_pct
FROM low_beta_rs_factor_daily FINAL
WHERE date = (SELECT max(date) FROM low_beta_rs_factor_daily)
ORDER BY composite_score DESC;

-- 视图：按策略标签统计
CREATE OR REPLACE VIEW v_low_beta_rs_by_tag AS
SELECT 
    date,
    strategy_tag,
    count() as stock_count,
    avg(composite_score) as avg_score,
    avg(beta) as avg_beta,
    avg(relative_strength) as avg_rs,
    argMax(symbol, composite_score) as leader_symbol
FROM low_beta_rs_factor_daily FINAL
GROUP BY date, strategy_tag
ORDER BY avg_score DESC;
