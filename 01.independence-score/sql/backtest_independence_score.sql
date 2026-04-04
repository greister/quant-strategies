-- 独立强度因子历史回测
-- 回测逻辑：选取独立强度高分股票，计算未来 N 日收益

-- ============================================================
-- 参数设置
-- ============================================================
-- 回测日期范围
SET start_date = '2025-01-01';
SET end_date = '2025-03-20';

-- 选股阈值：独立强度分数 >= 3
SET score_threshold = 3;

-- 持有期（天）
SET hold_days = 5;

-- ============================================================
-- 1. 创建回测信号表
-- 选取每日独立强度 >= 阈值的股票作为买入信号
-- ============================================================

CREATE TABLE IF NOT EXISTS backtest_signals (
    signal_date Date,
    symbol String,
    score Float64,
    raw_score Int32,
    sector String,
    entry_price Float64  -- 信号日收盘价（后复权）
) ENGINE = MergeTree()
ORDER BY (signal_date, symbol);

-- 插入回测信号
INSERT INTO backtest_signals
WITH
-- 获取信号日的后复权收盘价
signal_with_price AS (
    SELECT
        i.date as signal_date,
        i.symbol,
        i.score,
        i.raw_score,
        i.sector,
        d.close as entry_price
    FROM independence_score_daily i
    INNER JOIN raw_stocks_daily d
        ON i.symbol = d.symbol AND i.date = d.date
    WHERE i.date >= {start_date:Date} AND i.date <= {end_date:Date}
      AND i.score >= {score_threshold:Float64}
)
SELECT * FROM signal_with_price;

-- ============================================================
-- 2. 计算未来 N 日收益
-- ============================================================

CREATE TABLE IF NOT EXISTS backtest_returns (
    signal_date Date,
    symbol String,
    score Float64,
    sector String,
    entry_price Float64,
    exit_price Float64,
    hold_days Int32,
    return_rate Float64,  -- 收益率
    return_annualized Float64,  -- 年化收益率
    max_drawdown Float64,  -- 最大回撤
    hit_target Bool,  -- 是否盈利
    sector_return Float64  -- 同期板块收益（对比）
) ENGINE = MergeTree()
ORDER BY (signal_date, symbol);

-- 插入回测收益数据
INSERT INTO backtest_returns
WITH
-- 获取未来 N 日的价格和板块收益
future_data AS (
    SELECT
        s.signal_date,
        s.symbol,
        s.score,
        s.sector,
        s.entry_price,
        -- 未来第 N 天的收盘价
        argMax(d.close, d.date) as exit_price,
        -- 持有期间最大价格（用于计算最大回撤）
        max(d.high) as max_price,
        -- 持有期间最小价格
        min(d.low) as min_price,
        -- 计算天数
        count() as actual_hold_days
    FROM backtest_signals s
    INNER JOIN raw_stocks_daily d
        ON s.symbol = d.symbol
    WHERE d.date > s.signal_date
      AND d.date <= s.signal_date + INTERVAL {hold_days:Int32} DAY
    GROUP BY s.signal_date, s.symbol, s.score, s.sector, s.entry_price
),
-- 计算板块同期收益
sector_returns AS (
    SELECT
        s.signal_date,
        s.sector,
        avg(sr.sector_return) as sector_return
    FROM backtest_signals s
    LEFT JOIN (
        -- 计算板块日收益
        SELECT
            date,
            sector,
            avg((close - open) / open * 100) as sector_return
        FROM raw_stocks_daily d
        JOIN v_stock_sectors ss ON d.symbol = ss.symbol
        GROUP BY date, sector
    ) sr ON s.sector = sr.sector
        AND sr.date > s.signal_date
        AND sr.date <= s.signal_date + INTERVAL {hold_days:Int32} DAY
    GROUP BY s.signal_date, s.sector
)
SELECT
    f.signal_date,
    f.symbol,
    f.score,
    f.sector,
    f.entry_price,
    f.exit_price,
    {hold_days:Int32} as hold_days,
    -- 收益率
    (f.exit_price - f.entry_price) / f.entry_price * 100 as return_rate,
    -- 年化收益率（假设 252 个交易日）
    (f.exit_price - f.entry_price) / f.entry_price * 100 * 252 / {hold_days:Int32} as return_annualized,
    -- 最大回撤
    (f.min_price - f.max_price) / f.max_price * 100 as max_drawdown,
    -- 是否盈利
    f.exit_price > f.entry_price as hit_target,
    -- 板块收益
    COALESCE(sr.sector_return, 0) as sector_return
FROM future_data f
LEFT JOIN sector_returns sr
    ON f.signal_date = sr.signal_date AND f.sector = sr.sector;

-- ============================================================
-- 3. 回测统计汇总
-- ============================================================

CREATE TABLE IF NOT EXISTS backtest_summary (
    hold_days Int32,
    score_threshold Float64,
    total_signals Int32,
    win_count Int32,
    loss_count Int32,
    win_rate Float64,
    avg_return Float64,
    avg_annualized_return Float64,
    max_return Float64,
    min_return Float64,
    avg_max_drawdown Float64,
    avg_sector_return Float64,
    excess_return Float64,
    sharpe_ratio Float64
) ENGINE = MergeTree()
ORDER BY (hold_days, score_threshold);

-- 插入汇总统计
INSERT INTO backtest_summary
SELECT
    {hold_days:Int32} as hold_days,
    {score_threshold:Float64} as score_threshold,
    count() as total_signals,
    countIf(return_rate > 0) as win_count,
    countIf(return_rate <= 0) as loss_count,
    round(countIf(return_rate > 0) * 100.0 / count(), 2) as win_rate,
    round(avg(return_rate), 4) as avg_return,
    round(avg(return_annualized), 4) as avg_annualized_return,
    round(max(return_rate), 4) as max_return,
    round(min(return_rate), 4) as min_return,
    round(avg(max_drawdown), 4) as avg_max_drawdown,
    round(avg(sector_return), 4) as avg_sector_return,
    round(avg(return_rate) - avg(sector_return), 4) as excess_return,
    -- 简化夏普比率（假设无风险利率 3%）
    round((avg(return_annualized) - 3) / stddevSamp(return_annualized), 4) as sharpe_ratio
FROM backtest_returns;

-- ============================================================
-- 4. 查询回测结果
-- ============================================================

-- 查看汇总统计
SELECT * FROM backtest_summary
ORDER BY hold_days, score_threshold;

-- 查看详细信号列表
SELECT
    signal_date,
    symbol,
    score,
    sector,
    round(return_rate, 2) as return_rate,
    round(sector_return, 2) as sector_return,
    round(return_rate - sector_return, 2) as excess_return,
    hit_target
FROM backtest_returns
ORDER BY signal_date DESC, score DESC
LIMIT 50;

-- 按板块统计
SELECT
    sector,
    count() as signal_count,
    round(avg(return_rate), 2) as avg_return,
    round(avg(sector_return), 2) as avg_sector_return,
    round(avg(return_rate - sector_return), 2) as avg_excess,
    round(countIf(hit_target) * 100.0 / count(), 2) as win_rate
FROM backtest_returns
GROUP BY sector
ORDER BY avg_excess DESC;

-- 月度收益分布
SELECT
    toYYYYMM(signal_date) as month,
    count() as signals,
    round(avg(return_rate), 2) as avg_return,
    round(avg(sector_return), 2) as sector_return,
    round(avg(return_rate - sector_return), 2) as excess
FROM backtest_returns
GROUP BY month
ORDER BY month;
