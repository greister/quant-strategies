-- 可配置阈值的分时独立强度因子计算
-- 参数:
--   {trade_date:Date}          - 交易日期
--   {sector_threshold:Float64} - 板块下跌阈值，默认-0.5 (%)
--   {stock_threshold:Float64}  - 个股收益阈值，默认0 (%)
--   {excess_threshold:Float64} - 超额收益阈值，默认1 (%)
--
-- 推荐阈值组合:
--   严格模式: sector=-0.5, stock=0,   excess=1.0  (精选独立龙头)
--   中等模式: sector=-0.3, stock=-0.3, excess=0.5  (平衡质量与数量)
--   宽松模式: sector=-0.1, stock=0,   excess=0.3  (广泛捕捉信号)

WITH
-- 计算个股5分钟收益率
stock_returns AS (
    SELECT
        symbol,
        datetime,
        close,
        prev_close,
        (close - prev_close) / prev_close * 100 as stock_return
    FROM (
        SELECT
            symbol,
            datetime,
            close,
            lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as prev_close
        FROM raw_stocks_5min
        WHERE toDate(datetime) = {trade_date:Date}
    )
),

-- 获取股票板块归属
stock_with_sector AS (
    SELECT
        sr.symbol,
        sr.datetime,
        sr.stock_return,
        ss.sector_code
    FROM stock_returns sr
    INNER JOIN v_stock_sectors ss ON sr.symbol = ss.symbol
    WHERE sr.stock_return IS NOT NULL
      AND abs(sr.stock_return) < 50  -- 剔除异常收益率
),

-- 计算板块5分钟收益率
sector_returns AS (
    SELECT
        sector_code,
        datetime,
        avg(stock_return) as sector_return
    FROM stock_with_sector
    GROUP BY sector_code, datetime
),

-- 合并个股和板块收益率，计算超额收益和逆势标志
combined_data AS (
    SELECT
        sws.symbol,
        sws.sector_code,
        sws.datetime,
        sws.stock_return,
        sr.sector_return,
        sws.stock_return - sr.sector_return as excess_return,
        -- 可配置的逆势判断条件
        sr.sector_return < {sector_threshold:Float64} 
            AND (sws.stock_return > {stock_threshold:Float64} 
                 OR (sws.stock_return - sr.sector_return) > {excess_threshold:Float64}) as is_contra_move
    FROM stock_with_sector sws
    INNER JOIN sector_returns sr ON sws.sector_code = sr.sector_code AND sws.datetime = sr.datetime
    WHERE abs(sr.sector_return) < 50  -- 剔除异常板块收益
),

-- 统计每个股票的逆势区间数量
independence_score AS (
    SELECT
        symbol,
        sector_code,
        countIf(is_contra_move) as independence_score,
        count(*) as total_intervals,
        round(countIf(is_contra_move) * 100.0 / count(*), 2) as independence_ratio,
        avgIf(stock_return, is_contra_move) as avg_contra_return,
        maxIf(excess_return, is_contra_move) as max_excess_return
    FROM combined_data
    GROUP BY symbol, sector_code
)

SELECT
    {trade_date:Date} as trade_date,
    symbol,
    sector_code,
    independence_score,
    total_intervals,
    independence_ratio,
    round(avg_contra_return, 4) as avg_contra_return,
    round(max_excess_return, 4) as max_excess_return
FROM independence_score
WHERE independence_score > 0
ORDER BY independence_score DESC, independence_ratio DESC
