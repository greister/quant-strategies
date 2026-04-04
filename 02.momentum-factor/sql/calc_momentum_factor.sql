-- 动量因子计算
-- 参数: trade_date (Date)

WITH
-- 获取股票名称和行业映射
stock_info AS (
    SELECT 
        s.symbol,
        g.name AS stock_name,
        i.block_name AS industry_name
    FROM stock_industry_mapping s
    LEFT JOIN gtja_stock_names g ON s.symbol = g.symbol
    LEFT JOIN gtja_industry_list i ON substring(s.industry_code, 3) = i.block_code
    WHERE i.block_name != ''
),

-- 获取当前价格
current_prices AS (
    SELECT
        symbol,
        close AS price_current,
        (close - open) / open * 100 AS return_1d
    FROM raw_stocks_daily
    WHERE date = {trade_date:Date}
),

-- 获取20日前价格（计算动量）
price_20d_ago AS (
    SELECT
        symbol,
        close AS price_20d_ago,
        (close - open) / open * 100 AS return_20d
    FROM raw_stocks_daily
    WHERE date = {trade_date:Date} - INTERVAL 20 DAY
),

-- 获取5日收益率
returns_5d AS (
    SELECT
        symbol,
        (close - argMin(open, date)) / argMin(open, date) * 100 AS return_5d
    FROM raw_stocks_daily
    WHERE date BETWEEN {trade_date:Date} - INTERVAL 5 DAY AND {trade_date:Date}
    GROUP BY symbol
),

-- 获取10日收益率
returns_10d AS (
    SELECT
        symbol,
        (close - argMin(open, date)) / argMin(open, date) * 100 AS return_10d
    FROM raw_stocks_daily
    WHERE date BETWEEN {trade_date:Date} - INTERVAL 10 DAY AND {trade_date:Date}
    GROUP BY symbol
),

-- 计算动量因子
momentum_calc AS (
    SELECT
        cp.symbol,
        s.stock_name,
        s.industry_name AS sector,
        cp.price_current,
        p20.price_20d_ago,
        cp.return_1d,
        r5.return_5d,
        r10.return_10d,
        p20.return_20d,
        -- 动量得分：综合20日、10日、5日收益率
        (p20.return_20d * 0.5 + r10.return_10d * 0.3 + r5.return_5d * 0.2) AS momentum_score
    FROM current_prices cp
    JOIN stock_info s ON cp.symbol = s.symbol
    LEFT JOIN price_20d_ago p20 ON cp.symbol = p20.symbol
    LEFT JOIN returns_5d r5 ON cp.symbol = r5.symbol
    LEFT JOIN returns_10d r10 ON cp.symbol = r10.symbol
    WHERE p20.price_20d_ago IS NOT NULL
)

-- 插入结果表
INSERT INTO momentum_factor_daily (
    date, symbol, name, sector,
    momentum_score,
    price_current, price_20d_ago,
    return_1d, return_5d, return_10d, return_20d
)
SELECT
    {trade_date:Date} AS date,
    symbol,
    stock_name AS name,
    sector,
    toFloat32(momentum_score) AS momentum_score,
    toFloat32(price_current) AS price_current,
    toFloat32(price_20d_ago) AS price_20d_ago,
    toFloat32(return_1d) AS return_1d,
    toFloat32(return_5d) AS return_5d,
    toFloat32(return_10d) AS return_10d,
    toFloat32(return_20d) AS return_20d
FROM momentum_calc
WHERE momentum_score IS NOT NULL
ORDER BY momentum_score DESC;

-- 更新排名信息
ALTER TABLE momentum_factor_daily
    UPDATE 
        rank = r.rank,
        rank_pct = r.rank_pct
    FROM (
        SELECT 
            symbol,
            row_number() OVER (ORDER BY momentum_score DESC) as rank,
            row_number() OVER (ORDER BY momentum_score DESC) / count() OVER () as rank_pct
        FROM momentum_factor_daily
        WHERE date = {trade_date:Date}
    ) r
    WHERE momentum_factor_daily.symbol = r.symbol
      AND momentum_factor_daily.date = {trade_date:Date};
