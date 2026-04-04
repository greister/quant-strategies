-- 低贝塔抗跌 + 相对强度混合策略计算
-- 参数: trade_date (Date)

WITH
-- 获取股票名称和行业映射
stock_info AS (
    SELECT 
        s.symbol,
        g.name AS stock_name,
        i.block_name AS industry_name
    FROM v_stock_industry_mapping s
    LEFT JOIN v_gtja_stock_names g ON s.symbol = g.symbol
    LEFT JOIN v_gtja_industry_list i ON substring(s.industry_code, 3) = i.block_code
    WHERE i.block_name != ''
),

-- 获取市场基准收益（沪深300）
market_returns AS (
    SELECT 
        date,
        close,
        (close - lag(close) OVER (ORDER BY date)) / lag(close) OVER (ORDER BY date) AS market_return
    FROM v_raw_index_daily
    WHERE symbol = '000300.SH'
      AND date BETWEEN {trade_date:Date} - INTERVAL 120 DAY AND {trade_date:Date}
),

-- 计算个股的日收益率（用于Beta计算）
stock_daily_returns AS (
    SELECT 
        symbol,
        date,
        close,
        (close - lag(close) OVER (PARTITION BY symbol ORDER BY date)) / lag(close) OVER (PARTITION BY symbol ORDER BY date) AS stock_return
    FROM raw_stocks_daily
    WHERE date BETWEEN {trade_date:Date} - INTERVAL 120 DAY AND {trade_date:Date}
),

-- 计算Beta值（60日滚动）
beta_calc AS (
    SELECT 
        sdr.symbol,
        -- Beta = Cov(个股收益, 市场收益) / Var(市场收益)
        covarSamp(sdr.stock_return, mr.market_return) / varSamp(mr.market_return) AS beta
    FROM stock_daily_returns sdr
    JOIN market_returns mr ON sdr.date = mr.date
    WHERE sdr.date BETWEEN {trade_date:Date} - INTERVAL 60 DAY AND {trade_date:Date}
      AND sdr.stock_return IS NOT NULL
      AND mr.market_return IS NOT NULL
    GROUP BY sdr.symbol
    HAVING count() >= 40  -- 确保有足够数据点
),

-- 获取当前价格和当日数据
current_data AS (
    SELECT
        symbol,
        close AS price_current,
        open AS price_open,
        high AS price_high,
        low AS price_low,
        volume,
        (close - open) / open * 100 AS return_1d,
        (high - low) / open * 100 AS intraday_range
    FROM raw_stocks_daily
    WHERE date = {trade_date:Date}
),

-- 获取20日均价（用于相对强度计算）
avg_20d AS (
    SELECT
        symbol,
        avg(close) AS avg_price_20d
    FROM raw_stocks_daily
    WHERE date BETWEEN {trade_date:Date} - INTERVAL 20 DAY AND {trade_date:Date}
    GROUP BY symbol
),

-- 获取市场基准20日均价
market_avg_20d AS (
    SELECT avg(close) AS avg_price_20d
    FROM v_raw_index_daily
    WHERE symbol = '000300.SH'
      AND date BETWEEN {trade_date:Date} - INTERVAL 20 DAY AND {trade_date:Date}
),

-- 获取市场当前价格
market_current AS (
    SELECT close AS price_current
    FROM v_raw_index_daily
    WHERE symbol = '000300.SH'
      AND date = {trade_date:Date}
),

-- 获取个股20日收益率
returns_20d AS (
    SELECT
        symbol,
        (close - argMin(open, date)) / argMin(open, date) * 100 AS return_20d
    FROM raw_stocks_daily
    WHERE date BETWEEN {trade_date:Date} - INTERVAL 20 DAY AND {trade_date:Date}
    GROUP BY symbol
),

-- 获取市场20日收益率
market_return_20d AS (
    SELECT
        (close - argMin(open, date)) / argMin(open, date) * 100 AS return_20d
    FROM v_raw_index_daily
    WHERE symbol = '000300.SH'
      AND date BETWEEN {trade_date:Date} - INTERVAL 20 DAY AND {trade_date:Date}
),

-- 计算20日平均成交量
volume_20d AS (
    SELECT
        symbol,
        avg(volume) AS avg_volume_20d
    FROM raw_stocks_daily
    WHERE date BETWEEN {trade_date:Date} - INTERVAL 20 DAY AND {trade_date:Date}
    GROUP BY symbol
),

-- 综合计算
strategy_calc AS (
    SELECT
        cd.symbol,
        si.stock_name,
        si.industry_name AS sector,
        
        -- 基础价格数据
        cd.price_current,
        cd.price_open,
        cd.return_1d,
        cd.intraday_range,
        cd.volume,
        
        -- Beta值（防御性指标）
        toFloat32(b.beta) AS beta,
        
        -- 相对强度指标
        -- 公式: (个股20日收益 - 市场20日收益) / |市场20日收益|
        toFloat32(r20.return_20d) AS return_20d,
        toFloat32((r20.return_20d - mr20.return_20d) / abs(mr20.return_20d + 0.0001)) AS relative_strength,
        
        -- 20日均线偏离
        toFloat32((cd.price_current - a20.avg_price_20d) / a20.avg_price_20d * 100) AS ma20_deviation,
        
        -- 成交量比率
        toFloat32(v20.avg_volume_20d) AS avg_volume_20d,
        toFloat32(cd.volume / v20.avg_volume_20d) AS volume_ratio,
        
        -- 综合得分计算
        -- 低贝塔：Beta < 0.8 给高分，Beta > 1.2 给低分
        CASE 
            WHEN b.beta < 0.5 THEN 40
            WHEN b.beta < 0.8 THEN 30
            WHEN b.beta < 1.0 THEN 20
            WHEN b.beta < 1.2 THEN 10
            ELSE 0
        END AS beta_score,
        
        -- 相对强度：RS > 1.0 表示跑赢市场
        CASE 
            WHEN r20.return_20d > mr20.return_20d * 1.5 THEN 40
            WHEN r20.return_20d > mr20.return_20d * 1.2 THEN 30
            WHEN r20.return_20d > mr20.return_20d THEN 20
            WHEN r20.return_20d > mr20.return_20d * 0.5 THEN 10
            ELSE 0
        END AS rs_score,
        
        -- 量价配合：放量上涨加分
        CASE 
            WHEN cd.volume > v20.avg_volume_20d * 1.5 AND cd.return_1d > 0 THEN 20
            WHEN cd.volume > v20.avg_volume_20d * 1.2 AND cd.return_1d > 0 THEN 15
            WHEN cd.volume > v20.avg_volume_20d AND cd.return_1d > 0 THEN 10
            ELSE 0
        END AS volume_score
        
    FROM current_data cd
    JOIN stock_info si ON cd.symbol = si.symbol
    LEFT JOIN beta_calc b ON cd.symbol = b.symbol
    LEFT JOIN avg_20d a20 ON cd.symbol = a20.symbol
    LEFT JOIN returns_20d r20 ON cd.symbol = r20.symbol
    LEFT JOIN volume_20d v20 ON cd.symbol = v20.symbol
    CROSS JOIN market_return_20d mr20
    WHERE b.beta IS NOT NULL
      AND r20.return_20d IS NOT NULL
),

-- 计算综合得分并筛选
final_scores AS (
    SELECT
        *,
        -- 综合得分 (0-100)
        toFloat32(beta_score + rs_score + volume_score) AS composite_score,
        
        -- 策略标签
        CASE 
            WHEN beta < 0.8 AND relative_strength > 0 THEN '低贝塔强势'
            WHEN beta < 0.8 AND relative_strength <= 0 THEN '低贝塔防守'
            WHEN beta >= 0.8 AND relative_strength > 0 THEN '高贝塔进攻'
            ELSE '弱势'
        END AS strategy_tag,
        
        -- 日内交易信号
        CASE 
            WHEN beta < 0.8 
                 AND relative_strength > 0 
                 AND volume_ratio > 1.2 
                 AND return_1d BETWEEN 0 AND 3 
            THEN '买入信号'
            WHEN return_1d > 5 OR volume_ratio < 0.8 
            THEN '观望/卖出'
            ELSE '持有'
        END AS intraday_signal
        
    FROM strategy_calc
    WHERE beta < 1.2  -- 过滤掉高波动股票
)

-- 插入结果表
INSERT INTO low_beta_rs_factor_daily (
    date, symbol, name, sector,
    beta, relative_strength, composite_score,
    return_1d, return_20d, ma20_deviation,
    volume_ratio, intraday_range,
    beta_score, rs_score, volume_score,
    strategy_tag, intraday_signal
)
SELECT
    {trade_date:Date} AS date,
    symbol,
    stock_name AS name,
    sector,
    beta,
    relative_strength,
    composite_score,
    return_1d,
    return_20d,
    ma20_deviation,
    volume_ratio,
    intraday_range,
    beta_score,
    rs_score,
    volume_score,
    strategy_tag,
    intraday_signal
FROM final_scores
WHERE composite_score IS NOT NULL
ORDER BY composite_score DESC;

-- 更新排名
ALTER TABLE low_beta_rs_factor_daily
    UPDATE 
        rank = r.rank,
        rank_pct = r.rank_pct
    FROM (
        SELECT 
            symbol,
            row_number() OVER (ORDER BY composite_score DESC) as rank,
            row_number() OVER (ORDER BY composite_score DESC) / count() OVER () as rank_pct
        FROM low_beta_rs_factor_daily
        WHERE date = {trade_date:Date}
    ) r
    WHERE low_beta_rs_factor_daily.symbol = r.symbol
      AND low_beta_rs_factor_daily.date = {trade_date:Date};
