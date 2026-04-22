#!/bin/bash
set -e

# 独立强度因子批量计算脚本
# 计算股票在板块下跌时的逆势表现得分
# 数据源：stock_industry_mapping (通达信行业分类 T 级)

# 变量定义
DB_NAME="${CLICKHOUSE_DB:-tdx2db_rust}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-tdx2db}"
DATE="${1:-$(date +%Y-%m-%d)}"

echo "Calculating independence score for date: $DATE"

# 执行 INSERT INTO independence_score_daily
clickhouse-client --user="$CH_USER" --password="$CH_PASSWORD" --database="$DB_NAME" --param_trade_date="$DATE" -q "
INSERT INTO independence_score_daily
WITH
-- 每只股票取一个主行业分类（通达信 T 级行业，取最细分行业）
-- stock_industry_mapping.symbol 格式为纯代码如 000001，需转换为 sz000001
stock_sector_mapping AS (
    SELECT
        concat(
            multiIf(
                substring(symbol, 1, 2) IN ('00', '30', '15'), 'sz',
                substring(symbol, 1, 2) IN ('60', '68', '51'), 'sh',
                substring(symbol, 1, 2) IN ('82', '83', '87', '43', '92'), 'bj',
                'sz'
            ),
            symbol
        ) as symbol,
        industry_name as sector_code
    FROM (
        SELECT symbol, industry_code, industry_name,
            row_number() OVER (PARTITION BY symbol ORDER BY length(industry_code) DESC, industry_code) as rn
        FROM stock_industry_mapping
        WHERE industry_code LIKE 'T%'
          AND industry_code != 'T00'
          AND industry_name != ''
    )
    WHERE rn = 1
),

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
    INNER JOIN stock_sector_mapping ss ON sr.symbol = ss.symbol
    WHERE sr.stock_return IS NOT NULL
),

-- 计算板块5分钟收益率（板块内股票平均）
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
        sr.sector_return < -0.2 AND sws.stock_return > sr.sector_return as is_contra_move
    FROM stock_with_sector sws
    INNER JOIN sector_returns sr ON sws.sector_code = sr.sector_code AND sws.datetime = sr.datetime
),

-- 统计每个股票的逆势区间数量
independence_score AS (
    SELECT
        symbol,
        sector_code,
        countIf(is_contra_move) as ind_score,
        count(*) as total_intervals,
        round(countIf(is_contra_move) * 100.0 / count(*), 2) as independence_ratio,
        avgIf(stock_return, is_contra_move) as avg_contra_return,
        maxIf(excess_return, is_contra_move) as max_excess_return
    FROM combined_data
    GROUP BY symbol, sector_code
)

SELECT
    symbol,
    {trade_date:Date} as date,
    ind_score as score,
    ind_score as raw_score,
    1.0 as margin_weight,
    sector_code as sector,
    total_intervals as sector_stock_count,
    ind_score as contra_count,
    independence_ratio,
    avg_contra_return,
    max_excess_return,
    total_intervals,
    row_number() OVER (ORDER BY ind_score DESC) as rn
FROM independence_score
WHERE ind_score > 0
ORDER BY ind_score DESC, independence_ratio DESC
"

echo "Done. Top 10 scores:"

# 查询并显示当日 Top 10 结果
clickhouse-client --user="$CH_USER" --password="$CH_PASSWORD" --database="$DB_NAME" --param_trade_date="$DATE" -q "
SELECT
    date,
    symbol,
    sector,
    score,
    contra_count
FROM independence_score_daily
WHERE date = {trade_date:Date}
ORDER BY score DESC
LIMIT 10
FORMAT PrettyCompact
"
