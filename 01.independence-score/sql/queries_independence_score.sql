-- 独立强度评分查询示例
-- 用于分析股票在板块下跌时的逆势表现

-- ============================================
-- 查询 1: 某日独立强度排名
-- 查询指定日期的独立强度排名，查看当日哪些股票表现最独立
-- 参数: {query_date:Date} - 查询日期
-- ============================================
SELECT
    symbol,
    score,
    sector,
    contra_count,
    rank() OVER (ORDER BY score DESC) AS rank
FROM independence_score_daily
WHERE date = {query_date:Date}
ORDER BY score DESC
LIMIT 20;

-- ============================================
-- 查询 2: 某股票历史独立强度走势
-- 查询指定股票的历史独立强度变化趋势
-- 参数: {stock_symbol:String} - 股票代码
-- ============================================
SELECT
    date,
    score,
    contra_count,
    sector
FROM independence_score_daily
WHERE symbol = {stock_symbol:String}
ORDER BY date DESC
LIMIT 60;

-- ============================================
-- 查询 3: 板块内独立强度排名
-- 查询指定日期和板块内的独立强度排名
-- 参数: {query_date:Date} - 查询日期
--         {sector_name:String} - 板块名称
-- ============================================
SELECT
    symbol,
    score,
    contra_count,
    rank() OVER (PARTITION BY sector ORDER BY score DESC) AS sector_rank
FROM independence_score_daily
WHERE date = {query_date:Date}
  AND sector = {sector_name:String}
ORDER BY score DESC;

-- ============================================
-- 查询 4: 独立强度连续高分股票（近 5 日）
-- 找出近 5 个交易日独立强度持续保持高分的股票
-- 筛选条件: 最低分 > 2（持续保持一定独立性）
-- ============================================
WITH recent_scores AS (
    SELECT
        symbol,
        groupArray(score) AS score_history,
        arrayAvg(score_history) AS avg_score,
        arrayMin(score_history) AS min_score,
        arrayMax(score_history) AS max_score,
        length(score_history) AS days_count
    FROM independence_score_daily
    WHERE date >= today() - 5
    GROUP BY symbol
    HAVING length(score_history) >= 3  -- 至少3个交易日有数据
)
SELECT
    symbol,
    score_history,
    round(avg_score, 2) AS avg_score,
    min_score,
    max_score,
    days_count
FROM recent_scores
WHERE min_score > 2  -- 持续保持一定独立性
ORDER BY avg_score DESC
LIMIT 50;
