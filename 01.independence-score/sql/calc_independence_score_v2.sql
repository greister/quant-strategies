-- ============================================================================
-- 分时独立强度因子计算 - 改进版 (V2)
-- 新增功能:
--   1. 成交量加权 (Volume-Weighted): 高成交量区间的逆势表现权重更高
--   2. 波动率调整 (Volatility-Adjusted): 根据个股波动率动态调整得分
-- 计算股票在板块下跌时的逆势表现得分
-- 参数: {trade_date:Date} - 交易日期
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 阈值参数说明:
--   sector_return_threshold: -0.5%  - 板块下跌阈值，低于此值视为板块下跌
--   stock_return_threshold:  0%     - 个股上涨阈值，高于此值视为个股上涨
--   excess_return_threshold: 1%     - 超额收益阈值，高于此值视为显著跑赢板块
--   volume_weight_factor:    0.3    - 成交量加权系数 (0-1之间，越大成交量影响越大)
--   volatility_lookback:     20     - 波动率计算回看天数
--   volatility_target:       2.0    - 目标日波动率(%)，用于标准化调整
-- ----------------------------------------------------------------------------

WITH
-- ============================================================================
-- 第一步: 计算个股5分钟收益率及成交量数据
-- ============================================================================
stock_returns AS (
    SELECT
        symbol,
        datetime,
        close,
        volume,
        prev_close,
        -- 计算个股5分钟收益率(%)
        (close - prev_close) / prev_close * 100 as stock_return,
        -- 计算5分钟成交额(价格×成交量)，用于后续成交量加权
        close * volume as turnover
    FROM (
        SELECT
            symbol,
            datetime,
            close,
            volume,
            -- 使用窗口函数获取前一时刻收盘价
            lagInFrame(close) OVER (
                PARTITION BY symbol, toDate(datetime) 
                ORDER BY datetime 
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            ) as prev_close
        FROM raw_stocks_5min
        WHERE toDate(datetime) = {trade_date:Date}
    )
    WHERE prev_close IS NOT NULL  -- 过滤首条记录(无前值)
),

-- ============================================================================
-- 第二步: 计算当日个股成交量分位数(用于成交量加权)
-- 原理: 将当日成交量按大小排序，计算每个时刻的成交量百分位
-- ============================================================================
volume_stats AS (
    SELECT
        symbol,
        datetime,
        volume,
        turnover,
        -- 计算该时刻成交量在当日的百分位排名 (0-1之间)
        -- rank() 计算排名，count(*) 计算总区间数
        (rank() OVER (PARTITION BY symbol ORDER BY volume) - 1.0) / 
        NULLIF(count(*) OVER (PARTITION BY symbol) - 1, 0) as volume_percentile,
        -- 计算相对平均成交量的比率
        volume / NULLIF(avg(volume) OVER (PARTITION BY symbol), 0) as volume_ratio
    FROM stock_returns
),

-- ============================================================================
-- 第三步: 计算个股历史波动率(20日回看)
-- 原理: 使用日收益率的标准差作为波动率度量，用于风险调整
-- ============================================================================
stock_volatility AS (
    SELECT
        symbol,
        -- 年化波动率 = 日收益率标准差 × sqrt(252)
        stdDevSampPop((close - prev_close) / prev_close * 100) * sqrt(252) as volatility_annual,
        -- 日波动率 = 5分钟收益率标准差 × sqrt(48) (每日48个5分钟区间)
        stdDevSampPop((close - prev_close) / prev_close * 100) * sqrt(48) as volatility_daily,
        count(*) as sample_count
    FROM (
        -- 获取回看期内日K线数据
        SELECT
            symbol,
            close,
            lagInFrame(close) OVER (
                PARTITION BY symbol 
                ORDER BY datetime 
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            ) as prev_close
        FROM raw_stocks_5min
        WHERE toDate(datetime) BETWEEN 
            {trade_date:Date} - INTERVAL 20 DAY AND {trade_date:Date}
    )
    WHERE prev_close IS NOT NULL
    GROUP BY symbol
    HAVING count(*) >= 10  -- 确保有足够样本量
),

-- ============================================================================
-- 第四步: 获取股票板块归属
-- 使用 v_stock_sectors 视图兼容 tdx2db-rust 的 stock_sectors 表结构
-- ============================================================================
stock_with_sector AS (
    SELECT
        sr.symbol,
        sr.datetime,
        sr.stock_return,
        sr.volume,
        sr.turnover,
        vs.volume_percentile,
        vs.volume_ratio,
        ss.sector_code
    FROM stock_returns sr
    INNER JOIN v_stock_sectors ss ON sr.symbol = ss.symbol
    INNER JOIN volume_stats vs ON sr.symbol = vs.symbol AND sr.datetime = vs.datetime
    WHERE sr.stock_return IS NOT NULL
),

-- ============================================================================
-- 第五步: 计算板块5分钟收益率及板块成交量
-- 原理: 板块收益率 = 板块内个股收益率的简单平均
--       板块成交量 = 板块内个股成交量的总和
-- ============================================================================
sector_returns AS (
    SELECT
        sector_code,
        datetime,
        -- 板块收益率 = 个股收益率平均
        avg(stock_return) as sector_return,
        -- 板块总成交量
        sum(volume) as sector_volume,
        -- 板块总成交额
        sum(turnover) as sector_turnover,
        -- 板块内上涨股票数
        countIf(stock_return > 0) as rising_count,
        -- 板块内下跌股票数
        countIf(stock_return < 0) as falling_count,
        -- 板块内股票总数
        count(*) as total_stocks
    FROM stock_with_sector
    GROUP BY sector_code, datetime
),

-- ============================================================================
-- 第六步: 合并个股和板块数据，计算超额收益、逆势标志及成交量权重
-- ============================================================================
combined_data AS (
    SELECT
        sws.symbol,
        sws.sector_code,
        sws.datetime,
        sws.stock_return,
        sws.volume,
        sws.turnover,
        sws.volume_percentile,
        sws.volume_ratio,
        sr.sector_return,
        sr.sector_volume,
        sr.sector_turnover,
        sr.rising_count,
        sr.falling_count,
        sr.total_stocks,
        -- 超额收益 = 个股收益率 - 板块收益率
        sws.stock_return - sr.sector_return as excess_return,
        -- 逆势标志: 板块下跌(<-0.5%)且(个股上涨(>0%)或超额收益>1%)
        sr.sector_return < -0.5 AND 
            (sws.stock_return > 0 OR (sws.stock_return - sr.sector_return) > 1) as is_contra_move,
        -- 基础得分: 满足逆势条件得1分，否则0分
        if(sr.sector_return < -0.5 AND 
            (sws.stock_return > 0 OR (sws.stock_return - sr.sector_return) > 1), 1, 0) as base_score,
        -- 成交量权重因子: 使用成交量百分位进行加权
        -- 公式: 1 + volume_weight_factor × (volume_percentile - 0.5) × 2
        -- 结果范围: [1-volume_weight_factor, 1+volume_weight_factor]
        1 + 0.3 * (sws.volume_percentile - 0.5) * 2 as volume_weight,
        -- 成交活跃度得分: 相对板块平均的活跃度
        sws.volume / NULLIF(sr.sector_volume / sr.total_stocks, 0) as relative_volume
    FROM stock_with_sector sws
    INNER JOIN sector_returns sr 
        ON sws.sector_code = sr.sector_code 
        AND sws.datetime = sr.datetime
),

-- ============================================================================
-- 第七步: 计算成交量加权的独立强度得分
-- 原理: 高成交量时刻的逆势表现赋予更高权重
-- ============================================================================
volume_weighted_scores AS (
    SELECT
        symbol,
        sector_code,
        -- 基础逆势区间计数(未加权)
        countIf(is_contra_move) as raw_contra_count,
        -- 总区间数
        count(*) as total_intervals,
        -- 成交量加权逆势得分 = Σ(基础得分 × 成交量权重)
        sum(base_score * volume_weight) as volume_weighted_score,
        -- 原始独立强度比例(%)
        round(countIf(is_contra_move) * 100.0 / count(*), 2) as independence_ratio,
        -- 平均成交量权重
        avg(volume_weight) as avg_volume_weight,
        -- 最大单笔超额收益
        maxIf(excess_return, is_contra_move) as max_excess_return,
        -- 平均逆势区间超额收益
        avgIf(excess_return, is_contra_move) as avg_contra_excess,
        -- 平均逆势区间个股收益率
        avgIf(stock_return, is_contra_move) as avg_contra_return,
        -- 相对成交活跃度(逆势时刻的平均)
        avgIf(relative_volume, is_contra_move) as avg_contra_volume
    FROM combined_data
    GROUP BY symbol, sector_code
),

-- ============================================================================
-- 第八步: 应用波动率调整因子
-- 原理: 
--   1. 低波动率股票: 同样逆势表现，风险更低，得分上调
--   2. 高波动率股票: 同样逆势表现，风险更高，得分下调
--   调整公式: adjusted_score = weighted_score × (target_volatility / actual_volatility)^alpha
--   alpha=0.5 表示波动率调整强度适中
-- ============================================================================
volatility_adjusted AS (
    SELECT
        vws.symbol,
        vws.sector_code,
        vws.raw_contra_count,
        vws.total_intervals,
        vws.volume_weighted_score,
        vws.independence_ratio,
        vws.max_excess_return,
        vws.avg_contra_excess,
        vws.avg_contra_return,
        vws.avg_contra_volume,
        -- 获取历史波动率，缺失则使用目标波动率
        COALESCE(sv.volatility_daily, 2.0) as volatility_daily,
        COALESCE(sv.volatility_annual, 30.0) as volatility_annual,
        -- 波动率调整因子 = (目标波动率 / 实际波动率)^0.5
        -- 低波动股票因子>1，高波动股票因子<1
        pow(2.0 / NULLIF(COALESCE(sv.volatility_daily, 2.0), 0), 0.5) as volatility_factor,
        -- 波动率调整后的最终得分
        vws.volume_weighted_score * 
            pow(2.0 / NULLIF(COALESCE(sv.volatility_daily, 2.0), 0), 0.5) as volatility_adjusted_score
    FROM volume_weighted_scores vws
    LEFT JOIN stock_volatility sv ON vws.symbol = sv.symbol
),

-- ============================================================================
-- 第九步: 计算综合独立强度得分及排名
-- 综合得分 = 波动率调整得分 × 板块强度因子
-- 板块强度因子: 所属板块整体表现越好(逆势能力越强)，个股得分越高
-- ============================================================================
final_scores AS (
    SELECT
        symbol,
        sector_code,
        raw_contra_count,
        total_intervals,
        independence_ratio,
        -- 保留3位小数的各阶段得分
        round(volume_weighted_score, 3) as volume_weighted_score,
        round(volatility_adjusted_score, 3) as volatility_adjusted_score,
        -- 综合得分(最终独立强度因子)
        round(volatility_adjusted_score, 3) as final_score,
        -- 波动率指标
        round(volatility_daily, 2) as volatility_daily,
        round(volatility_annual, 2) as volatility_annual,
        round(volatility_factor, 3) as volatility_factor,
        -- 收益指标
        round(max_excess_return, 4) as max_excess_return,
        round(avg_contra_excess, 4) as avg_contra_excess,
        round(avg_contra_return, 4) as avg_contra_return,
        round(avg_contra_volume, 2) as avg_contra_volume,
        -- 日内排名(按综合得分)
        rank() OVER (ORDER BY volatility_adjusted_score DESC) as daily_rank
    FROM volatility_adjusted
)

-- ============================================================================
-- 最终结果输出
-- ============================================================================
SELECT
    {trade_date:Date} as trade_date,
    symbol,
    sector_code,
    -- 核心得分指标
    final_score as independence_score_v2,      -- 综合独立强度得分(主要指标)
    raw_contra_count as base_score,             -- 基础逆势区间数
    volume_weighted_score,                      -- 成交量加权得分
    volatility_adjusted_score,                  -- 波动率调整得分
    -- 统计指标
    total_intervals,                            -- 总区间数
    independence_ratio,                         -- 独立强度比例(%)
    -- 波动率指标
    volatility_daily,                           -- 日波动率(%)
    volatility_annual,                          -- 年化波动率(%)
    volatility_factor,                          -- 波动率调整因子
    -- 收益指标
    max_excess_return,                          -- 最大超额收益(%)
    avg_contra_excess,                          -- 平均超额收益(%)
    avg_contra_return,                          -- 平均逆势收益率(%)
    avg_contra_volume,                          -- 平均逆势成交量比
    -- 排名
    daily_rank                                  -- 日内排名
FROM final_scores
WHERE final_score > 0                           -- 只保留有逆势表现的股票
ORDER BY final_score DESC, independence_ratio DESC
