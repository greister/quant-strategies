-- ============================================================================
-- tdx2db-rust 表结构适配器
-- 解决策略系统与实际 tdx2db-rust 表结构的兼容性问题
-- ============================================================================

-- ============================================================================
-- 1. 板块/行业相关表适配
-- ============================================================================

-- 原表: stock_sectors (symbol, sector_code)
-- 实际表: stock_industry_mapping (symbol, industry_code, industry_name)
-- 创建兼容视图: 使用 industry_code 作为 sector_code
CREATE OR REPLACE VIEW v_stock_sectors AS
SELECT 
    symbol,
    industry_code AS sector_code
FROM stock_industry_mapping;

-- 原表: stock_industry_mapping (symbol, industry_code)
-- 实际表: stock_industry_mapping (symbol, industry_code, industry_name)
-- 创建兼容视图
CREATE OR REPLACE VIEW v_stock_industry_mapping AS
SELECT 
    symbol,
    industry_code
FROM stock_industry_mapping;

-- 原表: gtja_industry_list (block_code, block_name)
-- 实际表: gtja_industry_list (block_code, block_name, ...)
-- 创建兼容视图（字段名相同，直接透传）
CREATE OR REPLACE VIEW v_gtja_industry_list AS
SELECT 
    block_code,
    block_name
FROM gtja_industry_list;

-- 原表: gtja_stock_names (symbol, name)
-- 实际表: gtja_stock_names (code, symbol, name, ...)
-- 创建兼容视图
CREATE OR REPLACE VIEW v_gtja_stock_names AS
SELECT 
    symbol,
    name
FROM gtja_stock_names;

-- ============================================================================
-- 2. 指数数据表适配
-- ============================================================================

-- 原表: raw_index_daily (symbol, date, open, high, low, close, volume, amount)
-- 说明: 指数数据存储在 raw_stocks_daily 中，代码格式为 sh000001, sz399001 等
-- 创建视图: 从 raw_stocks_daily 中提取主要指数数据
CREATE OR REPLACE VIEW v_raw_index_daily AS
SELECT 
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    amount
FROM raw_stocks_daily
WHERE symbol IN (
    -- 主要指数
    'sh000001',  -- 上证指数
    'sh000002',  -- A股指数
    'sh000003',  -- B股指数
    'sh000016',  -- 上证50
    'sh000300',  -- 沪深300
    'sh000905',  -- 中证500
    'sz399001',  -- 深证成指
    'sz399006',  -- 创业板指
    'sz399300',  -- 沪深300(深圳)
    'sz399905'   -- 中证500(深圳)
);

-- ============================================================================
-- 3. 便捷视图（简化常用查询）
-- ============================================================================

-- 带板块信息的股票日线视图
CREATE OR REPLACE VIEW v_stocks_daily_with_sector AS
SELECT 
    d.*,
    s.industry_code AS sector_code,
    s.industry_name AS sector_name
FROM raw_stocks_daily d
LEFT JOIN stock_industry_mapping s ON d.symbol = s.symbol;

-- 带板块信息的5分钟线视图
CREATE OR REPLACE VIEW v_stocks_5min_with_sector AS
SELECT 
    m.*,
    s.industry_code AS sector_code,
    s.industry_name AS sector_name
FROM raw_stocks_5min m
LEFT JOIN stock_industry_mapping s ON m.symbol = s.symbol;

-- ============================================================================
-- 4. 数据验证查询
-- ============================================================================

-- 验证视图是否正常工作
-- SELECT * FROM v_stock_sectors LIMIT 10;
-- SELECT * FROM v_stock_industry_mapping LIMIT 10;
-- SELECT * FROM v_gtja_industry_list LIMIT 10;
-- SELECT * FROM v_gtja_stock_names LIMIT 10;
-- SELECT * FROM v_raw_index_daily LIMIT 10;
