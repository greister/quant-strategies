-- ============================================================================
-- tdx2db-rust 表结构适配器
-- 解决策略系统与新 tdx2db-rust 表结构的兼容性问题
-- ============================================================================
-- 说明:
--   tdx2db-rust 标准 schema 与策略系统早期使用的表结构存在差异
--   此文件创建视图来提供兼容的表名和字段名
-- ============================================================================

-- ============================================================================
-- 1. 板块/行业相关表适配
-- ============================================================================

-- 原表: stock_sectors (symbol, sector_code)
-- 新表: stock_sectors (symbol, sector)
-- 创建兼容视图
CREATE OR REPLACE VIEW v_stock_sectors AS
SELECT 
    symbol,
    sector AS sector_code
FROM stock_sectors;

-- 原表: stock_industry_mapping (symbol, industry_code)
-- 新表: stock_sectors (symbol, sector)
-- 说明: 用 stock_sectors 替代 industry_mapping，因为概念相似
CREATE OR REPLACE VIEW v_stock_industry_mapping AS
SELECT 
    symbol,
    sector AS industry_code
FROM stock_sectors;

-- 原表: gtja_industry_list (block_code, block_name)
-- 新表: sectors (code, name)
CREATE OR REPLACE VIEW v_gtja_industry_list AS
SELECT 
    code AS block_code,
    name AS block_name
FROM sectors;

-- 原表: gtja_stock_names (symbol, name)
-- 新表: 需要从 raw_stocks_daily 中提取或使用其他方式
-- 说明: 创建视图从最新日线数据提取股票名称（如果有）
-- 注意: tdx2db-rust 标准 schema 中没有单独的股票名称表
--       如果需要，可以考虑从其他数据源导入
CREATE OR REPLACE VIEW v_gtja_stock_names AS
SELECT DISTINCT
    symbol,
    symbol AS name  -- 暂时用 symbol 作为 name，建议从外部数据源补充
FROM raw_stocks_daily;

-- ============================================================================
-- 2. 指数数据表适配
-- ============================================================================

-- 原表: raw_index_daily (symbol, date, open, high, low, close, volume, amount)
-- 说明: tdx2db-rust 标准 schema 中没有独立的指数表
--       可以用特定 symbol 格式（如 sh000001）从 raw_stocks_daily 获取
--       或者需要单独导入指数数据
-- 创建视图: 假设指数数据会以 sh/sz + 6位数字代码的形式存储在 raw_stocks_daily
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
WHERE symbol MATCHES '^(sh|sz)[0-9]{6}$'
  AND symbol IN ('sh000001', 'sh000002', 'sh000003', 'sh000016', 'sh000300',
                 'sh000905', 'sz399001', 'sz399006', 'sz399300', 'sz399905');

-- ============================================================================
-- 3. 便捷视图（简化常用查询）
-- ============================================================================

-- 带板块信息的股票日线视图
CREATE OR REPLACE VIEW v_stocks_daily_with_sector AS
SELECT 
    d.*,
    s.sector AS sector_code
FROM raw_stocks_daily d
LEFT JOIN stock_sectors s ON d.symbol = s.symbol;

-- 带板块信息的5分钟线视图
CREATE OR REPLACE VIEW v_stocks_5min_with_sector AS
SELECT 
    m.*,
    s.sector AS sector_code
FROM raw_stocks_5min m
LEFT JOIN stock_sectors s ON m.symbol = s.symbol;

-- ============================================================================
-- 4. 数据验证查询
-- ============================================================================

-- 验证视图是否正常工作
-- SELECT * FROM v_stock_sectors LIMIT 10;
-- SELECT * FROM v_stock_industry_mapping LIMIT 10;
-- SELECT * FROM v_gtja_industry_list LIMIT 10;
-- SELECT * FROM v_gtja_stock_names LIMIT 10;
-- SELECT * FROM v_raw_index_daily LIMIT 10;

-- ============================================================================
-- 5. 缺失数据的处理建议
-- ============================================================================

/*
对于缺失的表，有以下处理方案:

1. gtja_stock_names (股票名称表)
   - 方案A: 从 tdx2db-rust 的数据源补充（如果有）
   - 方案B: 使用 akshare 等 Python 库导入
   - 方案C: 在查询时只使用 symbol，名称通过其他方式获取

2. raw_index_daily (指数日线)
   - 方案A: 从 tdx2db-rust 导入指数数据（如果有这个功能）
   - 方案B: 使用 akshare 导入主要指数数据
   - 方案C: 策略计算中不使用指数数据

示例: 使用 akshare 补充缺失表
```python
import akshare as ak

# 获取股票名称
stock_info = ak.stock_info_a_code_name()
# 导入到 ClickHouse

# 获取指数数据
index_daily = ak.index_zh_a_hist(symbol="000001", period="daily")
# 导入到 ClickHouse
```
*/
