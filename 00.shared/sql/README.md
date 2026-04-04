# tdx2db-rust 表结构适配

## 概述

策略系统早期使用的 ClickHouse 表结构与 tdx2db-rust 项目的标准 schema 存在差异。此适配器通过创建视图来解决兼容性问题。

## 差异对比

| 策略系统原表/字段 | tdx2db-rust 实际表/字段 | 适配方案 |
|------------------|------------------------|----------|
| `stock_sectors(sector_code)` | `stock_sectors(sector)` | 创建视图 `v_stock_sectors` |
| `stock_industry_mapping` | 不存在 | 映射到 `v_stock_industry_mapping` |
| `gtja_stock_names` | 不存在 | 创建视图 `v_gtja_stock_names` |
| `gtja_industry_list` | `sectors` | 创建视图 `v_gtja_industry_list` |
| `raw_index_daily` | 不存在 | 创建视图 `v_raw_index_daily` |

## 使用方法

### 1. 在 ClickHouse 中创建适配视图

```bash
clickhouse-client --database=tdx2db_rust < 00.shared/sql/tdx2db_rust_adapter.sql
```

### 2. 更新后的 SQL 和 Python 脚本会自动使用视图

所有策略 SQL 文件已更新为使用 `v_` 前缀的视图名称：
- `v_stock_sectors` 替代 `stock_sectors`
- `v_stock_industry_mapping` 替代 `stock_industry_mapping`
- `v_gtja_stock_names` 替代 `gtja_stock_names`
- `v_gtja_industry_list` 替代 `gtja_industry_list`
- `v_raw_index_daily` 替代 `raw_index_daily`

## 视图定义说明

### v_stock_sectors
```sql
CREATE VIEW v_stock_sectors AS
SELECT symbol, sector AS sector_code
FROM stock_sectors;
```

### v_stock_industry_mapping
```sql
CREATE VIEW v_stock_industry_mapping AS
SELECT symbol, sector AS industry_code
FROM stock_sectors;
```

### v_gtja_industry_list
```sql
CREATE VIEW v_gtja_industry_list AS
SELECT code AS block_code, name AS block_name
FROM sectors;
```

### v_gtja_stock_names
```sql
CREATE VIEW v_gtja_stock_names AS
SELECT DISTINCT symbol, symbol AS name
FROM raw_stocks_daily;
```
**注意**: 股票名称需要从其他数据源补充，当前视图仅使用 symbol 作为占位。

### v_raw_index_daily
```sql
CREATE VIEW v_raw_index_daily AS
SELECT symbol, date, open, high, low, close, volume, amount
FROM raw_stocks_daily
WHERE symbol IN ('sh000001', 'sh000002', ...);
```
**注意**: 需要从其他数据源导入指数数据，或确保 tdx2db-rust 采集了指数数据。

## 补充缺失数据

对于缺失的表，建议从 akshare 导入：

```python
import akshare as ak

# 股票名称
stock_info = ak.stock_info_a_code_name()
# 导入到 gtja_stock_names 表

# 指数数据
index_daily = ak.index_zh_a_hist(symbol="000001", period="daily")
# 导入到 raw_index_daily 表
```

## 已更新的文件清单

### SQL 文件
- `01.independence-score/sql/calc_independence_score.sql`
- `01.independence-score/sql/calc_time_weighted_score.sql`
- `01.independence-score/sql/backtest_independence_score.sql`
- `02.momentum-factor/sql/calc_momentum_factor.sql`
- `02.momentum-factor/sql/calc_low_beta_rs.sql`
- `03.low-beta-hybrid/sql/calc_low_beta_hybrid.sql`

### Python 文件
- `01.independence-score/scripts/calc_independence_score_margin_weighted.py`

### 配置文件
- `00.shared/config/database.env`
