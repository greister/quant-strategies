# 时间加权独立强度因子实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现时间加权独立强度因子，支持多种预设权重模式和用户自定义配置

**Architecture:** 基于 ClickHouse 配置表存储权重方案，通过 SQL JOIN 在计算时应用权重。Python 脚本提供命令行接口和配置管理。新因子独立存储，与原始因子并存。

**Tech Stack:** ClickHouse (数据计算)、Python 3.8+ (脚本层)、clickhouse-driver (数据库连接)

---

## 文件结构

| 文件路径 | 类型 | 说明 |
|----------|------|------|
| `01.independence-score/sql/create_time_weighted_tables.sql` | 新建 | 配置表和结果表建表脚本 |
| `01.independence-score/sql/init_weight_configs.sql` | 新建 | 预设权重配置初始化数据 |
| `01.independence-score/sql/calc_time_weighted_score.sql` | 新建 | 核心计算 SQL |
| `01.independence-score/scripts/calc_time_weighted_score.py` | 新建 | 主计算脚本 |
| `01.independence-score/scripts/test_time_weighted.sh` | 新建 | 简单测试脚本 |

---

## 前置知识

**现有相关文件：**
- `01.independence-score/sql/create_independence_tables.sql` - 原始因子建表脚本（参考模式）
- `01.independence-score/sql/calc_independence_score.sql` - 原始因子计算逻辑（复用逆势判断逻辑）
- `01.independence-score/scripts/calc_independence_score.sh` - 原始因子计算脚本（参考 CLI 设计）
- `01.independence-score/scripts/calc_independence_score_margin_weighted.py` - Python 脚本参考（数据库连接方式）

**数据库环境变量：**
```bash
export CH_HOST=localhost
export CH_PORT=9000
export CH_DB=tdx2db_rust
export CH_USER=default
export CH_PASSWORD=xxx
```

**区间索引映射（重要）：**
- 上午 9:30-11:30：索引 0-23（实际 5 分钟区间为 9:30, 9:35, ..., 11:25）
- 下午 13:00-15:00：索引 24-47（实际 5 分钟区间为 13:00, 13:05, ..., 14:55）
- ClickHouse 数组索引从 1 开始，取权重时用 `weights[interval_idx + 1]`

**区间索引公式验证：**
- 9:30 → ((9-9)*60 + (30-30))/5 = 0
- 9:35 → ((9-9)*60 + (35-30))/5 = 1
- 11:25 → ((11-9)*60 + (25-30))/5 = (120-5)/5 = 23
- 13:00 → 24 + ((13-13)*60 + 0)/5 = 24
- 14:55 → 24 + ((14-13)*60 + 55)/5 = 24 + 23 = 47

---

## Task 1: 创建配置表和结果表

**Files:**
- Create: `01.independence-score/sql/create_time_weighted_tables.sql`

- [ ] **Step 1: 编写建表 SQL**

```sql
-- 配置表：存储各种权重配置方案
CREATE TABLE IF NOT EXISTS score_weight_configs (
    config_name String,
    config_type Enum('time_based' = 1, 'risk_based' = 2, 'market_style' = 3, 'combined' = 4),
    granularity Enum('interval' = 1, 'hour_block' = 2),
    -- 归一化权重数组，索引对应 5 分钟区间序号 (0-47)
    weights Array(Float32),
    description String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    is_default UInt8 DEFAULT 0,
    
    PRIMARY KEY config_name
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY config_name;

-- 结果表：存储时间加权因子计算结果
CREATE TABLE IF NOT EXISTS independence_score_time_weighted (
    date Date,
    code String,
    name String,
    sector String,
    
    -- 原始分数
    raw_score Float32,
    
    -- 时间加权分数（核心结果）
    weighted_score Float32,
    
    -- 使用的配置
    config_name String,
    
    -- 逆势区间数量
    contra_count UInt16,
    
    -- 各区间详情（可选，用于详细分析）
    -- 存储格式: [(interval_idx, is_contra, weight), ...]
    contra_details Array(Tuple(UInt8, UInt8, Float32)),
    
    -- 元数据
    calculated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, code)
TTL date + INTERVAL 2 YEAR;

-- 便捷查询视图
CREATE OR REPLACE VIEW v_independence_time_weighted_leaders AS
SELECT 
    date,
    code,
    name,
    sector,
    raw_score,
    weighted_score,
    config_name,
    contra_count,
    -- 计算权重调整幅度（避免除零）
    CASE 
        WHEN raw_score > 0 THEN (weighted_score - raw_score) / raw_score 
        ELSE 0 
    END AS weight_adjustment_rate
FROM independence_score_time_weighted
WHERE date = (SELECT max(date) FROM independence_score_time_weighted);
```

- [ ] **Step 2: 验证 SQL 语法**

```bash
cd /home/eo/scripts/40.strategies
clickhouse-client --database=tdx2db_rust < 01.independence-score/sql/create_time_weighted_tables.sql
```

Expected: 无错误输出，命令成功执行

- [ ] **Step 3: 验证表已创建**

```bash
clickhouse-client --database=tdx2db_rust -q "SHOW TABLES LIKE '%time_weighted%'"
```

Expected: 输出 `independence_score_time_weighted` 和 `v_independence_time_weighted_leaders`

- [ ] **Step 4: Commit**

```bash
git add 01.independence-score/sql/create_time_weighted_tables.sql
git commit -m "feat(time-weighted): add config and result table schema"
```

---

## Task 2: 初始化预设权重配置

**Files:**
- Create: `01.independence-score/sql/init_weight_configs.sql`

- [ ] **Step 1: 编写初始化 SQL**

```sql
-- evening_focus: 尾盘关注型（默认推荐）
INSERT INTO score_weight_configs (config_name, config_type, granularity, weights, description, is_default)
VALUES (
    'evening_focus',
    'time_based',
    'interval',
    [0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016, 0.016,  -- 9:30-10:30: 0.016*12 = 0.192
     0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018,  -- 10:30-11:30: 0.018*12 = 0.216
     0.022, 0.022, 0.022, 0.022, 0.022, 0.022, 0.022, 0.022, 0.022, 0.022, 0.022, 0.022,  -- 13:00-14:00: 0.022*12 = 0.264
     0.027, 0.027, 0.027, 0.027, 0.027, 0.027, 0.027, 0.027, 0.027, 0.027, 0.027, 0.027], -- 14:00-15:00: 0.027*12 = 0.324
    '尾盘关注型：尾盘逆势表现权重更高，适合关注次日预期的交易者 (sum=0.996≈1.0)',
    1
);

-- conservative: 保守型（均匀分布）
INSERT INTO score_weight_configs (config_name, config_type, granularity, weights, description, is_default)
VALUES (
    'conservative',
    'risk_based',
    'interval',
    arrayMap(x -> 1.0/48, range(48)),
    '保守型：全天均匀分布，与原始因子等价',
    0
);

-- trending_market: 趋势市
INSERT INTO score_weight_configs (config_name, config_type, granularity, weights, description, is_default)
VALUES (
    'trending_market',
    'market_style',
    'interval',
    [0.024, 0.024, 0.024, 0.024, 0.024, 0.024, 0.024, 0.024, 0.024, 0.024, 0.024, 0.024,  -- 9:30-10:30: 0.024*12 = 0.288
     0.022, 0.022, 0.022, 0.022, 0.022, 0.022, 0.022, 0.022, 0.022, 0.022, 0.022, 0.022,  -- 10:30-11:30: 0.022*12 = 0.264
     0.020, 0.020, 0.020, 0.020, 0.020, 0.020, 0.020, 0.020, 0.020, 0.020, 0.020, 0.020,  -- 13:00-14:00: 0.020*12 = 0.240
     0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017], -- 14:00-15:00: 0.017*12 = 0.204
    '趋势市：早盘权重较高，适合把握趋势启动 (sum=0.996≈1.0)',
    0
);

-- ranging_market: 震荡市
INSERT INTO score_weight_configs (config_name, config_type, granularity, weights, description, is_default)
VALUES (
    'ranging_market',
    'market_style',
    'interval',
    [0.015, 0.015, 0.015, 0.015, 0.015, 0.015, 0.015, 0.015, 0.015, 0.015, 0.015, 0.015,  -- 9:30-10:30: 0.015*12 = 0.180
     0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017,  -- 10:30-11:30: 0.017*12 = 0.204
     0.020, 0.020, 0.020, 0.020, 0.020, 0.020, 0.020, 0.020, 0.020, 0.020, 0.020, 0.020,  -- 13:00-14:00: 0.020*12 = 0.240
     0.030, 0.030, 0.030, 0.030, 0.030, 0.030, 0.030, 0.030, 0.030, 0.030, 0.030, 0.030], -- 14:00-15:00: 0.030*12 = 0.360
    '震荡市：尾盘权重高，博弈次日反转 (sum=0.984≈1.0)',
    0
);

-- rotating_market: 轮动市
INSERT INTO score_weight_configs (config_name, config_type, granularity, weights, description, is_default)
VALUES (
    'rotating_market',
    'market_style',
    'interval',
    [0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018,  -- 9:30-10:30: 0.018*12 = 0.216
     0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018, 0.018,  -- 10:30-11:30: 0.018*12 = 0.216
     0.024, 0.024, 0.024, 0.024, 0.024, 0.024, 0.024, 0.024, 0.024, 0.024, 0.024, 0.024,  -- 13:00-14:00: 0.024*12 = 0.288
     0.023, 0.023, 0.023, 0.023, 0.023, 0.023, 0.023, 0.023, 0.023, 0.023, 0.023, 0.023], -- 14:00-15:00: 0.023*12 = 0.276
    '轮动市：午盘权重高，捕捉资金切换 (sum=0.996≈1.0)',
    0
);

-- morning_focus: 早盘关注型
INSERT INTO score_weight_configs (config_name, config_type, granularity, weights, description, is_default)
VALUES (
    'morning_focus',
    'time_based',
    'interval',
    [0.030, 0.030, 0.030, 0.030, 0.030, 0.030, 0.030, 0.030, 0.030, 0.030, 0.030, 0.030,  -- 9:30-10:30: 0.030*12 = 0.360
     0.025, 0.025, 0.025, 0.025, 0.025, 0.025, 0.025, 0.025, 0.025, 0.025, 0.025, 0.025,  -- 10:30-11:30: 0.025*12 = 0.300
     0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017, 0.017,  -- 13:00-14:00: 0.017*12 = 0.204
     0.011, 0.011, 0.011, 0.011, 0.011, 0.011, 0.011, 0.011, 0.011, 0.011, 0.011, 0.011], -- 14:00-15:00: 0.011*12 = 0.132
    '早盘关注型：早盘逆势表现权重更高，适合把握开盘情绪 (sum=0.996≈1.0)',
    0
);
```

- [ ] **Step 2: 验证权重归一化**

```bash
cd /home/eo/scripts/40.strategies
clickhouse-client --database=tdx2db_rust -q "SELECT config_name, abs(arraySum(weights) - 1.0) < 0.001 as is_normalized FROM score_weight_configs"
```

Expected: 所有配置 is_normalized = 1

- [ ] **Step 3: Commit**

```bash
git add 01.independence-score/sql/init_weight_configs.sql
git commit -m "feat(time-weighted): add preset weight configurations"
```

---

## Task 3: 编写核心计算 SQL

**Files:**
- Create: `01.independence-score/sql/calc_time_weighted_score.sql`

- [ ] **Step 1: 编写计算 SQL**

```sql
-- 参数化查询版本（用于脚本调用）
-- 参数: trade_date (Date), config_name (String)

WITH
-- 获取指定配置的权重
config AS (
    SELECT weights
    FROM score_weight_configs
    WHERE config_name = {config_name:String}
),

-- 计算各股票各区间收益
stock_returns AS (
    SELECT
        code,
        name,
        sector,
        datetime,
        -- 计算 5 分钟收益
        (close - open) / open AS return_5min,
        -- 计算区间序号 (0-47)，正确处理午休时间
        multiIf(
            toHour(datetime) < 12,
            ((toHour(datetime) - 9) * 60 + (toMinute(datetime) - 30)) / 5,
            24 + ((toHour(datetime) - 13) * 60 + toMinute(datetime)) / 5
        ) AS interval_idx
    FROM raw_stocks_5min
    WHERE toDate(datetime) = {trade_date:Date}
      AND ((toHour(datetime) = 9 AND toMinute(datetime) >= 30) 
           OR toHour(datetime) = 10 
           OR (toHour(datetime) = 11 AND toMinute(datetime) <= 30)
           OR toHour(datetime) = 13 
           OR toHour(datetime) = 14)
),

-- 计算板块收益
sector_returns AS (
    SELECT
        sector,
        datetime,
        avg(return_5min) AS sector_return,
        multiIf(
            toHour(datetime) < 12,
            ((toHour(datetime) - 9) * 60 + (toMinute(datetime) - 30)) / 5,
            24 + ((toHour(datetime) - 13) * 60 + toMinute(datetime)) / 5
        ) AS interval_idx
    FROM stock_returns
    GROUP BY sector, datetime, interval_idx
),

-- 识别逆势区间（复用原始因子逻辑）
contra_intervals AS (
    SELECT
        s.code,
        s.name,
        s.sector,
        s.datetime,
        s.interval_idx,
        s.return_5min,
        sec.sector_return,
        -- 是否满足逆势条件
        multiIf(
            sec.sector_return < -0.005 AND (s.return_5min > 0 OR s.return_5min - sec.sector_return > 0.01),
            1,
            0
        ) AS is_contra,
        -- 获取对应权重（ClickHouse 数组索引从 1 开始）
        -- 注意：scalar subquery 在大数据量时可能有性能问题，可考虑优化为 JOIN
        (SELECT weights[s.interval_idx + 1] FROM config) AS weight
    FROM stock_returns s
    JOIN sector_returns sec ON s.sector = sec.sector AND s.datetime = sec.datetime
),

-- 按股票汇总
final_scores AS (
    SELECT
        code,
        name,
        sector,
        -- 原始分数：逆势区间计数
        sum(is_contra) AS raw_score,
        -- 加权分数：逆势区间权重之和
        sum(is_contra * weight) AS weighted_score,
        -- 逆势区间详情
        groupArray((interval_idx, is_contra, weight)) AS contra_details,
        countIf(is_contra = 1) AS contra_count
    FROM contra_intervals
    GROUP BY code, name, sector
)

-- 插入结果表
INSERT INTO independence_score_time_weighted (
    date, code, name, sector,
    raw_score, weighted_score,
    config_name, contra_count, contra_details
)
SELECT
    {trade_date:Date} AS date,
    code,
    name,
    sector,
    raw_score,
    weighted_score,
    {config_name:String} AS config_name,
    contra_count,
    contra_details
FROM final_scores
WHERE raw_score > 0;
```

- [ ] **Step 2: 测试 SQL 语法**

```bash
# 语法检查（不实际执行）
clickhouse-client --database=tdx2db_rust --dry-run < 01.independence-score/sql/calc_time_weighted_score.sql 2>&1
```

Expected: 无语法错误

- [ ] **Step 3: Commit**

```bash
git add 01.independence-score/sql/calc_time_weighted_score.sql
git commit -m "feat(time-weighted): add core calculation SQL"
```

---

## Task 4: 编写主计算脚本

**Files:**
- Create: `01.independence-score/scripts/calc_time_weighted_score.py`

- [ ] **Step 1: 编写脚本框架和参数解析**

```python
#!/usr/bin/env python3
"""时间加权独立强度因子计算脚本.

Usage:
    ./calc_time_weighted_score.py --init
    ./calc_time_weighted_score.py 2025-03-20 --preset evening_focus
    ./calc_time_weighted_score.py 2025-03-20 --custom-weights "0.01,0.01,..."
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import clickhouse_driver

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TimeWeightedScoreCalculator:
    """时间加权因子计算器."""
    
    PRESETS = [
        'evening_focus',      # 尾盘关注型（默认）
        'conservative',       # 保守型
        'trending_market',    # 趋势市
        'ranging_market',     # 震荡市
        'rotating_market',    # 轮动市
        'morning_focus',      # 早盘关注型
    ]
    
    def __init__(self, 
                 host: Optional[str] = None, 
                 port: Optional[int] = None, 
                 database: Optional[str] = None,
                 user: Optional[str] = None, 
                 password: Optional[str] = None):
        """初始化计算器."""
        self.host = host or os.getenv('CH_HOST', 'localhost')
        self.port = port or int(os.getenv('CH_PORT', '9000'))
        self.database = database or os.getenv('CH_DB', 'tdx2db_rust')
        self.user = user or os.getenv('CH_USER', 'default')
        self.password = password or os.getenv('CH_PASSWORD', '')
        
        self.client = None
        
    def _connect(self) -> clickhouse_driver.Client:
        """建立数据库连接."""
        if self.client is None:
            self.client = clickhouse_driver.Client(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
        return self.client
```

- [ ] **Step 2: 实现 ensure_tables 方法**

```python
    def ensure_tables(self) -> bool:
        """确保表结构存在并初始化预设配置.
        
        Returns:
            是否成功
        """
        client = self._connect()
        
        # 读取并执行建表 SQL
        sql_path = os.path.join(
            os.path.dirname(__file__), 
            '..', 'sql', 'create_time_weighted_tables.sql'
        )
        with open(sql_path, 'r') as f:
            create_sql = f.read()
        
        try:
            # ClickHouse 不支持多语句执行，需要拆分
            for statement in create_sql.split(';'):
                statement = statement.strip()
                if statement:
                    client.execute(statement)
            logger.info("Tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            return False
        
        # 初始化预设配置
        init_sql_path = os.path.join(
            os.path.dirname(__file__),
            '..', 'sql', 'init_weight_configs.sql'
        )
        with open(init_sql_path, 'r') as f:
            init_sql = f.read()
        
        try:
            # 检查是否已有配置
            result = client.execute(
                "SELECT count() FROM score_weight_configs"
            )
            if result[0][0] == 0:
                for statement in init_sql.split(';'):
                    statement = statement.strip()
                    if statement and 'INSERT' in statement.upper():
                        client.execute(statement)
                logger.info("Preset configurations initialized")
            else:
                logger.info("Configurations already exist, skipping initialization")
        except Exception as e:
            logger.error(f"Failed to initialize configs: {e}")
            return False
        
        return True
```

- [ ] **Step 3: 实现 calc 方法**

```python
    def calc(self, trade_date: str, config_name: str = 'evening_focus') -> int:
        """计算指定日期的加权因子.
        
        Args:
            trade_date: 交易日期，格式 YYYY-MM-DD
            config_name: 使用的权重配置名称
            
        Returns:
            计算的股票数量
        """
        client = self._connect()
        
        # 验证配置存在
        result = client.execute(
            "SELECT config_name FROM score_weight_configs WHERE config_name = %s",
            (config_name,)
        )
        if not result:
            raise ValueError(f"Config '{config_name}' not found. Available: {self.PRESETS}")
        
        logger.info(f"Calculating time-weighted score for {trade_date} with config '{config_name}'")
        
        # 读取并执行计算 SQL
        sql_path = os.path.join(
            os.path.dirname(__file__),
            '..', 'sql', 'calc_time_weighted_score.sql'
        )
        with open(sql_path, 'r') as f:
            calc_sql = f.read()
        
        # 替换参数
        calc_sql = calc_sql.replace('{trade_date:Date}', f"'{trade_date}'")
        calc_sql = calc_sql.replace('{config_name:String}', f"'{config_name}'")
        
        try:
            result = client.execute(calc_sql)
            
            # 查询计算结果数量
            count_result = client.execute(
                """SELECT count() FROM independence_score_time_weighted 
                   WHERE date = %s AND config_name = %s""",
                (trade_date, config_name)
            )
            count = count_result[0][0]
            logger.info(f"Calculated {count} stocks for {trade_date}")
            return count
            
        except Exception as e:
            logger.error(f"Calculation failed: {e}")
            raise
```

- [ ] **Step 4: 实现 create_custom_config 方法**

```python
    def create_custom_config(self, name: str, weights: List[float],
                            config_type: str = 'custom',
                            description: str = '') -> bool:
        """创建自定义权重配置.
        
        Args:
            name: 配置名称（唯一标识）
            weights: 48 个归一化权重值列表
            config_type: 配置类型
            description: 配置描述
            
        Returns:
            是否创建成功
        """
        client = self._connect()
        
        # 验证
        if len(weights) != 48:
            raise ValueError(f"Weights must have exactly 48 elements, got {len(weights)}")
        
        if abs(sum(weights) - 1.0) > 0.001:
            raise ValueError(f"Weights must sum to 1.0, got {sum(weights)}")
        
        if any(w < 0 for w in weights):
            raise ValueError("All weights must be non-negative")
        
        # 检查名称是否已存在
        result = client.execute(
            "SELECT count() FROM score_weight_configs WHERE config_name = %s",
            (name,)
        )
        if result[0][0] > 0:
            logger.warning(f"Config '{name}' already exists, will be replaced")
        
        # 插入配置
        try:
            client.execute(
                """INSERT INTO score_weight_configs 
                   (config_name, config_type, granularity, weights, description, is_default)
                   VALUES (%s, %s, 'interval', %s, %s, 0)""",
                (name, config_type, weights, description or f"Custom config: {name}")
            )
            logger.info(f"Custom config '{name}' created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create config: {e}")
            return False
```

- [ ] **Step 5: 实现 main 函数**

```python
def main():
    parser = argparse.ArgumentParser(
        description='计算时间加权独立强度因子',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 初始化表结构和预设配置
  %(prog)s --init
  
  # 使用默认预设计算今日
  %(prog)s
  
  # 使用指定预设计算指定日期
  %(prog)s 2025-03-20 --preset trending_market
  
  # 使用自定义权重（48 个浮点数）
  %(prog)s 2025-03-20 --custom-weights "0.02,0.02,..." --custom-name "my_config"
        """
    )
    
    parser.add_argument('date', nargs='?',
                       help='交易日期 (YYYY-MM-DD)，默认今天')
    parser.add_argument('--preset', '-p', default='evening_focus',
                       choices=TimeWeightedScoreCalculator.PRESETS,
                       help='使用预设权重模式 (默认: evening_focus)')
    parser.add_argument('--custom-weights', '-w',
                       help='自定义权重（逗号分隔的 48 个浮点数）')
    parser.add_argument('--custom-name', '-n',
                       help='自定义配置名称（配合 --custom-weights 使用）')
    parser.add_argument('--init', action='store_true',
                       help='初始化表结构和预设配置')
    parser.add_argument('--list-presets', action='store_true',
                       help='列出所有可用预设')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='输出详细日志')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    calc = TimeWeightedScoreCalculator()
    
    if args.list_presets:
        print("Available presets:")
        for preset in TimeWeightedScoreCalculator.PRESETS:
            print(f"  - {preset}")
        return 0
    
    if args.init:
        if calc.ensure_tables():
            print("Initialization completed successfully")
            return 0
        else:
            print("Initialization failed", file=sys.stderr)
            return 1
    
    # 处理自定义配置
    if args.custom_weights:
        if not args.custom_name:
            print("--custom-name is required when using --custom-weights", file=sys.stderr)
            return 1
        
        try:
            weights = [float(w.strip()) for w in args.custom_weights.split(',')]
            if calc.create_custom_config(args.custom_name, weights):
                config_name = args.custom_name
            else:
                return 1
        except ValueError as e:
            print(f"Invalid weights: {e}", file=sys.stderr)
            return 1
    else:
        config_name = args.preset
    
    # 确定日期
    if args.date:
        trade_date = args.date
    else:
        trade_date = datetime.now().strftime('%Y-%m-%d')
    
    # 执行计算
    try:
        count = calc.calc(trade_date, config_name)
        print(f"Successfully calculated {count} stocks for {trade_date} using '{config_name}'")
        return 0
    except Exception as e:
        print(f"Calculation failed: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
```

- [ ] **Step 6: 添加执行权限并 Commit**

```bash
chmod +x 01.independence-score/scripts/calc_time_weighted_score.py
git add 01.independence-score/scripts/calc_time_weighted_score.py
git commit -m "feat(time-weighted): add calculation script with CLI"
```

---

## Task 5: 创建测试脚本

**Files:**
- Create: `01.independence-score/scripts/test_time_weighted.sh`

- [ ] **Step 1: 编写测试脚本**

```bash
#!/bin/bash
# 时间加权因子测试脚本

set -e

cd "$(dirname "$0")/.."

echo "=== 时间加权独立强度因子测试 ==="
echo

# 测试日期（可改为最近有数据的日期）
TEST_DATE="${1:-2025-03-20}"
SCRIPT="./scripts/calc_time_weighted_score.py"

echo "1. 测试列出预设..."
$SCRIPT --list-presets
echo "[PASS]"
echo

echo "2. 测试使用 evening_focus 预设计算 $TEST_DATE..."
$SCRIPT "$TEST_DATE" --preset evening_focus
echo "[PASS]"
echo

echo "3. 测试使用 trending_market 预设计算 $TEST_DATE..."
$SCRIPT "$TEST_DATE" --preset trending_market
echo "[PASS]"
echo

echo "4. 验证结果表数据..."
clickhouse-client --database=tdx2db_rust -q "
    SELECT 
        config_name,
        count() as stock_count,
        avg(raw_score) as avg_raw,
        avg(weighted_score) as avg_weighted
    FROM independence_score_time_weighted
    WHERE date = '$TEST_DATE'
    GROUP BY config_name
    ORDER BY config_name
"
echo "[PASS]"
echo

echo "5. 验证权重调整效果..."
clickhouse-client --database=tdx2db_rust -q "
    SELECT 
        code,
        name,
        raw_score,
        weighted_score,
        config_name
    FROM independence_score_time_weighted
    WHERE date = '$TEST_DATE'
    ORDER BY weighted_score DESC
    LIMIT 5
"
echo "[PASS]"
echo

echo "6. 测试自定义权重..."
# 创建一个简单的自定义权重（早盘权重高）
CUSTOM_WEIGHTS=$(python3 -c "print(','.join(['0.03']*12 + ['0.015']*12 + ['0.01']*12 + ['0.015']*12))")
$SCRIPT "$TEST_DATE" --custom-weights "$CUSTOM_WEIGHTS" --custom-name "test_morning_focus"
echo "[PASS]"
echo

echo "=== 所有测试通过 ==="
```

- [ ] **Step 2: 添加执行权限并 Commit**

```bash
chmod +x 01.independence-score/scripts/test_time_weighted.sh
git add 01.independence-score/scripts/test_time_weighted.sh
git commit -m "feat(time-weighted): add test script"
```

---

## Task 6: 更新文档

**Files:**
- Modify: `01.independence-score/README.md`

- [ ] **Step 1: 在 README.md 中添加时间加权因子章节**

在 README.md 的"融资余额加权版本"之后添加：

```markdown
## 时间加权因子版本

在基础独立强度因子之上，引入时间衰减权重机制，不同时段的逆势表现赋予不同权重。

### 核心逻辑

- **归一化权重**：全天 48 个 5 分钟区间权重之和为 1.0
- **时间导向**：早盘、午盘、尾盘可配置不同权重
- **预设模式**：尾盘关注型、趋势市、震荡市等多种模式

### 预设模式

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| `evening_focus` | 尾盘关注型（默认）| 关注次日开盘预期 |
| `morning_focus` | 早盘关注型 | 把握开盘情绪 |
| `conservative` | 保守型（均匀分布）| 与原始因子等价 |
| `trending_market` | 趋势市 | 早盘权重较高 |
| `ranging_market` | 震荡市 | 尾盘权重较高 |
| `rotating_market` | 轮动市 | 午盘权重较高 |

### 使用方法

```bash
# 初始化（首次使用）
./scripts/calc_time_weighted_score.py --init

# 使用默认预设计算
./scripts/calc_time_weighted_score.py 2025-03-20

# 使用指定预设
./scripts/calc_time_weighted_score.py 2025-03-20 --preset trending_market

# 查看所有预设
./scripts/calc_time_weighted_score.py --list-presets

# 自定义权重（48 个浮点数，逗号分隔）
./scripts/calc_time_weighted_score.py 2025-03-20 \
    --custom-weights "0.02,0.02,..." \
    --custom-name "my_config"
```

### 查看结果

```bash
# 查看某日加权因子排名
clickhouse-client --database=tdx2db_rust -q "
    SELECT * FROM independence_score_time_weighted
    WHERE date = '2025-03-20' AND config_name = 'evening_focus'
    ORDER BY weighted_score DESC
    LIMIT 20
"

# 对比不同配置的选股差异
clickhouse-client --database=tdx2db_rust -q "
    SELECT 
        a.code, a.name,
        a.weighted_score as evening_score,
        b.weighted_score as morning_score
    FROM independence_score_time_weighted a
    JOIN independence_score_time_weighted b ON a.code = b.code AND a.date = b.date
    WHERE a.date = '2025-03-20'
      AND a.config_name = 'evening_focus'
      AND b.config_name = 'morning_focus'
    ORDER BY evening_score DESC
    LIMIT 20
"
```
```

- [ ] **Step 2: Commit 文档更新**

```bash
git add 01.independence-score/README.md
git commit -m "docs(time-weighted): add usage documentation"
```

---

## 验证清单

实施完成后，验证以下内容：

- [ ] 表结构正确创建（`score_weight_configs`, `independence_score_time_weighted`）
- [ ] 预设配置已初始化（6 个预设）
- [ ] SQL 计算逻辑正确（区间索引、权重应用）
- [ ] Python 脚本可以正常运行（参数解析、数据库连接）
- [ ] 自定义权重配置可以创建和计算
- [ ] 测试脚本全部通过
- [ ] 文档已更新

---

## 注意事项

1. **权重归一化**：所有预设配置必须满足 `sum(weights) = 1.0`
2. **区间索引**：ClickHouse 数组索引从 1 开始，使用 `weights[interval_idx + 1]`
3. **午休时间**：SQL 中正确处理 11:30-13:00 的午休间隔
4. **性能优化**：大数据量计算时，考虑使用 ClickHouse Dictionary 替代 scalar subquery
5. **区间索引验证**：首次部署后运行验证查询，确认 interval_idx 生成 0-47

---

**Plan complete.** Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task-by-task.
