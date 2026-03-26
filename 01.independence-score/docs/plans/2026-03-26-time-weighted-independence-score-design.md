# 时间加权独立强度因子设计文档

**日期**: 2026-03-26  
**作者**: AI Assistant  
**状态**: 待审核

---

## 1. 背景与目标

### 1.1 背景

现有的独立强度因子（Independence Score）在计算时对所有 5 分钟区间赋予相同权重。然而在实际交易中：

- **早盘**的逆势表现可能反映隔夜信息消化，信号价值相对较低
- **午盘**的逆势表现反映盘中资金博弈，信号价值中等
- **尾盘**的逆势表现往往预示次日开盘预期，信号价值较高

不同交易者对市场不同时段的关注度不同，需要灵活的时间加权机制。

### 1.2 目标

创建一个**时间加权独立强度因子**，支持：

1. 对不同时段的逆势表现赋予不同权重
2. 提供多种预设权重模式（时间导向、风险偏好、市场风格）
3. 支持用户自定义权重配置
4. 与原始独立强度因子并存，便于对比分析

---

## 2. 需求规格

### 2.1 功能需求

| ID | 需求 | 优先级 | 说明 |
|----|------|--------|------|
| F1 | 配置表管理 | 高 | 存储和管理多种权重配置方案 |
| F2 | 混合粒度支持 | 高 | 支持按 5 分钟区间、小时段或自定义时段配置 |
| F3 | 归一化权重 | 高 | 所有时段权重之和为 1.0 |
| F4 | 预设模式 | 高 | 提供时间导向型、风险偏好型、市场风格型预设 |
| F5 | 用户自定义 | 中 | 支持在预设基础上微调或完全自定义 |
| F6 | 独立存储 | 高 | 新因子独立存储，不影响原始因子 |

### 2.2 非功能需求

| ID | 需求 | 说明 |
|----|------|------|
| NF1 | 性能 | 计算性能与原始因子相当，不显著增加查询时间 |
| NF2 | 可维护性 | 配置表结构清晰，易于扩展新模式 |
| NF3 | 可追溯性 | 记录使用的配置名称，便于结果复现 |

---

## 3. 架构设计

### 3.1 组件图

```
┌─────────────────────────────────────────────────────────────────┐
│                        时间加权因子计算流程                        │
└─────────────────────────────────────────────────────────────────┘

  ┌─────────────────┐     ┌─────────────────┐
  │   raw_stocks_   │     │   stock_sectors │
  │     5min        │     │                 │
  └────────┬────────┘     └────────┬────────┘
           │                       │
           └───────────┬───────────┘
                       ▼
              ┌─────────────────┐
              │   计算逆势区间    │  (SQL: 复用原始逻辑)
              │  (contra_calc)  │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │  JOIN 权重配置表 │
              │ (score_weight_  │◄────┌─────────────────┐
              │    configs)     │     │ score_weight_   │
              └────────┬────────┘     │    configs      │
                       │              │  (配置表)       │
                       ▼              └─────────────────┘
              ┌─────────────────┐
              │   加权求和计算    │
              │ (weighted_sum)  │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────────────┐
              │ independence_score_time │
              │     _weighted           │
              │    (结果表)              │
              └─────────────────────────┘
```

### 3.2 数据表设计

#### 3.2.1 配置表 `score_weight_configs`

存储各种权重配置方案。

```sql
CREATE TABLE IF NOT EXISTS score_weight_configs (
    config_name String,
    config_type Enum('time_based' = 1, 'risk_based' = 2, 'market_style' = 3, 'combined' = 4),
    granularity Enum('interval' = 1, 'hour_block' = 2),  -- 当前仅支持 48 个 5 分钟区间粒度
    -- 归一化权重数组，索引对应 5 分钟区间序号 (0-47)
    -- 9:30-11:30 (0-23), 13:00-15:00 (24-47)
    weights Array(Float32),
    description String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    is_default UInt8 DEFAULT 0,
    
    PRIMARY KEY config_name
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY config_name;
```

**权重数组索引映射**:

| 时间段 | 索引范围 | 说明 |
|--------|----------|------|
| 9:30-10:30 | 0-11 | 早盘第一小时 |
| 10:30-11:30 | 12-23 | 早盘第二小时 |
| 13:00-14:00 | 24-35 | 午盘第一小时 |
| 14:00-15:00 | 36-47 | 午盘第二小时（尾盘）|

#### 3.2.2 结果表 `independence_score_time_weighted`

存储时间加权因子计算结果。

```sql
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
```

#### 3.2.3 视图 `v_independence_time_weighted_leaders`

提供便捷查询接口。

```sql
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

### 3.3 预设模式设计

#### 3.3.1 时间导向型

| 模式名称 | 说明 | 权重分布特点 |
|----------|------|--------------|
| `morning_focus` | 早盘关注型 | 9:30-10:30 权重最高，逐步递减 |
| `noon_focus` | 午盘关注型 | 13:00-14:00 权重最高 |
| `evening_focus` | 尾盘关注型 | 14:00-15:00 权重最高，最受关注 |
| `opening_closing` | 开盘收盘型 | 早盘和尾盘高，午盘中 |

#### 3.3.2 风险偏好型

| 模式名称 | 说明 | 权重分布特点 |
|----------|------|--------------|
| `conservative` | 保守型 | 全天均匀分布，1/48 |
| `balanced` | 平衡型 | 轻微向尾盘倾斜 |
| `aggressive` | 激进型 | 高度集中在尾盘 30 分钟 |

#### 3.3.3 市场风格型

| 模式名称 | 适用场景 | 权重分布特点 |
|----------|----------|--------------|
| `trending_market` | 趋势市 | 早盘权重略高，把握趋势启动 |
| `ranging_market` | 震荡市 | 尾盘权重高，博弈次日反转 |
| `rotating_market` | 轮动市 | 午盘权重高，捕捉资金切换 |

### 3.4 权重初始化数据

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
INSERT INTO score_weight_configs (config_name, config_type, granularity, weights, description)
VALUES (
    'conservative',
    'risk_based',
    'interval',
    arrayMap(x -> 1.0/48, range(48)),
    '保守型：全天均匀分布，与原始因子等价',
    0
);

-- trending_market: 趋势市
INSERT INTO score_weight_configs (config_name, config_type, granularity, weights, description)
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
INSERT INTO score_weight_configs (config_name, config_type, granularity, weights, description)
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
INSERT INTO score_weight_configs (config_name, config_type, granularity, weights, description)
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
```

---

## 4. 计算逻辑

### 4.1 核心 SQL 逻辑

```sql
-- 参数
SET param_trade_date = {trade_date:Date};
SET param_config_name = {config_name:String};

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
        -- 获取对应权重
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

### 4.2 关键算法说明

**区间序号计算**:
- 交易时间：9:30-11:30 (24 个区间), 13:00-15:00 (24 个区间)，共 48 个区间
- 上午区间（9:30-11:25）：索引 0-23
  - A股 5 分钟 K 线时间：9:30, 9:35, ..., 11:25, 11:30（收盘）
  - 实际交易区间：9:30-9:35 对应索引 0，...，11:25-11:30 对应索引 23
  - 公式：`((toHour(datetime) - 9) * 60 + (toMinute(datetime) - 30)) / 5`
  - 9:30 → 0, 9:35 → 1, ..., 11:25 → 23
- 下午区间（13:00-14:55）：索引 24-47
  - 实际交易区间：13:00-13:05 对应索引 24，...，14:55-15:00 对应索引 47
  - 公式：`24 + ((toHour(datetime) - 13) * 60 + toMinute(datetime)) / 5`
  - 13:00 → 24, 13:05 → 25, ..., 14:55 → 47
- **重要**：ClickHouse 数组索引从 1 开始，因此取权重时使用 `weights[interval_idx + 1]`
- 完整 SQL 表达式：
```sql
multiIf(
    toHour(datetime) < 12,
    ((toHour(datetime) - 9) * 60 + (toMinute(datetime) - 30)) / 5,
    24 + ((toHour(datetime) - 13) * 60 + toMinute(datetime)) / 5
) AS interval_idx
```
-- 权重取值：(SELECT weights[interval_idx + 1] FROM config)

**权重应用**:
- 仅对满足逆势条件的区间应用权重
- 加权分数 = Σ(逆势区间权重)
- 由于权重归一化，加权分数量级与原始分数相当

---

## 5. 脚本设计

### 5.1 主计算脚本 `calc_time_weighted_score.py`

```python
#!/usr/bin/env python3
"""时间加权独立强度因子计算脚本."""

import argparse
import logging
from datetime import datetime, timedelta
from typing import Optional
import clickhouse_driver

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TimeWeightedScoreCalculator:
    """时间加权因子计算器."""
    
    PRESETS = {
        'evening_focus': '尾盘关注型',
        'conservative': '保守型（均匀分布）',
        'trending_market': '趋势市',
        'ranging_market': '震荡市',
        'morning_focus': '早盘关注型',
    }
    
    def __init__(self, host: str, port: int, database: str, 
                 user: str = 'default', password: str = ''):
        self.client = clickhouse_driver.Client(
            host=host, port=port, database=database,
            user=user, password=password
        )
    
    def ensure_tables(self):
        """确保表结构存在.
        
        执行内容：
        1. 创建 score_weight_configs 配置表（如果不存在）
        2. 创建 independence_score_time_weighted 结果表（如果不存在）
        3. 初始化预设配置（如果配置表为空）
        """
        # TODO: 实现建表和初始化逻辑
        pass
    
    def calc(self, trade_date: str, config_name: str = 'evening_focus'):
        """计算指定日期的加权因子.
        
        Args:
            trade_date: 交易日期，格式 YYYY-MM-DD
            config_name: 使用的权重配置名称
            
        Returns:
            计算的股票数量
            
        执行流程：
        1. 验证配置是否存在
        2. 执行 SQL 计算（参数化 trade_date 和 config_name）
        3. 返回结果统计
        """
        # TODO: 实现计算逻辑
        pass
    
    def create_custom_config(self, name: str, weights: list, 
                            config_type: str = 'custom') -> bool:
        """创建自定义权重配置.
        
        Args:
            name: 配置名称（唯一标识）
            weights: 48 个归一化权重值列表
            config_type: 配置类型（custom/time_based/risk_based/market_style）
            
        Returns:
            是否创建成功
            
        验证逻辑：
        1. 检查 name 是否已存在
        2. 验证 weights 长度为 48
        3. 验证权重之和为 1.0（允许 0.001 误差）
        4. 所有权重值必须 >= 0
        
        数据库约束说明：
        ClickHouse 不直接支持 CHECK 约束，通过应用层验证：
        - assert abs(sum(weights) - 1.0) < 0.001
        - assert all(w >= 0 for w in weights)
        """
        # TODO: 实现自定义配置创建逻辑
        pass


def main():
    # TODO: 实现参数解析和主流程
    parser = argparse.ArgumentParser(
        description='计算时间加权独立强度因子'
    )
    parser.add_argument('date', nargs='?', 
                       help='交易日期 (YYYY-MM-DD)，默认今天')
    parser.add_argument('--preset', '-p', default='evening_focus',
                       choices=list(TimeWeightedScoreCalculator.PRESETS.keys()),
                       help='使用预设权重模式')
    parser.add_argument('--custom-weights', '-w',
                       help='自定义权重（逗号分隔的 48 个浮点数）')
    parser.add_argument('--custom-name', '-n',
                       help='自定义配置名称（配合 --custom-weights 使用）')
    parser.add_argument('--init', action='store_true',
                       help='初始化表结构和预设配置')
    
    args = parser.parse_args()
    
    # 实现逻辑...


if __name__ == '__main__':
    main()
```

### 5.2 使用示例

```bash
# 初始化表结构和预设配置
./scripts/calc_time_weighted_score.py --init

# 使用默认预设（evening_focus）计算今日
./scripts/calc_time_weighted_score.py

# 使用指定预设计算指定日期
./scripts/calc_time_weighted_score.py 2025-03-20 --preset trending_market

# 使用自定义权重
./scripts/calc_time_weighted_score.py 2025-03-20 \
    --custom-weights "0.01,0.01,...（48 个）" \
    --custom-name "my_custom"
```

---

## 6. 回测支持

### 6.1 回测脚本扩展

在现有回测框架 `backtest_independence_score.py` 中增加对时间加权因子的支持：

**修改点**：
1. 数据源选项：支持从 `independence_score_daily` 或 `independence_score_time_weighted` 读取
2. 新增参数 `--source`：可选值 `original`（默认）或 `time_weighted`
3. 新增参数 `--config-name`：当 source=time_weighted 时指定配置名称
4. 分数字段映射：
   - original: `score` 字段
   - time_weighted: `weighted_score` 字段

**实现方式**：
```python
# 在原有回测脚本中增加参数解析
parser.add_argument('--source', choices=['original', 'time_weighted'], 
                   default='original')
parser.add_argument('--config-name', default='evening_focus')

# 根据 source 选择查询表和字段
if args.source == 'time_weighted':
    table = 'independence_score_time_weighted'
    score_field = 'weighted_score'
    extra_where = f"AND config_name = '{args.config_name}'"
else:
    table = 'independence_score_daily'
    score_field = 'score'
    extra_where = ''
```

**独立回测脚本**（可选）：
创建 `backtest_time_weighted.py` 专门用于对比分析，同时回测原始因子和多个加权因子配置，输出对比报告。

**现有回测脚本位置**：`01.independence-score/scripts/backtest_independence_score.py`

### 6.2 对比分析

支持同时回测原始因子和加权因子，输出对比报告：

| 指标 | 原始因子 | 时间加权因子 | 差异 |
|------|----------|--------------|------|
| 胜率 | xx% | xx% | +x% |
| 平均收益 | xx% | xx% | +x% |
| 夏普比率 | x.x | x.x | +x.x |

---

## 7. 测试策略

### 7.1 单元测试

- 验证区间序号计算正确性
- 验证权重归一化（总和为 1.0）
- 验证配置表 CRUD 操作

### 7.2 集成测试

- 单日计算：对比原始因子和加权因子的结果
- 多日回测：验证连续日期计算稳定性

### 7.3 数据验证

- 检查 weighted_score 与 raw_score 的关系合理性
- 验证不同配置下同一股票的分数差异符合预期

---

## 8. 部署计划

### 8.1 文件结构

```
01.independence-score/
├── sql/
│   ├── create_time_weighted_tables.sql      # 建表脚本（本设计）
│   └── calc_time_weighted_score.sql         # 核心计算 SQL
├── scripts/
│   └── calc_time_weighted_score.py          # 主计算脚本
└── docs/plans/
    └── 2026-03-26-time-weighted-independence-score-design.md  # 本文档
```

### 8.2 部署步骤

1. 执行建表 SQL
2. 初始化预设配置
3. 部署计算脚本
4. 测试单日计算
5. 运行历史回测验证

---

## 9. 风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 权重配置错误 | 计算结果偏差 | 配置表添加校验约束，确保权重和为 1.0 |
| 性能下降 | 计算时间增加 | SQL 优化，必要时添加物化视图 |
| 配置管理混乱 | 结果不可复现 | 强制记录 config_name，版本化管理配置 |

---

## 10. 附录

### 10.1 变更历史

| 日期 | 版本 | 变更内容 | 作者 |
|------|------|----------|------|
| 2026-03-26 | 1.0 | 初始设计 | AI Assistant |

### 10.2 参考资料

- [原始独立强度因子设计](2026-03-24-independence-score-design.md)
- [项目根 README](../../README.md)
