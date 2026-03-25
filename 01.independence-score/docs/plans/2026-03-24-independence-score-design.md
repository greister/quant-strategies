# 分时独立强度因子策略设计

## 背景

基于 ClickHouse 5 分钟线数据，计算个股相对板块的"逆势分"，识别在板块下跌时表现抗跌的个股。

## 核心逻辑

### 独立强度因子 `independence_score`

**定义：** 个股在板块下跌时表现出的相对抗跌能力，量化为每日分值。

### 计算步骤

1. **板块跌幅判定**
   - 取个股所属板块/概念内所有股票的 5 分钟收益率均值
   - 板块收益率 = 板块内所有股票收益率的等权平均

2. **逆势判定条件**
   - 当板块跌幅 < -0.5% 时
   - 个股收益 > 0 或 (个股收益 - 板块收益) > 1%

3. **计分规则**
   - 每个满足条件的 5 分钟区间计 1 分
   - 全天累加得到 `raw_score`

4. **融资确认（可选）**
   - 当日融资余额变化率 > 0 时，分值 × 1.2 加权
   - 最终 `independence_score = raw_score × margin_weight`

## 数据依赖

### ClickHouse 表

| 表名 | 用途 |
|------|------|
| `raw_stocks_5min` | 5 分钟 K 线数据源 |
| `stock_sectors` | 股票-板块关联 |
| `stock_concepts` | 股票-概念关联 |

### PostgreSQL 表（可选确认层）

| 表名 | 用途 |
|------|------|
| `margin_trading_detail_sse` | 上海个股融资融券明细 |
| `margin_trading_detail_szse` | 深圳个股融资融券明细 |

## 输出

### 核心表

```sql
CREATE TABLE independence_score_daily (
    symbol String,
    date Date,
    score Float64,
    raw_score Int32,
    margin_weight Float64 DEFAULT 1.0,
    sector String,
    concept String,
    sector_count Int32 COMMENT '板块内股票数',
    contra_count Int32 COMMENT '逆势区间数'
)
```

### 视图

```sql
-- 每日排名视图
CREATE VIEW v_independence_leaders AS
SELECT date, symbol, score, sector, concept,
       rank() OVER (PARTITION BY date ORDER BY score DESC) as rank
FROM independence_score_daily
```

## 与现有系统的关系

| 组件 | 交互方式 |
|------|----------|
| `raw_stocks_5min` | 主数据源，直接查询 |
| `stock_sectors` / `stock_concepts` | JOIN 获取板块归属 |
| `margin_trading_detail_*` | 应用层 JOIN，可选确认 |
| `margin_zscore` (现有) | 互补因子，可叠加使用 |

## 可调参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `sector_decline_threshold` | -0.5% | 板块跌幅阈值 |
| `outperformance_threshold` | 1% | 超额收益阈值 |
| `margin_weight_factor` | 1.2 | 融资确认加权系数 |

## 后续扩展

1. **多板块聚合**：同时考虑行业和概念板块，取最大或平均得分
2. **时间衰减**：收盘前 30 分钟的逆势行为给予更高权重
3. **历史分位数**：将当日得分与过去 60 日历史比较，生成 Z-score
