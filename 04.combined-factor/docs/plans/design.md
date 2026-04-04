# 双因子组合策略设计文档

**创建日期**: 2026-03-27  
**策略状态**: 开发中

---

## 1. 策略背景

### 1.1 单因子局限性

| 单因子 | 优势 | 局限性 |
|--------|------|--------|
| 独立强度 | 抗跌能力强 | 可能选到防御股（涨不动） |
| 动量 | 上涨动能强 | 可能追到顶部（缺乏质地验证） |

### 1.2 双因子组合思路

```
独立强度（防守）+ 动量（进攻）= 攻守兼备
```

---

## 2. 因子定义

### 2.1 独立强度因子

- **来源**: `01.independence-score`
- **核心逻辑**: 板块下跌时个股抗跌能力
- **计分方式**: 逆势区间数量 × 时间权重

### 2.2 动量因子

- **来源**: `02.momentum-factor`
- **核心逻辑**: 价格趋势延续性
- **计分方式**: `0.5×20日收益 + 0.3×10日收益 + 0.2×5日收益`

### 2.3 综合得分

```
综合得分 = normalize(独立强度) × w1 × 100
        + normalize(动量) × w2 × 100

其中:
- normalize(x) = 1 - (排名 / 总数)  # 转为0-1区间，越高越好
- w1 + w2 = 1.0
- 默认 w1 = w2 = 0.5
```

---

## 3. 权重配置策略

### 3.1 市场环境自适应

| 市场环境 | 独立强度权重 | 动量权重 | 逻辑 |
|---------|-------------|---------|------|
| 牛市 | 30% | 70% | 强者恒强，追涨 |
| 震荡市 | 50% | 50% | 平衡配置 |
| 熊市 | 70% | 30% | 防守优先 |

### 3.2 动态调整思路（未来）

```python
# 根据市场波动率调整权重
if market_volatility > threshold:
    # 高波动时增加防御权重
    weight_ind = 0.6
    weight_mom = 0.4
else:
    # 低波动时增加进攻权重
    weight_ind = 0.4
    weight_mom = 0.6
```

---

## 4. 实现方案

### 4.1 数据流

```
┌─────────────────────┐     ┌─────────────────────┐
│ independence_score  │     │   momentum_score    │
│      _daily         │     │      _daily         │
└──────────┬──────────┘     └──────────┬──────────┘
           │                           │
           └───────────┬───────────────┘
                       ▼
            ┌─────────────────────┐
            │   INNER JOIN        │
            │   ON symbol, date   │
            └──────────┬──────────┘
                       ▼
            ┌─────────────────────┐
            │  Calculate Combined │
            │      Score          │
            └──────────┬──────────┘
                       ▼
            ┌─────────────────────┐
            │ combined_factor_    │
            │      daily          │
            └─────────────────────┘
```

### 4.2 核心SQL逻辑

```sql
-- 标准化排名
WITH 
ranked_ind AS (
    SELECT 
        symbol,
        score,
        rank() OVER (ORDER BY score DESC) as rank,
        count() OVER () as total
    FROM independence_score_daily
    WHERE date = '2026-03-26'
),
ranked_mom AS (
    SELECT 
        symbol,
        momentum_score,
        rank() OVER (ORDER BY momentum_score DESC) as rank,
        count() OVER () as total
    FROM momentum_factor_daily
    WHERE date = '2026-03-26'
)
-- 计算综合得分
SELECT 
    i.symbol,
    (1 - i.rank::Float32/i.total) * 0.5 * 100 +
    (1 - m.rank::Float32/m.total) * 0.5 * 100 as combined_score
FROM ranked_ind i
INNER JOIN ranked_mom m ON i.symbol = m.symbol
ORDER BY combined_score DESC;
```

---

## 5. 使用场景

### 5.1 每日选股流程

```
收盘后:
1. 确保独立强度因子已计算（01策略）
2. 确保动量因子已计算（02策略）
3. 执行双因子组合计算（本策略）
4. 取Top 20作为次日关注标的
5. 次日开盘根据市场环境微调
```

### 5.2 行业轮动增强

```
1. 计算各行业双因子平均分
2. 选取得分最高的3-5个行业
3. 在选定行业内选取Top标的
```

---

## 6. 风险控制

### 6.1 策略局限

| 局限 | 说明 | 应对 |
|------|------|------|
| 数据依赖 | 依赖两个上游因子 | 增加数据检查 |
| 权重主观 | 权重配置影响结果 | 提供多套预设 |
| 同向风险 | 两因子可能同时失效 | 设置止损机制 |

### 6.2 建议参数

```
选股数量: 10-30只
持有周期: 3-10天
止损线: -5%
止盈线: +15%
```

---

## 7. 开发计划

- [x] 创建目录结构
- [x] 编写建表 SQL
- [x] 编写计算 SQL
- [x] 编写 Python 计算脚本
- [ ] 编写回测脚本
- [ ] 测试数据正确性
- [ ] 优化权重配置
- [ ] 编写完整文档

---

## 8. 参考

- [01.independence-score](../../01.independence-score/) - 独立强度因子
- [02.momentum-factor](../../02.momentum-factor/) - 动量因子
- [03.low-beta-hybrid](../../03.low-beta-hybrid/) - 低贝塔混合策略参考
