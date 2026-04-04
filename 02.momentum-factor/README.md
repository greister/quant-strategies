# 动量因子策略 (Momentum Factor)

**策略编号**: 02  
**策略名称**: momentum-factor  
**创建日期**: 2026-03-26  
**状态**: 🚧 开发中

---

## 策略概述

### 核心逻辑

动量因子策略基于**价格动量效应**，即过去表现强势的股票在未来一段时间内倾向于继续表现强势。

### 计算逻辑

```
动量因子 = (当前价格 - N日前价格) / N日前价格 × 100%

或

动量因子 = 过去N日累计收益率
```

### 参数设置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `lookback_period` | 20 | 回看周期（交易日） |
| `rank_pct` | 0.2 | 选股分位数（前20%） |

---

## 目录结构

```
02.momentum-factor/
├── sql/                           # SQL 脚本
│   ├── create_tables.sql          # 建表和视图
│   ├── calc_momentum_factor.sql   # 基础动量因子计算
│   └── calc_low_beta_rs.sql       # 低贝塔+相对强度混合策略 ★NEW
├── scripts/                       # 可执行脚本
│   ├── calc_momentum.py           # 基础动量因子计算脚本
│   └── calc_low_beta_rs.py        # 低贝塔+RS策略计算脚本 ★NEW
├── docs/                          # 策略文档
│   └── plans/
│       └── design.md              # 策略设计文档
└── README.md                      # 本文件
```

---

## 🆕 低贝塔抗跌 + 相对强度混合策略

---

## 快速开始

### 1. 建表

```bash
clickhouse-client --database=tdx2db_rust < sql/create_tables.sql
```

### 2. 计算某日动量因子

```bash
./scripts/calc_momentum.py 2026-03-20
```

### 3. 查看结果

```bash
clickhouse-client --database=tdx2db_rust -q "
    SELECT * FROM momentum_factor_daily
    WHERE date = '2026-03-20'
    ORDER BY momentum_score DESC
    LIMIT 20
"
```

---

## 文件说明

### SQL 文件

| 文件 | 说明 |
|------|------|
| `create_tables.sql` | 创建结果表和视图 |
| `calc_momentum_factor.sql` | 动量因子计算逻辑 |
| `queries.sql` | 常用查询示例 |

### 脚本文件

| 文件 | 说明 |
|------|------|
| `calc_momentum.py` | Python计算脚本 |

---

## 开发计划

- [x] 创建目录结构
- [x] 编写建表 SQL
- [x] 编写动量因子计算 SQL
- [x] 编写低贝塔+RS混合策略 SQL ★NEW
- [x] 完善 Python 脚本
- [x] 编写低贝塔+RS策略 Python 脚本 ★NEW
- [ ] 测试 SQL 正确性
- [ ] 编写回测框架
- [ ] 验证策略有效性
- [ ] 编写完整文档

---

## 参考

- [项目根 README](../README.md)
- [AGENTS.md](../AGENTS.md)

---

## 🆕 低贝塔抗跌 + 相对强度混合策略

### 策略概述

在传统动量因子基础上，增加**防御性 (低Beta)** 和**进攻性 (相对强度)** 双重筛选。

### 核心逻辑

```
综合得分 = Beta得分 (40%) + 相对强度得分 (40%) + 成交量得分 (20%)
```

| 指标 | 权重 | 说明 |
|------|------|------|
| Beta得分 | 40% | Beta < 0.5 得40分，Beta < 0.8 得30分，以此类推 |
| 相对强度得分 | 40% | 跑赢市场越多得分越高 |
| 成交量得分 | 20% | 放量上涨时加分 |

### 策略分类

| 标签 | 条件 | 特征 |
|------|------|------|
| **低贝塔强势** | Beta < 0.8 且 RS > 0 | 防御+进攻兼备，最佳标的 |
| **低贝塔防守** | Beta < 0.8 且 RS ≤ 0 | 纯防御，适合熊市 |
| **高贝塔进攻** | Beta ≥ 0.8 且 RS > 0 | 激进进攻，波动大 |

### 日内交易信号

```
买入信号: Beta < 0.8 AND RS > 0 AND 成交量 > 1.2倍均量 AND 0% < 当日涨幅 < 3%
观望/卖出: 当日涨幅 > 5% OR 成交量 < 0.8倍均量
持有: 其他情况
```

### 使用方法

```bash
# 计算某日因子
./scripts/calc_low_beta_rs.py 2026-03-20

# 查看综合排名前20
./scripts/calc_low_beta_rs.py 2026-03-20 --top-n 20

# 只看"低贝塔强势"标签的股票
./scripts/calc_low_beta_rs.py 2026-03-20 --tag "低贝塔强势"

# 查看买入信号股票
./scripts/calc_low_beta_rs.py 2026-03-20 --signal "买入信号"

# 显示策略汇总统计
./scripts/calc_low_beta_rs.py 2026-03-20 --summary
```

### SQL 查询示例

```bash
# 查看某日买入信号股票
clickhouse-client --database=tdx2db_rust -q "
    SELECT symbol, name, sector, beta, relative_strength, composite_score
    FROM low_beta_rs_factor_daily
    WHERE date = '2026-03-20' AND intraday_signal = '买入信号'
    ORDER BY composite_score DESC
    LIMIT 20
"

# 按策略标签统计
clickhouse-client --database=tdx2db_rust -q "
    SELECT strategy_tag, count(), avg(composite_score)
    FROM low_beta_rs_factor_daily
    WHERE date = '2026-03-20'
    GROUP BY strategy_tag
"
```
