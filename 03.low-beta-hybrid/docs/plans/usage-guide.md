# 三策略并行运行指南

**文档版本**: v1.0  
**创建日期**: 2026-03-27

---

## 概述

本指南介绍如何并行运行三个策略，并生成综合信号。

---

## 策略矩阵

| 策略 | 核心逻辑 | 适用场景 | 信号特点 |
|------|---------|---------|---------|
| **01** 独立强度 | 5分钟逆势选股 | 震荡/下跌市 | 信号较多 |
| **02** 动量因子 | 价格趋势跟踪 | 趋势市 | 顺势而为 |
| **03** 低贝塔混合 | 低β防御 + 逆势 | 系统性下跌 | 回撤小、质量高 |

---

## 快速开始

### 一键运行三策略

```bash
#!/bin/bash
# run-all-strategies.sh

DATE=${1:-$(date +%Y-%m-%d)}
echo "=== 运行三策略: $DATE ==="

# 设置环境变量
source /home/eo/scripts/40.strategies/00.shared/config/database.env

cd /home/eo/scripts/40.strategies

# 策略1: 独立强度因子
echo "[1/4] 独立强度因子..."
01.independence-score/scripts/calc_independence_score.sh $DATE

# 策略2: 动量因子
echo "[2/4] 动量因子..."
02.momentum-factor/scripts/calc_momentum.py $DATE --output-json

# 策略3: 低贝塔混合
echo "[3/4] 低贝塔混合策略..."
03.low-beta-hybrid/scripts/calc_low_beta_hybrid.py $DATE --output-json

# 汇总: 找出重合信号
echo "[4/4] 三策略汇总..."
03.low-beta-hybrid/scripts/combine_signals.py $DATE --min-overlap 2

echo "✓ 所有策略运行完成!"
echo "输出目录: /tmp/strategy-output/"
ls -lh /tmp/strategy-output/*$DATE*.json
```

### 使用方法

```bash
# 运行今日策略
./run-all-strategies.sh

# 运行指定日期
./run-all-strategies.sh 2026-03-27
```

---

## 输出文件说明

运行后会生成以下JSON文件：

```
/tmp/strategy-output/
├── 01-independence-score-top20-20260327.json    # 独立强度TOP20
├── 02-momentum-factor-top20-20260327.json       # 动量因子TOP20
├── 03-low-beta-hybrid-top20-20260327.json       # 低贝塔混合TOP20
└── combined-signals-overlap2-20260327.json      # 三策略汇总
```

### 综合信号解读

`combined-signals-overlap2-20260327.json` 包含：

```json
{
  "type": "combined",
  "summary": {
    "total_combined": 15,      // 总共15只股票被≥2个策略选中
    "overlap_3_stocks": 3,     // 3只被三个策略同时选中（最高置信度）
    "overlap_2_stocks": 12     // 12只被两个策略选中
  },
  "stocks": [
    {
      "symbol": "sz301479",
      "name": "弘景光电",
      "overlap_count": 3,        // 被3个策略选中
      "strategies": ["01", "02", "03"],
      "avg_score": 85.6
    }
  ]
}
```

---

## 实战交易流程

### 每日收盘后（15:00-15:30）

```bash
# 1. 运行三策略
./run-all-strategies.sh $(date +%Y-%m-%d)

# 2. 查看综合信号
cat /tmp/strategy-output/combined-signals-overlap2-$(date +%Y-%m-%d).json | jq '.stocks[:10]'

# 3. 筛选三策略重合的股票（最高置信度）
cat /tmp/strategy-output/combined-signals-overlap2-$(date +%Y-%m-%d).json | jq '.stocks[] | select(.overlap_count == 3)'
```

### 次日交易（09:30）

1. **高置信度信号**（被3个策略选中）：
   - 开盘直接买入
   - 仓位：每只 8%
   - 持有：3-5天

2. **中等置信度信号**（被2个策略选中）：
   - 观察30分钟后决定
   - 仓位：每只 5%
   - 持有：2-3天

---

## 高级用法

### 只查看三策略重合的股票

```bash
./scripts/combine_signals.py 2026-03-27 --min-overlap 3
```

### 调整各策略的选股数量

修改脚本中的 `--top-n` 参数：

```bash
# 独立强度取前30名
01.independence-score/scripts/calc_independence_score.sh $DATE --top-n 30

# 低贝塔混合取前50名
03.low-beta-hybrid/scripts/calc_low_beta_hybrid.py $DATE --top-n 50
```

### 自定义输出目录

```bash
./scripts/calc_low_beta_hybrid.py 2026-03-27 --output-dir /path/to/custom/output
```

---

## 常见问题

### Q1: 三策略都没有信号怎么办？

**A**: 说明当前市场没有明显机会，建议观望。这种情况通常发生在：
- 单边牛市（独立强度、低贝塔信号少）
- 震荡市无趋势（动量信号少）
- 假期前后（数据不完整）

### Q2: 为什么三策略重合的股票很少？

**A**: 这是正常现象，说明：
- 三个策略逻辑不同，选股标准各异
- 重合度低 = 每个策略有独特性
- 重合的股票 = 被多重验证，质量更高

### Q3: 可以同时买入三个策略的所有TOP股票吗？

**A**: 不建议，因为：
- 股票数量太多（可能60+只）
- 资金分散，收益摊薄
- 建议只买重合的股票（15-20只）

---

## 最佳实践

1. **每日固定时间运行**: 收盘后15分钟内
2. **优先关注三策略重合**: 胜率最高
3. **控制总仓位**: 不超过80%
4. **设置止损**: 单只-4%，整体-10%
5. **定期回顾**: 每周检查策略表现

---

**参考**: 
- [策略1: 独立强度](../01.independence-score/README.md)
- [策略2: 动量因子](../02.momentum-factor/README.md)
- [策略3: 低贝塔混合](../README.md)
