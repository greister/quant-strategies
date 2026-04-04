# 双因子组合策略 - Agent 操作指南

## 快速开始

### 1. 建表

```bash
source ../00.shared/config/database.env
clickhouse-client --host="$CH_HOST" --port="$CH_PORT" --user="$CH_USER" --password="$CH_PASSWORD" \
    --database="$CH_DB" < sql/create_tables.sql
```

### 2. 计算双因子得分

```bash
# 使用默认权重（50:50）
./scripts/calc_combined_factor.py 2026-03-26

# 自定义权重
./scripts/calc_combined_factor.py 2026-03-26 --independence-weight 0.6 --momentum-weight 0.4
```

### 3. 查看结果

```bash
clickhouse-client --database=tdx2db_rust -q "
SELECT * FROM combined_factor_daily
WHERE date = '2026-03-26'
ORDER BY combined_score DESC
LIMIT 20
"
```

---

## 策略参数

### 权重调整

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--independence-weight` | 0.5 | 独立强度因子权重 |
| `--momentum-weight` | 0.5 | 动量因子权重 |
| `--top-n` | 20 | 选取前N名 |

### 权重配置建议

```bash
# 牛市配置
./scripts/calc_combined_factor.py 2026-03-26 --independence-weight 0.3 --momentum-weight 0.7

# 震荡市配置
./scripts/calc_combined_factor.py 2026-03-26 --independence-weight 0.5 --momentum-weight 0.5

# 熊市配置
./scripts/calc_combined_factor.py 2026-03-26 --independence-weight 0.7 --momentum-weight 0.3
```

---

## 数据依赖

确保以下表已有数据：

```sql
-- 检查独立强度因子
SELECT count() FROM independence_score_daily WHERE date = '2026-03-26';

-- 检查动量因子
SELECT count() FROM momentum_factor_daily WHERE date = '2026-03-26';
```

如缺少数据，先执行：

```bash
# 计算独立强度因子
cd ../01.independence-score
./scripts/calc_time_weighted_score.py 2026-03-26

# 计算动量因子
cd ../02.momentum-factor
./scripts/calc_momentum.py 2026-03-26
```

---

## 输出表结构

```sql
combined_factor_daily
├── date: Date              # 日期
├── symbol: String          # 股票代码
├── sector: String          # 板块
├── independence_score: Float32  # 独立强度分数
├── momentum_score: Float32      # 动量分数
├── independence_rank_pct: Float32  # 独立强度排名分位
├── momentum_rank_pct: Float32      # 动量排名分位
├── combined_score: Float32    # 综合得分
├── weight_ind: Float32        # 独立强度权重
├── weight_mom: Float32        # 动量权重
└── calculated_at: DateTime    # 计算时间
```

---

## 回测

```bash
./scripts/backtest_combined_factor.py \
    --start 2026-01-01 \
    --end 2026-03-26 \
    --top-n 20 \
    --hold-days 5
```

---

## 导入 Obsidian

计算完成后，结果会自动复制到：

```
30_Research/量化分析/策略执行结果/04-双因子组合/
```

如需手动复制：

```bash
cp results/2026-03-26_report.md "/mnt/d/obsidian/OrbitOS-vault/30_Research/量化分析/策略执行结果/04-双因子组合/"
```
