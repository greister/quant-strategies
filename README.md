# 策略仓库 (Strategy Repository)

量化交易策略开发与运行仓库。

## 目录结构

```
40.strategies/
├── 00.shared/              # 共享组件
│   ├── config/             # 数据库配置、环境变量
│   ├── utils/              # 通用工具函数
│   └── templates/          # 报告模板
│
├── 01.independence-score/  # 独立强度策略
│   ├── sql/                # SQL 脚本
│   ├── scripts/            # 计算脚本
│   └── docs/               # 策略文档
│
├── 02.momentum-factor/     # 动量因子策略
│   ├── sql/                # SQL 脚本
│   ├── scripts/            # 计算脚本
│   └── docs/               # 策略文档
│
├── 03.low-beta-hybrid/     # 低贝塔混合策略 ⭐ NEW
│   ├── sql/                # SQL 脚本
│   ├── scripts/            # 计算脚本（含三策略汇总）
│   └── docs/               # 策略文档
│
└── 04.xxx/                 # 未来策略
```

## 快速开始

### 环境设置

```bash
# 加载数据库配置
source 00.shared/config/database.env

# 或使用 export 设置
export CH_HOST=localhost CH_PORT=9000 CH_DB=tdx2db_rust
export PG_HOST=localhost PG_PORT=5432 PG_DB=quantdb
```

### 运行策略

```bash
# 独立强度策略
cd 01.independence-score
./scripts/calc_independence_score.sh 2026-03-20

# 动量因子策略
cd 02.momentum-factor
./scripts/calc_momentum.py 2026-03-20

# 低贝塔混合策略
cd 03.low-beta-hybrid
./scripts/calc_low_beta_hybrid.py 2026-03-20 --output-json

# 三策略汇总（找出重合信号）
./scripts/combine_signals.py 2026-03-20 --min-overlap 2
```

## 策略列表

| 编号 | 策略名称 | 说明 | 状态 |
|------|----------|------|------|
| 01 | independence-score | 分时独立强度因子 | ✅ 已完成 |
| 02 | momentum-factor | 动量因子策略 | 🚧 开发中 |
| 03 | low-beta-hybrid | 低贝塔混合策略（防御+逆势） | ✅ 已实现 |

## 开发规范

- 新策略创建编号子目录（如 `02.momentum-factor/`）
- 共享代码放入 `00.shared/`
- 报告文件名使用日期前缀：`YYYY-MM-DD_报告名称.md`
