# AGENTS.md - AI Coding Agent Guide

> 本文件供 AI 编程助手参考，帮助理解项目结构、开发规范和最佳实践。

---

## 项目概述

**策略仓库 (Strategy Repository)** 是一个量化交易策略开发与运行仓库，基于 ClickHouse 时序数据库和 PostgreSQL 关系型数据库构建。

### 核心功能

- **独立强度因子计算**：基于 5 分钟 K 线数据，识别板块下跌时表现抗跌的个股
- **融资余额加权**：结合融资融券数据对基础因子进行加权
- **历史回测框架**：验证因子历史表现，输出胜率、夏普比率等指标

### 技术栈

| 组件 | 技术 | 用途 |
|------|------|------|
| 时序数据库 | ClickHouse | 存储 5 分钟 K 线数据、计算结果 |
| 关系数据库 | PostgreSQL | 存储融资融券数据 |
| 脚本语言 | Python 3.8+ | 跨库集成、回测框架 |
| 自动化 | Bash | 批量计算脚本 |
| 依赖库 | clickhouse-driver, psycopg2-binary | 数据库连接 |

---

## 项目结构

```
40.strategies/
├── 00.shared/                    # 共享组件
│   ├── config/                   # 数据库配置
│   │   └── database.env          # 环境变量模板
│   ├── utils/                    # 通用工具函数 (预留)
│   └── templates/                # 报告模板 (预留)
│
├── 01.independence-score/        # 独立强度策略（当前唯一策略）
│   ├── sql/                      # SQL 脚本
│   │   ├── create_independence_tables.sql    # 建表和视图
│   │   ├── calc_independence_score.sql       # 核心计算逻辑
│   │   ├── queries_independence_score.sql    # 常用查询示例
│   │   └── backtest_independence_score.sql   # 回测 SQL
│   ├── scripts/                  # 可执行脚本
│   │   ├── calc_independence_score.sh        # 基础批量计算
│   │   ├── calc_independence_score_margin_weighted.py  # 融资加权版
│   │   └── backtest_independence_score.py    # 历史回测
│   ├── docs/                     # 策略文档
│   │   ├── plans/                # 设计文档和计划
│   │   │   ├── 2026-03-24-independence-score-design.md
│   │   │   ├── 2026-03-24-independence-score-implementation-plan.md
│   │   │   └── 2026-03-25-strategy-repo-restructure.md
│   │   └── 完整项目报告.md
│   ├── README.md                 # 策略说明文档
│   └── CLAUDE.md                 # Claude Code 专用指南
│
└── README.md                     # 项目根文档
```

### 目录命名规范

- **共享目录**：`00.shared/` - 存放所有策略共享的代码和配置
- **策略目录**：`NN.strategy-name/` - 两位数编号 + 策略名称
  - 编号从 01 开始递增
  - 使用小写字母和连字符
  - 示例：`01.independence-score`, `02.momentum-factor`

---

## 环境配置

### 1. 数据库环境变量

```bash
# ClickHouse 配置
export CH_HOST=localhost
export CH_PORT=9000
export CH_DB=tdx2db_rust
export CH_USER=default
export CH_PASSWORD=your_password

# PostgreSQL 配置
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=quantdb
export PG_USER=postgres
export PG_PASSWORD=your_password
```

或使用共享配置：
```bash
source 00.shared/config/database.env
```

### 2. Python 依赖安装

```bash
pip install clickhouse-driver psycopg2-binary
```

---

## 常用命令

### 数据库操作

```bash
# 建表
clickhouse-client --database=tdx2db_rust < 01.independence-score/sql/create_independence_tables.sql

# 查询某日结果
clickhouse-client --database=tdx2db_rust -q "
    SELECT * FROM independence_score_daily
    WHERE date = '2025-03-20'
    ORDER BY score DESC
    LIMIT 20
"
```

### 运行策略

```bash
cd 01.independence-score

# 基础计算（默认今日）
./scripts/calc_independence_score.sh

# 指定日期
./scripts/calc_independence_score.sh 2025-03-20

# 融资加权版
./scripts/calc_independence_score_margin_weighted.py 2025-03-20 --weight-factor 0.1

# 历史回测
./scripts/backtest_independence_score.py --start 2025-01-01 --end 2025-03-20 --threshold 3.0 --hold-days 5
```

---

## 代码规范

### SQL 规范

1. **文件组织**：每个 SQL 文件专注一个功能（建表、计算、查询、回测）
2. **参数化查询**：使用 ClickHouse 参数化语法 `{param:Type}`
   ```sql
   WHERE toDate(datetime) = {trade_date:Date}
   ```
3. **CTE 分层**：使用 WITH 子句将复杂逻辑分层
   ```sql
   WITH
   stock_returns AS (...),
   sector_returns AS (...),
   final_result AS (...)
   SELECT * FROM final_result
   ```
4. **窗口函数**：优先使用 `lagInFrame`, `countIf` 等高级函数
5. **中文注释**：关键逻辑添加中文注释

### Python 规范

1. **Shebang**：脚本文件必须包含 `#!/usr/bin/env python3`
2. **类型注解**：函数参数和返回值使用类型注解
3. **文档字符串**：类和方法使用 Google Style Docstrings
4. **环境变量**：数据库连接使用环境变量，提供默认值
5. **日志记录**：使用 `logging` 模块，格式统一
   ```python
   logging.basicConfig(
       level=logging.INFO,
       format='%(asctime)s - %(levelname)s - %(message)s'
   )
   ```

### Bash 规范

1. **Shebang**：`#!/bin/bash`
2. **严格模式**：`set -e` 在首行启用
3. **变量默认值**：`${VAR:-default}`
4. **错误处理**：关键命令检查返回值

---

## 开发流程

### 新增策略步骤

1. **创建目录结构**
   ```bash
   mkdir -p 02.new-strategy/{sql,scripts,docs}
   ```

2. **编写核心 SQL**
   - `sql/create_tables.sql` - 结果表和视图
   - `sql/calc_factor.sql` - 核心计算逻辑

3. **编写计算脚本**
   - `scripts/calc_factor.sh` - 批量计算入口
   - 复杂逻辑使用 Python

4. **编写文档**
   - `README.md` - 策略说明、使用方法
   - `CLAUDE.md` - 给 Claude Code 的上下文指南

5. **更新根 README**
   - 在策略列表中添加新策略

### 文档命名规范

- **报告文件**：`YYYY-MM-DD_报告名称.md`
- **计划文档**：`YYYY-MM-DD-计划名称.md`
- **设计文档**：`YYYY-MM-DD-策略名称-design.md`

---

## 数据架构

### ClickHouse 表

| 表名 | 来源 | 说明 |
|------|------|------|
| `raw_stocks_5min` | tdx2db-rust | 5 分钟 K 线数据 |
| `raw_stocks_daily` | tdx2db-rust | 日 K 线数据 |
| `stock_sectors` | tdx2db-rust | 股票板块映射 |
| `independence_score_daily` | 本策略生成 | 独立强度因子结果 |

### PostgreSQL 表

| 表名 | 来源 | 说明 |
|------|------|------|
| `margin_trading_detail_combined` | akshare | 融资融券明细合并视图 |

### 因子计算核心逻辑

```
独立强度因子 = Σ(符合计分条件的 5 分钟区间数)

计分条件（当板块跌幅 < -0.5% 时）：
- 个股涨幅 > 0%  → +1 分
- 相对板块超额收益 > 1% → +1 分

融资加权：
Weighted Score = Raw Score × (1 + 融资余额变化率 × 0.1)
```

---

## 测试策略

本项目没有传统单元测试，通过以下方式验证：

1. **SQL 语法检查**
   ```bash
   clickhouse-client --database=tdx2db_rust < sql/calc_independence_score.sql
   ```

2. **回测验证**
   ```bash
   ./scripts/backtest_independence_score.py --start 2025-01-01 --end 2025-03-20
   ```

3. **数据一致性检查**
   - 检查是否有 NULL 值
   - 检查日期范围是否正确
   - 检查分数分布是否合理

---

## 安全注意事项

1. **密码管理**
   - 不要在代码中硬编码密码
   - 使用环境变量或 `.env` 文件（已加入 `.gitignore`）
   - `database.env` 仅作为模板，实际密码需要手动设置

2. **数据库权限**
   - 生产环境使用只读用户运行查询
   - 写入操作使用独立账号

3. **数据安全**
   - 策略结果表使用 `ReplacingMergeTree` 引擎，支持重复写入去重
   - 重要计算结果建议备份

---

## 与外部系统集成

### tdx2db-rust 生态

本项目依赖 `tdx2db-rust` 提供的基础数据：
- 5 分钟 K 线：`raw_stocks_5min`
- 日 K 线：`raw_stocks_daily`
- 板块映射：`stock_sectors`

### akshare 数据源

融资融券数据通过 `akshare` 库获取并写入 PostgreSQL。

---

## 故障排查

### 常见问题

1. **连接失败**
   - 检查环境变量是否设置正确
   - 确认数据库服务是否启动

2. **SQL 执行错误**
   - 检查表是否存在：`SHOW TABLES`
   - 检查参数类型是否正确

3. **Python 导入错误**
   - 确认依赖已安装：`pip list | grep -E "clickhouse|psycopg2"`

### 调试技巧

```bash
# 查看 SQL 执行计划
clickhouse-client --database=tdx2db_rust -q "EXPLAIN ..."

# Python 调试模式
./scripts/calc_independence_score_margin_weighted.py 2025-03-20 -v
```

---

## 参考资料

- [项目根 README](README.md)
- [独立强度策略文档](01.independence-score/README.md)
- [Claude Code 指南](01.independence-score/CLAUDE.md)
- [完整项目报告](01.independence-score/docs/完整项目报告.md)
