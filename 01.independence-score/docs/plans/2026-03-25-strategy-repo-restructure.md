# Strategy Repository Restructure Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rename `40.tracky` to `40.strategies` and restructure with numbered subdirectories for each strategy.

**Architecture:** Use `00.shared/` for common components and numbered directories (`01.xxx/`, `02.xxx/`) for individual strategies. Current independence-score strategy becomes `01.independence-score/`.

**Tech Stack:** Bash, Git, existing SQL/Python scripts

---

### Task 1: Create New Directory Structure

**Files:**
- Create: `40.strategies/00.shared/`
- Create: `40.strategies/00.shared/config/`
- Create: `40.strategies/00.shared/utils/`
- Create: `40.strategies/00.shared/templates/`
- Create: `40.strategies/01.independence-score/`

**Step 1: Create new directory structure**

Run:
```bash
cd /home/eo/scripts
mkdir -p 40.strategies/00.shared/{config,utils,templates}
mkdir -p 40.strategies/01.independence-score
```

Expected: Directories created without errors

**Step 2: Verify structure**

Run:
```bash
find 40.strategies -type d | sort
```

Expected:
```
40.strategies
40.strategies/00.shared
40.strategies/00.shared/config
40.strategies/00.shared/templates
40.strategies/00.shared/utils
40.strategies/01.independence-score
```

**Step 3: Commit**

```bash
git add 40.strategies/
git commit -m "chore: create new strategy repository structure"
```

---

### Task 2: Move Current Strategy to 01.independence-score

**Files:**
- Move: `40.tracky/sql/` → `40.strategies/01.independence-score/sql/`
- Move: `40.tracky/scripts/` → `40.strategies/01.independence-score/scripts/`
- Move: `40.tracky/docs/` → `40.strategies/01.independence-score/docs/`
- Move: `40.tracky/README.md` → `40.strategies/01.independence-score/README.md`
- Move: `40.tracky/CLAUDE.md` → `40.strategies/01.independence-score/CLAUDE.md`

**Step 1: Move files**

Run:
```bash
cd /home/eo/scripts
cp -r 40.tracky/sql 40.strategies/01.independence-score/
cp -r 40.tracky/scripts 40.strategies/01.independence-score/
cp -r 40.tracky/docs 40.strategies/01.independence-score/
cp 40.tracky/README.md 40.strategies/01.independence-score/
cp 40.tracky/CLAUDE.md 40.strategies/01.independence-score/
```

**Step 2: Verify move**

Run:
```bash
ls -la 40.strategies/01.independence-score/
```

Expected: sql, scripts, docs directories and README.md, CLAUDE.md files present

**Step 3: Commit**

```bash
git add 40.strategies/01.independence-score/
git commit -m "chore: move independence-score strategy to 01.independence-score/"
```

---

### Task 3: Create Shared Configuration

**Files:**
- Create: `40.strategies/00.shared/config/database.env`
- Create: `40.strategies/00.shared/config/settings.yaml`

**Step 1: Create database.env**

```bash
cat > 40.strategies/00.shared/config/database.env << 'EOF'
# ClickHouse Configuration
CH_HOST=localhost
CH_PORT=9000
CH_DB=tdx2db_rust
CH_USER=default
CH_PASSWORD=

# PostgreSQL Configuration
PG_HOST=localhost
PG_PORT=5432
PG_DB=quantdb
PG_USER=postgres
PG_PASSWORD=
EOF
```

**Step 2: Create settings.yaml**

```bash
cat > 40.strategies/00.shared/config/settings.yaml << 'EOF'
# Strategy Global Settings

# Default thresholds for independence score
independence_score:
  sector_return_threshold: -0.5  # -0.5%
  stock_return_threshold: 0.0    # 0%
  excess_return_threshold: 1.0   # 1%
  margin_weight_factor: 0.1      # 10%

# Backtest defaults
backtest:
  default_hold_days: 5
  default_threshold: 3.0
  default_top_n: 20

# Report settings
report:
  output_format: markdown
  date_prefix_format: "%Y-%m-%d"
EOF
```

**Step 3: Commit**

```bash
git add 40.strategies/00.shared/config/
git commit -m "chore: add shared configuration files"
```

---

### Task 4: Create Shared Utilities

**Files:**
- Create: `40.strategies/00.shared/utils/db_connection.py`
- Create: `40.strategies/00.shared/utils/__init__.py`

**Step 1: Create db_connection.py**

```bash
cat > 40.strategies/00.shared/utils/db_connection.py << 'EOF'
"""Shared database connection utilities."""

import os
from typing import Optional
import psycopg2
from clickhouse_driver import Client


def get_clickhouse_client(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None
) -> Client:
    """Get ClickHouse client with environment fallback."""
    return Client(
        host=host or os.getenv('CH_HOST', 'localhost'),
        port=port or int(os.getenv('CH_PORT', '9000')),
        database=database or os.getenv('CH_DB', 'tdx2db_rust'),
        user=user or os.getenv('CH_USER', 'default'),
        password=password or os.getenv('CH_PASSWORD', ''),
    )


def get_postgres_connection(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None
):
    """Get PostgreSQL connection with environment fallback."""
    return psycopg2.connect(
        host=host or os.getenv('PG_HOST', 'localhost'),
        port=port or int(os.getenv('PG_PORT', '5432')),
        database=database or os.getenv('PG_DB', 'quantdb'),
        user=user or os.getenv('PG_USER', 'postgres'),
        password=password or os.getenv('PG_PASSWORD', ''),
    )
EOF
```

**Step 2: Create __init__.py**

```bash
cat > 40.strategies/00.shared/utils/__init__.py << 'EOF'
"""Shared utilities for strategy development."""

from .db_connection import get_clickhouse_client, get_postgres_connection

__all__ = ['get_clickhouse_client', 'get_postgres_connection']
EOF
```

**Step 3: Commit**

```bash
git add 40.strategies/00.shared/utils/
git commit -m "chore: add shared database utilities"
```

---

### Task 5: Create Root README

**Files:**
- Create: `40.strategies/README.md`

**Step 1: Create root README**

```bash
cat > 40.strategies/README.md << 'EOF'
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
└── 02.xxx/                 # 未来策略
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
./scripts/calc_independence_score.sh 2025-03-20
```

## 策略列表

| 编号 | 策略名称 | 说明 | 状态 |
|------|----------|------|------|
| 01 | independence-score | 分时独立强度因子 | ✅ 已完成 |

## 开发规范

- 新策略创建编号子目录（如 `02.momentum-factor/`）
- 共享代码放入 `00.shared/`
- 报告文件名使用日期前缀：`YYYY-MM-DD_报告名称.md`
EOF
```

**Step 2: Commit**

```bash
git add 40.strategies/README.md
git commit -m "docs: add root README for strategy repository"
```

---

### Task 6: Update Independence-Score README

**Files:**
- Modify: `40.strategies/01.independence-score/README.md`

**Step 1: Update paths in README**

Replace relative paths to reference shared resources:

```bash
sed -i 's|pip install|# 确保已安装依赖\n# pip install -r ../../00.shared/requirements.txt 2>/dev/null || pip install|' \
  40.strategies/01.independence-score/README.md
```

Add note about shared config:

```bash
cat >> 40.strategies/01.independence-score/README.md << 'EOF'

## 共享资源

- 数据库配置：`../00.shared/config/database.env`
- 工具函数：`../00.shared/utils/`
EOF
```

**Step 2: Commit**

```bash
git add 40.strategies/01.independence-score/README.md
git commit -m "docs: update independence-score README with shared resources"
```

---

### Task 7: Remove Old Directory

**Files:**
- Delete: `40.tracky/` (after verification)

**Step 1: Verify new structure is complete**

Run:
```bash
cd /home/eo/scripts
diff -r 40.tracky/sql 40.strategies/01.independence-score/sql
diff -r 40.tracky/scripts 40.strategies/01.independence-score/scripts
```

Expected: No differences (or only expected differences)

**Step 2: Remove old directory**

```bash
rm -rf 40.tracky
```

**Step 3: Commit**

```bash
git add -A 40.tracky/
git commit -m "chore: remove old 40.tracky directory (moved to 40.strategies/)"
```

---

### Task 8: Final Verification

**Step 1: Verify final structure**

Run:
```bash
cd /home/eo/scripts/40.strategies
find . -type f -name "*.md" -o -name "*.sql" -o -name "*.py" -o -name "*.sh" | sort
```

Expected: All files present in new locations

**Step 2: Test basic functionality**

Run:
```bash
cd /home/eo/scripts/40.strategies
source 00.shared/config/database.env
echo "CH_HOST=$CH_HOST"
echo "PG_HOST=$PG_HOST"
```

Expected: Environment variables loaded

**Step 3: Final commit**

```bash
git status  # Should be clean
git log --oneline -5
```

---

## Summary

After completion:
- `40.tracky` → `40.strategies`
- Current strategy in `01.independence-score/`
- Shared resources in `00.shared/`
- Ready for new strategies (`02.xxx/`, `03.xxx/`)
