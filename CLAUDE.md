# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **quantitative trading factor project** (分时独立强度因子策略) that calculates an "Independence Score" measuring how well stocks resist sector downturns using ClickHouse 5-minute K-line data.

**Core Logic:**
- When sector drops > 0.5% in a 5-minute interval, stocks get +1 point if they:
  - Rise in absolute terms (> 0%), OR
  - Outperform sector by > 1%
- Scores accumulate throughout the trading day

**Data Sources:**
- **ClickHouse** (`tdx2db_rust` database): 5-minute K-line (`raw_stocks_5min`), sector mappings (`stock_sectors`)
- **PostgreSQL** (`quantdb` database): Margin trading data (`margin_trading_detail_combined`)

## Common Commands

### Environment Setup
```bash
# Install Python dependencies
pip install clickhouse-driver psycopg2-binary

# Set environment variables
export CH_HOST=localhost CH_PORT=9000 CH_DB=tdx2db_rust CH_USER=default CH_PASSWORD=xxx
export PG_HOST=localhost PG_PORT=5432 PG_DB=quantdb PG_USER=postgres PG_PASSWORD=xxx
```

### Database Setup
```bash
clickhouse-client --database=tdx2db_rust < sql/create_independence_tables.sql
```

### Run Calculations
```bash
# Basic calculation for a specific date
./scripts/calc_independence_score.sh 2025-03-20

# Margin-weighted calculation
./scripts/calc_independence_score_margin_weighted.py 2025-03-20 --weight-factor 0.1

# Backtesting
./scripts/backtest_independence_score.py --start 2025-01-01 --end 2025-03-20 --threshold 3.0 --hold-days 5
```

### Query Results
```bash
clickhouse-client --database=tdx2db_rust -q "
    SELECT * FROM independence_score_daily
    WHERE date = '2025-03-20'
    ORDER BY score DESC
    LIMIT 20
"
```

## Architecture

### Data Flow
```
┌─────────────────┐     ┌─────────────────┐
│   ClickHouse    │     │   PostgreSQL    │
│  (tdx2db_rust)  │     │    (quantdb)    │
├─────────────────┤     ├─────────────────┤
│ raw_stocks_5min │     │ margin_trading_ │
│ stock_sectors   │◄────┤ detail_combined │
│ independence_   │     │                 │
│ score_daily     │     │                 │
└─────────────────┘     └─────────────────┘
```

### Key Components

**SQL Layer (`sql/`):**
- `calc_independence_score.sql` - Core calculation using ClickHouse window functions (`lagInFrame`, `countIf`)
- `create_independence_tables.sql` - Creates `independence_score_daily` table (ReplacingMergeTree) and `v_independence_leaders` view
- `queries_independence_score.sql` - Common query patterns
- `backtest_independence_score.sql` - SQL-based backtesting framework

**Script Layer (`scripts/`):**
- `calc_independence_score.sh` - Bash wrapper for ClickHouse SQL execution
- `calc_independence_score_margin_weighted.py` - Python integration fetching margin data from PostgreSQL and computing weighted scores
- `backtest_independence_score.py` - Backtesting framework with performance metrics (win rate, Sharpe ratio, max drawdown)

### Key Parameters (Adjustable)

In `sql/calc_independence_score.sql`:
- `sector_return_threshold`: -0.5% (sector decline threshold)
- `stock_return_threshold`: 0% (stock rise threshold)
- `excess_return_threshold`: 1% (outperformance threshold)

## File Organization

```
.
├── sql/                          # SQL scripts
│   ├── create_independence_tables.sql
│   ├── calc_independence_score.sql
│   ├── queries_independence_score.sql
│   └── backtest_independence_score.sql
├── scripts/                      # Executable scripts
│   ├── calc_independence_score.sh
│   ├── calc_independence_score_margin_weighted.py
│   └── backtest_independence_score.py
└── docs/                         # Documentation
    └── plans/                    # Design documents
```

## Dependencies

- **ClickHouse**: Time-series database for market data
- **PostgreSQL**: Relational database for margin trading data
- **Python 3.8+**: `clickhouse-driver`, `psycopg2-binary`
- **Bash**: For automation scripts

## Notes

- This is a data analysis/quantitative research project without traditional build tools
- No formal test suite - validation is done through backtesting
- No linting configuration - focus is on SQL and Python script correctness
- Reports should be saved to `docs/` with date prefix format: `YYYY-MM-DD_报告名称.md`
- The project integrates with the larger tdx2db-rust ecosystem (ClickHouse market data pipeline)
