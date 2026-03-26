#!/bin/bash
# 时间加权因子测试脚本

set -e

cd "$(dirname "$0")/.."

echo "=== 时间加权独立强度因子测试 ==="
echo

# 测试日期（可改为最近有数据的日期）
TEST_DATE="${1:-2025-03-20}"
SCRIPT="./scripts/calc_time_weighted_score.py"

echo "1. 测试列出预设..."
$SCRIPT --list-presets
echo "[PASS]"
echo

echo "2. 测试使用 evening_focus 预设计算 $TEST_DATE..."
$SCRIPT "$TEST_DATE" --preset evening_focus
echo "[PASS]"
echo

echo "3. 测试使用 trending_market 预设计算 $TEST_DATE..."
$SCRIPT "$TEST_DATE" --preset trending_market
echo "[PASS]"
echo

echo "4. 验证结果表数据..."
clickhouse-client --database=tdx2db_rust -q "
    SELECT 
        config_name,
        count() as stock_count,
        avg(raw_score) as avg_raw,
        avg(weighted_score) as avg_weighted
    FROM independence_score_time_weighted
    WHERE date = '$TEST_DATE'
    GROUP BY config_name
    ORDER BY config_name
"
echo "[PASS]"
echo

echo "5. 验证权重调整效果..."
clickhouse-client --database=tdx2db_rust -q "
    SELECT 
        code,
        name,
        raw_score,
        weighted_score,
        config_name
    FROM independence_score_time_weighted
    WHERE date = '$TEST_DATE'
    ORDER BY weighted_score DESC
    LIMIT 5
"
echo "[PASS]"
echo

echo "6. 测试自定义权重..."
# 创建一个简单的自定义权重（早盘权重高）
CUSTOM_WEIGHTS=$(python3 -c "print(','.join(['0.03']*12 + ['0.015']*12 + ['0.01']*12 + ['0.015']*12))")
$SCRIPT "$TEST_DATE" --custom-weights "$CUSTOM_WEIGHTS" --custom-name "test_morning_focus"
echo "[PASS]"
echo

echo "=== 所有测试通过 ==="
