#!/bin/bash
# 并行执行所有独立强度策略
# 使用方式: ./run_all_strategies.sh [日期]

set -e

# 加载配置
source ../00.shared/config/database.env

# 日期参数
TRADE_DATE="${1:-$(date +%Y-%m-%d)}"
# 如果今日无数据，使用最新交易日
LATEST_DATE=$(clickhouse-client --host="$CH_HOST" --port="$CH_PORT" --user="$CH_USER" --password="$CH_PASSWORD" \
    --query "SELECT max(toDate(datetime)) FROM raw_stocks_5min")

# 检查今日是否有数据
DATA_COUNT=$(clickhouse-client --host="$CH_HOST" --port="$CH_PORT" --user="$CH_USER" --password="$CH_PASSWORD" \
    --database="$CH_DB" --query "SELECT count() FROM raw_stocks_5min WHERE toDate(datetime) = '$TRADE_DATE'" 2>/dev/null || echo "0")

if [ "$DATA_COUNT" = "0" ]; then
    echo "⚠️  $TRADE_DATE 无数据，使用最近交易日: $LATEST_DATE"
    TRADE_DATE="$LATEST_DATE"
fi

echo "=========================================="
echo "🚀 启动多策略并行计算"
echo "📅 交易日: $TRADE_DATE"
echo "=========================================="
echo ""

# 确保表结构存在
echo "📋 检查并创建表结构..."
clickhouse-client --host="$CH_HOST" --port="$CH_PORT" --user="$CH_USER" --password="$CH_PASSWORD" \
    --database="$CH_DB" < sql/create_independence_tables.sql 2>/dev/null || true

# 输出目录
OUTPUT_DIR="./results"

# Obsidian Vault 路径
VAULT_DIR="/mnt/d/obsidian/OrbitOS-vault"
REPORT_TARGET_DIR="$VAULT_DIR/30_Research/量化分析/策略执行结果/01-独立强度因子"
mkdir -p "$OUTPUT_DIR"

# ============================================
# 策略 1: 基础版 (独立进程)
# ============================================
run_strategy_1() {
    echo "🔵 [策略1] 基础独立强度因子 - 开始"
    local start_time=$(date +%s)
    
    ./scripts/calc_independence_score.sh "$TRADE_DATE" > "$OUTPUT_DIR/strategy1_$TRADE_DATE.log" 2>&1
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # 获取Top10结果
    local top10=$(clickhouse-client --host="$CH_HOST" --port="$CH_PORT" --user="$CH_USER" --password="$CH_PASSWORD" \
        --database="$CH_DB" --format=JSONEachRow --query "
        SELECT 
            symbol,
            sector,
            score as weighted_score,
            contra_count,
            '基础版' as strategy
        FROM independence_score_daily
        WHERE date = '$TRADE_DATE'
        ORDER BY score DESC
        LIMIT 10
    " 2>/dev/null)
    
    echo "$top10" > "$OUTPUT_DIR/strategy1_$TRADE_DATE.json"
    echo "✅ [策略1] 完成 (耗时 ${duration}s)"
}

# ============================================
# 策略 2: 时间加权版 - 尾盘关注型
# ============================================
run_strategy_2() {
    echo "🟢 [策略2] 时间加权(尾盘关注型) - 开始"
    local start_time=$(date +%s)
    
    ./scripts/calc_time_weighted_score.py "$TRADE_DATE" --preset evening_focus > "$OUTPUT_DIR/strategy2_$TRADE_DATE.log" 2>&1
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    echo "✅ [策略2] 完成 (耗时 ${duration}s)"
}

# ============================================
# 策略 3: 时间加权版 - 早盘关注型
# ============================================
run_strategy_3() {
    echo "🟡 [策略3] 时间加权(早盘关注型) - 开始"
    local start_time=$(date +%s)
    
    ./scripts/calc_time_weighted_score.py "$TRADE_DATE" --preset morning_focus > "$OUTPUT_DIR/strategy3_$TRADE_DATE.log" 2>&1
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    echo "✅ [策略3] 完成 (耗时 ${duration}s)"
}

# ============================================
# 策略 4: 时间加权版 - 趋势市
# ============================================
run_strategy_4() {
    echo "🟠 [策略4] 时间加权(趋势市) - 开始"
    local start_time=$(date +%s)
    
    ./scripts/calc_time_weighted_score.py "$TRADE_DATE" --preset trending_market > "$OUTPUT_DIR/strategy4_$TRADE_DATE.log" 2>&1
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    echo "✅ [策略4] 完成 (耗时 ${duration}s)"
}

# ============================================
# 策略 5: 时间加权版 - 保守型
# ============================================
run_strategy_5() {
    echo "🟣 [策略5] 时间加权(保守型) - 开始"
    local start_time=$(date +%s)
    
    ./scripts/calc_time_weighted_score.py "$TRADE_DATE" --preset conservative > "$OUTPUT_DIR/strategy5_$TRADE_DATE.log" 2>&1
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    echo "✅ [策略5] 完成 (耗时 ${duration}s)"
}

# ============================================
# 策略 6: 融资加权版 (如果 PostgreSQL 有数据)
# ============================================
run_strategy_6() {
    echo "🔴 [策略6] 融资余额加权 - 开始"
    local start_time=$(date +%s)
    
    # 检查是否有融资数据
    local margin_count=$(psql "postgresql://$PG_USER:$PG_PASSWORD@$PG_HOST:$PG_PORT/$PG_DB" \
        -t -c "SELECT count(*) FROM margin_trading_detail_combined WHERE trade_date = '$TRADE_DATE'" 2>/dev/null | xargs)
    
    if [ -z "$margin_count" ] || [ "$margin_count" = "0" ]; then
        echo "⚠️ [策略6] 无融资数据，跳过"
        return
    fi
    
    ./scripts/calc_independence_score_margin_weighted.py "$TRADE_DATE" > "$OUTPUT_DIR/strategy6_$TRADE_DATE.log" 2>&1
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    echo "✅ [策略6] 完成 (耗时 ${duration}s)"
}

# ============================================
# 导出所有结果
# ============================================
export_results() {
    echo ""
    echo "📤 导出结果数据..."
    
    # 基础版 Top 20
    clickhouse-client --host="$CH_HOST" --port="$CH_PORT" --user="$CH_USER" --password="$CH_PASSWORD" \
        --database="$CH_DB" --format=JSONEachRow --query "
        SELECT 
            symbol,
            sector,
            score as weighted_score,
            raw_score,
            contra_count,
            '基础版' as strategy,
            $TRADE_DATE as date
        FROM independence_score_daily
        WHERE date = '$TRADE_DATE'
        ORDER BY score DESC
        LIMIT 20
    " > "$OUTPUT_DIR/basic_$TRADE_DATE.json" 2>/dev/null || echo "[]" > "$OUTPUT_DIR/basic_$TRADE_DATE.json"
    
    # 时间加权各配置 Top 20
    for preset in evening_focus morning_focus trending_market conservative; do
        clickhouse-client --host="$CH_HOST" --port="$CH_PORT" --user="$CH_USER" --password="$CH_PASSWORD" \
            --database="$CH_DB" --format=JSONEachRow --query "
            SELECT 
                symbol,
                name,
                sector,
                weighted_score,
                raw_score,
                contra_count,
                '$preset' as strategy,
                '$TRADE_DATE' as date
            FROM independence_score_time_weighted
            WHERE date = '$TRADE_DATE' AND config_name = '$preset'
            ORDER BY weighted_score DESC
            LIMIT 20
        " > "$OUTPUT_DIR/${preset}_$TRADE_DATE.json" 2>/dev/null || echo "[]" > "$OUTPUT_DIR/${preset}_$TRADE_DATE.json"
    done
    
    echo "✅ 结果导出完成: $OUTPUT_DIR/"
}

# ============================================
# 复制报告到 Obsidian Vault
# ============================================
copy_to_vault() {
    echo ""
    echo "📤 复制报告到 Obsidian Vault..."
    
    # 确保目标目录存在
    mkdir -p "$REPORT_TARGET_DIR"
    
    # 复制报告
    local report_file="$OUTPUT_DIR/${TRADE_DATE}_report.md"
    if [ -f "$report_file" ]; then
        cp "$report_file" "$REPORT_TARGET_DIR/${TRADE_DATE}_每日选股报告-综合版.md"
        echo "✅ 报告已复制到: $REPORT_TARGET_DIR/${TRADE_DATE}_每日选股报告-综合版.md"
    else
        echo "⚠️ 报告文件不存在: $report_file"
    fi
}

# ============================================
# 主执行流程
# ============================================
main() {
    local total_start=$(date +%s)
    
    # 并行执行所有策略
    run_strategy_1 &
    PID1=$!
    
    run_strategy_2 &
    PID2=$!
    
    run_strategy_3 &
    PID3=$!
    
    run_strategy_4 &
    PID4=$!
    
    run_strategy_5 &
    PID5=$!
    
    run_strategy_6 &
    PID6=$!
    
    # 等待所有后台任务完成
    wait $PID1 $PID2 $PID3 $PID4 $PID5 $PID6
    
    # 导出结果
    export_results
    
    # 复制到 Vault
    copy_to_vault
    
    local total_end=$(date +%s)
    local total_duration=$((total_end - total_start))
    
    echo ""
    echo "=========================================="
    echo "🎉 所有策略执行完成!"
    echo "📅 交易日: $TRADE_DATE"
    echo "⏱️  总耗时: ${total_duration}s"
    echo "📁 结果目录: $OUTPUT_DIR/"
    echo "=========================================="
    
    # 输出统计信息
    echo ""
    echo "📊 统计信息:"
    echo "------------------------------------------"
    
    # 基础版统计
    local basic_count=$(clickhouse-client --host="$CH_HOST" --port="$CH_PORT" --user="$CH_USER" --password="$CH_PASSWORD" \
        --database="$CH_DB" --query "SELECT count() FROM independence_score_daily WHERE date = '$TRADE_DATE'" 2>/dev/null | xargs)
    echo "基础版: $basic_count 只股票"
    
    # 时间加权统计
    for preset in evening_focus morning_focus trending_market conservative; do
        local count=$(clickhouse-client --host="$CH_HOST" --port="$CH_PORT" --user="$CH_USER" --password="$CH_PASSWORD" \
            --database="$CH_DB" --query "SELECT count() FROM independence_score_time_weighted WHERE date = '$TRADE_DATE' AND config_name = '$preset'" 2>/dev/null | xargs)
        echo "$preset: ${count:-0} 只股票"
    done
}

# 运行主程序
main
