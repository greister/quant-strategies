#!/bin/bash
# ============================================================================
# 独立强度因子 V2 计算脚本
# 改进版：成交量加权 + 波动率调整
# ============================================================================

set -e

# 默认参数
TRADE_DATE="${1:-$(date +%Y-%m-%d)}"
THRESHOLD="${2:-1.0}"  # 根据回测报告，使用 1.0 阈值（夏普比率 0.17）
TOP_N="${3:-20}"

# 数据库配置
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-tdx2db_rust}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-tdx2db}"

# 路径配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SQL_FILE="$PROJECT_DIR/sql/calc_independence_score_v2.sql"
RESULTS_DIR="$PROJECT_DIR/results"
LOG_FILE="$RESULTS_DIR/v2_calc_${TRADE_DATE}.log"

# 确保结果目录存在
mkdir -p "$RESULTS_DIR"

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=========================================="
log "独立强度因子 V2 计算开始"
log "交易日期: $TRADE_DATE"
log "阈值: $THRESHOLD"
log "Top N: $TOP_N"
log "=========================================="

# 检查 SQL 文件
if [[ ! -f "$SQL_FILE" ]]; then
    log "错误: SQL 文件不存在: $SQL_FILE"
    exit 1
fi

# 检查 ClickHouse 连接
log "检查 ClickHouse 连接..."
if ! clickhouse-client \
    --host="$CLICKHOUSE_HOST" \
    --port="$CLICKHOUSE_PORT" \
    --database="$CLICKHOUSE_DB" \
    --user="$CLICKHOUSE_USER" \
    --password="$CLICKHOUSE_PASSWORD" \
    --query="SELECT 1" > /dev/null 2>&1; then
    log "错误: 无法连接到 ClickHouse"
    exit 1
fi
log "ClickHouse 连接正常"

# 检查源数据表是否存在
log "检查源数据表..."
TABLE_CHECK=$(clickhouse-client \
    --host="$CLICKHOUSE_HOST" \
    --port="$CLICKHOUSE_PORT" \
    --database="$CLICKHOUSE_DB" \
    --user="$CLICKHOUSE_USER" \
    --password="$CLICKHOUSE_PASSWORD" \
    --query="SELECT count() FROM raw_stocks_5min WHERE toDate(datetime) = '$TRADE_DATE'" 2>/dev/null || echo "0")

if [[ "$TABLE_CHECK" == "0" ]]; then
    log "警告: $TRADE_DATE 没有 5 分钟数据"
    exit 0
fi

log "源数据检查通过: $TABLE_CHECK 条记录"

# 执行 V2 SQL 计算
log "执行 V2 版本 SQL 计算..."
START_TIME=$(date +%s)

# 读取 SQL 并替换参数
SQL_CONTENT=$(cat "$SQL_FILE")

# 执行查询并保存结果
RESULT_FILE="$RESULTS_DIR/${TRADE_DATE}_independence_score_v2.csv"

clickhouse-client \
    --host="$CLICKHOUSE_HOST" \
    --port="$CLICKHOUSE_PORT" \
    --database="$CLICKHOUSE_DB" \
    --user="$CLICKHOUSE_USER" \
    --password="$CLICKHOUSE_PASSWORD" \
    --param_trade_date="$TRADE_DATE" \
    --format=CSVWithNames \
    --query="$SQL_CONTENT" > "$RESULT_FILE" 2>> "$LOG_FILE"

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

# 统计结果
RESULT_COUNT=$(tail -n +2 "$RESULT_FILE" | wc -l)
log "计算完成: $RESULT_COUNT 只股票 (耗时 ${ELAPSED}秒)"

# 筛选 Top N
TOP_FILE="$RESULTS_DIR/${TRADE_DATE}_top${TOP_N}_v2.csv"
head -1 "$RESULT_FILE" > "$TOP_FILE"
awk -F',' -v threshold="$THRESHOLD" '
NR > 1 {
    # 假设 independence_score_v2 在第 3 列
    if ($3 >= threshold) print $0
}
' "$RESULT_FILE" | head -n "$TOP_N" >> "$TOP_FILE"

TOP_COUNT=$(tail -n +2 "$TOP_FILE" | wc -l)
log "筛选结果: $TOP_COUNT 只股票 (阈值 >= $THRESHOLD, Top $TOP_N)"

# 生成 Markdown 报告
REPORT_FILE="$RESULTS_DIR/${TRADE_DATE}_v2_report.md"

cat > "$REPORT_FILE" << EOF
# 独立强度因子 V2 选股报告

**报告日期**: $TRADE_DATE  
**策略版本**: V2 (成交量加权 + 波动率调整)  
**阈值**: $THRESHOLD (根据回测优化)  
**Top N**: $TOP_N

---

## 计算参数

| 参数 | 数值 | 说明 |
|------|------|------|
| 板块下跌阈值 | -0.5% | 低于此值视为板块下跌 |
| 个股上涨阈值 | 0% | 高于此值视为个股上涨 |
| 超额收益阈值 | 1% | 相对板块超额收益阈值 |
| 成交量加权系数 | 0.3 | 成交量影响权重 |
| 波动率回看 | 20日 | 历史波动率计算周期 |
| 目标波动率 | 2.0% | 日波动率目标值 |

---

## 选股结果 (Top $TOP_COUNT)

| 排名 | 股票代码 | 板块 | 综合得分 | 基础分 | 成交量加权分 | 波动率调整分 | 日波动率 |
|------|----------|------|----------|--------|--------------|--------------|----------|
EOF

# 添加 Top 股票数据
tail -n +2 "$TOP_FILE" | head -20 | while IFS=',' read -r trade_date symbol sector_code independence_score_v2 base_score volume_weighted_score volatility_adjusted_score total_intervals independence_ratio volatility_daily volatility_factor max_excess_return avg_contra_excess avg_contra_return avg_contra_volume daily_rank; do
    rank=$(echo "$daily_rank" | tr -d '"')
    score=$(echo "$independence_score_v2" | tr -d '"')
    base=$(echo "$base_score" | tr -d '"')
    vol_weight=$(echo "$volume_weighted_score" | tr -d '"')
    vol_adj=$(echo "$volatility_adjusted_score" | tr -d '"')
    vol_day=$(echo "$volatility_daily" | tr -d '"')
    
    printf "| %s | %s | %s | %s | %s | %s | %s | %s |\n" \
        "$rank" "$symbol" "$sector_code" "$score" "$base" "$vol_weight" "$vol_adj" "$vol_day" >> "$REPORT_FILE"
done

cat >> "$REPORT_FILE" << EOF

---

## 指标说明

- **综合得分 (independence_score_v2)**: 最终独立强度因子，已考虑成交量加权和波动率调整
- **基础分 (base_score)**: 原始逆势区间计数
- **成交量加权分**: 高成交量时刻逆势表现权重更高
- **波动率调整分**: 根据个股波动率标准化后的得分
- **日波动率**: 个股日内波动率，用于风险评估

---

## V2 版本改进

1. **成交量加权**: 高成交量区间的逆势表现给予更高权重
2. **波动率调整**: 低波动股票同样逆势表现得分更高，高波动股票得分下调
3. **阈值优化**: 根据回测报告，使用 1.0 阈值（夏普比率 0.17）

---

*报告生成时间: $(date '+%Y-%m-%d %H:%M:%S')*  
*数据来源: ClickHouse tdx2db_rust*
EOF

log "报告生成: $REPORT_FILE"

# 保存到数据库
log "保存结果到数据库..."

clickhouse-client \
    --host="$CLICKHOUSE_HOST" \
    --port="$CLICKHOUSE_PORT" \
    --database="$CLICKHOUSE_DB" \
    --user="$CLICKHOUSE_USER" \
    --password="$CLICKHOUSE_PASSWORD" \
    --query="
    CREATE TABLE IF NOT EXISTS independence_score_v2_daily (
        trade_date Date,
        symbol String,
        sector_code String,
        independence_score_v2 Float64,
        base_score Int32,
        volume_weighted_score Float64,
        volatility_adjusted_score Float64,
        total_intervals Int32,
        independence_ratio Float64,
        volatility_daily Float64,
        volatility_annual Float64,
        volatility_factor Float64,
        max_excess_return Float64,
        avg_contra_excess Float64,
        avg_contra_return Float64,
        avg_contra_volume Float64,
        daily_rank Int32,
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (trade_date, independence_score_v2 DESC)
"

# 导入数据
clickhouse-client \
    --host="$CLICKHOUSE_HOST" \
    --port="$CLICKHOUSE_PORT" \
    --database="$CLICKHOUSE_DB" \
    --user="$CLICKHOUSE_USER" \
    --password="$CLICKHOUSE_PASSWORD" \
    --query="INSERT INTO independence_score_v2_daily FORMAT CSVWithNames" < "$RESULT_FILE"

log "数据已保存到 independence_score_v2_daily 表"
log "=========================================="
log "V2 计算完成"
log "结果文件: $RESULT_FILE"
log "Top 股票: $TOP_FILE"
log "报告: $REPORT_FILE"
log "=========================================="

# 显示 Top 10
echo ""
echo "Top 10 股票 (V2):"
head -11 "$TOP_FILE" | column -t -s','
