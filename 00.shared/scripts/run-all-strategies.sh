#!/bin/bash
# 三策略并行运行脚本
# 一键运行独立强度、动量因子、低贝塔混合三个策略，并生成综合信号

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 默认日期为今天
DATE=${1:-$(date +%Y-%m-%d)}

# 输出目录
OUTPUT_DIR="/tmp/strategy-output"
mkdir -p $OUTPUT_DIR

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}   三策略并行运行系统${NC}"
echo -e "${BLUE}================================================${NC}"
echo -e "日期: ${YELLOW}$DATE${NC}"
echo -e "输出: ${YELLOW}$OUTPUT_DIR${NC}"
echo ""

# 设置环境变量
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/00.shared/config/database.env" ]; then
    source "$SCRIPT_DIR/00.shared/config/database.env"
    echo -e "${GREEN}✓ 环境变量已加载${NC}"
else
    echo -e "${YELLOW}⚠ 未找到 database.env，使用默认配置${NC}"
fi

cd "$SCRIPT_DIR"

# ============================================================
# 策略1: 独立强度因子
# ============================================================
echo ""
echo -e "${BLUE}[1/4] 策略1: 独立强度因子${NC}"
echo "------------------------------------------------"

if [ -f "01.independence-score/scripts/calc_time_weighted_score.py" ]; then
    python3 01.independence-score/scripts/calc_time_weighted_score.py $DATE --preset evening_focus
    echo -e "${GREEN}✓ 独立强度因子计算完成${NC}"
else
    echo -e "${YELLOW}⚠ 独立强度因子脚本未找到，跳过${NC}"
fi

# ============================================================
# 策略2: 动量因子
# ============================================================
echo ""
echo -e "${BLUE}[2/4] 策略2: 动量因子${NC}"
echo "------------------------------------------------"

if [ -f "02.momentum-factor/scripts/calc_momentum.py" ]; then
    python3 02.momentum-factor/scripts/calc_momentum.py $DATE --output-json --output-dir $OUTPUT_DIR
    echo -e "${GREEN}✓ 动量因子计算完成${NC}"
else
    echo -e "${YELLOW}⚠ 动量因子脚本未找到，跳过${NC}"
fi

# ============================================================
# 策略3: 低贝塔混合策略
# ============================================================
echo ""
echo -e "${BLUE}[3/4] 策略3: 低贝塔混合策略${NC}"
echo "------------------------------------------------"

if [ -f "03.low-beta-hybrid/scripts/calc_low_beta_hybrid.py" ]; then
    python3 03.low-beta-hybrid/scripts/calc_low_beta_hybrid.py $DATE --output-json --output-dir $OUTPUT_DIR
    echo -e "${GREEN}✓ 低贝塔混合策略计算完成${NC}"
else
    echo -e "${YELLOW}⚠ 低贝塔混合策略脚本未找到，跳过${NC}"
fi

# ============================================================
# 汇总: 三策略信号整合
# ============================================================
echo ""
echo -e "${BLUE}[4/4] 三策略信号汇总${NC}"
echo "------------------------------------------------"

if [ -f "03.low-beta-hybrid/scripts/combine_signals.py" ]; then
    python3 03.low-beta-hybrid/scripts/combine_signals.py $DATE --min-overlap 2 --output-dir $OUTPUT_DIR
    
    # 额外生成三策略重合（最高置信度）
    echo ""
    echo -e "${YELLOW}生成三策略重合信号（最高置信度）...${NC}"
    python3 03.low-beta-hybrid/scripts/combine_signals.py $DATE --min-overlap 3 --output-dir $OUTPUT_DIR 2>/dev/null || true
    
    echo -e "${GREEN}✓ 信号汇总完成${NC}"
else
    echo -e "${YELLOW}⚠ 汇总脚本未找到，跳过${NC}"
fi

# ============================================================
# 结果展示
# ============================================================
echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}   所有策略运行完成!${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""

# 显示生成的文件
echo -e "${BLUE}生成的文件:${NC}"
ls -lh $OUTPUT_DIR/*$DATE*.json 2>/dev/null | awk '{printf "  %-60s %s\n", $9, $5}' || echo "  暂无JSON文件"

echo ""

# 如果有综合信号文件，显示统计
COMBINED_FILE="$OUTPUT_DIR/combined-signals-overlap2-$DATE.json"
if [ -f "$COMBINED_FILE" ]; then
    echo -e "${BLUE}综合信号统计:${NC}"
    
    # 使用Python解析JSON（如果安装了jq可以用jq）
    python3 << EOF 2>/dev/null || true
import json
import sys

try:
    with open('$COMBINED_FILE', 'r') as f:
        data = json.load(f)
    
    summary = data.get('summary', {})
    total = summary.get('total_combined', 0)
    overlap3 = summary.get('overlap_3_stocks', 0)
    overlap2 = summary.get('overlap_2_stocks', 0)
    
    print(f"  总重合信号: {total}只")
    print(f"  三策略重合: {overlap3}只 {'🌟🌟🌟 最高置信度!' if overlap3 > 0 else ''}")
    print(f"  两策略重合: {overlap2}只 {'🌟🌟 较高置信度' if overlap2 > 0 else ''}")
    
    if data.get('stocks'):
        print(f"\n  前5名:")
        for i, stock in enumerate(data['stocks'][:5], 1):
            symbol = stock['symbol']
            name = stock['name']
            overlap = stock['overlap_count']
            strategies = ','.join(stock['strategies'])
            print(f"    {i}. {symbol} {name} ({overlap}个策略: {strategies})")
except Exception as e:
    print(f"  解析JSON失败: {e}")
EOF
fi

echo ""
echo -e "${YELLOW}使用建议:${NC}"
echo "  1. 优先关注三策略重合的股票（最高置信度）"
echo "  2. 次选两策略重合的股票（较高置信度）"
echo "  3. 单策略信号可作为观察参考"
echo ""
echo -e "详细查看: ${BLUE}cat $COMBINED_FILE | jq '.stocks'${NC}"
echo ""
