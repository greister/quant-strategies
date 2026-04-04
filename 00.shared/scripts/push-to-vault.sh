#!/bin/bash
# push-to-vault.sh - 推送策略结果到 Obsidian Vault，按策略区分

set -e

# 配置
OBSIDIAN_VAULT="D:\\obsidian\\OrbitOS-vault"
TARGET_DIR="$OBSIDIAN_VAULT\\30_Research\\量化分析\\策略执行结果"

echo "=== 策略结果推送至 Obsidian Vault ==="
echo "源目录: /tmp/strategy-output/"
echo "目标: $TARGET_DIR"
echo ""

# 检查源目录
if [ ! -d "/tmp/strategy-output" ]; then
    echo "❌ 错误: /tmp/strategy-output 目录不存在"
    exit 1
fi

# 按策略类型分别推送
strategy_files=(
    "01-independence-score"
    "02-momentum-factor"
    "03-low-beta-hybrid"
)

for strategy_prefix in "${strategy_files[@]}"; do
    echo "=== $strategy_prefix ==="
    files=$(ls /tmp/strategy-output/${strategy_prefix}-*.json 2>/dev/null || true)
    
    if [ -z "$files" ]; then
        echo "  无此策略的结果文件"
        continue
    fi
    
    for file in $files; do
        filename=$(basename "$file")
        echo "  📄 $filename"
    done
done

echo ""
echo "==========================================="
echo "请手动复制以下文件到目标目录:"
echo ""
echo "1. 独立强度因子 (01.independence-score)"
echo "   文件: /tmp/strategy-output/01-independence-score-*.json"
echo "   → 目标: $TARGET_DIR\\01-独立强度因子\\"
echo ""
echo "2. 动量因子 (02.momentum-factor)"
echo "   文件: /tmp/strategy-output/02-momentum-factor-*.json"
echo "   → 目标: $TARGET_DIR\\02-动量因子\\"
echo ""
echo "3. 低贝塔混合 (03.low-beta-hybrid)"
echo "   文件: /tmp/strategy-output/03-low-beta-hybrid-*.json"
echo "   → 目标: $TARGET_DIR\\03-低贝塔混合\\"
echo ""
echo "4. 综合信号 (combined)"
echo "   文件: /tmp/strategy-output/combined-signals-*.json"
echo "   → 目标: $TARGET_DIR\\99-综合信号\\"
echo ""

# 输出文件列表
ls -la /tmp/strategy-output/*.json 2>/dev/null || echo "(目录为空)"
