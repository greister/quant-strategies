#!/usr/bin/env python3
"""
生成每日选股综合报告并导入 Obsidian
"""

import json
import sys
from datetime import datetime
from pathlib import Path


def load_json(filepath):
    """加载 JSON 文件（支持多行 JSON）"""
    data = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                data.append(json.loads(line))
    return data


def generate_report(trade_date):
    """生成综合报告"""
    results_dir = Path(__file__).parent.parent / 'results'
    
    # 加载各策略数据
    strategies = {
        'evening_focus': load_json(results_dir / 'evening_focus.json'),
        'morning_focus': load_json(results_dir / 'morning_focus.json'),
        'trending_market': load_json(results_dir / 'trending_market.json'),
        'conservative': load_json(results_dir / 'conservative.json'),
    }
    
    summary = load_json(results_dir / 'summary.json')
    
    # 生成报告 Markdown
    report = f"""# {trade_date} 每日选股报告-综合版

> 📅 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
> 📊 数据源: 独立强度因子多策略分析

---

## 📈 策略概览

| 策略名称 | 股票数量 | 平均加权分 | 最高分 | 平均逆势次数 |
|---------|---------|-----------|--------|-------------|
"""
    
    # 策略名称映射
    name_map = {
        'evening_focus': '尾盘关注型',
        'morning_focus': '早盘关注型',
        'trending_market': '趋势市',
        'conservative': '保守型',
        'rotating_market': '轮动市'
    }
    
    for item in summary:
        cn_name = name_map.get(item['config_name'], item['config_name'])
        report += f"| {cn_name} | {item['stock_count']} | {item['avg_score']} | {item['max_score']} | {item['avg_contra']} |\n"
    
    report += """
---

## 🎯 策略说明

- **尾盘关注型**: 重视收盘前表现，关注次日开盘预期
- **早盘关注型**: 重视开盘情绪，把握早盘异动
- **趋势市**: 早盘权重较高，适合趋势行情
- **保守型**: 全天均匀加权，与原始因子等价

---

"""
    
    # 各策略详细结果
    for key, name in [
        ('evening_focus', '尾盘关注型 Top15'),
        ('morning_focus', '早盘关注型 Top15'),
        ('trending_market', '趋势市 Top15'),
        ('conservative', '保守型 Top15')
    ]:
        data = strategies.get(key, [])
        report += f"## 📊 {name}\n\n"
        report += "| 排名 | 代码 | 名称 | 板块 | 原始分 | 加权分 | 逆势次数 |\n"
        report += "|-----|-----|------|-----|--------|--------|----------|\n"
        
        for i, item in enumerate(data[:15], 1):
            name_display = item.get('name', '')
            if name_display and len(name_display) > 8:
                name_display = name_display[:8]
            sector = item.get('sector', '')
            if sector and len(sector) > 8:
                sector = sector[:8]
            report += f"| {i} | {item['symbol']} | {name_display} | {sector} | {item['raw_score']} | {item['weighted_score']} | {item['contra_count']} |\n"
        
        report += "\n"
    
    # 共识股票分析
    report += """---

## 🔥 多策略共识股票

以下股票在多个策略中均表现优异:

"""
    
    # 收集所有策略中的股票
    all_symbols = {}
    for key, data in strategies.items():
        for item in data[:10]:  # 只考虑 Top10
            symbol = item['symbol']
            if symbol not in all_symbols:
                all_symbols[symbol] = []
            all_symbols[symbol].append({
                'strategy': name_map.get(key, key),
                'rank': item.get('rank', 0),
                'weighted_score': item['weighted_score'],
                'name': item.get('name', ''),
                'sector': item.get('sector', '')
            })
    
    # 找出在多个策略中出现的股票
    consensus = {k: v for k, v in all_symbols.items() if len(v) >= 2}
    
    if consensus:
        report += "| 代码 | 名称 | 板块 | 出现次数 | 策略分布 | 最高加权分 |\n"
        report += "|-----|------|-----|----------|---------|-----------|\n"
        
        for symbol, appearances in sorted(consensus.items(), key=lambda x: len(x[1]), reverse=True):
            name = appearances[0]['name'][:8] if appearances[0]['name'] else ''
            sector = appearances[0]['sector'][:8] if appearances[0]['sector'] else ''
            count = len(appearances)
            strategies_str = ', '.join([a['strategy'] for a in appearances])
            max_score = max([a['weighted_score'] for a in appearances])
            report += f"| {symbol} | {name} | {sector} | {count} | {strategies_str} | {max_score} |\n"
    else:
        report += "> 暂无在多策略中同时出现的股票\n"
    
    report += """
---

## 📝 因子说明

**独立强度因子**衡量股票在板块下跌时的抗跌能力:

- 板块跌幅 < -0.5% 时，视为板块下跌区间
- 在该区间内，满足以下任一条件即计 1 分:
  - 个股上涨（涨幅 > 0%）
  - 相对板块超额收益 > 1%
- 全天累加得到独立强度分值

**时间加权说明**:
- 归一化权重：全天 48 个 5 分钟区间权重之和为 1.0
- 不同策略对早盘/午盘/尾盘赋予不同权重

---

*报告由多策略并行计算系统自动生成*
"""
    
    return report


def main():
    trade_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    
    report = generate_report(trade_date)
    
    # 输出到本地 results 目录
    output_file = Path(__file__).parent.parent / 'results' / f'{trade_date}_report.md'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"✅ 报告已生成: {output_file}")
    
    # 同步到 Obsidian Vault
    vault_dir = Path('/mnt/d/obsidian/OrbitOS-vault')
    if vault_dir.exists():
        vault_target_dir = vault_dir / '30_Research' / '量化分析' / '策略执行结果' / '01-独立强度因子'
        vault_target_dir.mkdir(parents=True, exist_ok=True)
        
        vault_file = vault_target_dir / f'{trade_date}_每日选股报告-综合版.md'
        with open(vault_file, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"✅ 报告已同步到 Obsidian Vault: {vault_file}")
    else:
        print(f"⚠️ Obsidian Vault 路径不存在: {vault_dir}")
    
    print(report)


if __name__ == '__main__':
    main()
