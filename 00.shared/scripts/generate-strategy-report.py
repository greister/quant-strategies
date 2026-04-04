#!/usr/bin/env python3
"""
generate-strategy-report.py - 生成策略执行报告 Markdown

按策略类型分别生成 Obsidian 格式的 Markdown 报告，便于在 Vault 中查看。
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


def load_json_file(filepath: Path) -> Optional[dict]:
    """加载 JSON 文件"""
    if not filepath.exists():
        return None
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"警告: 无法加载 {filepath}: {e}")
        return None


def generate_independence_score_report(data: dict, date: str) -> str:
    """生成独立强度因子策略报告"""
    stocks = data.get('data', [])
    top_n = data.get('top_n', len(stocks))
    
    lines = [
        "---",
        "date:", f"  - \"{date}\"",
        "tags:",
        "  - 量化分析",
        "  - 独立强度因子",
        "  - 策略01",
        "  - 逆势抗跌",
        "---",
        "",
        f"# 🛡️ 独立强度因子策略报告 - {date}",
        "",
        "> **策略说明**: 识别板块下跌时表现抗跌的个股，基于5分钟K线数据计算独立强度得分。",
        "> **数据来源**: 5分钟K线 + 融资余额加权",
        "> **目标**: 寻找在市场下跌时具有独立性的强势股票",
        "",
        "## 📊 策略执行摘要",
        "",
        f"- **交易日**: {date}",
        f"- **选股数量**: {len(stocks)} / {data.get('total_candidates', len(stocks))}",
        f"- **筛选条件**: 板块跌幅<-0.5%, 相对超额收益>1%",
        "",
        "## 🏆 精选股票列表",
        "",
        "| 排名 | 代码 | 名称 | 板块 | 原始得分 | 加权得分 | 逆势区间数 |",
        "|------|------|------|------|----------|----------|------------|",
    ]
    
    for i, stock in enumerate(stocks, 1):
        lines.append(
            f"| {i} | `{stock['symbol']}` | {stock['name']} | {stock['sector']} | "
            f"{stock['raw_score']:.2f} | {stock['weighted_score']:.2f} | {stock['contra_count']} |"
        )
    
    lines.extend([
        "",
        "## 📈 得分分布",
        "",
        "### 按板块分组",
        "",
    ])
    
    # 按板块分组
    sector_groups = {}
    for stock in stocks:
        sector = stock['sector']
        if sector not in sector_groups:
            sector_groups[sector] = []
        sector_groups[sector].append(stock)
    
    for sector, sector_stocks in sorted(sector_groups.items(), key=lambda x: -len(x[1])):
        avg_score = sum(s['weighted_score'] for s in sector_stocks) / len(sector_stocks)
        lines.extend([
            f"- **{sector}**: {len(sector_stocks)}只 (平均加权得分: {avg_score:.2f})",
        ])
    
    lines.extend([
        "",
        "## 💡 交易建议",
        "",
        "> [!tip] 操作建议",
        "> 1. 关注加权得分 > 10 的股票，这些个股在逆势中表现突出",
        "> 2. 结合融资余额变化，筛选资金持续流入的标的",
        "> 3. 建议次日开盘观察板块动向后再做决策",
        "> 4. 设置止损线：如果次日板块继续下跌且个股跟跌，考虑止损",
        "",
        "---",
        "",
        f"*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
    ])
    
    return "\n".join(lines)


def generate_low_beta_hybrid_report(data: dict, date: str) -> str:
    """生成低贝塔混合策略报告"""
    stocks = data.get('data', [])
    low_beta_pool = data.get('low_beta_pool', {})
    
    lines = [
        "---",
        "date:", f"  - \"{date}\"",
        "tags:",
        "  - 量化分析",
        "  - 低贝塔混合策略",
        "  - 策略03",
        "  - 稳健型",
        "---",
        "",
        f"# 🎯 低贝塔混合策略报告 - {date}",
        "",
        "> **策略说明**: 先筛选低贝塔股票(β<0.8)，再在其中计算独立强度得分，寻找稳健型抗跌标的。",
        "> **数据来源**: 日K线 + 5分钟K线",
        "> **基准指数**: 中证500 (sh000905)",
        "",
        "## 📊 策略执行摘要",
        "",
        f"- **交易日**: {date}",
        f"- **低贝塔池**: {low_beta_pool.get('count', 0)} 只 (β<0.8, 抗跌≥8天)",
    ]
    
    if low_beta_pool.get('count', 0) > 0:
        lines.extend([
            f"- **平均贝塔**: {low_beta_pool.get('avg_beta', 0):.3f}",
            f"- **入选混合策略**: {len(stocks)} 只",
        ])
    
    lines.extend([
        "",
        "## 🏆 混合策略精选",
        "",
    ])
    
    if not stocks:
        lines.extend([
            "> [!warning] 无入选股票",
            ">",
            "> 今日低贝塔池为空或低贝塔股票中没有满足独立强度条件的标的。",
            ">",
            "> 可能原因：",
            "> - 近期市场波动大，贝塔<0.8的股票较少",
            "> - 低贝塔股票中缺少逆势上涨的标的",
            "",
        ])
    else:
        lines.extend([
            "| 排名 | 代码 | 名称 | 板块 | 贝塔 | 抗跌天数 | 独立强度分 | 综合得分 |",
            "|------|------|------|------|------|----------|------------|----------|",
        ])
        
        for i, stock in enumerate(stocks, 1):
            lines.append(
                f"| {i} | `{stock['symbol']}` | {stock['name']} | {stock['sector']} | "
                f"{stock.get('beta', 0):.2f} | {stock.get('anti_fall_days', 0)} | "
                f"{stock.get('independence_score', 0):.1f} | {stock.get('hybrid_score', 0):.1f} |"
            )
        
        lines.extend([
            "",
            "## 📈 风险收益特征",
            "",
        ])
        
        # 计算统计数据
        avg_beta = sum(s.get('beta', 0) for s in stocks) / len(stocks) if stocks else 0
        avg_anti_fall = sum(s.get('anti_fall_days', 0) for s in stocks) / len(stocks) if stocks else 0
        
        lines.extend([
            f"- **平均贝塔**: {avg_beta:.3f} (理论上比指数波动小 {(1-avg_beta)*100:.0f}%)",
            f"- **平均抗跌天数**: {avg_anti_fall:.1f}",
            "",
            "## 💡 交易建议",
            "",
            "> [!tip] 稳健型策略特点",
            "> 1. 低贝塔股票波动性较小，适合风险厌恶型投资者",
            "> 2. 结合独立强度因子，在稳健基础上寻找相对强势标的",
            "> 3. 建议持有周期相对较长，发挥低贝塔的防御优势",
            "",
        ])
    
    lines.extend([
        "---",
        "",
        f"*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
    ])
    
    return "\n".join(lines)


def generate_combined_report(data: dict, date: str) -> str:
    """生成综合信号报告"""
    stocks = data.get('data', [])
    strategies = data.get('strategies', [])
    
    lines = [
        "---",
        "date:", f"  - \"{date}\"",
        "tags:",
        "  - 量化分析",
        "  - 综合信号",
        "  - 多因子叠加",
        "  - 高置信度",
        "---",
        "",
        f"# 🔥 多策略综合信号报告 - {date}",
        "",
        "> **策略说明**: 综合多个策略的信号，找出被多个策略共同选中的股票，提高置信度。",
        "> **重叠标准**: 至少被 2 个策略同时选中",
        "> **适用场景**: 寻找高确定性交易机会",
        "",
        "## 📊 综合执行摘要",
        "",
        f"- **交易日**: {date}",
        f"- **参与策略**: {', '.join(strategies) if strategies else 'N/A'}",
        f"- **多策略重叠**: {len(stocks)} 只股票",
        "",
        "## 🌟 高置信度股票列表",
        "",
    ]
    
    if not stocks:
        lines.extend([
            "> [!warning] 无重叠信号",
            ">",
            "> 今日各策略选股没有重叠，无法生成高置信度信号。",
            ">",
            "> 建议：",
            "> - 关注各策略单独的选股结果",
            "> - 等待市场出现更明确的信号",
            "",
        ])
    else:
        lines.extend([
            "| 排名 | 代码 | 名称 | 板块 | 策略重叠数 | 置信度 | 综合得分 | 来源策略 |",
            "|------|------|------|------|------------|--------|----------|----------|",
        ])
        
        for i, stock in enumerate(stocks, 1):
            overlap = stock.get('overlap_count', 0)
            confidence = "🌟🌟🌟" if overlap >= 3 else "⭐⭐" if overlap == 2 else "⭐"
            strategies_list = ', '.join(stock.get('strategies', []))[:20]
            
            lines.append(
                f"| {i} | `{stock['symbol']}` | {stock['name']} | {stock.get('sector', '-')} | "
                f"{overlap} | {confidence} | {stock.get('combined_score', 0):.2f} | {strategies_list} |"
            )
        
        lines.extend([
            "",
            "## 📈 置信度分布",
            "",
            "| 置信度 | 股票数 | 说明 |",
            "|--------|--------|------|",
        ])
        
        high_conf = len([s for s in stocks if s.get('overlap_count', 0) >= 3])
        medium_conf = len([s for s in stocks if s.get('overlap_count', 0) == 2])
        
        lines.extend([
            f"| 🌟🌟🌟 高置信度 | {high_conf} | 3+策略重叠，重点关注 |",
            f"| ⭐⭐ 中置信度 | {medium_conf} | 2策略重叠，适度关注 |",
            "",
            "## 💡 交易建议",
            "",
            "> [!tip] 高置信度交易原则",
            "> 1. 🌟🌟🌟 高置信度股票可作为核心持仓",
            "> 2. 多策略重叠意味着从不同维度都验证了标的的优势",
            "> 3. 优先选择与自身风险偏好匹配的策略类型",
            "> 4. 即使高置信度也需要设置止损，控制风险",
            "",
        ])
    
    lines.extend([
        "---",
        "",
        f"*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
    ])
    
    return "\n".join(lines)


def main():
    output_dir = Path("/tmp/strategy-output")
    
    if not output_dir.exists():
        print(f"❌ 错误: {output_dir} 不存在")
        sys.exit(1)
    
    # 查找所有 JSON 文件
    json_files = list(output_dir.glob("*.json"))
    
    if not json_files:
        print("❌ 未找到策略结果文件")
        sys.exit(1)
    
    print(f"=== 生成策略报告 ===")
    print(f"找到 {len(json_files)} 个结果文件\n")
    
    # 按策略类型分组处理
    generators = {
        '01-independence-score': generate_independence_score_report,
        '03-low-beta-hybrid': generate_low_beta_hybrid_report,
        'combined-signals': generate_combined_report,
    }
    
    generated = []
    
    for prefix, generator in generators.items():
        files = list(output_dir.glob(f"{prefix}-*.json"))
        
        for json_file in files:
            data = load_json_file(json_file)
            if not data:
                continue
            
            # 从文件名提取日期
            date_match = json_file.stem.split('-')[-3:]
            if len(date_match) == 3 and all(d.isdigit() for d in date_match):
                date = f"{date_match[0]}-{date_match[1]}-{date_match[2]}"
            else:
                date = data.get('date', datetime.now().strftime('%Y-%m-%d'))
            
            # 生成 Markdown
            markdown = generator(data, date)
            
            # 保存 Markdown
            md_filename = json_file.stem + ".md"
            md_path = output_dir / md_filename
            
            with open(md_path, 'w', encoding='utf-8') as f:
                f.write(markdown)
            
            generated.append(md_path.name)
            print(f"✓ {md_path.name}")
    
    print(f"\n=== 完成 ===")
    print(f"生成 {len(generated)} 个 Markdown 报告:")
    for name in generated:
        print(f"  - {name}")
    
    # 输出文件列表
    print(f"\n所有输出文件:")
    for f in sorted(output_dir.iterdir()):
        if f.is_file():
            size = f.stat().st_size
            print(f"  {f.name:50s} ({size:,} bytes)")


if __name__ == '__main__':
    main()
