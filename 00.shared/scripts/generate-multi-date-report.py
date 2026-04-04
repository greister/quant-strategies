#!/usr/bin/env python3
"""
generate-multi-date-report.py - 生成多日期对比报告

参考格式: 2026-03-26_多日期对比报告-独立强度因子.md
特点:
- 多日期对比
- 市场概览对比表
- TOP 5 逐日对比
- 行业热度轮动分析
- 持续性标的分析
- 量化统计
- 使用建议
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import subprocess


def query_clickhouse(query: str) -> List[Dict]:
    """执行 ClickHouse 查询并返回结果"""
    env = os.environ.copy()
    env.update({
        'CH_HOST': 'localhost',
        'CH_PORT': '9000',
        'CH_DB': 'tdx2db_rust',
        'CH_USER': 'default',
        'CH_PASSWORD': 'tdx2db'
    })
    
    cmd = [
        'clickhouse-client',
        f"--host={env['CH_HOST']}",
        f"--port={env['CH_PORT']}",
        f"--user={env['CH_USER']}",
        f"--password={env['CH_PASSWORD']}",
        f"--database={env['CH_DB']}",
        '--format=JSONCompact',
        f'--query={query}'
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"查询错误: {result.stderr}")
        return []
    
    try:
        data = json.loads(result.stdout)
        columns = [col['name'] for col in data['meta']]
        return [dict(zip(columns, row)) for row in data['data']]
    except Exception as e:
        print(f"解析错误: {e}")
        return []


def get_independence_score(date: str, top_n: int = 20) -> List[Dict]:
    """获取指定日期的独立强度因子数据"""
    query = f"""
    SELECT 
        symbol,
        name,
        sector,
        raw_score,
        weighted_score,
        contra_count
    FROM independence_score_time_weighted
    WHERE date = '{date}' AND config_name = 'evening_focus'
    ORDER BY raw_score DESC
    LIMIT {top_n}
    """
    return query_clickhouse(query)


def analyze_dates(dates: List[str]) -> Dict[str, Any]:
    """分析多个日期的数据"""
    analysis = {
        'dates': dates,
        'daily_data': {},
        'sector_heatmap': {},
        'persistent_stocks': {}
    }
    
    all_stocks = {}  # 用于追踪持续性
    
    for date in dates:
        stocks = get_independence_score(date, 20)
        analysis['daily_data'][date] = stocks
        
        if stocks:
            scores = [s['raw_score'] for s in stocks]
            analysis['daily_data'][date + '_stats'] = {
                'max_score': max(scores),
                'avg_score': sum(scores) / len(scores),
                'median_score': sorted(scores)[len(scores)//2],
                'count': len(stocks)
            }
            
            # 统计板块热度
            sector_counts = {}
            for stock in stocks:
                sector = stock['sector']
                sector_counts[sector] = sector_counts.get(sector, 0) + 1
                
                # 追踪股票出现次数
                symbol = stock['symbol']
                if symbol not in all_stocks:
                    all_stocks[symbol] = {'count': 0, 'dates': [], 'info': stock}
                all_stocks[symbol]['count'] += 1
                all_stocks[symbol]['dates'].append(date)
            
            analysis['sector_heatmap'][date] = sector_counts
    
    # 筛选持续性标的（出现2次及以上）
    analysis['persistent_stocks'] = {
        k: v for k, v in all_stocks.items() if v['count'] >= 2
    }
    
    return analysis


def get_market_feature(date: str, max_score: float) -> str:
    """根据最高分判断市场特征"""
    if max_score >= 4.0:
        return "🔥 强势独立行情日"
    elif max_score >= 2.5:
        return "📈 较好的独立行情"
    elif max_score >= 1.5:
        return "📊 正常的独立行情"
    else:
        return "😴 弱势整理行情"


def generate_market_overview(analysis: Dict) -> str:
    """生成市场概览对比部分"""
    lines = ["## 一、市场概览对比", "", "| 日期 | 最高独立强度分 | TOP 20 平均分 | 市场特征 |", "|------|--------------|--------------|---------|"]
    
    for date in analysis['dates']:
        stats = analysis['daily_data'].get(date + '_stats', {})
        max_score = stats.get('max_score', 0)
        avg_score = stats.get('avg_score', 0)
        feature = get_market_feature(date, max_score)
        
        star = " ⭐" if max_score >= 4.0 else ""
        lines.append(f"| {date} | **{max_score:.1f}**{star} | {avg_score:.1f} | {feature} |")
    
    lines.extend(["", "### 市场特征解读", ""])
    
    # 为每个日期生成解读
    for date in analysis['dates']:
        stocks = analysis['daily_data'].get(date, [])
        if stocks:
            top_sectors = {}
            for s in stocks[:5]:
                sector = s['sector']
                top_sectors[sector] = top_sectors.get(sector, 0) + 1
            
            sector_desc = ", ".join([f"{s}({c}只)" for s, c in sorted(top_sectors.items(), key=lambda x: -x[1])[:2]])
            lines.append(f"- **{date}**: {get_market_feature(date, analysis['daily_data'].get(date + '_stats', {}).get('max_score', 0)).split()[1]}，{sector_desc}")
    
    return "\n".join(lines)


def generate_top5_comparison(analysis: Dict) -> str:
    """生成 TOP 5 逐日对比"""
    lines = ["", "---", "", "## 二、TOP 5 标的逐日对比", ""]
    
    # 各日期冠军
    lines.extend(["### 🏆 各日期冠军标的", "", "| 日期 | 代码 | 名称 | 行业 | 独立强度分 | 亮点 |", "|------|------|------|------|-----------|------|"])
    
    for date in analysis['dates']:
        stocks = analysis['daily_data'].get(date, [])
        if stocks:
            champion = stocks[0]
            highlight = "全场最高分" if analysis['daily_data'].get(date + '_stats', {}).get('max_score', 0) >= 5.0 else "单日冠军"
            lines.append(f"| {date[5:]} | {champion['symbol']} | {champion['name']} | {champion['sector']} | **{champion['raw_score']:.1f}** | {highlight} |")
    
    # 各日期 TOP 5 详细
    for date in analysis['dates']:
        stocks = analysis['daily_data'].get(date, [])
        if not stocks:
            continue
            
        lines.extend(["", f"### 📊 {date} (TOP 5)", "", "| 排名 | 代码 | 名称 | 行业 | 独立强度分 |", "|-----|------|------|------|-----------|"])
        
        for i, stock in enumerate(stocks[:5], 1):
            lines.append(f"| {i} | {stock['symbol']} | {stock['name']} | {stock['sector']} | **{stock['raw_score']:.1f}** |")
        
        # 板块特征
        sectors = {}
        for s in stocks[:5]:
            sectors[s['sector']] = sectors.get(s['sector'], 0) + 1
        sector_feature = ", ".join([f"{s}({c}只)" for s, c in sorted(sectors.items(), key=lambda x: -x[1])])
        lines.extend(["", f"**板块特征**: {sector_feature}"])
    
    return "\n".join(lines)


def generate_sector_analysis(analysis: Dict) -> str:
    """生成行业热度轮动分析"""
    lines = ["", "---", "", "## 三、行业热度轮动分析", "", "### 3.1 各日期热门独立行情板块", "", "| 日期 | 最热门板块 | 入选数量 | 代表标的 |", "|------|-----------|---------|---------|"]
    
    for date in analysis['dates']:
        sector_counts = analysis['sector_heatmap'].get(date, {})
        if sector_counts:
            top_sector = max(sector_counts.items(), key=lambda x: x[1])
            stocks = analysis['daily_data'].get(date, [])
            representatives = [s['name'] for s in stocks if s['sector'] == top_sector[0]][:3]
            lines.append(f"| {date[5:]} | **{top_sector[0]}** | {top_sector[1]}只 | {', '.join(representatives)} |")
    
    # 板块轮动图
    lines.extend(["", "### 3.2 板块轮动特征", "", "```", "日期 progression →"])
    
    for date in analysis['dates']:
        sector_counts = analysis['sector_heatmap'].get(date, {})
        if sector_counts:
            top3 = [s for s, _ in sorted(sector_counts.items(), key=lambda x: -x[1])[:3]]
            lines.append(f"{date[5:]}: {' > '.join(top3)}")
    
    lines.append("```")
    
    return "\n".join(lines)


def generate_persistent_analysis(analysis: Dict) -> str:
    """生成持续性标的分析"""
    lines = ["", "---", "", "## 四、持续性独立标的", "", "### 4.1 多次上榜标的", "", "检查是否有股票在多个交易日都表现出独立行情：", "", "| 标的 | 代码 | 出现次数 | 上榜日期 |", "|------|------|---------|---------|"]
    
    persistent = analysis.get('persistent_stocks', {})
    if persistent:
        for symbol, info in sorted(persistent.items(), key=lambda x: -x[1]['count']):
            dates_str = ", ".join([d[5:] for d in info['dates']])
            lines.append(f"| {info['info']['name']} | {symbol} | {info['count']} | {dates_str} |")
        
        lines.extend(["", f"**结论**: 共有 {len(persistent)} 只标的在多个交易日持续表现独立行情。"])
    else:
        lines.extend(["| - | - | - | - |", "", "**结论**: 独立行情标的轮换较快，单日独立行情特征明显，持续性一般。"])
    
    return "\n".join(lines)


def generate_statistics(analysis: Dict) -> str:
    """生成量化统计"""
    lines = ["", "---", "", "## 五、量化统计", "", "### 5.1 每日统计数据", "", "| 日期 | 有效选股数 | 最高分 | 平均分 | 市场状态 |", "|------|-----------|--------|--------|---------|"]
    
    for date in analysis['dates']:
        stats = analysis['daily_data'].get(date + '_stats', {})
        count = stats.get('count', 0)
        max_score = stats.get('max_score', 0)
        avg_score = stats.get('avg_score', 0)
        
        if max_score >= 4.0:
            status = "🔥 强势"
        elif max_score >= 2.0:
            status = "📈 正常"
        else:
            status = "😴 弱势"
        
        lines.append(f"| {date[5:]} | {count} | {max_score:.1f} | {avg_score:.1f} | {status} |")
    
    return "\n".join(lines)


def generate_suggestions(analysis: Dict) -> str:
    """生成使用建议"""
    return """

---

## 六、使用建议

### 6.1 最佳使用场景

| 市场状态 | 独立强度分范围 | 操作建议 |
|---------|--------------|---------|
| 强势独立行情 | ≥4分 | 🔥 重点关注，次日高开概率高 |
| 正常独立行情 | 2-3分 | 📊 结合其他指标综合判断 |
| 弱势整理 | ≤1分 | 😴 观望为主，减少操作 |

### 6.2 操作建议

- **高分标的** (≥4分): 独立行情强烈，可重点关注
- **中等标的** (2-3分): 有一定独立性，需结合基本面
- **低分标的** (≤1分): 独立性弱，谨慎参与

> [!tip]
> 独立强度因子在**市场下跌时**效果最佳，此时能筛选出真正抗跌的标的。
> 市场普涨时独立行情标的较少，因子效果减弱。
"""


def generate_multi_date_report(dates: List[str], strategy_name: str = "独立强度因子") -> str:
    """生成完整的多日期对比报告"""
    print(f"正在分析 {len(dates)} 个交易日的数据...")
    analysis = analyze_dates(dates)
    
    # 报告头部
    header = f"""# 多日期对比报告 - {strategy_name}

**报告日期**: {datetime.now().strftime('%Y-%m-%d')}  
**对比日期**: {', '.join(dates)}  
**策略版本**: 基础版{strategy_name} v1.0  
**数据来源**: tdx2db_rust (ClickHouse)

---
"""
    
    # 组合各部分
    sections = [
        header,
        generate_market_overview(analysis),
        generate_top5_comparison(analysis),
        generate_sector_analysis(analysis),
        generate_persistent_analysis(analysis),
        generate_statistics(analysis),
        generate_suggestions(analysis),
        "",
        "---",
        "",
        f"**免责声明**: 本报告仅供研究参考，不构成投资建议。股市有风险，投资需谨慎。",
        "",
        "**报告生成**: AI量化分析系统  \n**数据来源**: tdx2db-rust / ClickHouse",
    ]
    
    return "\n".join(sections)


def main():
    """主函数"""
    # 默认分析最近几个交易日
    dates = ['2026-03-20', '2026-03-23', '2026-03-24', '2026-03-25']
    
    report = generate_multi_date_report(dates)
    
    # 保存报告
    output_file = f"/tmp/strategy-output/{datetime.now().strftime('%Y-%m-%d')}_多日期对比报告-独立强度因子.md"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n✓ 报告已生成: {output_file}")
    print(f"  包含 {len(dates)} 个交易日的对比分析")
    
    # 显示报告前50行
    print("\n=== 报告预览 ===")
    print('\n'.join(report.split('\n')[:50]))


if __name__ == '__main__':
    main()
