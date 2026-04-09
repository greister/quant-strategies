#!/usr/bin/env python3
"""
生成详细的策略执行报告，包含：
- 分时区间详细数据
- 价格、成交量变化
- 板块对比
- 逆势区间可视化
"""

import subprocess
import json
from datetime import datetime
from pathlib import Path

def run_query(query):
    """执行 ClickHouse 查询"""
    result = subprocess.run(
        ["clickhouse-client", "--password=tdx2db", "--database=tdx2db_rust", 
         "--format=JSONEachRow", "--query", query],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        return [json.loads(line) for line in result.stdout.strip().split('\n') if line]
    return []

def get_minute_data(symbol, date):
    """获取5分钟详细数据"""
    query = f"""
    SELECT 
        formatDateTime(datetime, '%H:%M') as time,
        close,
        open,
        high,
        low,
        volume,
        amount,
        round((close - open) / open * 100, 2) as change_pct,
        round((high - low) / low * 100, 2) as volatility_pct
    FROM raw_stocks_5min 
    WHERE toDate(datetime) = '{date}' AND symbol = '{symbol}'
    ORDER BY datetime
    """
    return run_query(query)

def get_sector_minute_data(sector_code, date):
    """获取板块5分钟数据"""
    query = f"""
    SELECT 
        formatDateTime(datetime, '%H:%M') as time,
        avg(close) as avg_close,
        sum(volume) as total_volume
    FROM raw_stocks_5min r
    JOIN gtja_concept_members g ON r.symbol = g.symbol
    WHERE toDate(datetime) = '{date}' AND g.block_name = '{sector_code}'
    GROUP BY datetime
    ORDER BY datetime
    """
    return run_query(query)

def get_contra_intervals(symbol, date, config_name='conservative'):
    """获取逆势区间详情"""
    query = f"""
    SELECT 
        contra_details,
        contra_count,
        raw_score,
        weighted_score
    FROM independence_score_time_weighted 
    WHERE date = '{date}' 
      AND symbol = '{symbol}'
      AND config_name = '{config_name}'
    """
    result = run_query(query)
    return result[0] if result else None

def generate_stock_detail_section(symbol, name, sector, date):
    """生成单个股票的详细分析"""
    
    # 获取基础信息
    contra_info = get_contra_intervals(symbol, date)
    if not contra_info:
        return f"\n### {symbol} ({name})\n\n数据获取失败\n"
    
    # 获取分时数据
    minute_data = get_minute_data(symbol, date)
    
    # 计算统计数据
    total_volume = sum(int(row['volume']) for row in minute_data)
    total_amount = sum(float(row['amount']) for row in minute_data)
    price_start = float(minute_data[0]['open'])
    price_end = float(minute_data[-1]['close'])
    price_change = ((price_end - price_start) / price_start) * 100
    
    # 生成逆势区间图表
    contra_details = contra_info.get('contra_details', [])
    
    section = f"""
## 📊 {symbol} ({name})

### 基础信息
- **所属板块**: {sector}
- **逆势次数**: {contra_info['contra_count']}
- **原始得分**: {contra_info['raw_score']:.4f}
- **加权得分**: {contra_info['weighted_score']:.4f}
- **当日涨跌**: {price_change:+.2f}%
- **总成交量**: {total_volume:,}
- **总成交额**: ¥{total_amount/10000:.2f}万

### 分时区间详情

| 时间 | 收盘价 | 涨跌幅 | 成交量 | 成交额(万) | 逆势得分 |
|------|--------|--------|--------|------------|----------|
"""
    
    # 添加每个区间的数据
    for i, row in enumerate(minute_data):
        interval_idx = i + 1
        # 查找该区间是否有逆势得分
        contra_score = 0
        for detail in contra_details:
            if detail[0] == interval_idx:
                contra_score = detail[1]
                break
        
        score_display = "✓" if contra_score > 0 else ""
        section += f"| {row['time']} | {row['close']} | {row['change_pct']:+.2f}% | {int(row['volume']):,} | ¥{float(row['amount'])/10000:.2f} | {score_display} |\n"
    
    # 添加逆势区间可视化
    section += f"""

### 逆势区间分布 (ASCII)

```
时间        逆势得分分布 (✓ = 得分)
─────────────────────────────────────────
"""
    
    # 按小时分组显示
    hours = {
        '09:30-10:30': [],
        '10:30-11:30': [],
        '13:00-14:00': [],
        '14:00-15:00': []
    }
    
    for i in range(48):
        interval_idx = i + 1
        contra_score = 0
        for detail in contra_details:
            if detail[0] == interval_idx:
                contra_score = detail[1]
                break
        
        # 根据区间索引确定时间段
        if i < 12:
            hours['09:30-10:30'].append('✓' if contra_score > 0 else '·')
        elif i < 24:
            hours['10:30-11:30'].append('✓' if contra_score > 0 else '·')
        elif i < 36:
            hours['13:00-14:00'].append('✓' if contra_score > 0 else '·')
        else:
            hours['14:00-15:00'].append('✓' if contra_score > 0 else '·')
    
    for hour, marks in hours.items():
        section += f"{hour}  {''.join(marks)}\n"
    
    section += """─────────────────────────────────────────
```

### 关键观察

"""
    
    # 分析成交量分布
    volumes = [int(row['volume']) for row in minute_data]
    avg_volume = sum(volumes) / len(volumes)
    high_volume_intervals = [i for i, v in enumerate(volumes) if v > avg_volume * 1.5]
    
    if high_volume_intervals:
        section += f"- **高成交量区间**: 第 {', '.join(str(i+1) for i in high_volume_intervals[:5])} 个区间成交量显著高于平均 ({avg_volume:,.0f})\n"
    
    # 分析价格变化
    max_price = max(float(row['high']) for row in minute_data)
    min_price = min(float(row['low']) for row in minute_data)
    section += f"- **价格波动**: 最高价 ¥{max_price}, 最低价 ¥{min_price}, 振幅 {((max_price-min_price)/min_price)*100:.2f}%\n"
    
    # 分析逆势区间分布
    morning_contra = sum(1 for d in contra_details if d[0] <= 24 and d[1] > 0)
    afternoon_contra = sum(1 for d in contra_details if d[0] > 24 and d[1] > 0)
    section += f"- **逆势分布**: 上午 {morning_contra} 次，下午 {afternoon_contra} 次\n"
    
    section += "\n---\n"
    
    return section

def generate_detailed_report(date):
    """生成完整的详细报告"""
    
    # 获取共识股票
    query = f"""
    SELECT 
        symbol, 
        name, 
        sector,
        contra_count,
        weighted_score,
        raw_score
    FROM independence_score_time_weighted 
    WHERE date = '{date}' AND config_name = 'conservative'
    ORDER BY weighted_score DESC
    LIMIT 10
    """
    
    top_stocks = run_query(query)
    
    report = f"""# {date} 详细策略执行报告

**报告日期**: {date}  
**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**报告类型**: 详细版（包含分时区间数据）

---

## 📈 执行概览

本次共执行 **4 个时间加权策略**：
- 🟢 尾盘关注型 (evening_focus)
- 🟡 早盘关注型 (morning_focus)  
- 🟠 趋势市 (trending_market)
- 🟣 保守型 (conservative)

**选股池**: {len(top_stocks)} 只股票

---

## 🔥 Top 10 股票详细分析

"""
    
    # 为每个股票生成详细分析
    for stock in top_stocks:
        print(f"正在生成 {stock['symbol']} ({stock['name']}) 的详细分析...")
        section = generate_stock_detail_section(
            stock['symbol'], 
            stock['name'], 
            stock['sector'], 
            date
        )
        report += section
    
    # 添加汇总统计
    report += f"""
## 📊 汇总统计

### 各策略 Top 10 对比

| 排名 | 保守型 | 趋势市 | 早盘型 | 尾盘型 |
|------|--------|--------|--------|--------|
"""
    
    # 获取各策略的Top 10
    for config in ['conservative', 'trending_market', 'morning_focus', 'evening_focus']:
        query = f"""
        SELECT symbol, name, weighted_score
        FROM independence_score_time_weighted 
        WHERE date = '{date}' AND config_name = '{config}'
        ORDER BY weighted_score DESC
        LIMIT 10
        """
        stocks = run_query(query)
        for i, stock in enumerate(stocks, 1):
            report += f"| {i} | {stock['symbol']} | {stock['name']} | {stock['weighted_score']:.4f} |\n"
    
    report += """
### 共识度分析

"""
    
    # 计算共识度
    all_symbols = {}
    for config in ['conservative', 'trending_market', 'morning_focus', 'evening_focus']:
        query = f"""
        SELECT symbol
        FROM independence_score_time_weighted 
        WHERE date = '{date}' AND config_name = '{config}'
        ORDER BY weighted_score DESC
        LIMIT 10
        """
        stocks = run_query(query)
        for stock in stocks:
            symbol = stock['symbol']
            all_symbols[symbol] = all_symbols.get(symbol, 0) + 1
    
    consensus_4 = [s for s, c in all_symbols.items() if c == 4]
    consensus_3 = [s for s, c in all_symbols.items() if c == 3]
    consensus_2 = [s for s, c in all_symbols.items() if c == 2]
    
    report += f"- **4策略共识**: {len(consensus_4)} 只 ({', '.join(consensus_4[:5])})\n"
    report += f"- **3策略共识**: {len(consensus_3)} 只\n"
    report += f"- **2策略共识**: {len(consensus_2)} 只\n"
    
    report += """
---

## 📝 数据说明

### 字段说明

| 字段 | 说明 |
|------|------|
| 收盘价 | 5分钟K线收盘价 |
| 涨跌幅 | 相对于前5分钟的涨跌百分比 |
| 成交量 | 该5分钟区间成交股数 |
| 成交额 | 该5分钟区间成交金额 |
| 逆势得分 | ✓ 表示该区间满足逆势条件 |

### 逆势条件
- 板块跌幅 < -0.5%
- 个股涨幅 > 0% 或 超额收益 > 1%

---

*本报告由 generate_detailed_report.py 自动生成*
"""
    
    return report

if __name__ == '__main__':
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    
    print(f"正在生成 {date} 的详细报告...")
    report = generate_detailed_report(date)
    
    # 保存报告
    output_dir = Path(__file__).parent.parent / 'results'
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / f'{date}_detailed_report.md'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"✅ 详细报告已生成: {output_file}")
    
    # 同时复制到 Obsidian Vault
    vault_dir = Path('/mnt/d/obsidian/OrbitOS-vault/30_Research/量化分析/策略执行结果/01-独立强度因子')
    if vault_dir.exists():
        vault_file = vault_dir / f'{date}_详细选股报告.md'
        with open(vault_file, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"✅ 已同步到 Obsidian Vault: {vault_file}")
