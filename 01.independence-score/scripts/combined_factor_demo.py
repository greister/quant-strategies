#!/usr/bin/env python3
"""
多因子组合演示
展示如何将独立强度因子和动量因子结合
"""

import os
from clickhouse_driver import Client

def get_client():
    return Client(
        host=os.getenv('CH_HOST', 'localhost'),
        port=int(os.getenv('CH_PORT', '9000')),
        database=os.getenv('CH_DB', 'tdx2db_rust'),
        user=os.getenv('CH_USER', 'default'),
        password=os.getenv('CH_PASSWORD', ''),
    )

def demo_single_factor():
    """单因子选股演示"""
    client = get_client()
    
    print("="*70)
    print("【单因子选股】只看独立强度")
    print("="*70)
    
    # 只看独立强度
    result = client.execute("""
        SELECT 
            symbol,
            sector,
            score as independence_score
        FROM independence_score_daily
        WHERE date = '2026-03-26'
          AND score >= 0.08
        ORDER BY score DESC
        LIMIT 10
    """)
    
    print("\n选股结果（Top 10）:")
    print(f"{'排名':<4} {'代码':<12} {'板块':<12} {'独立强度':>10}")
    print("-" * 45)
    for i, row in enumerate(result, 1):
        symbol, sector, score = row
        print(f"{i:<4} {symbol:<12} {sector:<12} {score:>10.3f}")

def demo_combined_factor():
    """双因子组合选股演示"""
    client = get_client()
    
    print("\n" + "="*70)
    print("【双因子组合】独立强度 + 动量")
    print("="*70)
    print("\n策略逻辑:")
    print("  1. 先选独立强度 >= 0.05 的股票（保证质地）")
    print("  2. 再选动量排名前30%的股票（保证趋势）")
    print("  3. 综合得分 = 独立强度×50% + 动量×50%")
    
    # 检查动量表是否存在
    tables = client.execute("SHOW TABLES LIKE 'momentum_factor_daily'")
    if not tables:
        print("\n⚠️ 动量因子表不存在，请先计算动量因子")
        print("运行: cd ../02.momentum-factor && ./scripts/calc_momentum_factor.py 2026-03-26")
        return
    
    # 双因子组合
    result = client.execute("""
        SELECT 
            i.symbol,
            i.sector,
            i.score as independence_score,
            m.momentum_score,
            m.return_20d,
            (i.score * 0.5 + m.momentum_score * 0.5) as combined_score
        FROM independence_score_daily i
        INNER JOIN momentum_factor_daily m 
            ON i.symbol = m.symbol AND i.date = m.date
        WHERE i.date = '2026-03-26'
          AND i.score >= 0.05
          AND m.rank_pct <= 0.3
        ORDER BY combined_score DESC
        LIMIT 10
    """)
    
    print("\n选股结果（Top 10）:")
    print(f"{'排名':<4} {'代码':<12} {'板块':<10} {'独立强度':>10} {'动量分':>8} {'20日收益%':>10} {'综合分':>8}")
    print("-" * 70)
    for i, row in enumerate(result, 1):
        symbol, sector, ind_score, mom_score, ret_20d, combined = row
        print(f"{i:<4} {symbol:<12} {sector:<10} {ind_score:>10.3f} {mom_score:>8.3f} {ret_20d:>10.1f} {combined:>8.3f}")

def demo_comparison():
    """对比单因子和双因子的差异"""
    client = get_client()
    
    print("\n" + "="*70)
    print("【对比分析】单因子 vs 双因子")
    print("="*70)
    
    # 检查动量表是否存在
    tables = client.execute("SHOW TABLES LIKE 'momentum_factor_daily'")
    if not tables:
        print("\n⚠️ 动量因子表不存在，跳过对比")
        return
    
    # 只在独立强度中出现的股票
    only_independence = client.execute("""
        SELECT i.symbol, i.sector, i.score, m.momentum_score
        FROM independence_score_daily i
        LEFT JOIN momentum_factor_daily m 
            ON i.symbol = m.symbol AND i.date = m.date
        WHERE i.date = '2026-03-26'
          AND i.score >= 0.08
          AND (m.momentum_score IS NULL OR m.rank_pct > 0.5)
        LIMIT 5
    """)
    
    # 双因子都优秀的股票
    both_good = client.execute("""
        SELECT i.symbol, i.sector, i.score, m.momentum_score, m.return_20d
        FROM independence_score_daily i
        INNER JOIN momentum_factor_daily m 
            ON i.symbol = m.symbol AND i.date = m.date
        WHERE i.date = '2026-03-26'
          AND i.score >= 0.08
          AND m.rank_pct <= 0.2
        LIMIT 5
    """)
    
    print("\n只在独立强度中表现好，但动量一般的股票:")
    print(f"{'代码':<12} {'板块':<12} {'独立强度':>10} {'动量分':>8}")
    print("-" * 50)
    for row in only_independence:
        print(f"{row[0]:<12} {row[1]:<12} {row[2]:>10.3f} {row[3] or 0:>8.3f}")
    
    print("\n独立强度 + 动量 都优秀的股票:")
    print(f"{'代码':<12} {'板块':<12} {'独立强度':>10} {'动量分':>8} {'20日收益%':>10}")
    print("-" * 60)
    for row in both_good:
        print(f"{row[0]:<12} {row[1]:<12} {row[2]:>10.3f} {row[3]:>8.3f} {row[4]:>10.1f}")

def main():
    print("\n" + "="*70)
    print("多因子组合策略演示")
    print("="*70)
    
    demo_single_factor()
    demo_combined_factor()
    demo_comparison()
    
    print("\n" + "="*70)
    print("结论")
    print("="*70)
    print("""
单因子策略的问题:
- 只看独立强度: 可能选到弱势股里的"相对强"
- 只看动量: 可能选到已经涨完、即将回调的股票

双因子组合的优势:
- 独立强度保证股票质地（抗跌能力）
- 动量保证趋势方向（上涨动能）
- 两者结合 = 选到强势市场中的强势股

建议:
1. 牛市中: 提高动量权重（如独立30% + 动量70%）
2. 震荡市: 平衡权重（如独立50% + 动量50%）
3. 熊市中: 提高独立强度权重（如独立70% + 动量30%）
    """)

if __name__ == '__main__':
    main()
