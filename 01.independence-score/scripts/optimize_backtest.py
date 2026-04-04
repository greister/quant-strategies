#!/usr/bin/env python3
"""
独立强度因子优化回测
测试不同参数组合和市场环境下的表现
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict

from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_client():
    """获取 ClickHouse 连接"""
    return Client(
        host=os.getenv('CH_HOST', 'localhost'),
        port=int(os.getenv('CH_PORT', '9000')),
        database=os.getenv('CH_DB', 'tdx2db_rust'),
        user=os.getenv('CH_USER', 'default'),
        password=os.getenv('CH_PASSWORD', ''),
    )


def get_market_returns(client, start_date, end_date):
    """获取每日市场收益"""
    query = """
    SELECT 
        date,
        avg((close - open) / open * 100) as market_return
    FROM raw_stocks_daily
    WHERE date >= %(start)s AND date <= %(end)s
    GROUP BY date
    ORDER BY date
    """
    result = client.execute(query, {'start': start_date, 'end': end_date})
    return {row[0]: row[1] for row in result}


def get_signals_with_returns(client, start_date, end_date, score_threshold, hold_days):
    """获取信号及收益"""
    query = """
    SELECT 
        i.date as signal_date,
        i.symbol,
        i.score,
        i.sector,
        d.close as entry_price,
        d_future.close as exit_price,
        (d_future.close - d.close) / d.close * 100 as return_rate
    FROM independence_score_daily i
    INNER JOIN raw_stocks_daily d ON i.symbol = d.symbol AND i.date = d.date
    LEFT JOIN raw_stocks_daily d_future ON i.symbol = d_future.symbol 
        AND d_future.date = i.date + INTERVAL %(hold)s DAY
    WHERE i.date >= %(start)s AND i.date <= %(end)s
      AND i.score >= %(threshold)s
    """
    result = client.execute(query, {
        'start': start_date, 
        'end': end_date, 
        'threshold': score_threshold,
        'hold': hold_days
    })
    return result


def analyze_by_market_condition(signals, market_returns):
    """按市场环境分析"""
    results = {
        'big_drop': [],      # 前一日大跌 <-0.5%
        'small_drop': [],    # 前一日微跌 -0.5% ~ 0%
        'up': [],            # 前一日上涨 >= 0%
    }
    
    # 排序日期
    sorted_dates = sorted(market_returns.keys())
    
    for row in signals:
        signal_date = row[0]
        return_rate = row[6]
        
        # 过滤无效数据
        if return_rate is None or return_rate <= -99:
            continue
        
        # 找到前一日的市场收益
        prev_date = None
        for i, d in enumerate(sorted_dates):
            if d == signal_date and i > 0:
                prev_date = sorted_dates[i-1]
                break
        
        if prev_date and prev_date in market_returns:
            prev_return = market_returns[prev_date]
            if prev_return < -0.5:
                results['big_drop'].append(return_rate)
            elif prev_return < 0:
                results['small_drop'].append(return_rate)
            else:
                results['up'].append(return_rate)
        else:
            results['up'].append(return_rate)
    
    return results


def calc_stats(returns):
    """计算统计指标（过滤掉无效数据）"""
    # 过滤掉 None 和 -100%（数据缺失导致的异常值）
    valid_returns = [r for r in returns if r is not None and r > -99]
    
    if not valid_returns:
        return {'count': 0, 'avg': 0, 'win_rate': 0}
    
    wins = sum(1 for r in valid_returns if r > 0)
    return {
        'count': len(valid_returns),
        'avg': sum(valid_returns) / len(valid_returns),
        'win_rate': wins / len(valid_returns) * 100
    }


def test_strategy_variants(client, start_date, end_date):
    """测试多种策略变体"""
    
    print("="*70)
    print("独立强度因子 - 优化回测测试")
    print("="*70)
    
    # 获取市场收益
    market_returns = get_market_returns(client, start_date, end_date)
    print(f"\n数据区间: {start_date} ~ {end_date}")
    print(f"交易日数: {len(market_returns)} 天")
    
    # 市场环境统计
    big_drop_days = sum(1 for r in market_returns.values() if r < -0.5)
    small_drop_days = sum(1 for r in market_returns.values() if -0.5 <= r < 0)
    up_days = sum(1 for r in market_returns.values() if r >= 0)
    print(f"\n市场环境分布:")
    print(f"  大跌日(<-0.5%): {big_drop_days} 天")
    print(f"  微跌日(-0.5%~0%): {small_drop_days} 天")
    print(f"  上涨日(>=0%): {up_days} 天")
    
    # 测试参数组合
    test_cases = [
        {'name': '基础策略', 'threshold': 0.05, 'hold_days': 5, 'top_n': None},
        {'name': '高阈值', 'threshold': 0.10, 'hold_days': 5, 'top_n': None},
        {'name': '超短持有', 'threshold': 0.05, 'hold_days': 1, 'top_n': None},
        {'name': 'Top10精选', 'threshold': 0.05, 'hold_days': 5, 'top_n': 10},
        {'name': '高阈值+超短', 'threshold': 0.10, 'hold_days': 1, 'top_n': None},
    ]
    
    print("\n" + "="*70)
    print("策略对比测试")
    print("="*70)
    
    for case in test_cases:
        print(f"\n【{case['name']}】")
        print(f"  参数: 阈值={case['threshold']}, 持有={case['hold_days']}天, TopN={case['top_n'] or '不限'}")
        
        signals = get_signals_with_returns(
            client, start_date, end_date, 
            case['threshold'], case['hold_days']
        )
        
        if case['top_n']:
            # 按日期分组，每天只取Top N
            signals_by_date = defaultdict(list)
            for s in signals:
                signals_by_date[s[0]].append(s)
            
            filtered = []
            for date, date_signals in signals_by_date.items():
                sorted_signals = sorted(date_signals, key=lambda x: x[2], reverse=True)
                filtered.extend(sorted_signals[:case['top_n']])
            signals = filtered
        
        # 整体统计
        returns = [s[6] for s in signals if s[6] is not None]
        if not returns:
            print("  无交易信号")
            continue
            
        stats = calc_stats(returns)
        print(f"  交易次数: {stats['count']}")
        print(f"  平均收益: {stats['avg']:.2f}%")
        print(f"  胜率: {stats['win_rate']:.1f}%")
        
        # 按市场环境分析
        by_condition = analyze_by_market_condition(signals, market_returns)
        
        print(f"\n  分环境表现:")
        for condition, name in [('big_drop', '大跌次日'), ('small_drop', '微跌次日'), ('up', '上涨次日')]:
            if by_condition[condition]:
                s = calc_stats(by_condition[condition])
                print(f"    {name}: 次数={s['count']}, 收益={s['avg']:.2f}%, 胜率={s['win_rate']:.1f}%")
    
    # 板块分析
    print("\n" + "="*70)
    print("板块表现分析 (Top10)")
    print("="*70)
    
    signals = get_signals_with_returns(client, start_date, end_date, 0.05, 5)
    sector_stats = defaultdict(lambda: {'returns': [], 'count': 0})
    
    for row in signals:
        sector = row[3] or '未知'
        ret = row[6]
        # 过滤无效数据
        if ret is None or ret <= -99:
            continue
        sector_stats[sector]['returns'].append(ret)
        sector_stats[sector]['count'] += 1
    
    # 计算板块统计并排序
    sector_results = []
    for sector, data in sector_stats.items():
        if data['count'] >= 5:  # 至少5次信号
            stats = calc_stats(data['returns'])
            sector_results.append({
                'sector': sector,
                'count': stats['count'],
                'avg': stats['avg'],
                'win_rate': stats['win_rate']
            })
    
    sector_results.sort(key=lambda x: x['avg'], reverse=True)
    
    print(f"\n{'板块':<15} {'信号数':>8} {'平均收益%':>10} {'胜率%':>8}")
    print("-" * 45)
    for r in sector_results[:10]:
        print(f"{r['sector']:<15} {r['count']:>8} {r['avg']:>10.2f} {r['win_rate']:>8.1f}")


def main():
    parser = argparse.ArgumentParser(description='Independence Score Optimization Backtest')
    parser.add_argument('--start', default='2026-02-05', help='Start date')
    parser.add_argument('--end', default='2026-03-26', help='End date')
    
    args = parser.parse_args()
    
    client = get_client()
    test_strategy_variants(client, args.start, args.end)


if __name__ == '__main__':
    main()
