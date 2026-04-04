#!/usr/bin/env python3
"""
三策略信号汇总脚本
汇总独立强度因子、动量因子、低贝塔混合策略的结果
找出被多个策略同时选中的股票（高置信度信号）

使用方法:
    ./combine_signals.py 2026-03-20
    ./combine_signals.py 2026-03-20 --min-overlap 2
"""

import os
import sys
import argparse
import logging
import json
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Set, Tuple

from clickhouse_driver import Client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


STRATEGY_CONFIG = {
    '01': {
        'name': 'independence-score',
        'table': 'independence_score_time_weighted',
        'score_field': 'weighted_score',
        'config_name': 'evening_focus',
        'description': '独立强度因子（5分钟逆势）'
    },
    '02': {
        'name': 'momentum-factor',
        'table': 'momentum_factor_daily',
        'score_field': 'momentum_score',
        'config_name': None,
        'description': '动量因子（价格趋势）'
    },
    '03': {
        'name': 'low-beta-hybrid',
        'table': 'low_beta_hybrid_daily',
        'score_field': 'hybrid_score',
        'config_name': 'evening_focus',
        'description': '低贝塔混合策略（防御+逆势）'
    }
}


class SignalCombiner:
    """信号汇总器"""
    
    def __init__(self):
        self.ch_client = None
        
    def connect_clickhouse(self) -> bool:
        """连接 ClickHouse"""
        try:
            self.ch_client = Client(
                host=os.getenv('CH_HOST', 'localhost'),
                port=int(os.getenv('CH_PORT', '9000')),
                database=os.getenv('CH_DB', 'tdx2db_rust'),
                user=os.getenv('CH_USER', 'default'),
                password=os.getenv('CH_PASSWORD', ''),
            )
            logger.info("Connected to ClickHouse")
            return True
        except Exception as e:
            logger.error(f"Failed to connect ClickHouse: {e}")
            return False
    
    def get_strategy_signals(self, trade_date: str, strategy_id: str, top_n: int = 30) -> List[Dict]:
        """获取单个策略的信号"""
        config = STRATEGY_CONFIG.get(strategy_id)
        if not config:
            return []
        
        try:
            # 构建查询
            if config['config_name']:
                query = f"""
                    SELECT 
                        symbol,
                        name,
                        sector,
                        {config['score_field']} as score,
                        rank
                    FROM {config['table']}
                    WHERE date = %(date)s AND config_name = %(config)s
                    ORDER BY {config['score_field']} DESC
                    LIMIT %(limit)s
                """
                params = {'date': trade_date, 'config': config['config_name'], 'limit': top_n}
            else:
                query = f"""
                    SELECT 
                        symbol,
                        name,
                        sector,
                        {config['score_field']} as score,
                        rank
                    FROM {config['table']}
                    WHERE date = %(date)s
                    ORDER BY {config['score_field']} DESC
                    LIMIT %(limit)s
                """
                params = {'date': trade_date, 'limit': top_n}
            
            result = self.ch_client.execute(query, params)
            
            signals = []
            for row in result:
                signals.append({
                    'symbol': row[0],
                    'name': row[1],
                    'sector': row[2],
                    'score': round(row[3], 4) if row[3] else 0,
                    'rank': row[4],
                    'strategy': strategy_id
                })
            
            return signals
            
        except Exception as e:
            logger.warning(f"Failed to get signals for strategy {strategy_id}: {e}")
            return []
    
    def combine_signals(self, trade_date: str, min_overlap: int = 2, top_n: int = 30) -> Dict:
        """
        汇总三策略信号
        
        Args:
            trade_date: 交易日期
            min_overlap: 最少需要几个策略选中（默认2）
            top_n: 每个策略取前N名
        
        Returns:
            Dict: 汇总结果
        """
        # 获取各策略信号
        all_signals = {}
        for strategy_id in ['01', '02', '03']:
            signals = self.get_strategy_signals(trade_date, strategy_id, top_n)
            all_signals[strategy_id] = signals
            logger.info(f"策略{strategy_id}: 获取 {len(signals)} 条信号")
        
        # 统计每个股票被哪些策略选中
        stock_strategies = defaultdict(list)
        stock_info = {}
        
        for strategy_id, signals in all_signals.items():
            for signal in signals:
                symbol = signal['symbol']
                stock_strategies[symbol].append({
                    'strategy': strategy_id,
                    'score': signal['score'],
                    'rank': signal['rank']
                })
                if symbol not in stock_info:
                    stock_info[symbol] = {
                        'name': signal['name'],
                        'sector': signal['sector']
                    }
        
        # 筛选被多个策略选中的股票
        combined_stocks = []
        for symbol, strategies in stock_strategies.items():
            if len(strategies) >= min_overlap:
                # 计算综合得分（各策略得分加权平均）
                total_score = sum(s['score'] for s in strategies)
                avg_score = total_score / len(strategies)
                
                # 计算平均排名
                avg_rank = sum(s['rank'] for s in strategies) / len(strategies)
                
                combined_stocks.append({
                    'symbol': symbol,
                    'name': stock_info[symbol]['name'],
                    'sector': stock_info[symbol]['sector'],
                    'overlap_count': len(strategies),
                    'strategies': [s['strategy'] for s in strategies],
                    'strategy_scores': {s['strategy']: s['score'] for s in strategies},
                    'strategy_ranks': {s['strategy']: s['rank'] for s in strategies},
                    'total_score': round(total_score, 4),
                    'avg_score': round(avg_score, 4),
                    'avg_rank': round(avg_rank, 1)
                })
        
        # 按重合度和综合得分排序
        combined_stocks.sort(key=lambda x: (-x['overlap_count'], -x['avg_score']))
        
        # 统计
        stats = {
            'total_overlap_3': sum(1 for s in combined_stocks if s['overlap_count'] == 3),
            'total_overlap_2': sum(1 for s in combined_stocks if s['overlap_count'] == 2),
            'by_sector': defaultdict(int)
        }
        
        for stock in combined_stocks:
            stats['by_sector'][stock['sector']] += 1
        
        return {
            'date': trade_date,
            'min_overlap': min_overlap,
            'combined_count': len(combined_stocks),
            'combined_stocks': combined_stocks,
            'individual_signals': {
                '01': len(all_signals['01']),
                '02': len(all_signals['02']),
                '03': len(all_signals['03'])
            },
            'stats': stats
        }
    
    def save_json(self, result: Dict, output_dir: str = '/tmp/strategy-output'):
        """保存结果为JSON"""
        os.makedirs(output_dir, exist_ok=True)
        
        date = result['date']
        overlap = result['min_overlap']
        output_file = Path(output_dir) / f'combined-signals-overlap{overlap}-{date}.json'
        
        output_data = {
            'type': 'combined',
            'name': '三策略综合信号',
            'description': f'被{overlap}个及以上策略同时选中的股票',
            'date': date,
            'generated_at': datetime.now().isoformat(),
            'strategies': {
                '01': STRATEGY_CONFIG['01']['description'],
                '02': STRATEGY_CONFIG['02']['description'],
                '03': STRATEGY_CONFIG['03']['description']
            },
            'summary': {
                'total_combined': result['combined_count'],
                'overlap_3_stocks': result['stats']['total_overlap_3'],
                'overlap_2_stocks': result['stats']['total_overlap_2']
            },
            'stocks': result['combined_stocks']
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Combined JSON saved: {output_file}")
        return str(output_file)
    
    def print_results(self, result: Dict):
        """打印结果"""
        print("\n" + "="*120)
        print(f"三策略信号汇总 - {result['date']}")
        print("="*120)
        
        print(f"\n各策略独立信号数:")
        for strategy_id, count in result['individual_signals'].items():
            config = STRATEGY_CONFIG[strategy_id]
            print(f"  策略{strategy_id} ({config['name']}): {count}只")
        
        print(f"\n综合统计 (最少{result['min_overlap']}个策略重合):")
        print(f"  三策略重合 (3个): {result['stats']['total_overlap_3']}只")
        print(f"  两策略重合 (2个): {result['stats']['total_overlap_2']}只")
        print(f"  总计: {result['combined_count']}只")
        
        if result['combined_stocks']:
            print(f"\n{'Rank':<6} {'Symbol':<12} {'Name':<16} {'Sector':<12} {'Overlap':<8} {'Strategies':<20} {'AvgScore':<10}")
            print("-"*120)
            
            for i, stock in enumerate(result['combined_stocks'][:20], 1):
                strategies_str = ','.join(stock['strategies'])
                print(f"{i:<6} {stock['symbol']:<12} {stock['name']:<16} {stock['sector']:<12} "
                      f"{stock['overlap_count']:<8} {strategies_str:<20} {stock['avg_score']:<10.4f}")
            
            if len(result['combined_stocks']) > 20:
                print(f"... 还有 {len(result['combined_stocks']) - 20} 只")
        
        print("="*120)
        print("\n图例:")
        print("  - Overlap: 被几个策略选中")
        print("  - Strategies: 选中该股票的策略编号")
        print("  - AvgScore: 各策略得分平均")
        print()


def main():
    parser = argparse.ArgumentParser(description='汇总三策略信号')
    parser.add_argument('date', help='交易日期 (YYYY-MM-DD 格式)')
    parser.add_argument('--min-overlap', type=int, default=2, 
                        help='最少需要几个策略选中 (默认2, 可选2或3)')
    parser.add_argument('--top-n', type=int, default=30, 
                        help='每个策略取前N名 (默认30)')
    parser.add_argument('--output-dir', default='/tmp/strategy-output', 
                        help='JSON输出目录')
    
    args = parser.parse_args()
    
    if args.min_overlap not in [2, 3]:
        print("Error: --min-overlap 只能是 2 或 3")
        sys.exit(1)
    
    # 初始化汇总器
    combiner = SignalCombiner()
    
    if not combiner.connect_clickhouse():
        sys.exit(1)
    
    # 汇总信号
    result = combiner.combine_signals(args.date, args.min_overlap, args.top_n)
    
    # 打印结果
    combiner.print_results(result)
    
    # 保存JSON
    json_file = combiner.save_json(result, args.output_dir)
    
    print(f"✓ 三策略信号汇总完成: {args.date}")
    print(f"  综合信号数: {result['combined_count']}只")
    print(f"  JSON输出: {json_file}")
    
    if result['stats']['total_overlap_3'] > 0:
        print(f"\n🌟 发现 {result['stats']['total_overlap_3']} 只三策略重合的股票！")


if __name__ == '__main__':
    main()
