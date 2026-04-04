#!/usr/bin/env python3
"""
低贝塔混合策略计算脚本
日内低贝塔抗跌 + 相对强度混合策略

使用方法:
    ./calc_low_beta_hybrid.py 2026-03-20
    ./calc_low_beta_hybrid.py 2026-03-20 --top-n 20 --output-json
"""

import os
import sys
import argparse
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

from clickhouse_driver import Client

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LowBetaHybridCalculator:
    """低贝塔混合策略计算器"""
    
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
    
    def calc(self, trade_date: str) -> Dict:
        """
        计算低贝塔混合策略
        
        Args:
            trade_date: 交易日期 (YYYY-MM-DD 格式)
        
        Returns:
            Dict: 包含计算结果统计
        """
        script_dir = Path(__file__).parent
        sql_file = script_dir.parent / 'sql' / 'calc_low_beta_hybrid.sql'
        
        if not sql_file.exists():
            logger.error(f"SQL file not found: {sql_file}")
            return {'success': False, 'error': 'SQL file not found'}
        
        try:
            calc_sql = sql_file.read_text(encoding='utf-8')
            
            # 参数替换
            calc_sql = calc_sql.replace('{trade_date:Date}', f"'{trade_date}'")
            
            # 拆分多语句 SQL
            statements = [s.strip() for s in calc_sql.split(';') if s.strip()]
            
            logger.info(f"=== Step 1: 计算低贝塔预筛选 ({trade_date}) ===")
            
            # 逐个执行 SQL 语句
            for i, stmt in enumerate(statements, 1):
                if stmt.startswith('--') or not stmt:
                    continue
                try:
                    self.ch_client.execute(stmt)
                    logger.debug(f"Executed statement {i}/{len(statements)}")
                except Exception as e:
                    # 忽略某些非关键错误（如表不存在时的ALTER）
                    if 'ALTER' in stmt.upper() and 'NOT_FOUND' in str(e):
                        logger.warning(f"ALTER skipped (no data): {e}")
                    else:
                        raise
            
            # 获取低贝塔池统计
            pool_result = self.ch_client.execute(
                """
                SELECT count(), avg(beta), avg(anti_fall_days)
                FROM low_beta_pool_daily
                WHERE date = %(date)s
                """,
                {'date': trade_date}
            )
            pool_count, avg_beta, avg_anti_fall = pool_result[0] if pool_result else (0, 0, 0)
            
            logger.info(f"低贝塔池股票数: {pool_count}")
            logger.info(f"平均贝塔: {avg_beta:.3f}")
            logger.info(f"平均抗跌次数: {avg_anti_fall:.1f}")
            
            logger.info(f"=== Step 2: 计算混合策略得分 ({trade_date}) ===")
            
            # 获取混合策略结果统计
            hybrid_result = self.ch_client.execute(
                """
                SELECT count(), avg(raw_score), max(raw_score), avg(hybrid_score)
                FROM low_beta_hybrid_daily
                WHERE date = %(date)s AND config_name = 'evening_focus'
                """,
                {'date': trade_date}
            )
            hybrid_count, avg_score, max_score, avg_hybrid = hybrid_result[0] if hybrid_result else (0, 0, 0, 0)
            
            logger.info(f"混合策略入选数: {hybrid_count}")
            logger.info(f"平均独立强度分: {avg_score:.2f}")
            logger.info(f"最高独立强度分: {max_score:.2f}")
            logger.info(f"平均综合得分: {avg_hybrid:.2f}")
            
            return {
                'success': True,
                'date': trade_date,
                'pool_count': pool_count,
                'avg_beta': avg_beta,
                'hybrid_count': hybrid_count,
                'avg_score': avg_score,
                'max_score': max_score
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate low beta hybrid: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_top_stocks(self, trade_date: str, top_n: int = 20) -> List[Dict]:
        """
        获取混合策略排名靠前的股票
        
        Args:
            trade_date: 交易日期
            top_n: 取前N名
        
        Returns:
            List[Dict]: 股票列表
        """
        try:
            result = self.ch_client.execute(
                """
                SELECT 
                    symbol,
                    name,
                    sector,
                    beta,
                    anti_fall_days,
                    raw_score,
                    hybrid_score,
                    rank,
                    rank_pct
                FROM low_beta_hybrid_daily
                WHERE date = %(date)s AND config_name = 'evening_focus'
                ORDER BY hybrid_score DESC
                LIMIT %(limit)s
                """,
                {'date': trade_date, 'limit': top_n}
            )
            
            stocks = []
            for row in result:
                stocks.append({
                    'rank': len(stocks) + 1,
                    'symbol': row[0],
                    'name': row[1],
                    'sector': row[2],
                    'beta': round(row[3], 3),
                    'anti_fall_days': row[4],
                    'raw_score': round(row[5], 2),
                    'hybrid_score': round(row[6], 2),
                    'db_rank': row[7],
                    'rank_pct': round(row[8] * 100, 1)
                })
            
            return stocks
        except Exception as e:
            logger.error(f"Failed to get top stocks: {e}")
            return []
    
    def save_json(self, stocks: List[Dict], trade_date: str, output_dir: str = '/tmp/strategy-output'):
        """保存结果为JSON"""
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = Path(output_dir) / f'03-low-beta-hybrid-top{len(stocks)}-{trade_date}.json'
        
        data = {
            'strategy': 'low-beta-hybrid',
            'strategy_name': '日内低贝塔抗跌+相对强度混合策略',
            'date': trade_date,
            'generated_at': datetime.now().isoformat(),
            'count': len(stocks),
            'top_stocks': stocks
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"JSON saved: {output_file}")
        return str(output_file)
    
    def print_results(self, stocks: List[Dict]):
        """打印结果"""
        if not stocks:
            print("No data found")
            return
        
        print("\n" + "="*100)
        print(f"{'Rank':<6} {'Symbol':<12} {'Name':<16} {'Sector':<12} {'Beta':<8} {'AntiDays':<10} {'Score':<10} {'Hybrid':<10}")
        print("-"*100)
        
        for s in stocks:
            print(f"{s['rank']:<6} {s['symbol']:<12} {s['name']:<16} {s['sector']:<12} "
                  f"{s['beta']:<8.3f} {s['anti_fall_days']:<10} {s['raw_score']:<10.2f} {s['hybrid_score']:<10.2f}")
        
        print("="*100)
        print(f"\n策略特点:")
        print(f"  - Beta: 贝塔值（对中证500），越低越抗跌")
        print(f"  - AntiDays: 过去20日抗跌次数")
        print(f"  - Score: 5分钟独立强度分")
        print(f"  - Hybrid: 综合得分 = (1-Beta)*50 + Score*10")
        print()


def main():
    parser = argparse.ArgumentParser(description='计算低贝塔混合策略')
    parser.add_argument('date', help='交易日期 (YYYY-MM-DD 格式)')
    parser.add_argument('--top-n', type=int, default=20, help='显示前N名 (默认20)')
    parser.add_argument('--output-json', action='store_true', help='输出JSON文件')
    parser.add_argument('--output-dir', default='/tmp/strategy-output', help='JSON输出目录')
    
    args = parser.parse_args()
    
    # 初始化计算器
    calc = LowBetaHybridCalculator()
    
    if not calc.connect_clickhouse():
        sys.exit(1)
    
    # 计算策略
    stats = calc.calc(args.date)
    if not stats['success']:
        print(f"Error: {stats.get('error', 'Unknown error')}")
        sys.exit(1)
    
    # 获取并显示结果
    stocks = calc.get_top_stocks(args.date, args.top_n)
    calc.print_results(stocks)
    
    # 保存JSON
    if args.output_json:
        json_file = calc.save_json(stocks, args.date, args.output_dir)
        print(f"✓ JSON输出: {json_file}")
    
    print(f"✓ 低贝塔混合策略计算完成: {args.date}")
    print(f"  低贝塔池: {stats['pool_count']}只 (β<0.8, 抗跌≥8次)")
    print(f"  混合策略入选: {stats['hybrid_count']}只")
    print(f"  显示前 {len(stocks)} 名")


if __name__ == '__main__':
    main()
