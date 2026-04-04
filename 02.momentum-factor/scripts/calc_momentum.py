#!/usr/bin/env python3
"""
动量因子计算脚本

使用方法:
    ./calc_momentum.py 2026-03-20
    ./calc_momentum.py 2026-03-20 --top-n 20
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

from clickhouse_driver import Client

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MomentumFactorCalculator:
    """动量因子计算器"""
    
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
    
    def calc(self, trade_date: str) -> int:
        """
        计算动量因子
        
        Args:
            trade_date: 交易日期 (YYYY-MM-DD 格式)
        
        Returns:
            int: 计算的股票数量，失败返回 -1
        """
        script_dir = Path(__file__).parent
        sql_file = script_dir.parent / 'sql' / 'calc_momentum_factor.sql'
        
        if not sql_file.exists():
            logger.error(f"SQL file not found: {sql_file}")
            return -1
        
        try:
            calc_sql = sql_file.read_text(encoding='utf-8')
            
            # 参数替换
            calc_sql = calc_sql.replace('{trade_date:Date}', f"'{trade_date}'")
            
            logger.info(f"Calculating momentum factor for {trade_date}")
            
            # 执行计算
            self.ch_client.execute(calc_sql)
            
            # 获取计算结果数量
            result = self.ch_client.execute(
                """
                SELECT count() 
                FROM momentum_factor_daily 
                WHERE date = %(date)s
                """,
                {'date': trade_date}
            )
            count = result[0][0]
            
            logger.info(f"Calculated {count} momentum scores for {trade_date}")
            return count
            
        except Exception as e:
            logger.error(f"Failed to calculate momentum factor: {e}")
            return -1
    
    def get_top_stocks(self, trade_date: str, top_n: int = 20) -> list:
        """
        获取动量因子排名靠前的股票
        
        Args:
            trade_date: 交易日期
            top_n: 取前N名
        
        Returns:
            list: 股票列表
        """
        try:
            result = self.ch_client.execute(
                """
                SELECT 
                    symbol,
                    name,
                    sector,
                    momentum_score,
                    return_20d,
                    rank
                FROM momentum_factor_daily
                WHERE date = %(date)s
                ORDER BY momentum_score DESC
                LIMIT %(limit)s
                """,
                {'date': trade_date, 'limit': top_n}
            )
            return result
        except Exception as e:
            logger.error(f"Failed to get top stocks: {e}")
            return []
    
    def print_results(self, stocks: list):
        """打印结果"""
        if not stocks:
            print("No data found")
            return
        
        print("\n" + "="*80)
        print(f"{'Rank':<6} {'Symbol':<12} {'Name':<16} {'Sector':<12} {'Score':<10} {'20D Return':<12}")
        print("-"*80)
        
        for i, row in enumerate(stocks, 1):
            symbol, name, sector, score, ret_20d, rank = row
            print(f"{i:<6} {symbol:<12} {name:<16} {sector:<12} {score:<10.2f} {ret_20d:<12.2f}")
        
        print("="*80 + "\n")


def main():
    parser = argparse.ArgumentParser(description='计算动量因子')
    parser.add_argument('date', help='交易日期 (YYYY-MM-DD 格式)')
    parser.add_argument('--top-n', type=int, default=20, help='显示前N名 (默认20)')
    
    args = parser.parse_args()
    
    # 初始化计算器
    calc = MomentumFactorCalculator()
    
    if not calc.connect_clickhouse():
        sys.exit(1)
    
    # 计算动量因子
    count = calc.calc(args.date)
    if count < 0:
        sys.exit(1)
    
    # 获取并显示结果
    stocks = calc.get_top_stocks(args.date, args.top_n)
    calc.print_results(stocks)
    
    print(f"✓ 动量因子计算完成: {args.date}")
    print(f"  计算股票数: {count}")
    print(f"  显示前 {len(stocks)} 名")


if __name__ == '__main__':
    main()
