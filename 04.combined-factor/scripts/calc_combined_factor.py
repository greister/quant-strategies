#!/usr/bin/env python3
"""
双因子组合策略计算脚本

结合独立强度因子和动量因子，计算综合得分。

使用方法:
    ./calc_combined_factor.py 2026-03-26
    ./calc_combined_factor.py 2026-03-26 --independence-weight 0.6 --momentum-weight 0.4
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from clickhouse_driver import Client

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CombinedFactorCalculator:
    """双因子组合计算器"""
    
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
    
    def check_dependencies(self, trade_date: str) -> bool:
        """检查依赖数据是否存在"""
        try:
            # 检查独立强度因子
            ind_count = self.ch_client.execute(
                "SELECT count() FROM independence_score_daily WHERE date = %(date)s",
                {'date': trade_date}
            )[0][0]
            
            # 检查动量因子
            mom_count = self.ch_client.execute(
                "SELECT count() FROM momentum_factor_daily WHERE date = %(date)s",
                {'date': trade_date}
            )[0][0]
            
            logger.info(f"Dependencies: independence={ind_count}, momentum={mom_count}")
            
            if ind_count == 0:
                logger.error(f"Missing independence score for {trade_date}")
                logger.error("Run: cd ../01.independence-score && ./scripts/calc_time_weighted_score.py " + trade_date)
                return False
            
            if mom_count == 0:
                logger.error(f"Missing momentum score for {trade_date}")
                logger.error("Run: cd ../02.momentum-factor && ./scripts/calc_momentum.py " + trade_date)
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to check dependencies: {e}")
            return False
    
    def calc(self, trade_date: str, weight_ind: float, weight_mom: float) -> int:
        """
        计算双因子组合得分
        
        Args:
            trade_date: 交易日期
            weight_ind: 独立强度权重
            weight_mom: 动量权重
            
        Returns:
            计算的股票数量
        """
        script_dir = Path(__file__).parent
        sql_file = script_dir.parent / 'sql' / 'calc_combined_score.sql'
        
        if not sql_file.exists():
            logger.error(f"SQL file not found: {sql_file}")
            return -1
        
        try:
            sql_content = sql_file.read_text(encoding='utf-8')
            
            # 参数替换
            sql_content = sql_content.replace('{trade_date:Date}', f"'{trade_date}'")
            sql_content = sql_content.replace('{weight_ind:Float32}', str(weight_ind))
            sql_content = sql_content.replace('{weight_mom:Float32}', str(weight_mom))
            
            logger.info(f"Calculating combined factor for {trade_date}")
            logger.info(f"Weights: independence={weight_ind}, momentum={weight_mom}")
            
            # 执行SQL
            self.ch_client.execute(sql_content)
            
            # 获取计算结果数量
            result = self.ch_client.execute(
                """
                SELECT count() 
                FROM combined_factor_daily 
                WHERE date = %(date)s AND weight_ind = %(w_ind)s AND weight_mom = %(w_mom)s
                """,
                {'date': trade_date, 'w_ind': weight_ind, 'w_mom': weight_mom}
            )
            count = result[0][0]
            
            logger.info(f"Calculated {count} combined scores for {trade_date}")
            return count
            
        except Exception as e:
            logger.error(f"Failed to calculate combined scores: {e}")
            return -1
    
    def get_top_scores(self, trade_date: str, limit: int = 20) -> list:
        """获取 Top 综合分数"""
        try:
            result = self.ch_client.execute(
                """
                SELECT
                    symbol,
                    sector,
                    independence_score,
                    momentum_score,
                    combined_score,
                    weight_ind,
                    weight_mom
                FROM combined_factor_daily
                WHERE date = %(date)s
                ORDER BY combined_score DESC
                LIMIT %(limit)s
                """,
                {'date': trade_date, 'limit': limit}
            )
            return result
        except Exception as e:
            logger.error(f"Failed to get top scores: {e}")
            return []


def main():
    parser = argparse.ArgumentParser(
        description='Calculate combined factor score',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 使用默认权重（50:50）
  %(prog)s 2026-03-26

  # 牛市配置（重动量）
  %(prog)s 2026-03-26 --independence-weight 0.3 --momentum-weight 0.7

  # 熊市配置（重质地）
  %(prog)s 2026-03-26 --independence-weight 0.7 --momentum-weight 0.3
        """
    )
    
    parser.add_argument(
        'date',
        nargs='?',
        default=datetime.now().strftime('%Y-%m-%d'),
        help='Trade date (YYYY-MM-DD), default: today'
    )
    
    parser.add_argument(
        '--independence-weight', '-i',
        type=float,
        default=0.5,
        help='Weight for independence score (default: 0.5)'
    )
    
    parser.add_argument(
        '--momentum-weight', '-m',
        type=float,
        default=0.5,
        help='Weight for momentum score (default: 0.5)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 验证权重
    if abs(args.independence_weight + args.momentum_weight - 1.0) > 0.001:
        logger.error("Weights must sum to 1.0")
        sys.exit(1)
    
    # 创建计算器
    calculator = CombinedFactorCalculator()
    if not calculator.connect_clickhouse():
        sys.exit(1)
    
    try:
        # 检查依赖
        if not calculator.check_dependencies(args.date):
            sys.exit(1)
        
        # 执行计算
        count = calculator.calc(
            args.date,
            args.independence_weight,
            args.momentum_weight
        )
        
        if count < 0:
            sys.exit(1)
        
        if count == 0:
            logger.warning(f"No scores calculated for {args.date}")
        else:
            # 显示 Top 结果
            top_scores = calculator.get_top_scores(args.date, 20)
            print(f"\nTop 20 Combined Factor Scores for {args.date}:")
            print(f"{'Symbol':<12} {'Sector':<12} {'Ind':>8} {'Mom':>8} {'Combined':>10} {'Weights':>12}")
            print("-" * 70)
            for row in top_scores:
                symbol, sector, ind, mom, combined, w_ind, w_mom = row
                sector_display = (sector or '')[:10]
                weights_str = f"{w_ind:.1f}:{w_mom:.1f}"
                print(f"{symbol:<12} {sector_display:<12} {ind:>8.3f} {mom:>8.3f} {combined:>10.3f} {weights_str:>12}")
        
        logger.info("Calculation completed successfully")
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
