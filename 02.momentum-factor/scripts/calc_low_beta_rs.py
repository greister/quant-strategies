#!/usr/bin/env python3
"""
低贝塔抗跌 + 相对强度混合策略计算脚本

使用方法:
    ./calc_low_beta_rs.py 2026-03-20
    ./calc_low_beta_rs.py 2026-03-20 --top-n 20 --tag "低贝塔强势"
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


class LowBetaRSFactorCalculator:
    """低贝塔 + 相对强度因子计算器"""
    
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
        计算低贝塔 + 相对强度因子
        
        Args:
            trade_date: 交易日期 (YYYY-MM-DD 格式)
        
        Returns:
            int: 计算的股票数量，失败返回 -1
        """
        script_dir = Path(__file__).parent
        sql_file = script_dir.parent / 'sql' / 'calc_low_beta_rs.sql'
        
        if not sql_file.exists():
            logger.error(f"SQL file not found: {sql_file}")
            return -1
        
        try:
            calc_sql = sql_file.read_text(encoding='utf-8')
            
            # 参数替换
            calc_sql = calc_sql.replace('{trade_date:Date}', f"'{trade_date}'")
            
            logger.info(f"Calculating Low Beta + RS factor for {trade_date}")
            
            # 执行计算
            self.ch_client.execute(calc_sql)
            
            # 获取计算结果数量
            result = self.ch_client.execute(
                """
                SELECT count() 
                FROM low_beta_rs_factor_daily 
                WHERE date = %(date)s
                """,
                {'date': trade_date}
            )
            count = result[0][0]
            
            logger.info(f"Calculated {count} Low Beta + RS scores for {trade_date}")
            return count
            
        except Exception as e:
            logger.error(f"Failed to calculate factor: {e}")
            return -1
    
    def get_top_stocks(self, trade_date: str, top_n: int = 20, tag: str = None) -> list:
        """
        获取排名靠前的股票
        
        Args:
            trade_date: 交易日期
            top_n: 取前N名
            tag: 策略标签过滤 (可选: "低贝塔强势", "低贝塔防守", "高贝塔进攻")
        
        Returns:
            list: 股票列表
        """
        try:
            query = """
                SELECT 
                    symbol,
                    name,
                    sector,
                    beta,
                    relative_strength,
                    composite_score,
                    return_1d,
                    return_20d,
                    volume_ratio,
                    strategy_tag,
                    intraday_signal,
                    rank
                FROM low_beta_rs_factor_daily
                WHERE date = %(date)s
            """
            params = {'date': trade_date}
            
            if tag:
                query += " AND strategy_tag = %(tag)s"
                params['tag'] = tag
            
            query += " ORDER BY composite_score DESC LIMIT %(limit)s"
            params['limit'] = top_n
            
            result = self.ch_client.execute(query, params)
            return result
        except Exception as e:
            logger.error(f"Failed to get top stocks: {e}")
            return []
    
    def get_signal_stocks(self, trade_date: str, signal: str = "买入信号") -> list:
        """
        获取特定交易信号的股票
        
        Args:
            trade_date: 交易日期
            signal: 交易信号 ("买入信号", "持有", "观望/卖出")
        
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
                    beta,
                    relative_strength,
                    composite_score,
                    return_1d,
                    volume_ratio,
                    rank
                FROM low_beta_rs_factor_daily
                WHERE date = %(date)s
                  AND intraday_signal = %(signal)s
                ORDER BY composite_score DESC
                """,
                {'date': trade_date, 'signal': signal}
            )
            return result
        except Exception as e:
            logger.error(f"Failed to get signal stocks: {e}")
            return []
    
    def print_results(self, stocks: list, title: str = "策略结果"):
        """打印结果"""
        if not stocks:
            print(f"\n{'='*80}")
            print(f"  {title}: 无数据")
            print(f"{'='*80}\n")
            return
        
        print(f"\n{'='*100}")
        print(f"  {title} (共 {len(stocks)} 只)")
        print(f"{'='*100}")
        print(f"{'Rank':<6} {'Symbol':<10} {'Name':<12} {'Sector':<10} {'Beta':<8} {'RS':<8} {'Score':<8} {'1D%':<8} {'Signal':<12}")
        print("-"*100)
        
        for i, row in enumerate(stocks, 1):
            symbol, name, sector, beta, rs, score, ret_1d, ret_20d, vol_ratio, tag, signal, rank = row[:12]
            print(f"{i:<6} {symbol:<10} {name[:10]:<12} {sector[:8]:<10} {beta:<8.2f} {rs:<8.2f} {score:<8.1f} {ret_1d:<8.2f} {signal:<12}")
        
        print(f"{'='*100}\n")
    
    def print_summary(self, trade_date: str):
        """打印策略汇总统计"""
        try:
            result = self.ch_client.execute(
                """
                SELECT 
                    strategy_tag,
                    count() as cnt,
                    avg(composite_score) as avg_score,
                    avg(beta) as avg_beta,
                    avg(relative_strength) as avg_rs
                FROM low_beta_rs_factor_daily
                WHERE date = %(date)s
                GROUP BY strategy_tag
                ORDER BY avg_score DESC
                """,
                {'date': trade_date}
            )
            
            print(f"\n{'='*60}")
            print(f"  策略分类统计 ({trade_date})")
            print(f"{'='*60}")
            print(f"{'分类':<16} {'数量':<8} {'平均得分':<10} {'平均Beta':<10} {'平均RS':<10}")
            print("-"*60)
            
            for row in result:
                tag, cnt, avg_score, avg_beta, avg_rs = row
                print(f"{tag:<16} {cnt:<8} {avg_score:<10.1f} {avg_beta:<10.2f} {avg_rs:<10.2f}")
            
            print(f"{'='*60}\n")
            
        except Exception as e:
            logger.error(f"Failed to print summary: {e}")


def main():
    parser = argparse.ArgumentParser(description='计算低贝塔 + 相对强度混合策略因子')
    parser.add_argument('date', help='交易日期 (YYYY-MM-DD 格式)')
    parser.add_argument('--top-n', type=int, default=20, help='显示前N名 (默认20)')
    parser.add_argument('--tag', type=str, help='按策略标签过滤 (低贝塔强势/低贝塔防守/高贝塔进攻)')
    parser.add_argument('--signal', type=str, help='按交易信号过滤 (买入信号/持有/观望/卖出)')
    parser.add_argument('--summary', action='store_true', help='显示策略汇总统计')
    
    args = parser.parse_args()
    
    # 初始化计算器
    calc = LowBetaRSFactorCalculator()
    
    if not calc.connect_clickhouse():
        sys.exit(1)
    
    # 计算因子
    count = calc.calc(args.date)
    if count < 0:
        sys.exit(1)
    
    print(f"\n✓ 低贝塔 + 相对强度策略计算完成: {args.date}")
    print(f"  计算股票数: {count}\n")
    
    # 显示汇总
    if args.summary:
        calc.print_summary(args.date)
    
    # 获取并显示结果
    if args.signal:
        stocks = calc.get_signal_stocks(args.date, args.signal)
        calc.print_results(stocks, f"交易信号: {args.signal}")
    else:
        stocks = calc.get_top_stocks(args.date, args.top_n, args.tag)
        title = f"综合排名前 {len(stocks)} 名"
        if args.tag:
            title += f" (标签: {args.tag})"
        calc.print_results(stocks, title)
    
    # 显示买入信号股票
    buy_signals = calc.get_signal_stocks(args.date, "买入信号")
    if buy_signals:
        calc.print_results(buy_signals[:10], "买入信号股票 (Top 10)")


if __name__ == '__main__':
    main()
