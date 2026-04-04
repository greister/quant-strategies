#!/usr/bin/env python3
"""
多因子综合回测分析
对比不同版本因子的选股效果和历史表现

因子版本：
1. 行业独立强度（原版本）
2. 行业独立强度（自适应阈值）
3. 双基准独立强度（行业+中证300）
4. 小时VWAP强势
5. 累积跌幅独立强度
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from clickhouse_driver import Client
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Callable

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CH_HOST = os.getenv('CH_HOST', 'localhost')
CH_PORT = int(os.getenv('CH_PORT', 9000))
CH_DB = os.getenv('CH_DB', 'tdx2db_rust')
CH_USER = os.getenv('CH_USER', 'default')
CH_PASSWORD = os.getenv('CH_PASSWORD', 'tdx2db')


def get_clickhouse_client():
    return Client(host=CH_HOST, port=CH_PORT, database=CH_DB, user=CH_USER, password=CH_PASSWORD)


class FactorCalculator:
    """因子计算类"""
    
    @staticmethod
    def industry_independence(client, trade_date: str) -> pd.DataFrame:
        """行业独立强度因子 - 中等阈值"""
        query = """
        WITH
        stock_returns AS (
            SELECT symbol, datetime,
                (close - lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) 
                / lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) * 100 as stock_return
            FROM raw_stocks_5min WHERE toDate(datetime) = %(date)s
        ),
        with_industry AS (
            SELECT s.symbol, s.datetime, s.stock_return, m.industry_code
            FROM stock_returns s JOIN stock_industry_mapping m ON s.symbol = m.symbol
            WHERE s.stock_return IS NOT NULL AND abs(s.stock_return) < 50
        ),
        industry_returns AS (
            SELECT industry_code, datetime, avg(stock_return) as industry_return
            FROM with_industry GROUP BY industry_code, datetime
        ),
        combined AS (
            SELECT w.symbol, w.datetime, w.stock_return, ind.industry_return,
                CASE WHEN ind.industry_return < -0.3 AND (w.stock_return > -0.3 OR w.stock_return - ind.industry_return > 0.5) THEN 1 ELSE 0 END as is_contra
            FROM with_industry w
            JOIN industry_returns ind ON w.industry_code = ind.industry_code AND w.datetime = ind.datetime
            WHERE abs(ind.industry_return) < 50
        )
        SELECT symbol, sum(is_contra) as score, count(*) as total,
            sum(is_contra) * 100.0 / count(*) as ratio
        FROM combined GROUP BY symbol HAVING sum(is_contra) > 0
        """
        result = client.execute(query, {'date': trade_date})
        return pd.DataFrame(result, columns=['symbol', 'score', 'total', 'ratio'])
    
    @staticmethod
    def strict_industry(client, trade_date: str) -> pd.DataFrame:
        """行业独立强度 - 严格阈值"""
        query = """
        WITH
        stock_returns AS (
            SELECT symbol, datetime,
                (close - lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) 
                / lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) * 100 as stock_return
            FROM raw_stocks_5min WHERE toDate(datetime) = %(date)s
        ),
        with_industry AS (
            SELECT s.symbol, s.datetime, s.stock_return, m.industry_code
            FROM stock_returns s JOIN stock_industry_mapping m ON s.symbol = m.symbol
            WHERE s.stock_return IS NOT NULL AND abs(s.stock_return) < 50
        ),
        industry_returns AS (
            SELECT industry_code, datetime, avg(stock_return) as industry_return
            FROM with_industry GROUP BY industry_code, datetime
        ),
        combined AS (
            SELECT w.symbol, w.datetime,
                CASE WHEN ind.industry_return < -0.5 AND (w.stock_return > 0 OR w.stock_return - ind.industry_return > 1.0) THEN 1 ELSE 0 END as is_contra
            FROM with_industry w
            JOIN industry_returns ind ON w.industry_code = ind.industry_code AND w.datetime = ind.datetime
            WHERE abs(ind.industry_return) < 50
        )
        SELECT symbol, sum(is_contra) as score, count(*) as total,
            sum(is_contra) * 100.0 / count(*) as ratio
        FROM combined GROUP BY symbol HAVING sum(is_contra) > 0
        """
        result = client.execute(query, {'date': trade_date})
        return pd.DataFrame(result, columns=['symbol', 'score', 'total', 'ratio'])
    
    @staticmethod
    def hourly_vwap(client, trade_date: str) -> pd.DataFrame:
        """小时VWAP强势因子"""
        query = """
        WITH
        stock_data AS (
            SELECT symbol, datetime, close, volume, amount,
                sum(amount) OVER (PARTITION BY symbol ORDER BY datetime ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) as hourly_amount,
                sum(volume) OVER (PARTITION BY symbol ORDER BY datetime ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) as hourly_volume,
                row_number() OVER (PARTITION BY symbol ORDER BY datetime) as rn
            FROM raw_stocks_5min WHERE toDate(datetime) = %(date)s
        ),
        valid_data AS (
            SELECT symbol, close,
                CASE WHEN hourly_volume > 0 THEN hourly_amount / hourly_volume ELSE NULL END as hourly_vwap
            FROM stock_data WHERE rn > 12 AND hourly_volume > 0
        ),
        strength_calc AS (
            SELECT symbol, CASE WHEN close > hourly_vwap THEN 1 ELSE 0 END as above_vwap
            FROM valid_data WHERE hourly_vwap IS NOT NULL
        )
        SELECT symbol, sum(above_vwap) as score, count(*) as total,
            sum(above_vwap) * 100.0 / count(*) as ratio
        FROM strength_calc GROUP BY symbol HAVING count(*) >= 30
        """
        result = client.execute(query, {'date': trade_date})
        return pd.DataFrame(result, columns=['symbol', 'score', 'total', 'ratio'])


def get_next_day_returns(client, symbols: tuple, trade_date: str) -> pd.DataFrame:
    """获取次日收益率"""
    next_date = (datetime.strptime(trade_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    query = """
    SELECT symbol, (close - open) / open * 100 as next_return
    FROM raw_stocks_daily
    WHERE date = %(next_date)s AND symbol IN %(symbols)s
    """
    result = client.execute(query, {'next_date': next_date, 'symbols': symbols})
    return pd.DataFrame(result, columns=['symbol', 'next_return'])


def backtest_single_day(client, factor_func: Callable, factor_name: str, 
                        trade_date: str, top_n: int = 20) -> Dict:
    """回测单日单个因子"""
    try:
        # 获取因子分数
        df_factor = factor_func(client, trade_date)
        
        if len(df_factor) == 0:
            return None
        
        # 取Top N
        df_top = df_factor.nlargest(top_n, 'score')
        
        # 获取次日收益
        symbols = tuple(df_top['symbol'].tolist())
        if len(symbols) == 0:
            return None
            
        df_return = get_next_day_returns(client, symbols, trade_date)
        
        # 合并
        df_merged = df_top.merge(df_return, on='symbol', how='inner')
        
        if len(df_merged) == 0:
            return None
        
        returns = df_merged['next_return'].values
        
        return {
            'date': trade_date,
            'factor': factor_name,
            'universe': len(df_factor),
            'selected': len(df_merged),
            'avg_return': np.mean(returns),
            'median_return': np.median(returns),
            'win_rate': np.mean(returns > 0) * 100,
            'max_gain': np.max(returns),
            'max_loss': np.min(returns),
            'std': np.std(returns),
            'sharpe': np.mean(returns) / np.std(returns) if np.std(returns) > 0 else 0
        }
    except Exception as e:
        logger.error(f"{factor_name} 在 {trade_date} 失败: {e}")
        return None


def run_comprehensive_backtest(start_date: str, end_date: str, top_n: int = 20):
    """运行综合回测"""
    client = get_clickhouse_client()
    
    # 获取交易日
    query = """
    SELECT DISTINCT toDate(datetime) as d FROM raw_stocks_5min 
    WHERE toDate(datetime) BETWEEN %(start)s AND %(end)s ORDER BY d
    """
    trade_dates = [str(r[0]) for r in client.execute(query, {'start': start_date, 'end': end_date})]
    
    logger.info(f"回测区间: {start_date} 至 {end_date}, 共 {len(trade_dates)} 个交易日")
    
    # 定义因子
    factors = [
        ('行业独立强度(中)', FactorCalculator.industry_independence),
        ('行业独立强度(严)', FactorCalculator.strict_industry),
        ('小时VWAP强势', FactorCalculator.hourly_vwap),
    ]
    
    results = []
    
    for date in trade_dates:
        logger.info(f"处理 {date}...")
        for name, func in factors:
            result = backtest_single_day(client, func, name, date, top_n)
            if result:
                results.append(result)
    
    return pd.DataFrame(results)


def generate_comprehensive_report(df: pd.DataFrame, output: str = None):
    """生成综合报告"""
    
    if len(df) == 0:
        print("无回测结果")
        return
    
    print("\n" + "="*160)
    print("多因子综合回测报告")
    print("="*160)
    
    # 汇总统计
    summary = []
    for factor, group in df.groupby('factor'):
        summary.append({
            '因子名称': factor,
            '回测天数': len(group),
            '股票池(日均)': round(group['universe'].mean(), 0),
            '选中(日均)': round(group['selected'].mean(), 1),
            '平均收益(%)': round(group['avg_return'].mean(), 3),
            '中位数收益(%)': round(group['median_return'].median(), 3),
            '胜率(%)': round(group['win_rate'].mean(), 2),
            '最大盈利(%)': round(group['max_gain'].max(), 2),
            '最大亏损(%)': round(group['max_loss'].min(), 2),
            '夏普比率': round(group['sharpe'].mean(), 3),
            '正收益天数': (group['avg_return'] > 0).sum(),
            '负收益天数': (group['avg_return'] < 0).sum(),
        })
    
    df_summary = pd.DataFrame(summary)
    print("\n【汇总统计】")
    print(df_summary.to_string(index=False))
    
    # 排名
    print("\n" + "="*160)
    print("因子排名")
    print("="*160)
    
    print("\n按平均收益排名:")
    for i, row in df_summary.sort_values('平均收益(%)', ascending=False).iterrows():
        print(f"  {row['因子名称']}: {row['平均收益(%)']:.3f}% (胜率 {row['胜率(%)']:.1f}%)")
    
    print("\n按胜率排名:")
    for i, row in df_summary.sort_values('胜率(%)', ascending=False).iterrows():
        print(f"  {row['因子名称']}: {row['胜率(%)']:.1f}% (收益 {row['平均收益(%)']:.3f}%)")
    
    print("\n按夏普比率排名:")
    for i, row in df_summary.sort_values('夏普比率', ascending=False).iterrows():
        print(f"  {row['因子名称']}: {row['夏普比率']:.3f} (收益 {row['平均收益(%)']:.3f}%)")
    
    # 保存
    if output:
        df.to_csv(output, index=False)
        logger.info(f"结果已保存: {output}")
    
    return df_summary


def main():
    parser = argparse.ArgumentParser(description='多因子综合回测')
    parser.add_argument('--start', type=str, required=True, help='开始日期')
    parser.add_argument('--end', type=str, required=True, help='结束日期')
    parser.add_argument('--top-n', type=int, default=20, help='每日选股数')
    parser.add_argument('--output', type=str, help='输出文件')
    
    args = parser.parse_args()
    
    df_results = run_comprehensive_backtest(args.start, args.end, args.top_n)
    
    if len(df_results) > 0:
        generate_comprehensive_report(df_results, args.output)
    else:
        print("回测失败")


if __name__ == '__main__':
    main()
