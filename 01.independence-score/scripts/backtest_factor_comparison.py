#!/usr/bin/env python3
"""
多因子历史回测比较分析
对比不同版本因子的选股效果和历史表现
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from clickhouse_driver import Client
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 数据库连接配置
CH_HOST = os.getenv('CH_HOST', 'localhost')
CH_PORT = int(os.getenv('CH_PORT', 9000))
CH_DB = os.getenv('CH_DB', 'tdx2db_rust')
CH_USER = os.getenv('CH_USER', 'default')
CH_PASSWORD = os.getenv('CH_PASSWORD', 'tdx2db')


def get_clickhouse_client():
    """获取ClickHouse连接"""
    return Client(
        host=CH_HOST,
        port=CH_PORT,
        database=CH_DB,
        user=CH_USER,
        password=CH_PASSWORD
    )


def get_trade_dates(client, start_date: str, end_date: str) -> List[str]:
    """获取交易日列表"""
    query = """
    SELECT DISTINCT toDate(datetime) as trade_date
    FROM raw_stocks_5min
    WHERE toDate(datetime) BETWEEN %(start)s AND %(end)s
    ORDER BY trade_date
    """
    result = client.execute(query, {'start': start_date, 'end': end_date})
    return [str(r[0]) for r in result]


def get_next_day_return(client, symbols: List[str], trade_date: str) -> pd.DataFrame:
    """获取次日收益率"""
    next_date = (datetime.strptime(trade_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    
    query = """
    SELECT 
        symbol,
        (close - open) / open * 100 as next_day_return
    FROM raw_stocks_daily
    WHERE date = %(next_date)s
      AND symbol IN %(symbols)s
    """
    result = client.execute(query, {'next_date': next_date, 'symbols': symbols})
    return pd.DataFrame(result, columns=['symbol', 'next_day_return'])


def calc_industry_independence_score(client, trade_date: str) -> pd.DataFrame:
    """计算行业独立强度因子"""
    query = """
    WITH
    stock_returns AS (
        SELECT
            symbol,
            datetime,
            (close - lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) / lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) * 100 as stock_return
        FROM raw_stocks_5min
        WHERE toDate(datetime) = %(trade_date)s
    ),
    with_industry AS (
        SELECT sr.symbol, sr.datetime, sr.stock_return, m.industry_code
        FROM stock_returns sr
        JOIN stock_industry_mapping m ON sr.symbol = m.symbol
        WHERE sr.stock_return IS NOT NULL AND abs(sr.stock_return) < 50
    ),
    industry_returns AS (
        SELECT industry_code, datetime, avg(stock_return) as industry_return
        FROM with_industry
        GROUP BY industry_code, datetime
    ),
    combined AS (
        SELECT 
            w.symbol, w.industry_code, w.datetime, w.stock_return, 
            ind.industry_return, w.stock_return - ind.industry_return as excess_return,
            CASE WHEN ind.industry_return < -0.3 AND (w.stock_return > -0.3 OR w.stock_return - ind.industry_return > 0.5) THEN 1 ELSE 0 END as is_contra
        FROM with_industry w
        JOIN industry_returns ind ON w.industry_code = ind.industry_code AND w.datetime = ind.datetime
        WHERE abs(ind.industry_return) < 50
    )
    SELECT 
        symbol,
        sum(is_contra) as score,
        count(*) as total_intervals,
        sum(is_contra) * 100.0 / count(*) as score_ratio
    FROM combined
    GROUP BY symbol
    HAVING sum(is_contra) > 0
    """
    result = client.execute(query, {'trade_date': trade_date})
    df = pd.DataFrame(result, columns=['symbol', 'score', 'total_intervals', 'score_ratio'])
    df['factor_name'] = '行业独立强度'
    return df


def calc_hourly_vwap_score(client, trade_date: str) -> pd.DataFrame:
    """计算小时VWAP强势因子"""
    query = """
    WITH
    stock_data AS (
        SELECT 
            symbol,
            datetime,
            close,
            volume,
            amount,
            sum(amount) OVER (PARTITION BY symbol ORDER BY datetime ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) as hourly_amount,
            sum(volume) OVER (PARTITION BY symbol ORDER BY datetime ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) as hourly_volume,
            row_number() OVER (PARTITION BY symbol ORDER BY datetime) as rn
        FROM raw_stocks_5min
        WHERE toDate(datetime) = %(trade_date)s
    ),
    valid_data AS (
        SELECT 
            symbol, datetime, close, volume, amount,
            CASE WHEN hourly_volume > 0 THEN hourly_amount / hourly_volume ELSE NULL END as hourly_vwap
        FROM stock_data
        WHERE rn > 12 AND hourly_volume > 0
    ),
    strength_calc AS (
        SELECT 
            symbol,
            CASE WHEN close > hourly_vwap THEN 1 ELSE 0 END as above_vwap
        FROM valid_data
        WHERE hourly_vwap IS NOT NULL
    )
    SELECT 
        symbol,
        sum(above_vwap) as score,
        count(*) as total_intervals,
        sum(above_vwap) * 100.0 / count(*) as score_ratio
    FROM strength_calc
    GROUP BY symbol
    HAVING count(*) >= 30
    """
    result = client.execute(query, {'trade_date': trade_date})
    df = pd.DataFrame(result, columns=['symbol', 'score', 'total_intervals', 'score_ratio'])
    df['factor_name'] = '小时VWAP强势'
    return df


def backtest_factor(client, factor_func, trade_date: str, top_n: int = 20) -> Dict:
    """回测单个因子"""
    # 获取因子分数
    df_factor = factor_func(client, trade_date)
    
    if len(df_factor) == 0:
        return None
    
    # 取Top N股票
    df_top = df_factor.nlargest(top_n, 'score')
    
    # 获取次日收益
    symbols = df_top['symbol'].tolist()
    df_return = get_next_day_return(client, symbols, trade_date)
    
    # 合并数据
    df_merged = df_top.merge(df_return, on='symbol', how='inner')
    
    if len(df_merged) == 0:
        return None
    
    # 计算指标
    returns = df_merged['next_day_return'].values
    
    return {
        'trade_date': trade_date,
        'factor_name': df_factor['factor_name'].iloc[0],
        'stock_count': len(df_factor),
        'selected_count': len(df_merged),
        'avg_return': np.mean(returns),
        'median_return': np.median(returns),
        'win_rate': np.mean(returns > 0) * 100,
        'max_return': np.max(returns),
        'min_return': np.min(returns),
        'std_return': np.std(returns),
        'sharpe': np.mean(returns) / np.std(returns) if np.std(returns) > 0 else 0
    }


def run_backtest_comparison(client, start_date: str, end_date: str, top_n: int = 20):
    """运行多因子回测比较"""
    
    # 获取交易日
    trade_dates = get_trade_dates(client, start_date, end_date)
    logger.info(f"回测区间: {start_date} 至 {end_date}, 共 {len(trade_dates)} 个交易日")
    
    # 定义要测试的因子
    factors = [
        ('行业独立强度', calc_industry_independence_score),
        ('小时VWAP强势', calc_hourly_vwap_score),
    ]
    
    # 存储结果
    all_results = []
    
    for trade_date in trade_dates:
        logger.info(f"处理日期: {trade_date}")
        
        for factor_name, factor_func in factors:
            try:
                result = backtest_factor(client, factor_func, trade_date, top_n)
                if result:
                    all_results.append(result)
            except Exception as e:
                logger.error(f"{factor_name} 在 {trade_date} 回测失败: {e}")
    
    # 转换为DataFrame
    df_results = pd.DataFrame(all_results)
    
    return df_results


def generate_report(df_results: pd.DataFrame, output_file: str = None):
    """生成回测报告"""
    
    if len(df_results) == 0:
        print("没有回测结果")
        return
    
    # 按因子分组统计
    report = []
    
    for factor_name, group in df_results.groupby('factor_name'):
        report.append({
            '因子名称': factor_name,
            '回测天数': len(group),
            '平均选股数': group['stock_count'].mean(),
            '平均选中数': group['selected_count'].mean(),
            '平均次日收益(%)': round(group['avg_return'].mean(), 3),
            '中位数次日收益(%)': round(group['median_return'].median(), 3),
            '胜率(%)': round(group['win_rate'].mean(), 2),
            '最大单日收益(%)': round(group['max_return'].max(), 2),
            '最大单日亏损(%)': round(group['min_return'].min(), 2),
            '收益波动率': round(group['std_return'].mean(), 3),
            '夏普比率': round(group['sharpe'].mean(), 3),
            '正收益天数': (group['avg_return'] > 0).sum(),
            '负收益天数': (group['avg_return'] < 0).sum()
        })
    
    df_report = pd.DataFrame(report)
    
    # 打印报告
    print("\n" + "="*150)
    print("多因子回测比较报告")
    print("="*150)
    print(df_report.to_string(index=False))
    
    # 详细分析
    print("\n" + "="*150)
    print("详细分析")
    print("="*150)
    
    for factor_name, group in df_results.groupby('factor_name'):
        print(f"\n【{factor_name}】")
        print(f"  回测区间: {group['trade_date'].min()} 至 {group['trade_date'].max()}")
        print(f"  总选股次数: {group['stock_count'].sum():.0f}")
        print(f"  日均选股: {group['stock_count'].mean():.1f} 只")
        print(f"  次日平均收益: {group['avg_return'].mean():.3f}%")
        print(f"  胜率: {group['win_rate'].mean():.2f}%")
        print(f"  盈亏比: {abs(group['avg_return'][group['avg_return']>0].mean() / group['avg_return'][group['avg_return']<0].mean()):.2f}")
    
    # 保存到文件
    if output_file:
        df_results.to_csv(output_file, index=False)
        logger.info(f"详细结果已保存: {output_file}")
    
    return df_report


def main():
    parser = argparse.ArgumentParser(description='多因子历史回测比较')
    parser.add_argument('--start', type=str, required=True, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, required=True, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--top-n', type=int, default=20, help='每日选股数量 (默认20)')
    parser.add_argument('--output', type=str, help='输出CSV文件路径')
    
    args = parser.parse_args()
    
    client = get_clickhouse_client()
    
    # 运行回测
    df_results = run_backtest_comparison(client, args.start, args.end, args.top_n)
    
    if len(df_results) > 0:
        # 生成报告
        df_report = generate_report(df_results, args.output)
        
        # 找出最佳因子
        best_factor = df_report.loc[df_report['平均次日收益(%)'].idxmax(), '因子名称']
        print(f"\n🏆 最佳因子（按平均收益）: {best_factor}")
    else:
        print("回测失败，没有结果")


if __name__ == '__main__':
    main()
