#!/usr/bin/env python3
"""
累积跌幅独立强度因子计算
解决"温水煮青蛙"缓跌行情下的算法失效问题

改进点：
1. 原逻辑：单区间跌幅 < -0.3% 触发
2. 新逻辑：增加累积跌幅触发
   - 连续3个区间累积跌幅 < -0.4%
   - 或连续5个区间累积跌幅 < -0.6%
"""

import os
import sys
import argparse
from datetime import datetime
from clickhouse_driver import Client
import pandas as pd
import numpy as np
import logging

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


def calculate_cumulative_returns(df, window=3):
    """计算累积收益"""
    return df.rolling(window=window).apply(
        lambda x: (x.iloc[-1] - x.iloc[0]) / x.iloc[0] * 100 if len(x) == window and x.iloc[0] != 0 else np.nan
    )


def calc_cumulative_decline_score(client, trade_date: str):
    """计算累积跌幅独立强度因子"""
    
    # 1. 获取个股5分钟收益
    logger.info(f"获取 {trade_date} 个股5分钟数据...")
    stock_query = """
    SELECT 
        symbol,
        datetime,
        (close - lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) / lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) * 100 as stock_return,
        close
    FROM raw_stocks_5min
    WHERE toDate(datetime) = %(trade_date)s
    """
    stock_data = client.execute(stock_query, {'trade_date': trade_date})
    df_stock = pd.DataFrame(stock_data, columns=['symbol', 'datetime', 'stock_return', 'close'])
    df_stock = df_stock[df_stock['stock_return'].notna() & (df_stock['stock_return'].abs() < 50)]
    logger.info(f"个股数据: {len(df_stock)} 条记录")
    
    # 2. 获取中证300数据
    logger.info("获取中证300数据...")
    csi300_query = """
    SELECT 
        datetime,
        (close - lagInFrame(close) OVER (ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) / lagInFrame(close) OVER (ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) * 100 as csi300_return,
        close as csi300_close
    FROM raw_stocks_5min
    WHERE toDate(datetime) = %(trade_date)s AND symbol = 'sh000300'
    """
    csi300_data = client.execute(csi300_query, {'trade_date': trade_date})
    df_csi300 = pd.DataFrame(csi300_data, columns=['datetime', 'csi300_return', 'csi300_close'])
    df_csi300 = df_csi300[df_csi300['csi300_return'].abs() < 50]
    logger.info(f"中证300数据: {len(df_csi300)} 条记录")
    
    # 计算累积跌幅
    df_csi300 = df_csi300.sort_values('datetime').reset_index(drop=True)
    df_csi300['cum_3interval'] = df_csi300['csi300_close'].rolling(window=3).apply(
        lambda x: (x.iloc[-1] - x.iloc[0]) / x.iloc[0] * 100 if len(x) == 3 else np.nan
    )
    df_csi300['cum_5interval'] = df_csi300['csi300_close'].rolling(window=5).apply(
        lambda x: (x.iloc[-1] - x.iloc[0]) / x.iloc[0] * 100 if len(x) == 5 else np.nan
    )
    
    # 3. 获取行业映射
    mapping_query = "SELECT symbol, industry_code FROM stock_industry_mapping"
    mapping_data = client.execute(mapping_query)
    df_mapping = pd.DataFrame(mapping_data, columns=['symbol', 'industry_code'])
    
    # 4. 合并数据
    logger.info("合并数据...")
    df = df_stock.merge(df_mapping, on='symbol', how='inner')
    df = df.merge(df_csi300, on='datetime', how='inner')
    
    # 5. 计算行业收益
    industry_returns = df.groupby(['industry_code', 'datetime'])['stock_return'].mean().reset_index()
    industry_returns.columns = ['industry_code', 'datetime', 'industry_return']
    df = df.merge(industry_returns, on=['industry_code', 'datetime'], how='inner')
    df = df[df['industry_return'].abs() < 50]
    
    # 6. 计算超额收益
    df['excess_industry'] = df['stock_return'] - df['industry_return']
    df['excess_csi300'] = df['stock_return'] - df['csi300_return']
    
    # 7. 判断独立强度
    # 行业独立强度
    df['is_industry_contra'] = (
        (df['industry_return'] < -0.3) & 
        ((df['stock_return'] > -0.3) | (df['excess_industry'] > 0.5))
    ).astype(int)
    
    # 中证300独立强度 - 原逻辑（单区间急跌）
    df['is_csi300_immediate'] = (
        (df['csi300_return'] < -0.3) & 
        ((df['stock_return'] > -0.3) | (df['excess_csi300'] > 0.5))
    ).astype(int)
    
    # 中证300独立强度 - 新增（累积跌幅缓跌）
    df['is_csi300_cumulative'] = (
        ((df['cum_3interval'] < -0.4) | (df['cum_5interval'] < -0.6)) & 
        ((df['stock_return'] > -0.3) | (df['excess_csi300'] > 0.3))
    ).astype(int)
    
    # 综合中证300独立强度
    df['is_csi300_contra'] = ((df['is_csi300_immediate'] == 1) | (df['is_csi300_cumulative'] == 1)).astype(int)
    
    # 8. 汇总分数
    logger.info("计算独立强度分数...")
    result = df.groupby(['symbol', 'industry_code']).agg({
        'is_industry_contra': 'sum',
        'is_csi300_immediate': 'sum',
        'is_csi300_cumulative': 'sum',
        'is_csi300_contra': 'sum',
        'stock_return': 'count'
    }).reset_index()
    
    result.columns = ['symbol', 'sector_code', 'industry_score', 'csi300_immediate', 
                      'csi300_cumulative', 'csi300_total', 'total_intervals']
    
    # 过滤至少有一个分数的股票
    result = result[(result['industry_score'] > 0) | (result['csi300_total'] > 0)]
    
    # 计算综合分数
    result['dual_score'] = (result['industry_score'] * 0.6 + result['csi300_total'] * 0.4).round(2)
    
    # 计算占比
    result['industry_ratio'] = (result['industry_score'] * 100.0 / result['total_intervals']).round(2)
    result['csi300_ratio'] = (result['csi300_total'] * 100.0 / result['total_intervals']).round(2)
    
    # 添加日期
    result.insert(0, 'trade_date', trade_date)
    
    # 排序
    result = result.sort_values(['dual_score', 'industry_score', 'csi300_total'], ascending=False)
    
    logger.info(f"计算完成: {len(result)} 只股票")
    
    return result


def main():
    parser = argparse.ArgumentParser(description='累积跌幅独立强度因子计算')
    parser.add_argument('--date', type=str, required=True, help='交易日期 (YYYY-MM-DD)')
    parser.add_argument('--output', type=str, help='输出CSV文件路径')
    
    args = parser.parse_args()
    
    client = get_clickhouse_client()
    
    # 计算累积跌幅因子
    result = calc_cumulative_decline_score(client, args.date)
    
    # 显示前20名
    print("\n" + "="*120)
    print(f"累积跌幅独立强度因子 Top 20 - {args.date}")
    print("="*120)
    display_cols = ['trade_date', 'symbol', 'industry_score', 'csi300_immediate', 
                    'csi300_cumulative', 'csi300_total', 'dual_score']
    print(result[display_cols].head(20).to_string(index=False))
    
    # 统计信息
    print("\n" + "="*120)
    print("统计对比")
    print("="*120)
    print(f"总股票数: {len(result)}")
    print(f"\n中证300独立强度分布:")
    print(f"  - 仅急跌触发: {(result['csi300_immediate'] > 0).sum()} 只")
    print(f"  - 仅累积触发: {(result['csi300_cumulative'] > 0).sum()} 只")
    print(f"  - 总计触发:   {(result['csi300_total'] > 0).sum()} 只")
    print(f"\n行业独立强度>0: {(result['industry_score'] > 0).sum()} 只")
    print(f"双基准均>0: {((result['industry_score'] > 0) & (result['csi300_total'] > 0)).sum()} 只")
    
    # 保存到CSV
    if args.output:
        result.to_csv(args.output, index=False)
        logger.info(f"结果已保存到: {args.output}")


if __name__ == '__main__':
    main()
