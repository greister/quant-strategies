#!/usr/bin/env python3
"""
双基准独立强度因子计算
同时计算相对于行业板块和中证300的独立强度
"""

import os
import sys
import argparse
from datetime import datetime
from clickhouse_driver import Client
import pandas as pd
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


def calc_dual_bench_score(client, trade_date: str):
    """计算双基准独立强度因子"""
    
    # 1. 获取个股5分钟收益
    logger.info(f"获取 {trade_date} 个股5分钟数据...")
    stock_query = """
    SELECT 
        symbol,
        datetime,
        (close - lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) / lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) * 100 as stock_return
    FROM raw_stocks_5min
    WHERE toDate(datetime) = %(trade_date)s
    """
    stock_data = client.execute(stock_query, {'trade_date': trade_date})
    df_stock = pd.DataFrame(stock_data, columns=['symbol', 'datetime', 'stock_return'])
    
    # 过滤有效数据
    df_stock = df_stock[
        df_stock['stock_return'].notna() & 
        (df_stock['stock_return'].abs() < 50)
    ]
    logger.info(f"个股数据: {len(df_stock)} 条记录")
    
    # 2. 获取中证300收益
    logger.info("获取中证300数据...")
    csi300_query = """
    SELECT 
        datetime,
        (close - lagInFrame(close) OVER (ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) / lagInFrame(close) OVER (ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) * 100 as csi300_return
    FROM raw_stocks_5min
    WHERE toDate(datetime) = %(trade_date)s AND symbol = 'sh000300'
    """
    csi300_data = client.execute(csi300_query, {'trade_date': trade_date})
    df_csi300 = pd.DataFrame(csi300_data, columns=['datetime', 'csi300_return'])
    df_csi300 = df_csi300[df_csi300['csi300_return'].abs() < 50]
    logger.info(f"中证300数据: {len(df_csi300)} 条记录")
    
    # 3. 获取行业映射
    logger.info("获取行业映射...")
    mapping_query = """
    SELECT symbol, industry_code
    FROM stock_industry_mapping
    """
    mapping_data = client.execute(mapping_query)
    df_mapping = pd.DataFrame(mapping_data, columns=['symbol', 'industry_code'])
    logger.info(f"行业映射: {len(df_mapping)} 只股票")
    
    # 4. 合并数据
    logger.info("合并数据...")
    df = df_stock.merge(df_mapping, on='symbol', how='inner')
    df = df.merge(df_csi300, on='datetime', how='inner')
    
    # 5. 计算行业收益
    logger.info("计算行业收益...")
    industry_returns = df.groupby(['industry_code', 'datetime'])['stock_return'].mean().reset_index()
    industry_returns.columns = ['industry_code', 'datetime', 'sector_return']
    df = df.merge(industry_returns, on=['industry_code', 'datetime'], how='inner')
    
    # 过滤异常行业收益
    df = df[df['sector_return'].abs() < 50]
    
    # 6. 计算超额收益和独立强度标志
    df['excess_sector'] = df['stock_return'] - df['sector_return']
    df['excess_csi300'] = df['stock_return'] - df['csi300_return']
    
    # 行业独立强度: sector < -0.3% AND (stock > -0.3% OR excess > 0.5%)
    df['is_sector_contra'] = (
        (df['sector_return'] < -0.3) & 
        ((df['stock_return'] > -0.3) | (df['excess_sector'] > 0.5))
    ).astype(int)
    
    # 中证300独立强度: csi300 < -0.3% AND (stock > -0.3% OR excess > 0.5%)
    df['is_csi300_contra'] = (
        (df['csi300_return'] < -0.3) & 
        ((df['stock_return'] > -0.3) | (df['excess_csi300'] > 0.5))
    ).astype(int)
    
    # 7. 汇总分数
    logger.info("计算独立强度分数...")
    result = df.groupby(['symbol', 'industry_code']).agg({
        'is_sector_contra': 'sum',
        'is_csi300_contra': 'sum',
        'stock_return': 'count',
        'excess_sector': lambda x: x[df.loc[x.index, 'is_sector_contra'] == 1].max() if any(df.loc[x.index, 'is_sector_contra'] == 1) else None,
        'excess_csi300': lambda x: x[df.loc[x.index, 'is_csi300_contra'] == 1].max() if any(df.loc[x.index, 'is_csi300_contra'] == 1) else None,
    }).reset_index()
    
    result.columns = ['symbol', 'sector_code', 'sector_score', 'csi300_score', 'total_intervals', 'max_sector_excess', 'max_csi300_excess']
    
    # 过滤至少有一个分数的股票
    result = result[(result['sector_score'] > 0) | (result['csi300_score'] > 0)]
    
    # 计算综合分数 (行业权重0.6，中证300权重0.4)
    result['dual_score'] = (result['sector_score'] * 0.6 + result['csi300_score'] * 0.4).round(2)
    
    # 计算占比
    result['sector_independence_ratio'] = (result['sector_score'] * 100.0 / result['total_intervals']).round(2)
    result['csi300_independence_ratio'] = (result['csi300_score'] * 100.0 / result['total_intervals']).round(2)
    
    # 添加日期
    result.insert(0, 'trade_date', trade_date)
    
    # 排序
    result = result.sort_values(['dual_score', 'sector_score', 'csi300_score'], ascending=False)
    
    logger.info(f"计算完成: {len(result)} 只股票")
    
    return result


def main():
    parser = argparse.ArgumentParser(description='双基准独立强度因子计算')
    parser.add_argument('--date', type=str, required=True, help='交易日期 (YYYY-MM-DD)')
    parser.add_argument('--output', type=str, help='输出CSV文件路径')
    parser.add_argument('--save-db', action='store_true', help='保存到数据库')
    
    args = parser.parse_args()
    
    client = get_clickhouse_client()
    
    # 计算双基准分数
    result = calc_dual_bench_score(client, args.date)
    
    # 显示前20名
    print("\n" + "="*100)
    print(f"双基准独立强度因子 Top 20 - {args.date}")
    print("="*100)
    print(result.head(20).to_string(index=False))
    
    # 统计信息
    print("\n" + "="*100)
    print("统计信息")
    print("="*100)
    print(f"总股票数: {len(result)}")
    print(f"行业独立强度>0: {(result['sector_score'] > 0).sum()}")
    print(f"中证300独立强度>0: {(result['csi300_score'] > 0).sum()}")
    print(f"双基准均>0: {((result['sector_score'] > 0) & (result['csi300_score'] > 0)).sum()}")
    print(f"\n行业独立强度分布:")
    print(result['sector_score'].describe())
    print(f"\n中证300独立强度分布:")
    print(result['csi300_score'].describe())
    
    # 保存到CSV
    if args.output:
        result.to_csv(args.output, index=False)
        logger.info(f"结果已保存到: {args.output}")
    
    # 保存到数据库
    if args.save_db:
        # 创建表
        create_table_query = """
        CREATE TABLE IF NOT EXISTS independence_score_dual_bench (
            trade_date Date,
            symbol String,
            sector_code String,
            sector_score Int32,
            csi300_score Int32,
            dual_score Float32,
            total_intervals Int32,
            sector_independence_ratio Float32,
            csi300_independence_ratio Float32,
            max_sector_excess Float32,
            max_csi300_excess Float32
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (trade_date, symbol)
        """
        client.execute(create_table_query)
        
        # 插入数据
        data = [tuple(row) for row in result.values]
        insert_query = """
        INSERT INTO independence_score_dual_bench 
        (trade_date, symbol, sector_code, sector_score, csi300_score, dual_score, 
         total_intervals, sector_independence_ratio, csi300_independence_ratio,
         max_sector_excess, max_csi300_excess)
        VALUES
        """
        client.execute(insert_query, data)
        logger.info(f"结果已保存到数据库表 independence_score_dual_bench")


if __name__ == '__main__':
    main()
