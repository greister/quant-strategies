#!/usr/bin/env python3
"""
小时均线强势因子计算
统计全天有多少个5分钟区间满足：当前价 > 过去1小时均价

因子定义：
- 过去1小时 = 过去12个5分钟区间
- 强势条件：当前close > 过去12个区间的均价
- 选股标准：统计全天满足条件的区间数量
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


def calc_hourly_ma_strength(client, trade_date: str):
    """计算小时均线强势因子"""
    
    # 1. 获取5分钟数据
    logger.info(f"获取 {trade_date} 5分钟数据...")
    query = """
    SELECT 
        symbol,
        datetime,
        close,
        open,
        high,
        low,
        volume
    FROM raw_stocks_5min
    WHERE toDate(datetime) = %(trade_date)s
    ORDER BY symbol, datetime
    """
    data = client.execute(query, {'trade_date': trade_date})
    df = pd.DataFrame(data, columns=['symbol', 'datetime', 'close', 'open', 'high', 'low', 'volume'])
    logger.info(f"原始数据: {len(df)} 条记录，{df['symbol'].nunique()} 只股票")
    
    # 2. 计算小时均线（过去12个区间=1小时）
    logger.info("计算小时均线...")
    
    # 按股票分组计算
    results = []
    
    for symbol, group in df.groupby('symbol'):
        if len(group) < 25:  # 至少需要25个区间（约2.5小时+12小时均线计算）
            continue
            
        group = group.sort_values('datetime').reset_index(drop=True)
        
        # 计算过去1小时（12个区间）的均价
        group['hourly_ma'] = group['close'].rolling(window=12, min_periods=12).mean().shift(1)
        
        # 过滤掉没有足够历史数据的区间
        valid = group[group['hourly_ma'].notna()].copy()
        
        if len(valid) < 30:  # 至少需要30个有效区间
            continue
        
        # 判断强势：当前价 > 过去1小时均价
        valid['above_ma'] = (valid['close'] > valid['hourly_ma']).astype(int)
        
        # 计算强势幅度
        valid['strength_ratio'] = (valid['close'] - valid['hourly_ma']) / valid['hourly_ma'] * 100
        
        # 统计指标
        total_intervals = len(valid)
        above_ma_count = valid['above_ma'].sum()
        above_ma_ratio = above_ma_count / total_intervals * 100
        avg_strength = valid['strength_ratio'].mean()
        max_strength = valid['strength_ratio'].max()
        min_strength = valid['strength_ratio'].min()
        
        # 连续在均线上方的最大次数
        valid['above_ma_group'] = (valid['above_ma'] != valid['above_ma'].shift()).cumsum()
        consecutive_above = valid[valid['above_ma'] == 1].groupby('above_ma_group').size().max()
        if pd.isna(consecutive_above):
            consecutive_above = 0
        
        # 首次突破均线时间
        first_above = valid[valid['above_ma'] == 1]['datetime'].min() if above_ma_count > 0 else None
        
        # 最后收盘位置
        last_close_vs_ma = valid.iloc[-1]['close'] > valid.iloc[-1]['hourly_ma']
        
        results.append({
            'trade_date': trade_date,
            'symbol': symbol,
            'total_intervals': total_intervals,
            'above_ma_count': above_ma_count,
            'above_ma_ratio': round(above_ma_ratio, 2),
            'avg_strength_ratio': round(avg_strength, 4),
            'max_strength_ratio': round(max_strength, 4),
            'min_strength_ratio': round(min_strength, 4),
            'max_consecutive_above': int(consecutive_above),
            'first_above_time': first_above,
            'last_close_above_ma': int(last_close_vs_ma)
        })
    
    result_df = pd.DataFrame(results)
    
    if len(result_df) == 0:
        logger.warning("没有计算出任何结果")
        return result_df
    
    # 计算综合分数
    # 综合分数 = 均线上方占比×0.7 + 连续强势次数×5 + 收盘在均线上方×10
    result_df['composite_score'] = (
        result_df['above_ma_ratio'] * 0.7 + 
        result_df['max_consecutive_above'] * 5 + 
        result_df['last_close_above_ma'] * 10
    ).round(2)
    
    # 排序
    result_df = result_df.sort_values(['composite_score', 'above_ma_count'], ascending=False)
    
    logger.info(f"计算完成: {len(result_df)} 只股票")
    
    return result_df


def main():
    parser = argparse.ArgumentParser(description='小时均线强势因子计算')
    parser.add_argument('--date', type=str, required=True, help='交易日期 (YYYY-MM-DD)')
    parser.add_argument('--output', type=str, help='输出CSV文件路径')
    parser.add_argument('--save-db', action='store_true', help='保存到数据库')
    parser.add_argument('--min-ratio', type=float, default=60, help='最小强势占比筛选（默认60%）')
    
    args = parser.parse_args()
    
    client = get_clickhouse_client()
    
    # 计算因子
    result = calc_hourly_ma_strength(client, args.date)
    
    if len(result) == 0:
        print("没有数据")
        return
    
    # 显示前30名
    print("\n" + "="*130)
    print(f"小时均线强势因子 Top 30 - {args.date}")
    print("="*130)
    display_cols = ['symbol', 'above_ma_count', 'above_ma_ratio', 'avg_strength_ratio', 
                    'max_consecutive_above', 'composite_score']
    print(result[display_cols].head(30).to_string(index=False))
    
    # 显示强势股票筛选
    strong_stocks = result[result['above_ma_ratio'] >= args.min_ratio]
    print(f"\n" + "="*130)
    print(f"强势股票筛选（均线上方占比 >= {args.min_ratio}%）: {len(strong_stocks)} 只")
    print("="*130)
    if len(strong_stocks) > 0:
        print(strong_stocks[display_cols].head(20).to_string(index=False))
    
    # 统计信息
    print("\n" + "="*130)
    print("统计信息")
    print("="*130)
    print(f"总股票数: {len(result)}")
    print(f"均线上方占比分布:")
    print(f"  - 100% (全天强势): {(result['above_ma_ratio'] == 100).sum()} 只")
    print(f"  - >=80% (高度强势): {(result['above_ma_ratio'] >= 80).sum()} 只")
    print(f"  - >=60% (中度强势): {(result['above_ma_ratio'] >= 60).sum()} 只")
    print(f"  - >=40% (轻度强势): {(result['above_ma_ratio'] >= 40).sum()} 只")
    print(f"  - <40% (弱势): {(result['above_ma_ratio'] < 40).sum()} 只")
    print(f"\n平均强势占比: {result['above_ma_ratio'].mean():.2f}%")
    print(f"最大连续均线上方区间数: {result['max_consecutive_above'].max()}")
    print(f"收盘在均线上方股票: {result['last_close_above_ma'].sum()} 只")
    
    # 保存到CSV
    if args.output:
        result.to_csv(args.output, index=False)
        logger.info(f"结果已保存到: {args.output}")
    
    # 保存到数据库
    if args.save_db:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS hourly_ma_strength (
            trade_date Date,
            symbol String,
            total_intervals Int32,
            above_ma_count Int32,
            above_ma_ratio Float32,
            avg_strength_ratio Float32,
            max_strength_ratio Float32,
            min_strength_ratio Float32,
            max_consecutive_above Int32,
            first_above_time Nullable(DateTime),
            last_close_above_ma Int8,
            composite_score Float32
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (trade_date, symbol)
        """
        client.execute(create_table_query)
        
        # 插入数据
        data = []
        for _, row in result.iterrows():
            data.append((
                row['trade_date'],
                row['symbol'],
                int(row['total_intervals']),
                int(row['above_ma_count']),
                float(row['above_ma_ratio']),
                float(row['avg_strength_ratio']),
                float(row['max_strength_ratio']),
                float(row['min_strength_ratio']),
                int(row['max_consecutive_above']),
                row['first_above_time'],
                int(row['last_close_above_ma']),
                float(row['composite_score'])
            ))
        
        insert_query = """
        INSERT INTO hourly_ma_strength 
        (trade_date, symbol, total_intervals, above_ma_count, above_ma_ratio,
         avg_strength_ratio, max_strength_ratio, min_strength_ratio,
         max_consecutive_above, first_above_time, last_close_above_ma, composite_score)
        VALUES
        """
        client.execute(insert_query, data)
        logger.info(f"结果已保存到数据库表 hourly_ma_strength")


if __name__ == '__main__':
    main()
