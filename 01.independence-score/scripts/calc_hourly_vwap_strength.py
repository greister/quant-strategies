#!/usr/bin/env python3
"""
小时VWAP强势因子计算
统计全天有多少个5分钟区间满足：当前价 > 过去1小时VWAP（成交量加权均价）

VWAP定义：过去12个区间的总成交额 / 总成交量

与简单MA的区别：
- MA12：简单平均，(C1+C2+...+C12)/12
- VWAP：成交量加权，(A1+A2+...+A12)/(V1+V2+...+V12)
  其中 A=amount(成交额), V=volume(成交量)
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


def calc_hourly_vwap_strength(client, trade_date: str):
    """计算小时VWAP强势因子"""
    
    # 1. 获取5分钟数据（包含成交额和成交量）
    logger.info(f"获取 {trade_date} 5分钟数据...")
    query = """
    SELECT 
        symbol,
        datetime,
        close,
        volume,
        amount
    FROM raw_stocks_5min
    WHERE toDate(datetime) = %(trade_date)s
    ORDER BY symbol, datetime
    """
    data = client.execute(query, {'trade_date': trade_date})
    df = pd.DataFrame(data, columns=['symbol', 'datetime', 'close', 'volume', 'amount'])
    logger.info(f"原始数据: {len(df)} 条记录，{df['symbol'].nunique()} 只股票")
    
    # 2. 过滤只保留有行业映射的有效股票（从stock_industry_mapping获取）
    mapping_query = "SELECT DISTINCT symbol FROM stock_industry_mapping"
    valid_symbols = client.execute(mapping_query)
    valid_symbol_set = set([s[0] for s in valid_symbols])
    
    df = df[df['symbol'].isin(valid_symbol_set)]
    # 排除成交量为0的停牌股票
    df = df[df['volume'] > 0]
    logger.info(f"过滤后数据: {len(df)} 条记录，{df['symbol'].nunique()} 只有效股票")
    
    # 3. 计算小时VWAP（过去12个区间=1小时）
    logger.info("计算小时VWAP（成交量加权均价）...")
    
    results = []
    
    for symbol, group in df.groupby('symbol'):
        if len(group) < 35:  # 至少需要35个区间（确保有足够的历史数据）
            continue
            
        group = group.sort_values('datetime').reset_index(drop=True)
        
        # 计算滚动12个区间的VWAP
        # VWAP = 滚动12个区间的总成交额 / 总成交量
        group['rolling_amount'] = group['amount'].rolling(window=12, min_periods=1).sum().shift(1)
        group['rolling_volume'] = group['volume'].rolling(window=12, min_periods=1).sum().shift(1)
        group['hourly_vwap'] = group['rolling_amount'] / group['rolling_volume']
        
        # 过滤掉没有足够历史数据的区间（至少有12个区间的数据）
        valid = group[group['hourly_vwap'].notna() & (group['rolling_volume'] > 0)].copy()
        
        if len(valid) < 30:  # 至少需要30个有效区间
            continue
        
        # 判断强势：当前价 > VWAP
        valid['above_vwap'] = (valid['close'] > valid['hourly_vwap']).astype(int)
        
        # 计算强势幅度
        valid['strength_ratio'] = (valid['close'] - valid['hourly_vwap']) / valid['hourly_vwap'] * 100
        
        # 统计指标
        total_intervals = len(valid)
        above_vwap_count = valid['above_vwap'].sum()
        above_vwap_ratio = above_vwap_count / total_intervals * 100
        avg_strength = valid['strength_ratio'].mean()
        max_strength = valid['strength_ratio'].max()
        min_strength = valid['strength_ratio'].min()
        
        # 成交量加权的强势占比
        volume_weighted_strength = (valid['above_vwap'] * valid['volume']).sum() / valid['volume'].sum() * 100
        
        # 成交额加权的强势占比
        amount_weighted_strength = (valid['above_vwap'] * valid['amount']).sum() / valid['amount'].sum() * 100
        
        # 连续在VWAP上方的最大次数
        valid['above_vwap_group'] = (valid['above_vwap'] != valid['above_vwap'].shift()).cumsum()
        consecutive_above = valid[valid['above_vwap'] == 1].groupby('above_vwap_group').size().max()
        if pd.isna(consecutive_above):
            consecutive_above = 0
        
        # 首次突破VWAP时间
        first_above = valid[valid['above_vwap'] == 1]['datetime'].min() if above_vwap_count > 0 else None
        
        # 最后收盘位置
        last_close_vs_vwap = valid.iloc[-1]['close'] > valid.iloc[-1]['hourly_vwap']
        
        # 平均VWAP偏离度
        avg_vwap_deviation = abs(valid['close'] - valid['hourly_vwap']).mean() / valid['hourly_vwap'].mean() * 100
        
        results.append({
            'trade_date': trade_date,
            'symbol': symbol,
            'total_intervals': total_intervals,
            'above_vwap_count': above_vwap_count,
            'above_vwap_ratio': round(above_vwap_ratio, 2),
            'volume_weighted_strength': round(volume_weighted_strength, 2),
            'amount_weighted_strength': round(amount_weighted_strength, 2),
            'avg_strength_ratio': round(avg_strength, 4),
            'max_strength_ratio': round(max_strength, 4),
            'min_strength_ratio': round(min_strength, 4),
            'avg_vwap_deviation': round(avg_vwap_deviation, 4),
            'max_consecutive_above': int(consecutive_above),
            'first_above_time': first_above,
            'last_close_above_vwap': int(last_close_vs_vwap)
        })
    
    result_df = pd.DataFrame(results)
    
    if len(result_df) == 0:
        logger.warning("没有计算出任何结果")
        return result_df
    
    # 计算综合分数
    # 综合分数 = VWAP上方占比×0.5 + 成交量加权×0.3 + 成交额加权×0.2 + 连续强势×10
    result_df['composite_score'] = (
        result_df['above_vwap_ratio'] * 0.5 + 
        result_df['volume_weighted_strength'] * 0.3 +
        result_df['amount_weighted_strength'] * 0.2 +
        result_df['max_consecutive_above'] * 10
    ).round(2)
    
    # 排序
    result_df = result_df.sort_values(['composite_score', 'above_vwap_ratio'], ascending=False)
    
    logger.info(f"计算完成: {len(result_df)} 只股票")
    
    return result_df


def main():
    parser = argparse.ArgumentParser(description='小时VWAP强势因子计算')
    parser.add_argument('--date', type=str, required=True, help='交易日期 (YYYY-MM-DD)')
    parser.add_argument('--output', type=str, help='输出CSV文件路径')
    parser.add_argument('--save-db', action='store_true', help='保存到数据库')
    parser.add_argument('--min-ratio', type=float, default=60, help='最小强势占比筛选（默认60%）')
    
    args = parser.parse_args()
    
    client = get_clickhouse_client()
    
    # 计算因子
    result = calc_hourly_vwap_strength(client, args.date)
    
    if len(result) == 0:
        print("没有数据")
        return
    
    # 显示前30名
    print("\n" + "="*140)
    print(f"小时VWAP强势因子 Top 30 - {args.date}")
    print("="*140)
    display_cols = ['symbol', 'above_vwap_count', 'above_vwap_ratio', 'volume_weighted_strength', 
                    'amount_weighted_strength', 'max_consecutive_above', 'composite_score']
    print(result[display_cols].head(30).to_string(index=False))
    
    # 显示强势股票筛选
    strong_stocks = result[result['above_vwap_ratio'] >= args.min_ratio]
    print(f"\n" + "="*140)
    print(f"强势股票筛选（VWAP上方占比 >= {args.min_ratio}%）: {len(strong_stocks)} 只")
    print("="*140)
    if len(strong_stocks) > 0:
        print(strong_stocks[display_cols].head(20).to_string(index=False))
    
    # 统计信息
    print("\n" + "="*140)
    print("统计信息")
    print("="*140)
    print(f"总股票数: {len(result)}")
    print(f"\nVWAP上方占比分布:")
    print(f"  - 100% (全天强势): {(result['above_vwap_ratio'] == 100).sum()} 只")
    print(f"  - >=80% (高度强势): {(result['above_vwap_ratio'] >= 80).sum()} 只")
    print(f"  - >=60% (中度强势): {(result['above_vwap_ratio'] >= 60).sum()} 只")
    print(f"  - >=40% (轻度强势): {(result['above_vwap_ratio'] >= 40).sum()} 只")
    print(f"  - <40% (弱势): {(result['above_vwap_ratio'] < 40).sum()} 只")
    print(f"\n平均强势占比: {result['above_vwap_ratio'].mean():.2f}%")
    print(f"平均成交量加权强势: {result['volume_weighted_strength'].mean():.2f}%")
    print(f"平均成交额加权强势: {result['amount_weighted_strength'].mean():.2f}%")
    print(f"最大连续VWAP上方区间数: {result['max_consecutive_above'].max()}")
    print(f"收盘在VWAP上方股票: {result['last_close_above_vwap'].sum()} 只")
    
    # 保存到CSV
    if args.output:
        result.to_csv(args.output, index=False)
        logger.info(f"结果已保存到: {args.output}")
    
    # 保存到数据库
    if args.save_db:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS hourly_vwap_strength (
            trade_date Date,
            symbol String,
            total_intervals Int32,
            above_vwap_count Int32,
            above_vwap_ratio Float32,
            volume_weighted_strength Float32,
            amount_weighted_strength Float32,
            avg_strength_ratio Float32,
            max_strength_ratio Float32,
            min_strength_ratio Float32,
            avg_vwap_deviation Float32,
            max_consecutive_above Int32,
            first_above_time Nullable(DateTime),
            last_close_above_vwap Int8,
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
                int(row['above_vwap_count']),
                float(row['above_vwap_ratio']),
                float(row['volume_weighted_strength']),
                float(row['amount_weighted_strength']),
                float(row['avg_strength_ratio']),
                float(row['max_strength_ratio']),
                float(row['min_strength_ratio']),
                float(row['avg_vwap_deviation']),
                int(row['max_consecutive_above']),
                row['first_above_time'],
                int(row['last_close_above_vwap']),
                float(row['composite_score'])
            ))
        
        insert_query = """
        INSERT INTO hourly_vwap_strength 
        (trade_date, symbol, total_intervals, above_vwap_count, above_vwap_ratio,
         volume_weighted_strength, amount_weighted_strength, avg_strength_ratio,
         max_strength_ratio, min_strength_ratio, avg_vwap_deviation,
         max_consecutive_above, first_above_time, last_close_above_vwap, composite_score)
        VALUES
        """
        client.execute(insert_query, data)
        logger.info(f"结果已保存到数据库表 hourly_vwap_strength")


if __name__ == '__main__':
    main()
