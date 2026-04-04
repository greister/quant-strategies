#!/usr/bin/env python3
"""
独立强度因子阈值对比分析工具
比较不同阈值设置下的信号质量和数量
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from clickhouse_driver import Client
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

# 阈值方案定义
THRESHOLD_SCHEMES = {
    'strict': {
        'name': '严格模式',
        'desc': '精选独立龙头',
        'sector': -0.5,
        'stock': 0.0,
        'excess': 1.0,
        'condition': 'sector<-0.5% AND (stock>0 OR excess>1%)'
    },
    'moderate': {
        'name': '中等模式',
        'desc': '平衡质量与数量',
        'sector': -0.3,
        'stock': -0.3,
        'excess': 0.5,
        'condition': 'sector<-0.3% AND (stock>-0.3% OR excess>0.5%)'
    },
    'relaxed': {
        'name': '宽松模式',
        'desc': '广泛捕捉信号',
        'sector': -0.1,
        'stock': 0.0,
        'excess': 0.3,
        'condition': 'sector<-0.1% AND (stock>0 OR excess>0.3%)'
    },
    'ultra': {
        'name': '极宽松模式',
        'desc': '最大化覆盖',
        'sector': 0.0,
        'stock': -0.5,
        'excess': 0.0,
        'condition': 'sector<0 AND stock>-0.5%'
    }
}


def get_clickhouse_client():
    """获取ClickHouse连接"""
    return Client(
        host=CH_HOST,
        port=CH_PORT,
        database=CH_DB,
        user=CH_USER,
        password=CH_PASSWORD
    )


def analyze_threshold_scheme(client, trade_date: str, scheme_key: str) -> dict:
    """分析单个阈值方案"""
    scheme = THRESHOLD_SCHEMES[scheme_key]
    
    query = """
    WITH
    stock_returns AS (
        SELECT
            symbol,
            datetime,
            close,
            lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as prev_close,
            (close - lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) / lagInFrame(close) OVER (PARTITION BY symbol, toDate(datetime) ORDER BY datetime ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) * 100 as stock_return
        FROM raw_stocks_5min
        WHERE toDate(datetime) = %(trade_date)s
    ),
    stock_with_sector AS (
        SELECT
            sr.symbol,
            sr.datetime,
            sr.stock_return,
            ss.sector_code
        FROM stock_returns sr
        INNER JOIN v_stock_sectors ss ON sr.symbol = ss.symbol
        WHERE sr.stock_return IS NOT NULL
          AND abs(sr.stock_return) < 50
    ),
    sector_returns AS (
        SELECT
            sector_code,
            datetime,
            avg(stock_return) as sector_return
        FROM stock_with_sector
        GROUP BY sector_code, datetime
    ),
    combined AS (
        SELECT 
            sws.symbol,
            sws.datetime,
            sws.stock_return,
            sr.sector_return,
            sws.stock_return - sr.sector_return as excess_return,
            CASE WHEN sr.sector_return < %(sector)s 
                 AND (sws.stock_return > %(stock)s OR sws.stock_return - sr.sector_return > %(excess)s) 
                 THEN 1 ELSE 0 END as is_contra
        FROM stock_with_sector sws
        INNER JOIN sector_returns sr ON sws.sector_code = sr.sector_code AND sws.datetime = sr.datetime
        WHERE abs(sr.sector_return) < 50
    ),
    scored_stocks AS (
        SELECT 
            symbol,
            sum(is_contra) as score,
            count() as total_intervals,
            avgIf(stock_return, is_contra=1) as avg_contra_return,
            maxIf(excess_return, is_contra=1) as max_excess_return
        FROM combined
        GROUP BY symbol
        HAVING sum(is_contra) > 0
    ),
    next_day_returns AS (
        SELECT 
            s.symbol,
            (d2.close - d2.open) / d2.open * 100 as next_day_return
        FROM scored_stocks s
        JOIN raw_stocks_daily d2 ON s.symbol = d2.symbol
        WHERE d2.date = %(next_date)s
    )
    SELECT 
        count(*) as stock_count,
        sum(score) as total_scores,
        round(avg(score), 2) as avg_score,
        round(avg(avg_contra_return), 4) as avg_contra_return,
        round(max(max_excess_return), 4) as max_excess_return,
        count(nd.symbol) as stocks_with_next_day,
        round(avg(nd.next_day_return), 4) as avg_next_day_return,
        round(sumIf(nd.next_day_return, nd.next_day_return > 0) / countIf(nd.next_day_return > 0), 4) as avg_positive_return,
        round(sumIf(nd.next_day_return, nd.next_day_return < 0) / countIf(nd.next_day_return < 0), 4) as avg_negative_return
    FROM scored_stocks s
    LEFT JOIN next_day_returns nd ON s.symbol = nd.symbol
    """
    
    next_date = (datetime.strptime(trade_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    
    result = client.execute(query, {
        'trade_date': trade_date,
        'next_date': next_date,
        'sector': scheme['sector'],
        'stock': scheme['stock'],
        'excess': scheme['excess']
    })
    
    if result:
        row = result[0]
        return {
            'scheme': scheme['name'],
            'condition': scheme['condition'],
            'description': scheme['desc'],
            'stock_count': row[0] or 0,
            'total_scores': row[1] or 0,
            'avg_score': row[2] or 0,
            'avg_contra_return': row[3] or 0,
            'max_excess_return': row[4] or 0,
            'next_day_coverage': row[5] or 0,
            'avg_next_day_return': row[6] or 0,
            'avg_positive_return': row[7] or 0,
            'avg_negative_return': row[8] or 0
        }
    return {}


def print_comparison(results: list, trade_date: str):
    """打印对比结果"""
    print("\n" + "="*100)
    print(f"独立强度因子阈值方案对比 - {trade_date}")
    print("="*100)
    
    print(f"\n{'方案':<12} {'条件':<45} {'股票数':>8} {'总分':>8} {'均分':>8} {'次日收益':>10}")
    print("-"*100)
    
    for r in results:
        print(f"{r['scheme']:<12} {r['condition']:<45} {r['stock_count']:>8} {r['total_scores']:>8} {r['avg_score']:>8.2f} {r['avg_next_day_return']:>9.2f}%")
    
    print("\n" + "="*100)
    print("详细分析")
    print("="*100)
    
    for r in results:
        print(f"\n【{r['scheme']}】- {r['description']}")
        print(f"  条件: {r['condition']}")
        print(f"  覆盖股票: {r['stock_count']:,} 只")
        print(f"  总分值: {r['total_scores']:,}")
        print(f"  平均分数: {r['avg_score']:.2f}")
        print(f"  平均逆势收益: {r['avg_contra_return']:.4f}%")
        print(f"  最大超额收益: {r['max_excess_return']:.4f}%")
        if r['next_day_coverage'] > 0:
            print(f"  次日平均收益: {r['avg_next_day_return']:.2f}% (基于{r['next_day_coverage']}只股票)")
    
    print("\n" + "="*100)
    print("建议")
    print("="*100)
    
    # 找到最平衡的方案
    best_coverage = max(results, key=lambda x: x['stock_count'])
    best_return = max(results, key=lambda x: x['avg_next_day_return'] or -999)
    
    print(f"""
基于 {trade_date} 数据分析:

1. 最广泛覆盖: 【{best_coverage['scheme']}】- {best_coverage['stock_count']:,} 只股票
   适合: 量化策略需要充足样本量

2. 最佳次日收益: 【{best_return['scheme']}】- {best_return['avg_next_day_return']:.2f}%
   适合: 追求选股质量而非数量

3. 推荐方案:
   - 大盘下跌日: 使用 严格/中等 模式 (板块跌幅要求较高时仍能选出独立股)
   - 震荡市: 使用 中等/宽松 模式
   - 策略回测: 先用 中等 模式测试，根据结果微调
""")


def main():
    parser = argparse.ArgumentParser(description='独立强度因子阈值对比分析')
    parser.add_argument('--date', type=str, help='分析日期 (YYYY-MM-DD)，默认昨天')
    parser.add_argument('--schemes', nargs='+', choices=list(THRESHOLD_SCHEMES.keys()), 
                       default=list(THRESHOLD_SCHEMES.keys()),
                       help='要对比的方案')
    
    args = parser.parse_args()
    
    if args.date:
        trade_date = args.date
    else:
        trade_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    client = get_clickhouse_client()
    results = []
    
    for scheme_key in args.schemes:
        logger.info(f"分析方案: {THRESHOLD_SCHEMES[scheme_key]['name']}")
        result = analyze_threshold_scheme(client, trade_date, scheme_key)
        if result:
            results.append(result)
    
    print_comparison(results, trade_date)
    
    # 保存结果到文件
    output_dir = '/tmp/strategy-output'
    os.makedirs(output_dir, exist_ok=True)
    
    from datetime import datetime as dt
    report_file = f"{output_dir}/{dt.now().strftime('%Y-%m-%d')}_阈值方案对比报告.md"
    
    with open(report_file, 'w') as f:
        f.write(f"# 独立强度因子阈值方案对比报告\n\n")
        f.write(f"**分析日期**: {trade_date}\n")
        f.write(f"**生成时间**: {dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## 方案对比\n\n")
        f.write("| 方案 | 条件 | 股票数 | 总分 | 均分 | 次日收益 |\n")
        f.write("|------|------|--------|------|------|----------|\n")
        for r in results:
            f.write(f"| {r['scheme']} | {r['condition']} | {r['stock_count']:,} | {r['total_scores']:,} | {r['avg_score']:.2f} | {r['avg_next_day_return']:.2f}% |\n")
        
        f.write("\n## 详细分析\n\n")
        for r in results:
            f.write(f"### {r['scheme']}\n")
            f.write(f"- **条件**: {r['condition']}\n")
            f.write(f"- **覆盖股票**: {r['stock_count']:,} 只\n")
            f.write(f"- **总分值**: {r['total_scores']:,}\n")
            f.write(f"- **平均分数**: {r['avg_score']:.2f}\n")
            f.write(f"- **平均逆势收益**: {r['avg_contra_return']:.4f}%\n")
            f.write(f"- **最大超额收益**: {r['max_excess_return']:.4f}%\n")
            if r['next_day_coverage'] > 0:
                f.write(f"- **次日平均收益**: {r['avg_next_day_return']:.2f}%\n")
            f.write("\n")
    
    logger.info(f"报告已保存: {report_file}")


if __name__ == '__main__':
    main()
