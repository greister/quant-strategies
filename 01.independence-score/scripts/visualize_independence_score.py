#!/usr/bin/env python3
"""
独立强度因子可视化脚本
生成权重分布图、得分分布图、回测收益图等
"""

import os
import sys
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # 无GUI环境
from clickhouse_driver import Client
from datetime import datetime

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False


def connect_clickhouse():
    """连接ClickHouse"""
    return Client(
        host=os.getenv('CH_HOST', 'localhost'),
        port=int(os.getenv('CH_PORT', '9000')),
        database=os.getenv('CH_DB', 'tdx2db_rust'),
        user=os.getenv('CH_USER', 'default'),
        password=os.getenv('CH_PASSWORD', ''),
    )


def plot_weight_distribution(output_path='/tmp/weight_distribution.png'):
    """绘制权重分布图"""
    client = connect_clickhouse()
    
    # 获取权重配置
    result = client.execute("""
        SELECT config_name, weights
        FROM score_weight_configs
        WHERE config_name IN ('evening_focus', 'morning_focus', 'conservative')
    """)
    
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))
    
    config_names = {
        'evening_focus': 'Evening Focus\n(尾盘关注型)',
        'morning_focus': 'Morning Focus\n(早盘关注型)',
        'conservative': 'Conservative\n(保守型)'
    }
    
    colors = {
        'evening_focus': '#FF6B6B',
        'morning_focus': '#4ECDC4',
        'conservative': '#95E1D3'
    }
    
    # 去重配置
    seen_configs = set()
    unique_results = []
    for config_name, weights in result:
        if config_name not in seen_configs:
            seen_configs.add(config_name)
            unique_results.append((config_name, weights))
    
    for idx, (config_name, weights) in enumerate(unique_results[:3]):
        ax = axes[idx]
        
        # 绘制柱状图
        x = list(range(48))
        bars = ax.bar(x, weights, color=colors.get(config_name, '#333'), alpha=0.7)
        
        # 添加分割线
        ax.axvline(x=23.5, color='red', linestyle='--', linewidth=2, label='午休')
        
        # 标注早盘/午盘/尾盘
        ax.axvspan(0, 12, alpha=0.1, color='yellow', label='Morning')
        ax.axvspan(24, 36, alpha=0.1, color='orange', label='Afternoon')
        ax.axvspan(36, 48, alpha=0.1, color='red', label='Evening')
        
        ax.set_title(config_names.get(config_name, config_name), fontsize=12, fontweight='bold')
        ax.set_xlabel('Time Interval (0-47)', fontsize=10)
        ax.set_ylabel('Weight', fontsize=10)
        ax.set_ylim(0, max(weights) * 1.2)
        
        # 添加统计信息
        avg_weight = sum(weights) / len(weights)
        ax.text(0.02, 0.98, f'Max: {max(weights):.3f}\nMin: {min(weights):.3f}\nAvg: {avg_weight:.3f}',
                transform=ax.transAxes, fontsize=9, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Weight distribution saved to: {output_path}")
    return output_path


def plot_score_distribution(date='2026-03-20', output_path='/tmp/score_distribution.png'):
    """绘制得分分布图"""
    client = connect_clickhouse()
    
    # 获取得分数据
    result = client.execute(f"""
        SELECT raw_score, COUNT() as count
        FROM independence_score_time_weighted
        WHERE date = '{date}' AND config_name = 'evening_focus'
        GROUP BY raw_score
        ORDER BY raw_score
    """)
    
    scores = [r[0] for r in result]
    counts = [r[1] for r in result]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # 左图：柱状图
    colors = ['#FF6B6B' if s >= 4 else '#4ECDC4' if s >= 2 else '#95E1D3' for s in scores]
    bars = ax1.bar(scores, counts, color=colors, alpha=0.7, edgecolor='black')
    
    # 在柱子上添加数值
    for bar, count in zip(bars, counts):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(count)}',
                ha='center', va='bottom', fontsize=9)
    
    ax1.set_xlabel('Independence Score', fontsize=12)
    ax1.set_ylabel('Number of Stocks', fontsize=12)
    ax1.set_title(f'Score Distribution - {date}', fontsize=14, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)
    
    # 添加图例
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#FF6B6B', label='High Score (≥4)'),
        Patch(facecolor='#4ECDC4', label='Medium Score (2-3)'),
        Patch(facecolor='#95E1D3', label='Low Score (<2)')
    ]
    ax1.legend(handles=legend_elements, loc='upper right')
    
    # 右图：饼图
    high = sum([c for s, c in result if s >= 4])
    medium = sum([c for s, c in result if 2 <= s < 4])
    low = sum([c for s, c in result if s < 2])
    
    sizes = [high, medium, low]
    labels = [f'High\n({high})', f'Medium\n({medium})', f'Low\n({low})']
    colors_pie = ['#FF6B6B', '#4ECDC4', '#95E1D3']
    explode = (0.1, 0, 0)
    
    ax2.pie(sizes, explode=explode, labels=labels, colors=colors_pie,
            autopct='%1.1f%%', shadow=True, startangle=90)
    ax2.set_title(f'Score Categories - {date}', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Score distribution saved to: {output_path}")
    return output_path


def plot_sector_comparison(date='2026-03-20', output_path='/tmp/sector_comparison.png'):
    """绘制行业对比图"""
    client = connect_clickhouse()
    
    # 获取行业数据
    result = client.execute(f"""
        SELECT 
            sector,
            COUNT() as count,
            AVG(raw_score) as avg_score,
            MAX(raw_score) as max_score
        FROM independence_score_time_weighted
        WHERE date = '{date}' AND config_name = 'evening_focus'
        GROUP BY sector
        HAVING count >= 5
        ORDER BY avg_score DESC
        LIMIT 10
    """)
    
    sectors = [r[0] for r in result]
    counts = [r[1] for r in result]
    avg_scores = [r[2] for r in result]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # 左图：行业平均得分
    colors = plt.cm.viridis([i/len(sectors) for i in range(len(sectors))])
    bars1 = ax1.barh(sectors, avg_scores, color=colors, alpha=0.8)
    ax1.set_xlabel('Average Independence Score', fontsize=12)
    ax1.set_title(f'Sector Average Score - {date}', fontsize=14, fontweight='bold')
    ax1.grid(axis='x', alpha=0.3)
    
    # 在柱子上添加数值
    for bar, score in zip(bars1, avg_scores):
        width = bar.get_width()
        ax1.text(width, bar.get_y() + bar.get_height()/2.,
                f'{score:.2f}',
                ha='left', va='center', fontsize=9)
    
    # 右图：行业入选数量
    bars2 = ax2.barh(sectors, counts, color=colors, alpha=0.8)
    ax2.set_xlabel('Number of Stocks', fontsize=12)
    ax2.set_title(f'Sector Stock Count - {date}', fontsize=14, fontweight='bold')
    ax2.grid(axis='x', alpha=0.3)
    
    # 在柱子上添加数值
    for bar, count in zip(bars2, counts):
        width = bar.get_width()
        ax2.text(width, bar.get_y() + bar.get_height()/2.,
                f'{int(count)}',
                ha='left', va='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Sector comparison saved to: {output_path}")
    return output_path


def plot_backtest_performance(output_path='/tmp/backtest_performance.png'):
    """绘制回测收益图"""
    
    # 回测数据 (来自回测报告)
    stocks_0320 = ['弘景光电', 'ST长方', '钜泉科技', '共达电声', '秋田微', 
                   '大为股份', '先导基电', '广东明珠', '豪鹏科技', '中电港']
    returns_0320 = [0.00, 2.16, -3.06, 2.15, 13.87, 0.86, 0.21, -3.39, 6.01, 0.64]
    
    stocks_0324 = ['海星股份', '美信科技', '新宏泰', '*ST天喻', '长飞光纤',
                   '裕太微', '顺发恒能', '赛微微电', '燕东微', '捷邦科技']
    returns_0324 = [3.18, 3.39, -2.27, 1.72, 10.00, 1.96, 3.48, -2.58, -1.66, 2.59]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # 2026-03-20 收益图
    colors1 = ['#FF6B6B' if r > 0 else '#4ECDC4' for r in returns_0320]
    bars1 = ax1.bar(range(len(stocks_0320)), returns_0320, color=colors1, alpha=0.7, edgecolor='black')
    ax1.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax1.axhline(y=1.95, color='green', linestyle='--', linewidth=2, label=f'Avg: +1.95%')
    ax1.set_xticks(range(len(stocks_0320)))
    ax1.set_xticklabels(stocks_0320, rotation=45, ha='right', fontsize=9)
    ax1.set_ylabel('Return (%)', fontsize=12)
    ax1.set_title('2026-03-20 Signals (T+3 Return)', fontsize=14, fontweight='bold')
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)
    
    # 添加数值标签
    for bar, ret in zip(bars1, returns_0320):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{ret:+.1f}%',
                ha='center', va='bottom' if height > 0 else 'top',
                fontsize=8)
    
    # 2026-03-24 收益图
    colors2 = ['#FF6B6B' if r > 0 else '#4ECDC4' for r in returns_0324]
    bars2 = ax2.bar(range(len(stocks_0324)), returns_0324, color=colors2, alpha=0.7, edgecolor='black')
    ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax2.axhline(y=2.08, color='green', linestyle='--', linewidth=2, label=f'Avg: +2.08%')
    ax2.set_xticks(range(len(stocks_0324)))
    ax2.set_xticklabels(stocks_0324, rotation=45, ha='right', fontsize=9)
    ax2.set_ylabel('Return (%)', fontsize=12)
    ax2.set_title('2026-03-24 Signals (T+1 Return)', fontsize=14, fontweight='bold')
    ax2.legend()
    ax2.grid(axis='y', alpha=0.3)
    
    # 添加数值标签
    for bar, ret in zip(bars2, returns_0324):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{ret:+.1f}%',
                ha='center', va='bottom' if height > 0 else 'top',
                fontsize=8)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Backtest performance saved to: {output_path}")
    return output_path


def plot_score_return_correlation(output_path='/tmp/score_return_corr.png'):
    """绘制分数与收益相关性图"""
    
    # 合并回测数据
    scores = [6, 6, 4, 4, 4, 4, 4, 4, 4, 4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2]
    returns = [0.00, 2.16, -3.06, 2.15, 13.87, 0.86, 0.21, -3.39, 6.01, 0.64,
               3.18, 3.39, -2.27, 1.72, 10.00, 1.96, 3.48, -2.58, -1.66, 2.59]
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # 散点图
    colors = ['#FF6B6B' if s >= 4 else '#4ECDC4' for s in scores]
    ax.scatter(scores, returns, c=colors, s=100, alpha=0.6, edgecolors='black')
    
    # 添加趋势线
    z = [sum([r for s, r in zip(scores, returns) if s == score]) / 
         sum([1 for s in scores if s == score])
         for score in sorted(set(scores))]
    x_unique = sorted(set(scores))
    ax.plot(x_unique, z, 'g--', linewidth=2, label='Average Return')
    
    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax.set_xlabel('Independence Score', fontsize=12)
    ax.set_ylabel('Return (%)', fontsize=12)
    ax.set_title('Score vs Return Correlation', fontsize=14, fontweight='bold')
    ax.grid(alpha=0.3)
    ax.legend()
    
    # 添加统计信息
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#FF6B6B', label='High Score (≥4)'),
        Patch(facecolor='#4ECDC4', label='Medium Score (2-3)')
    ]
    ax.legend(handles=legend_elements, loc='upper left')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Score-return correlation saved to: {output_path}")
    return output_path


def generate_all_plots(output_dir='/tmp'):
    """生成所有图表"""
    print("=" * 60)
    print("Generating Independence Score Visualizations")
    print("=" * 60)
    
    plots = []
    
    # 1. 权重分布图
    print("\n[1/5] Generating weight distribution...")
    plots.append(plot_weight_distribution(f'{output_dir}/weight_distribution.png'))
    
    # 2. 得分分布图
    print("\n[2/5] Generating score distribution...")
    plots.append(plot_score_distribution('2026-03-20', f'{output_dir}/score_distribution.png'))
    
    # 3. 行业对比图
    print("\n[3/5] Generating sector comparison...")
    plots.append(plot_sector_comparison('2026-03-20', f'{output_dir}/sector_comparison.png'))
    
    # 4. 回测收益图
    print("\n[4/5] Generating backtest performance...")
    plots.append(plot_backtest_performance(f'{output_dir}/backtest_performance.png'))
    
    # 5. 分数收益相关性图
    print("\n[5/5] Generating score-return correlation...")
    plots.append(plot_score_return_correlation(f'{output_dir}/score_return_corr.png'))
    
    print("\n" + "=" * 60)
    print("All visualizations generated successfully!")
    print("=" * 60)
    for plot in plots:
        print(f"  - {plot}")
    
    return plots


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='独立强度因子可视化')
    parser.add_argument('--output-dir', default='/tmp', help='输出目录')
    parser.add_argument('--type', choices=['weight', 'score', 'sector', 'backtest', 'corr', 'all'],
                        default='all', help='图表类型')
    parser.add_argument('--date', default='2026-03-20', help='数据日期')
    
    args = parser.parse_args()
    
    if args.type == 'all':
        generate_all_plots(args.output_dir)
    elif args.type == 'weight':
        plot_weight_distribution(f'{args.output_dir}/weight_distribution.png')
    elif args.type == 'score':
        plot_score_distribution(args.date, f'{args.output_dir}/score_distribution.png')
    elif args.type == 'sector':
        plot_sector_comparison(args.date, f'{args.output_dir}/sector_comparison.png')
    elif args.type == 'backtest':
        plot_backtest_performance(f'{args.output_dir}/backtest_performance.png')
    elif args.type == 'corr':
        plot_score_return_correlation(f'{args.output_dir}/score_return_corr.png')
