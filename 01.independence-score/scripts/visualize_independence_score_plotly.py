#!/usr/bin/env python3
"""
独立强度因子可视化脚本 (Plotly 版本)
支持交互式图表和中文字体
"""

import os
import sys
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from clickhouse_driver import Client
from datetime import datetime

# 导入中文字体配置
from plotly_config import (
    setup_chinese_font, 
    get_chinese_layout, 
    save_chinese_figure,
    get_color_scheme
)

# 设置中文字体
setup_chinese_font('microsoft_yahei')


def connect_clickhouse():
    """连接ClickHouse"""
    return Client(
        host=os.getenv('CH_HOST', 'localhost'),
        port=int(os.getenv('CH_PORT', '9000')),
        database=os.getenv('CH_DB', 'tdx2db_rust'),
        user=os.getenv('CH_USER', 'default'),
        password=os.getenv('CH_PASSWORD', ''),
    )


def plot_weight_distribution_plotly(output_path='/tmp/visualizations/weight_distribution_plotly.html'):
    """
    绘制时间权重分布图 (Plotly 交互式)
    """
    client = connect_clickhouse()
    
    # 获取权重配置
    result = client.execute("""
        SELECT DISTINCT config_name, weights
        FROM score_weight_configs
        WHERE config_name IN ('evening_focus', 'morning_focus', 'conservative')
    """)
    
    # 创建子图
    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=('尾盘关注型 (Evening Focus)', '早盘关注型 (Morning Focus)', '保守型 (Conservative)'),
        shared_yaxes=True
    )
    
    colors = {
        'evening_focus': '#FF6B6B',
        'morning_focus': '#4ECDC4',
        'conservative': '#95E1D3'
    }
    
    # 时间标签
    time_labels = []
    for i in range(48):
        if i < 24:
            hour = 9 + i // 12
            minute = (i % 12) * 5 if i >= 6 else 30 + (i % 6) * 5
            if i < 6:
                hour = 9
                minute = 30 + i * 5
            elif i < 18:
                hour = 10
                minute = ((i - 6) % 12) * 5
            else:
                hour = 11
                minute = ((i - 18) % 6) * 5
        else:
            j = i - 24
            hour = 13 + j // 12
            minute = (j % 12) * 5
        time_labels.append(f"{hour:02d}:{minute:02d}")
    
    seen_configs = set()
    col = 1
    
    for config_name, weights in result:
        if config_name in seen_configs:
            continue
        seen_configs.add(config_name)
        
        if col > 3:
            break
        
        color = colors.get(config_name, '#333')
        
        # 添加柱状图
        fig.add_trace(
            go.Bar(
                x=list(range(48)),
                y=weights,
                marker_color=color,
                name=config_name,
                hovertemplate='时间: %{text}<br>权重: %{y:.4f}<extra></extra>',
                text=time_labels,
                showlegend=False
            ),
            row=1, col=col
        )
        
        # 添加午休分割线
        fig.add_vline(x=23.5, line_dash="dash", line_color="red", 
                      annotation_text="午休", row=1, col=col)
        
        # 添加统计信息
        avg_w = sum(weights) / len(weights)
        fig.add_annotation(
            x=0.5, y=max(weights) * 1.15,
            text=f"最大: {max(weights):.3f}<br>最小: {min(weights):.3f}<br>平均: {avg_w:.3f}",
            showarrow=False,
            font=dict(size=10),
            bgcolor='rgba(255,255,255,0.8)',
            row=1, col=col
        )
        
        col += 1
    
    # 更新布局
    fig.update_layout(
        title=dict(
            text='时间权重分布对比 - 三种配置方案',
            font=dict(size=20),
            x=0.5
        ),
        height=500,
        width=1500,
        template='plotly_white'
    )
    
    # 更新所有子图的X轴
    for i in range(1, 4):
        fig.update_xaxes(title_text="时间区间 (0-47)", row=1, col=i)
    
    fig.update_yaxes(title_text="权重", row=1, col=1)
    
    # 保存
    save_chinese_figure(fig, output_path, width=1500, height=500)
    return output_path


def plot_score_distribution_plotly(date='2026-03-20', output_path='/tmp/visualizations/score_distribution_plotly.html'):
    """
    绘制得分分布图 (Plotly 交互式)
    """
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
    
    # 分类颜色
    colors = []
    for s in scores:
        if s >= 4:
            colors.append('#FF6B6B')  # 高分-红色
        elif s >= 2:
            colors.append('#4ECDC4')  # 中等-青色
        else:
            colors.append('#95E1D3')  # 低分-浅绿
    
    # 创建子图
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{'type': 'bar'}, {'type': 'pie'}]],
        subplot_titles=(f'得分分布 - {date}', '得分分类占比')
    )
    
    # 柱状图
    fig.add_trace(
        go.Bar(
            x=[f"{s}分" for s in scores],
            y=counts,
            marker_color=colors,
            text=counts,
            textposition='outside',
            hovertemplate='得分: %{x}<br>股票数: %{y}<extra></extra>',
            showlegend=False
        ),
        row=1, col=1
    )
    
    # 饼图数据
    high = sum([c for s, c in result if s >= 4])
    medium = sum([c for s, c in result if 2 <= s < 4])
    low = sum([c for s, c in result if s < 2])
    
    fig.add_trace(
        go.Pie(
            labels=['高分 (≥4)', '中等 (2-3)', '低分 (<2)'],
            values=[high, medium, low],
            marker_colors=['#FF6B6B', '#4ECDC4', '#95E1D3'],
            textinfo='label+percent',
            hovertemplate='%{label}<br>数量: %{value}<br>占比: %{percent}<extra></extra>',
            showlegend=True
        ),
        row=1, col=2
    )
    
    # 更新布局
    fig.update_layout(
        title=dict(
            text=f'独立强度得分分布分析 - {date}',
            font=dict(size=20),
            x=0.5
        ),
        height=600,
        width=1200,
        template='plotly_white',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=-0.2,
            xanchor='center',
            x=0.75
        )
    )
    
    fig.update_xaxes(title_text="独立强度得分", row=1, col=1)
    fig.update_yaxes(title_text="股票数量", row=1, col=1)
    
    save_chinese_figure(fig, output_path, width=1200, height=600)
    return output_path


def plot_sector_comparison_plotly(date='2026-03-20', output_path='/tmp/visualizations/sector_comparison_plotly.html'):
    """
    绘制行业对比图 (Plotly 交互式)
    """
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
    avg_scores = [round(r[2], 2) for r in result]
    
    # 创建子图
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{'type': 'bar'}, {'type': 'bar'}]],
        subplot_titles=('行业平均得分', '行业入选数量')
    )
    
    colors = px.colors.sequential.Viridis[:len(sectors)]
    
    # 平均得分柱状图（水平）
    fig.add_trace(
        go.Bar(
            y=sectors,
            x=avg_scores,
            orientation='h',
            marker_color=colors,
            text=avg_scores,
            textposition='outside',
            hovertemplate='行业: %{y}<br>平均得分: %{x:.2f}<extra></extra>',
            showlegend=False
        ),
        row=1, col=1
    )
    
    # 入选数量柱状图（水平）
    fig.add_trace(
        go.Bar(
            y=sectors,
            x=counts,
            orientation='h',
            marker_color=colors,
            text=counts,
            textposition='outside',
            hovertemplate='行业: %{y}<br>入选数量: %{x}<extra></extra>',
            showlegend=False
        ),
        row=1, col=2
    )
    
    fig.update_layout(
        title=dict(
            text=f'行业独立强度对比 - {date}',
            font=dict(size=20),
            x=0.5
        ),
        height=600,
        width=1400,
        template='plotly_white'
    )
    
    fig.update_xaxes(title_text="平均得分", row=1, col=1)
    fig.update_xaxes(title_text="股票数量", row=1, col=2)
    
    save_chinese_figure(fig, output_path, width=1400, height=600)
    return output_path


def plot_backtest_performance_plotly(output_path='/tmp/visualizations/backtest_performance_plotly.html'):
    """
    绘制回测收益图 (Plotly 交互式)
    """
    # 回测数据
    stocks_0320 = ['弘景光电', 'ST长方', '钜泉科技', '共达电声', '秋田微', 
                   '大为股份', '先导基电', '广东明珠', '豪鹏科技', '中电港']
    returns_0320 = [0.00, 2.16, -3.06, 2.15, 13.87, 0.86, 0.21, -3.39, 6.01, 0.64]
    
    stocks_0324 = ['海星股份', '美信科技', '新宏泰', '*ST天喻', '长飞光纤',
                   '裕太微', '顺发恒能', '赛微微电', '燕东微', '捷邦科技']
    returns_0324 = [3.18, 3.39, -2.27, 1.72, 10.00, 1.96, 3.48, -2.58, -1.66, 2.59]
    
    # 创建子图
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('2026-03-20 信号 (T+3收益)', '2026-03-24 信号 (T+1收益)')
    )
    
    # 颜色
    colors_0320 = ['#FF6B6B' if r > 0 else '#4ECDC4' for r in returns_0320]
    colors_0324 = ['#FF6B6B' if r > 0 else '#4ECDC4' for r in returns_0324]
    
    # 2026-03-20 数据
    fig.add_trace(
        go.Bar(
            x=stocks_0320,
            y=returns_0320,
            marker_color=colors_0320,
            text=[f'{r:+.1f}%' for r in returns_0320],
            textposition='outside',
            hovertemplate='股票: %{x}<br>收益: %{y:.2f}%<extra></extra>',
            showlegend=False
        ),
        row=1, col=1
    )
    
    # 2026-03-24 数据
    fig.add_trace(
        go.Bar(
            x=stocks_0324,
            y=returns_0324,
            marker_color=colors_0324,
            text=[f'{r:+.1f}%' for r in returns_0324],
            textposition='outside',
            hovertemplate='股票: %{x}<br>收益: %{y:.2f}%<extra></extra>',
            showlegend=False
        ),
        row=1, col=2
    )
    
    # 添加平均线
    avg_0320 = sum(returns_0320) / len(returns_0320)
    avg_0324 = sum(returns_0324) / len(returns_0324)
    
    fig.add_hline(y=avg_0320, line_dash="dash", line_color="green",
                  annotation_text=f"平均: {avg_0320:+.2f}%", row=1, col=1)
    fig.add_hline(y=avg_0324, line_dash="dash", line_color="green",
                  annotation_text=f"平均: {avg_0324:+.2f}%", row=1, col=2)
    
    # 零线
    fig.add_hline(y=0, line_color="black", line_width=1, row=1, col=1)
    fig.add_hline(y=0, line_color="black", line_width=1, row=1, col=2)
    
    fig.update_layout(
        title=dict(
            text='回测收益表现对比',
            font=dict(size=20),
            x=0.5
        ),
        height=600,
        width=1600,
        template='plotly_white'
    )
    
    fig.update_xaxes(tickangle=45, row=1, col=1)
    fig.update_xaxes(tickangle=45, row=1, col=2)
    fig.update_yaxes(title_text="收益率 (%)", row=1, col=1)
    
    save_chinese_figure(fig, output_path, width=1600, height=600)
    return output_path


def plot_score_return_corr_plotly(output_path='/tmp/visualizations/score_return_corr_plotly.html'):
    """
    绘制分数-收益相关性图 (Plotly 交互式)
    """
    # 合并数据
    scores = [6, 6, 4, 4, 4, 4, 4, 4, 4, 4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2]
    returns = [0.00, 2.16, -3.06, 2.15, 13.87, 0.86, 0.21, -3.39, 6.01, 0.64,
               3.18, 3.39, -2.27, 1.72, 10.00, 1.96, 3.48, -2.58, -1.66, 2.59]
    names = ['弘景光电', 'ST长方', '钜泉科技', '共达电声', '秋田微', 
             '大为股份', '先导基电', '广东明珠', '豪鹏科技', '中电港',
             '海星股份', '美信科技', '新宏泰', '*ST天喻', '长飞光纤',
             '裕太微', '顺发恒能', '赛微微电', '燕东微', '捷邦科技']
    
    # 按分数计算平均收益
    score_groups = {}
    for s, r in zip(scores, returns):
        if s not in score_groups:
            score_groups[s] = []
        score_groups[s].append(r)
    
    avg_by_score = {s: sum(rs)/len(rs) for s, rs in score_groups.items()}
    
    fig = go.Figure()
    
    # 散点图
    colors = ['#FF6B6B' if s >= 4 else '#4ECDC4' for s in scores]
    fig.add_trace(
        go.Scatter(
            x=scores,
            y=returns,
            mode='markers+text',
            text=names,
            textposition='top center',
            marker=dict(
                size=12,
                color=colors,
                line=dict(width=2, color='DarkSlateGrey')
            ),
            hovertemplate='股票: %{text}<br>分数: %{x}<br>收益: %{y:.2f}%<extra></extra>',
            name='个股收益'
        )
    )
    
    # 平均收益线
    sorted_scores = sorted(avg_by_score.keys())
    sorted_avgs = [avg_by_score[s] for s in sorted_scores]
    
    fig.add_trace(
        go.Scatter(
            x=sorted_scores,
            y=sorted_avgs,
            mode='lines+markers',
            line=dict(color='green', width=3, dash='dash'),
            marker=dict(size=10, symbol='diamond'),
            name='平均收益'
        )
    )
    
    # 零线
    fig.add_hline(y=0, line_color="black", line_width=1)
    
    # 添加注释
    fig.add_annotation(
        x=4.5, y=12,
        text='4-5分区间收益最佳',
        showarrow=True,
        arrowhead=2,
        arrowcolor='red',
        font=dict(size=12, color='red')
    )
    
    fig.update_layout(
        title=dict(
            text='独立强度分数与收益相关性分析',
            font=dict(size=20),
            x=0.5
        ),
        xaxis=dict(
            title='独立强度分数',
            tickmode='linear',
            dtick=1
        ),
        yaxis=dict(title='收益率 (%)'),
        height=700,
        width=1000,
        template='plotly_white',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        )
    )
    
    save_chinese_figure(fig, output_path, width=1000, height=700)
    return output_path


def generate_all_plots_plotly(output_dir='/tmp/visualizations'):
    """生成所有 Plotly 图表"""
    print("=" * 60)
    print("Generating Independence Score Visualizations (Plotly)")
    print("=" * 60)
    
    os.makedirs(output_dir, exist_ok=True)
    plots = []
    
    print("\n[1/5] Generating weight distribution...")
    plots.append(plot_weight_distribution_plotly(f'{output_dir}/weight_distribution_plotly.html'))
    
    print("\n[2/5] Generating score distribution...")
    plots.append(plot_score_distribution_plotly('2026-03-20', f'{output_dir}/score_distribution_plotly.html'))
    
    print("\n[3/5] Generating sector comparison...")
    plots.append(plot_sector_comparison_plotly('2026-03-20', f'{output_dir}/sector_comparison_plotly.html'))
    
    print("\n[4/5] Generating backtest performance...")
    plots.append(plot_backtest_performance_plotly(f'{output_dir}/backtest_performance_plotly.html'))
    
    print("\n[5/5] Generating score-return correlation...")
    plots.append(plot_score_return_corr_plotly(f'{output_dir}/score_return_corr_plotly.html'))
    
    print("\n" + "=" * 60)
    print("All Plotly visualizations generated successfully!")
    print("=" * 60)
    for plot in plots:
        print(f"  - {plot}")
    
    return plots


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='独立强度因子可视化 (Plotly版本)')
    parser.add_argument('--output-dir', default='/tmp/visualizations', help='输出目录')
    parser.add_argument('--type', choices=['weight', 'score', 'sector', 'backtest', 'corr', 'all'],
                        default='all', help='图表类型')
    parser.add_argument('--date', default='2026-03-20', help='数据日期')
    parser.add_argument('--font', default='microsoft_yahei', help='中文字体')
    
    args = parser.parse_args()
    
    # 设置字体
    setup_chinese_font(args.font)
    
    if args.type == 'all':
        generate_all_plots_plotly(args.output_dir)
    elif args.type == 'weight':
        plot_weight_distribution_plotly(f'{args.output_dir}/weight_distribution_plotly.html')
    elif args.type == 'score':
        plot_score_distribution_plotly(args.date, f'{args.output_dir}/score_distribution_plotly.html')
    elif args.type == 'sector':
        plot_sector_comparison_plotly(args.date, f'{args.output_dir}/sector_comparison_plotly.html')
    elif args.type == 'backtest':
        plot_backtest_performance_plotly(f'{args.output_dir}/backtest_performance_plotly.html')
    elif args.type == 'corr':
        plot_score_return_corr_plotly(f'{args.output_dir}/score_return_corr_plotly.html')
