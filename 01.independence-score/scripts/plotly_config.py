#!/usr/bin/env python3
"""
Plotly 中文字体配置模块
用于设置 plotly 图表的中文字体支持
"""

import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

# 中文字体配置
CHINESE_FONTS = {
    'microsoft_yahei': 'Microsoft YaHei,微软雅黑',  # 微软雅黑（推荐）
    'simsun': 'SimSun,宋体',  # 宋体
    'kaiti': 'KaiTi,楷体',  # 楷体
    'fangsong': 'FangSong,仿宋',  # 仿宋
    'noto_sans_sc': 'Noto Sans SC',  # Noto Sans SC
    'noto_serif_sc': 'Noto Serif SC',  # Noto Serif SC
    'dengxian': 'DengXian,等线',  # 等线
    'harmonyos': 'HarmonyOS Sans SC',  # 鸿蒙字体
}

# 默认字体（优先使用系统常见字体）
DEFAULT_CHINESE_FONT = 'Microsoft YaHei,微软雅黑,Arial,sans-serif'

# 字体路径（Linux/Windows）
FONT_PATHS = [
    '/mnt/c/Windows/Fonts/msyh.ttc',  # 微软雅黑（WSL）
    '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc',  # 文泉驿微米黑
    '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',  # Noto CJK
]


def setup_chinese_font(font_name='microsoft_yahei'):
    """
    设置 Plotly 中文字体
    
    Args:
        font_name: 字体名称，可选 microsoft_yahei, simsun, kaiti, noto_sans_sc 等
    
    Returns:
        dict: 字体配置字典
    """
    font = CHINESE_FONTS.get(font_name, DEFAULT_CHINESE_FONT)
    
    # 创建模板配置
    template = go.layout.Template()
    
    # 设置全局字体
    template.layout.font = dict(
        family=font,
        size=14,
        color='#333333'
    )
    
    # 设置标题字体
    template.layout.title.font = dict(
        family=font,
        size=20,
        color='#1a1a1a'
    )
    
    # 设置坐标轴字体
    template.layout.xaxis.title.font = dict(family=font, size=14)
    template.layout.yaxis.title.font = dict(family=font, size=14)
    template.layout.xaxis.tickfont = dict(family=font, size=12)
    template.layout.yaxis.tickfont = dict(family=font, size=12)
    
    # 设置图例字体
    template.layout.legend.font = dict(family=font, size=12)
    
    # 设置注释字体
    template.layout.annotationdefaults.font = dict(family=font, size=12)
    
    # 应用模板
    pio.templates['chinese'] = template
    pio.templates.default = 'chinese'
    
    return {
        'family': font,
        'template': template
    }


def get_chinese_layout(title='', xaxis_title='', yaxis_title='', **kwargs):
    """
    获取支持中文的 Plotly 布局配置
    
    Args:
        title: 图表标题
        xaxis_title: X轴标题
        yaxis_title: Y轴标题
        **kwargs: 其他布局参数
    
    Returns:
        go.Layout: 布局对象
    """
    font_family = DEFAULT_CHINESE_FONT
    
    layout = go.Layout(
        title=dict(
            text=title,
            font=dict(family=font_family, size=20, color='#1a1a1a'),
            x=0.5,
            xanchor='center'
        ),
        xaxis=dict(
            title=dict(text=xaxis_title, font=dict(family=font_family, size=14)),
            tickfont=dict(family=font_family, size=12),
            gridcolor='rgba(128,128,128,0.2)',
            linecolor='rgba(128,128,128,0.5)',
        ),
        yaxis=dict(
            title=dict(text=yaxis_title, font=dict(family=font_family, size=14)),
            tickfont=dict(family=font_family, size=12),
            gridcolor='rgba(128,128,128,0.2)',
            linecolor='rgba(128,128,128,0.5)',
        ),
        legend=dict(
            font=dict(family=font_family, size=12),
            bgcolor='rgba(255,255,255,0.8)',
            bordercolor='rgba(128,128,128,0.3)',
            borderwidth=1
        ),
        font=dict(family=font_family, size=14),
        paper_bgcolor='white',
        plot_bgcolor='rgba(240,240,240,0.3)',
        hoverlabel=dict(
            font=dict(family=font_family, size=12),
            bgcolor='rgba(255,255,255,0.95)',
            bordercolor='rgba(128,128,128,0.3)',
            borderwidth=1
        ),
        margin=dict(t=80, b=60, l=60, r=40),
        **kwargs
    )
    
    return layout


def create_chinese_figure(data, layout=None, title='', **kwargs):
    """
    创建支持中文的 Plotly 图表
    
    Args:
        data: 图表数据（trace 或 trace 列表）
        layout: 布局对象（可选）
        title: 图表标题
        **kwargs: 其他参数
    
    Returns:
        go.Figure: 图表对象
    """
    if layout is None:
        layout = get_chinese_layout(title=title, **kwargs)
    
    fig = go.Figure(data=data, layout=layout)
    return fig


def save_chinese_figure(fig, filepath, **kwargs):
    """
    保存支持中文的图表
    
    Args:
        fig: Plotly 图表对象
        filepath: 保存路径
        **kwargs: 其他保存参数
    """
    # 默认配置
    default_config = {
        'scale': 2,  # 高清输出
        'width': 1200,
        'height': 600,
    }
    default_config.update(kwargs)
    
    # 保存为图片
    if filepath.endswith('.html'):
        fig.write_html(filepath, include_plotlyjs='cdn')
    elif filepath.endswith('.json'):
        fig.write_json(filepath)
    else:
        fig.write_image(filepath, **default_config)
    
    print(f"图表已保存: {filepath}")


# 颜色方案（中文友好的配色）
CHINESE_COLOR_SCHEMES = {
    'traditional': [
        '#C23531',  # 朱砂红
        '#2F4554',  # 墨蓝
        '#61A0A8',  # 青灰
        '#D48265',  # 赭石
        '#91C7AE',  # 青绿
        '#749F83',  # 草绿
        '#CA8622',  # 藤黄
        '#BDA29A',  # 赭石浅
        '#6E7074',  # 灰色
        '#546570',  # 深灰
    ],
    'modern': [
        '#FF6B6B',  # 珊瑚红
        '#4ECDC4',  # 青绿
        '#45B7D1',  # 天蓝
        '#96CEB4',  # 薄荷绿
        '#FFEAA7',  # 鹅黄
        '#DDA0DD',  # 梅红
        '#98D8C8',  # 青碧
        '#F7DC6F',  # 金黄
        '#BB8FCE',  # 紫罗兰
        '#85C1E9',  # 淡蓝
    ],
    'financial': [
        '#FF0000',  # 涨红
        '#00FF00',  # 跌绿
        '#FFD700',  # 金色
        '#4169E1',  # 皇家蓝
        '#FF6347',  # 番茄红
        '#32CD32',  # 酸橙绿
        '#FF8C00',  # 深橙
        '#9370DB',  # 中紫
        '#20B2AA',  # 浅海绿
        '#FF1493',  # 深粉
    ]
}


def get_color_scheme(name='modern'):
    """
    获取配色方案
    
    Args:
        name: 配色方案名称
    
    Returns:
        list: 颜色列表
    """
    return CHINESE_COLOR_SCHEMES.get(name, CHINESE_COLOR_SCHEMES['modern'])


# 测试函数
def test_chinese_font():
    """测试中文字体显示"""
    setup_chinese_font('microsoft_yahei')
    
    fig = go.Figure(data=[
        go.Bar(
            x=['苹果', '香蕉', '橙子', '葡萄', '西瓜'],
            y=[20, 14, 23, 18, 30],
            text=['20个', '14个', '23个', '18个', '30个'],
            textposition='auto',
        )
    ])
    
    fig.update_layout(
        title='水果销量统计（中文测试）',
        xaxis_title='水果种类',
        yaxis_title='销量（个）',
        font=dict(family=DEFAULT_CHINESE_FONT, size=14)
    )
    
    return fig


if __name__ == '__main__':
    # 测试
    print("测试中文字体配置...")
    fig = test_chinese_font()
    fig.show()
    print("测试完成！")
