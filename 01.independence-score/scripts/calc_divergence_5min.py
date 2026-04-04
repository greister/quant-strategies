#!/usr/bin/env python3
"""
5分钟背离信号计算脚本
计算个股与CSI300指数、行业指数的背离信号

背离定义:
- 顶背离: 价格创新高，但相对强度/动量减弱
- 底背离: 价格创新低，但相对强度/动量增强

输出: Markdown报告到指定目录
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from pathlib import Path

from clickhouse_driver import Client

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class DivergenceSignal:
    """背离信号"""
    symbol: str
    name: str
    sector: str
    datetime: datetime
    signal_type: str  # 'top_divergence' 或 'bottom_divergence'
    divergence_target: str  # 'CSI300' 或 'SECTOR'
    price_change: float
    ref_change: float
    divergence_strength: float
    interval_count: int  # 连续背离区间数


class DivergenceCalculator:
    """5分钟背离信号计算器"""
    
    def __init__(self):
        self.ch_client = None
        self.trade_date = None
        
    def connect_clickhouse(self) -> bool:
        """连接 ClickHouse"""
        try:
            self.ch_client = Client(
                host=os.getenv('CH_HOST', 'localhost'),
                port=int(os.getenv('CH_PORT', '9000')),
                database=os.getenv('CH_DB', 'tdx2db_rust'),
                user=os.getenv('CH_USER', 'default'),
                password=os.getenv('CH_PASSWORD', ''),
            )
            logger.info("Connected to ClickHouse")
            return True
        except Exception as e:
            logger.error(f"Failed to connect ClickHouse: {e}")
            return False
    
    def get_market_index_data(self, symbol: str = 'sh000300') -> List[Tuple]:
        """获取市场指数5分钟数据"""
        query = """
        SELECT 
            datetime,
            close,
            open,
            high,
            low,
            volume
        FROM raw_stocks_5min
        WHERE symbol = %(symbol)s
          AND toDate(datetime) = %(date)s
        ORDER BY datetime
        """
        try:
            result = self.ch_client.execute(query, {
                'symbol': symbol,
                'date': self.trade_date
            })
            logger.info(f"Fetched {len(result)} records for {symbol}")
            return result
        except Exception as e:
            logger.error(f"Failed to fetch index data: {e}")
            return []
    
    def get_sector_index_data(self) -> Dict[str, List[Tuple]]:
        """获取各行业指数的5分钟数据（使用行业成分股平均）"""
        query = """
        WITH 
        -- 获取股票的行业归属
        stock_sectors AS (
            SELECT symbol, industry_code as sector_code
            FROM stock_industry_mapping
        ),
        -- 计算各行业5分钟收益率
        sector_5min AS (
            SELECT 
                s.sector_code,
                r.datetime,
                avg(r.close) as avg_close,
                avg(r.open) as avg_open,
                avg(r.high) as avg_high,
                avg(r.low) as avg_low,
                sum(r.volume) as total_volume
            FROM raw_stocks_5min r
            JOIN stock_sectors s ON r.symbol = s.symbol
            WHERE toDate(r.datetime) = %(date)s
            GROUP BY s.sector_code, r.datetime
        )
        SELECT 
            sector_code,
            datetime,
            avg_close,
            avg_open,
            avg_close - lagInFrame(avg_close) OVER (
                PARTITION BY sector_code 
                ORDER BY datetime 
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            ) as price_change
        FROM sector_5min
        ORDER BY sector_code, datetime
        """
        try:
            result = self.ch_client.execute(query, {'date': self.trade_date})
            
            # 按行业分组
            sector_data = {}
            for row in result:
                sector_code = row[0]
                if sector_code not in sector_data:
                    sector_data[sector_code] = []
                sector_data[sector_code].append(row)
            
            logger.info(f"Fetched data for {len(sector_data)} sectors")
            return sector_data
        except Exception as e:
            logger.error(f"Failed to fetch sector data: {e}")
            return {}
    
    def get_stock_5min_data(self) -> List[Tuple]:
        """获取个股5分钟数据"""
        query = """
        SELECT 
            r.symbol,
            g.name,
            s.industry_code as sector,
            r.datetime,
            r.close,
            r.open,
            r.high,
            r.low,
            r.volume,
            (r.close - lagInFrame(r.close) OVER (
                PARTITION BY r.symbol, toDate(r.datetime) 
                ORDER BY r.datetime 
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            )) / lagInFrame(r.close) OVER (
                PARTITION BY r.symbol, toDate(r.datetime) 
                ORDER BY r.datetime 
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            ) * 100 as return_pct
        FROM raw_stocks_5min r
        JOIN stock_industry_mapping s ON r.symbol = s.symbol
        LEFT JOIN gtja_stock_names g ON r.symbol = g.symbol
        WHERE toDate(r.datetime) = %(date)s
        ORDER BY r.symbol, r.datetime
        """
        try:
            result = self.ch_client.execute(query, {'date': self.trade_date})
            logger.info(f"Fetched {len(result)} stock 5min records")
            return result
        except Exception as e:
            logger.error(f"Failed to fetch stock data: {e}")
            return []
    
    def calculate_divergence(self, 
                           stock_returns: List[float], 
                           ref_returns: List[float],
                           window: int = 6) -> List[Tuple[int, str, float]]:
        """
        计算背离信号
        
        Args:
            stock_returns: 个股收益率序列
            ref_returns: 参考收益率序列（指数或行业）
            window: 检测窗口大小（区间数）
        
        Returns:
            List of (index, signal_type, strength)
        """
        signals = []
        n = len(stock_returns)
        
        if n < window * 2:
            return signals
        
        for i in range(window, n - window):
            # 获取前后窗口数据
            prev_stock = stock_returns[i-window:i]
            curr_stock = stock_returns[i]
            next_stock = stock_returns[i+1:i+window+1]
            
            prev_ref = ref_returns[i-window:i]
            curr_ref = ref_returns[i]
            next_ref = ref_returns[i+1:i+window+1]
            
            # 计算局部极值
            stock_max_prev = max(prev_stock) if prev_stock else curr_stock
            stock_min_prev = min(prev_stock) if prev_stock else curr_stock
            ref_max_prev = max(prev_ref) if prev_ref else curr_ref
            ref_min_prev = min(prev_ref) if prev_ref else curr_ref
            
            # 顶背离检测：股价创新高，但参考指标未创新高
            if curr_stock > stock_max_prev * 1.001:  # 股价创新高（0.1%容差）
                if curr_ref <= ref_max_prev * 1.0005:  # 参考指标未创新高
                    if stock_max_prev != 0 and ref_max_prev != 0:
                        strength = (curr_stock / stock_max_prev - 1) - (curr_ref / ref_max_prev - 1)
                        signals.append((i, 'top_divergence', strength))
            
            # 底背离检测：股价创新低，但参考指标未创新低
            elif curr_stock < stock_min_prev * 0.999:  # 股价创新低
                if curr_ref >= ref_min_prev * 0.9995:  # 参考指标未创新低
                    if curr_stock != 0 and curr_ref != 0:
                        strength = (stock_min_prev / curr_stock - 1) - (ref_min_prev / curr_ref - 1)
                        signals.append((i, 'bottom_divergence', strength))
        
        return signals
    
    def analyze_divergence(self, trade_date: str) -> List[DivergenceSignal]:
        """分析背离信号"""
        self.trade_date = trade_date
        
        logger.info(f"Starting divergence analysis for {trade_date}")
        
        # 获取数据
        csi300_data = self.get_market_index_data('sh000300')
        if not csi300_data:
            logger.warning("No CSI300 data available")
            return []
        
        # 提取CSI300收益率序列
        csi300_returns = []
        csi300_datetimes = []
        for row in csi300_data:
            dt, close, open_p, high, low, volume = row
            if close and open_p:
                ret = (close - open_p) / open_p * 100
                csi300_returns.append(ret)
                csi300_datetimes.append(dt)
        
        # 获取行业数据
        sector_data = self.get_sector_index_data()
        
        # 获取个股数据
        stock_data = self.get_stock_5min_data()
        
        all_signals = []
        
        # 按股票分组处理
        stock_groups = {}
        for row in stock_data:
            symbol, name, sector, dt, close, open_p, high, low, volume, ret = row
            if symbol not in stock_groups:
                stock_groups[symbol] = {
                    'name': name or symbol,
                    'sector': sector,
                    'data': []
                }
            stock_groups[symbol]['data'].append((dt, close, open_p, ret))
        
        logger.info(f"Processing {len(stock_groups)} stocks...")
        
        # 计算每只股票的背离信号
        for symbol, info in stock_groups.items():
            stock_returns = [d[3] for d in info['data'] if d[3] is not None]
            
            if len(stock_returns) < 12:  # 需要足够的数据点
                continue
            
            # 1. 计算与CSI300的背离
            if len(stock_returns) == len(csi300_returns):
                signals = self.calculate_divergence(stock_returns, csi300_returns)
                for idx, sig_type, strength in signals:
                    if idx < len(info['data']):
                        dt = info['data'][idx][0]
                        price_change = stock_returns[idx]
                        ref_change = csi300_returns[idx]
                        
                        all_signals.append(DivergenceSignal(
                            symbol=symbol,
                            name=info['name'],
                            sector=info['sector'],
                            datetime=dt,
                            signal_type=sig_type,
                            divergence_target='CSI300',
                            price_change=price_change,
                            ref_change=ref_change,
                            divergence_strength=strength,
                            interval_count=1
                        ))
            
            # 2. 计算与行业的背离
            if info['sector'] in sector_data:
                sector_returns = [s[3] for s in sector_data[info['sector']] if s[3] is not None]
                if len(stock_returns) == len(sector_returns):
                    signals = self.calculate_divergence(stock_returns, sector_returns)
                    for idx, sig_type, strength in signals:
                        if idx < len(info['data']):
                            dt = info['data'][idx][0]
                            price_change = stock_returns[idx]
                            ref_change = sector_returns[idx]
                            
                            all_signals.append(DivergenceSignal(
                                symbol=symbol,
                                name=info['name'],
                                sector=info['sector'],
                                datetime=dt,
                                signal_type=sig_type,
                                divergence_target='SECTOR',
                                price_change=price_change,
                                ref_change=ref_change,
                                divergence_strength=strength,
                                interval_count=1
                            ))
        
        logger.info(f"Found {len(all_signals)} divergence signals")
        return all_signals
    
    def generate_report(self, 
                       signals: List[DivergenceSignal], 
                       output_path: str,
                       trade_date: str) -> bool:
        """生成Markdown报告"""
        try:
            # 确保输出目录存在
            output_dir = Path(output_path)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # 按信号类型分组
            top_div_csi = [s for s in signals if s.signal_type == 'top_divergence' and s.divergence_target == 'CSI300']
            bottom_div_csi = [s for s in signals if s.signal_type == 'bottom_divergence' and s.divergence_target == 'CSI300']
            top_div_sector = [s for s in signals if s.signal_type == 'top_divergence' and s.divergence_target == 'SECTOR']
            bottom_div_sector = [s for s in signals if s.signal_type == 'bottom_divergence' and s.divergence_target == 'SECTOR']
            
            # 按强度排序
            top_div_csi.sort(key=lambda x: x.divergence_strength, reverse=True)
            bottom_div_csi.sort(key=lambda x: x.divergence_strength, reverse=True)
            
            # 生成文件名
            report_file = output_dir / f"{trade_date}_5min_divergence_report.md"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(f"# 5分钟背离信号报告\n\n")
                f.write(f"> **报告日期**: {trade_date}  \n")
                f.write(f"> **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  \n")
                f.write(f"> **数据来源**: tdx2db-rust (ClickHouse)  \n\n")
                
                # 摘要
                f.write("## 📊 信号摘要\n\n")
                f.write(f"| 背离类型 | CSI300背离 | 行业背离 | 合计 |\n")
                f.write(f"|---------|:---------:|:-------:|:----:|\n")
                f.write(f"| 顶背离 | {len(top_div_csi)} | {len(top_div_sector)} | {len(top_div_csi) + len(top_div_sector)} |\n")
                f.write(f"| 底背离 | {len(bottom_div_csi)} | {len(bottom_div_sector)} | {len(bottom_div_csi) + len(bottom_div_sector)} |\n")
                f.write(f"| **合计** | **{len(top_div_csi) + len(bottom_div_csi)}** | **{len(top_div_sector) + len(bottom_div_sector)}** | **{len(signals)}** |\n\n")
                
                # 顶背离 - CSI300
                if top_div_csi:
                    f.write("## 🔴 顶背离信号 (vs CSI300)\n\n")
                    f.write("个股价格相对CSI300创出新高，但强度减弱，可能见顶回落。\n\n")
                    f.write(f"| 时间 | 股票代码 | 股票名称 | 行业 | 个股涨跌 | 指数涨跌 | 背离强度 |\n")
                    f.write(f"|------|---------|---------|------|:-------:|:-------:|:-------:|\n")
                    for s in top_div_csi[:20]:  # 只显示前20
                        f.write(f"| {s.datetime.strftime('%H:%M')} | {s.symbol} | {s.name} | {s.sector} | {s.price_change:+.2f}% | {s.ref_change:+.2f}% | {s.divergence_strength:.3f} |\n")
                    f.write("\n")
                
                # 底背离 - CSI300
                if bottom_div_csi:
                    f.write("## 🟢 底背离信号 (vs CSI300)\n\n")
                    f.write("个股价格相对CSI300创出新低，但强度增强，可能见底反弹。\n\n")
                    f.write(f"| 时间 | 股票代码 | 股票名称 | 行业 | 个股涨跌 | 指数涨跌 | 背离强度 |\n")
                    f.write(f"|------|---------|---------|------|:-------:|:-------:|:-------:|\n")
                    for s in bottom_div_csi[:20]:
                        f.write(f"| {s.datetime.strftime('%H:%M')} | {s.symbol} | {s.name} | {s.sector} | {s.price_change:+.2f}% | {s.ref_change:+.2f}% | {s.divergence_strength:.3f} |\n")
                    f.write("\n")
                
                # 顶背离 - 行业
                if top_div_sector:
                    f.write("## 🔴 顶背离信号 (vs 行业)\n\n")
                    f.write("个股价格相对行业指数创出新高，但强度减弱。\n\n")
                    f.write(f"| 时间 | 股票代码 | 股票名称 | 行业 | 个股涨跌 | 行业涨跌 | 背离强度 |\n")
                    f.write(f"|------|---------|---------|------|:-------:|:-------:|:-------:|\n")
                    for s in top_div_sector[:15]:
                        f.write(f"| {s.datetime.strftime('%H:%M')} | {s.symbol} | {s.name} | {s.sector} | {s.price_change:+.2f}% | {s.ref_change:+.2f}% | {s.divergence_strength:.3f} |\n")
                    f.write("\n")
                
                # 底背离 - 行业
                if bottom_div_sector:
                    f.write("## 🟢 底背离信号 (vs 行业)\n\n")
                    f.write("个股价格相对行业指数创出新低，但强度增强。\n\n")
                    f.write(f"| 时间 | 股票代码 | 股票名称 | 行业 | 个股涨跌 | 行业涨跌 | 背离强度 |\n")
                    f.write(f"|------|---------|---------|------|:-------:|:-------:|:-------:|\n")
                    for s in bottom_div_sector[:15]:
                        f.write(f"| {s.datetime.strftime('%H:%M')} | {s.symbol} | {s.name} | {s.sector} | {s.price_change:+.2f}% | {s.ref_change:+.2f}% | {s.divergence_strength:.3f} |\n")
                    f.write("\n")
                
                # 说明
                f.write("---\n\n")
                f.write("## 📌 说明\n\n")
                f.write("### 背离定义\n\n")
                f.write("- **顶背离**: 股价创近期新高，但相对于基准（CSI300/行业）的强度减弱\n")
                f.write("- **底背离**: 股价创近期新低，但相对于基准（CSI300/行业）的强度增强\n\n")
                f.write("### 计算方法\n\n")
                f.write("1. 取6个5分钟K线为一个检测窗口\n")
                f.write("2. 比较个股与基准的相对强弱变化\n")
                f.write("3. 当个股创新高/低而基准未同步时，判定为背离\n\n")
                f.write("### 使用建议\n\n")
                f.write("- 顶背离信号：考虑减仓或观望\n")
                f.write("- 底背离信号：关注反弹机会\n")
                f.write("- 背离强度越大，信号越显著\n")
                f.write("- 建议结合成交量和其他技术指标综合判断\n")
            
            logger.info(f"Report saved to: {report_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to generate report: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description='计算5分钟背离信号')
    parser.add_argument('date', nargs='?', default=datetime.now().strftime('%Y-%m-%d'),
                       help='交易日期 (YYYY-MM-DD)，默认今天')
    parser.add_argument('--output', '-o', 
                       default='/mnt/d/obsidian/OrbitOS-vault',
                       help='报告输出目录')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='详细输出')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 创建计算器
    calc = DivergenceCalculator()
    
    if not calc.connect_clickhouse():
        sys.exit(1)
    
    # 计算背离信号
    signals = calc.analyze_divergence(args.date)
    
    if not signals:
        logger.warning("No divergence signals found")
        sys.exit(0)
    
    # 生成报告
    success = calc.generate_report(signals, args.output, args.date)
    
    if success:
        print(f"\n✓ 背离信号计算完成！")
        print(f"  总信号数: {len(signals)}")
        print(f"  报告已保存到: {args.output}/{args.date}_5min_divergence_report.md")
    else:
        print("\n✗ 报告生成失败")
        sys.exit(1)


if __name__ == '__main__':
    main()
