#!/usr/bin/env python3
"""
独立强度因子历史回测脚本

回测逻辑：
1. 选取历史某日的独立强度高分股票作为买入信号
2. 计算持有 N 天后的收益率
3. 统计胜率、平均收益、最大回撤等指标
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict

from clickhouse_driver import Client

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """回测配置"""
    start_date: str
    end_date: str
    score_threshold: float = 3.0
    hold_days: int = 5
    top_n: Optional[int] = None  # 只选前 N 名，None 表示不限制


@dataclass
class Signal:
    """交易信号"""
    signal_date: str
    symbol: str
    score: float
    raw_score: int
    sector: str
    entry_price: float


@dataclass
class TradeResult:
    """交易结果"""
    signal: Signal
    exit_price: float
    hold_days: int
    return_rate: float
    return_annualized: float
    max_drawdown: float
    sector_return: float


class IndependenceScoreBacktest:
    """独立强度因子回测器"""

    def __init__(self):
        self.ch_client = None
        self.config: Optional[BacktestConfig] = None

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

    def get_signals(self, config: BacktestConfig) -> List[Signal]:
        """获取交易信号"""
        query = """
        SELECT
            i.date as signal_date,
            i.symbol,
            i.score,
            i.raw_score,
            i.sector,
            d.close as entry_price
        FROM independence_score_daily i
        INNER JOIN raw_stocks_daily d
            ON i.symbol = d.symbol AND i.date = d.date
        WHERE i.date >= %(start_date)s
          AND i.date <= %(end_date)s
          AND i.score >= %(threshold)s
        ORDER BY i.date, i.score DESC
        """

        try:
            result = self.ch_client.execute(query, {
                'start_date': config.start_date,
                'end_date': config.end_date,
                'threshold': config.score_threshold
            })

            signals = []
            for row in result:
                signals.append(Signal(
                    signal_date=row[0],
                    symbol=row[1],
                    score=row[2],
                    raw_score=row[3],
                    sector=row[4],
                    entry_price=row[5]
                ))

            # 如果设置了 top_n，每天只取前 N 名
            if config.top_n:
                signals_by_date = defaultdict(list)
                for s in signals:
                    signals_by_date[s.signal_date].append(s)

                filtered_signals = []
                for date, date_signals in signals_by_date.items():
                    filtered_signals.extend(
                        sorted(date_signals, key=lambda x: x.score, reverse=True)[:config.top_n]
                    )
                signals = filtered_signals

            logger.info(f"Generated {len(signals)} signals")
            return signals

        except Exception as e:
            logger.error(f"Failed to get signals: {e}")
            return []

    def calculate_returns(self, signals: List[Signal], hold_days: int) -> List[TradeResult]:
        """计算收益率"""
        if not signals:
            return []

        # 批量查询未来价格
        symbols = [s.symbol for s in signals]
        dates = [s.signal_date for s in signals]

        query = """
        SELECT
            symbol,
            date,
            close,
            high,
            low
        FROM raw_stocks_daily
        WHERE symbol IN %(symbols)s
          AND date > %(min_date)s
          AND date <= %(max_date)s
        """

        try:
            result = self.ch_client.execute(query, {
                'symbols': symbols,
                'min_date': min(dates),
                'max_date': max([d + timedelta(days=hold_days+5) for d in dates])
            })

            # 构建价格字典
            price_data = defaultdict(dict)
            for symbol, date, close, high, low in result:
                price_data[symbol][date] = {
                    'close': close,
                    'high': high,
                    'low': low
                }

            # 计算收益
            results = []
            for signal in signals:
                symbol_prices = price_data.get(signal.symbol, {})
                signal_date = signal.signal_date

                # 找到持有期结束日期
                exit_date = signal_date + timedelta(days=hold_days)

                # 获取退出价格
                exit_price = None
                max_price = signal.entry_price
                min_price = signal.entry_price

                for date in sorted(symbol_prices.keys()):
                    if signal_date < date <= exit_date:
                        price_info = symbol_prices[date]
                        if date == exit_date or exit_price is None:
                            exit_price = price_info['close']
                        max_price = max(max_price, price_info['high'])
                        min_price = min(min_price, price_info['low'])

                if exit_price is None:
                    continue

                # 计算收益率
                return_rate = (exit_price - signal.entry_price) / signal.entry_price * 100
                return_annualized = return_rate * 252 / hold_days
                max_drawdown = (min_price - max_price) / max_price * 100 if max_price > 0 else 0

                # 计算板块收益（简化版）
                sector_return = 0  # 实际应该查询板块收益

                results.append(TradeResult(
                    signal=signal,
                    exit_price=exit_price,
                    hold_days=hold_days,
                    return_rate=return_rate,
                    return_annualized=return_annualized,
                    max_drawdown=max_drawdown,
                    sector_return=sector_return
                ))

            logger.info(f"Calculated returns for {len(results)} trades")
            return results

        except Exception as e:
            logger.error(f"Failed to calculate returns: {e}")
            return []

    def analyze_results(self, results: List[TradeResult]) -> Dict:
        """分析回测结果"""
        if not results:
            return {}

        returns = [r.return_rate for r in results]
        annualized_returns = [r.return_annualized for r in results]
        drawdowns = [r.max_drawdown for r in results]

        win_count = sum(1 for r in returns if r > 0)
        loss_count = len(returns) - win_count

        # 计算夏普比率（简化版，假设无风险利率 3%）
        avg_annualized = sum(annualized_returns) / len(annualized_returns)
        std_annualized = (sum((r - avg_annualized) ** 2 for r in annualized_returns) / len(annualized_returns)) ** 0.5
        sharpe_ratio = (avg_annualized - 3) / std_annualized if std_annualized > 0 else 0

        analysis = {
            'total_trades': len(results),
            'win_count': win_count,
            'loss_count': loss_count,
            'win_rate': win_count / len(results) * 100,
            'avg_return': sum(returns) / len(returns),
            'avg_annualized_return': avg_annualized,
            'max_return': max(returns),
            'min_return': min(returns),
            'avg_max_drawdown': sum(drawdowns) / len(drawdowns),
            'sharpe_ratio': sharpe_ratio,
        }

        return analysis

    def print_report(self, analysis: Dict, results: List[TradeResult]):
        """打印回测报告"""
        if not analysis:
            logger.warning("No results to report")
            return

        print("\n" + "="*60)
        print("独立强度因子回测报告")
        print("="*60)

        print(f"\n【回测参数】")
        print(f"  回测区间: {self.config.start_date} ~ {self.config.end_date}")
        print(f"  选股阈值: score >= {self.config.score_threshold}")
        print(f"  持有期: {self.config.hold_days} 天")
        if self.config.top_n:
            print(f"  每日选股: Top {self.config.top_n}")

        print(f"\n【交易统计】")
        print(f"  总交易次数: {analysis['total_trades']}")
        print(f"  盈利次数: {analysis['win_count']}")
        print(f"  亏损次数: {analysis['loss_count']}")
        print(f"  胜率: {analysis['win_rate']:.2f}%")

        print(f"\n【收益指标】")
        print(f"  平均收益率: {analysis['avg_return']:.2f}%")
        print(f"  平均年化收益: {analysis['avg_annualized_return']:.2f}%")
        print(f"  最大单笔收益: {analysis['max_return']:.2f}%")
        print(f"  最大单笔亏损: {analysis['min_return']:.2f}%")
        print(f"  平均最大回撤: {analysis['avg_max_drawdown']:.2f}%")
        print(f"  夏普比率: {analysis['sharpe_ratio']:.2f}")

        # 按板块统计
        sector_stats = defaultdict(lambda: {'count': 0, 'total_return': 0, 'wins': 0})
        for r in results:
            sector = r.signal.sector
            sector_stats[sector]['count'] += 1
            sector_stats[sector]['total_return'] += r.return_rate
            if r.return_rate > 0:
                sector_stats[sector]['wins'] += 1

        print(f"\n【板块表现】")
        print(f"  {'板块':<20} {'信号数':>8} {'胜率%':>8} {'平均收益%':>10}")
        print("  " + "-"*50)
        for sector, stats in sorted(sector_stats.items(), key=lambda x: x[1]['total_return']/x[1]['count'], reverse=True)[:10]:
            count = stats['count']
            win_rate = stats['wins'] / count * 100
            avg_return = stats['total_return'] / count
            print(f"  {sector:<20} {count:>8} {win_rate:>8.1f} {avg_return:>10.2f}")

        print("\n" + "="*60)

    def run(self, config: BacktestConfig) -> bool:
        """运行回测"""
        self.config = config
        logger.info(f"Starting backtest: {config.start_date} ~ {config.end_date}")

        if not self.connect_clickhouse():
            return False

        try:
            # 1. 获取信号
            signals = self.get_signals(config)
            if not signals:
                logger.warning("No signals generated")
                return False

            # 2. 计算收益
            results = self.calculate_returns(signals, config.hold_days)
            if not results:
                logger.warning("No results calculated")
                return False

            # 3. 分析结果
            analysis = self.analyze_results(results)

            # 4. 打印报告
            self.print_report(analysis, results)

            return True

        except Exception as e:
            logger.error(f"Error in backtest: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description='Independence Score Backtest')
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--threshold', type=float, default=3.0, help='Score threshold (default: 3.0)')
    parser.add_argument('--hold-days', type=int, default=5, help='Hold days (default: 5)')
    parser.add_argument('--top-n', type=int, help='Select top N stocks per day')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = BacktestConfig(
        start_date=args.start,
        end_date=args.end,
        score_threshold=args.threshold,
        hold_days=args.hold_days,
        top_n=args.top_n
    )

    backtest = IndependenceScoreBacktest()
    success = backtest.run(config)

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
