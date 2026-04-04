#!/usr/bin/env python3
"""
融资余额加权独立强度因子计算脚本

从 PostgreSQL 获取融资融券数据，在 ClickHouse 中计算加权独立强度因子。

加权逻辑:
- 基础独立强度分数来自 5 分钟 K 线逆势表现
- 融资余额增加的股票获得额外加权
- 加权公式: weighted_score = raw_score * (1 + change_rate * 0.1)
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from clickhouse_driver import Client

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MarginWeightedIndependenceScore:
    """融资余额加权独立强度因子计算器"""

    def __init__(self):
        self.pg_conn = None
        self.ch_client = None

    def connect_postgres(self) -> bool:
        """连接 PostgreSQL"""
        try:
            self.pg_conn = psycopg2.connect(
                host=os.getenv('PG_HOST', 'localhost'),
                port=os.getenv('PG_PORT', '5432'),
                database=os.getenv('PG_DB', 'quantdb'),
                user=os.getenv('PG_USER', 'postgres'),
                password=os.getenv('PG_PASSWORD', ''),
            )
            logger.info("Connected to PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Failed to connect PostgreSQL: {e}")
            return False

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

    def get_margin_data(self, trade_date: str) -> List[Tuple]:
        """
        从 PostgreSQL 获取融资融券数据

        Returns:
            List of (symbol, margin_balance, change_rate)
        """
        query = """
        WITH today_data AS (
            SELECT
                ts_code,
                margin_balance_buy,
                trade_date
            FROM margin_trading_detail_combined
            WHERE trade_date = %s
        ),
        prev_data AS (
            SELECT
                ts_code,
                margin_balance_buy,
                trade_date
            FROM margin_trading_detail_combined
            WHERE trade_date = %s - INTERVAL '1 day'
        )
        SELECT
            t.ts_code,
            t.margin_balance_buy,
            CASE
                WHEN p.margin_balance_buy IS NOT NULL AND p.margin_balance_buy > 0
                THEN (t.margin_balance_buy - p.margin_balance_buy)::FLOAT / p.margin_balance_buy
                ELSE 0
            END as change_rate
        FROM today_data t
        LEFT JOIN prev_data p ON t.ts_code = p.ts_code
        WHERE t.margin_balance_buy IS NOT NULL
        """

        try:
            with self.pg_conn.cursor() as cur:
                cur.execute(query, (trade_date, trade_date))
                results = cur.fetchall()
                logger.info(f"Fetched {len(results)} margin records for {trade_date}")
                return results
        except Exception as e:
            logger.error(f"Failed to fetch margin data: {e}")
            return []

    def create_margin_temp_table(self, trade_date: str) -> bool:
        """在 ClickHouse 中创建融资数据临时表"""
        try:
            # 删除旧临时表
            self.ch_client.execute("DROP TABLE IF EXISTS margin_change_temp")

            # 创建临时表
            self.ch_client.execute("""
                CREATE TABLE margin_change_temp (
                    symbol String,
                    date Date,
                    margin_balance Int64,
                    change_rate Float64
                ) ENGINE = Memory
            """)
            logger.info("Created margin_change_temp table")
            return True
        except Exception as e:
            logger.error(f"Failed to create temp table: {e}")
            return False

    def import_margin_data(self, margin_data: List[Tuple], trade_date: str) -> bool:
        """导入融资数据到 ClickHouse 临时表"""
        if not margin_data:
            logger.warning("No margin data to import")
            return False

        try:
            # 转换数据格式
            formatted_data = [
                (symbol, trade_date, int(margin_balance or 0), float(change_rate or 0))
                for symbol, margin_balance, change_rate in margin_data
            ]

            # 批量插入
            self.ch_client.execute(
                "INSERT INTO margin_change_temp VALUES",
                formatted_data
            )
            logger.info(f"Imported {len(formatted_data)} records to ClickHouse")
            return True
        except Exception as e:
            logger.error(f"Failed to import margin data: {e}")
            return False

    def calculate_weighted_score(self, trade_date: str) -> bool:
        """计算加权独立强度因子并插入结果表"""
        try:
            # 先计算基础独立强度分数
            self.ch_client.execute("""
                INSERT INTO independence_score_daily
                WITH
                stock_returns AS (
                    SELECT
                        symbol,
                        datetime,
                        close,
                        prev_close,
                        (close - prev_close) / prev_close * 100 as stock_return
                    FROM (
                        SELECT
                            symbol,
                            datetime,
                            close,
                            lagInFrame(close) OVER (
                                PARTITION BY symbol, toDate(datetime)
                                ORDER BY datetime
                                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                            ) as prev_close
                        FROM raw_stocks_5min
                        WHERE toDate(datetime) = %(date)s
                    )
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
                ),
                sector_returns AS (
                    SELECT
                        sector_code,
                        datetime,
                        avg(stock_return) as sector_return
                    FROM stock_with_sector
                    GROUP BY sector_code, datetime
                ),
                combined_data AS (
                    SELECT
                        sws.symbol,
                        sws.sector_code,
                        sws.datetime,
                        sws.stock_return,
                        sr.sector_return,
                        sr.sector_return < -0.5 AND (
                            sws.stock_return > 0 OR
                            (sws.stock_return - sr.sector_return) > 1
                        ) as is_contra_move
                    FROM stock_with_sector sws
                    INNER JOIN sector_returns sr
                        ON sws.sector_code = sr.sector_code
                        AND sws.datetime = sr.datetime
                ),
                independence_score AS (
                    SELECT
                        symbol,
                        sector_code,
                        countIf(is_contra_move) as raw_score,
                        count(*) as sector_stock_count,
                        countIf(is_contra_move) as contra_count
                    FROM combined_data
                    GROUP BY symbol, sector_code
                )
                SELECT
                    symbol,
                    %(date)s as date,
                    raw_score as score,
                    raw_score as raw_score,
                    1.0 as margin_weight,
                    sector_code as sector,
                    sector_stock_count,
                    contra_count
                FROM independence_score
                WHERE raw_score > 0
            """, {'date': trade_date})

            logger.info(f"Inserted base independence scores for {trade_date}")
            return True
        except Exception as e:
            logger.error(f"Failed to calculate weighted score: {e}")
            return False

    def apply_margin_weight(self, trade_date: str, weight_factor: float = 0.1) -> bool:
        """
        应用融资余额加权

        Args:
            trade_date: 交易日期
            weight_factor: 加权系数，默认 0.1 表示 10% 的融资变化率转化为 10% 的分数加成
        """
        try:
            # 更新已有记录的 margin_weight 和 score
            self.ch_client.execute("""
                ALTER TABLE independence_score_daily
                UPDATE
                    margin_weight = 1.0 + m.change_rate * %(factor)s,
                    score = raw_score * (1.0 + m.change_rate * %(factor)s)
                FROM margin_change_temp m
                WHERE independence_score_daily.symbol = m.symbol
                  AND independence_score_daily.date = %(date)s
                  AND m.date = %(date)s
            """, {'date': trade_date, 'factor': weight_factor})

            logger.info(f"Applied margin weight for {trade_date}")
            return True
        except Exception as e:
            logger.error(f"Failed to apply margin weight: {e}")
            return False

    def get_top_scores(self, trade_date: str, limit: int = 20) -> List[Tuple]:
        """获取 Top 加权分数"""
        try:
            result = self.ch_client.execute("""
                SELECT
                    symbol,
                    score,
                    raw_score,
                    margin_weight,
                    sector,
                    contra_count
                FROM independence_score_daily
                WHERE date = %(date)s
                ORDER BY score DESC
                LIMIT %(limit)s
            """, {'date': trade_date, 'limit': limit})
            return result
        except Exception as e:
            logger.error(f"Failed to get top scores: {e}")
            return []

    def cleanup(self):
        """清理资源"""
        if self.pg_conn:
            self.pg_conn.close()
            logger.info("Closed PostgreSQL connection")
        if self.ch_client:
            # ClickHouse driver doesn't need explicit close
            pass

    def run(self, trade_date: str, weight_factor: float = 0.1) -> bool:
        """运行完整流程"""
        logger.info(f"Starting margin-weighted independence score calculation for {trade_date}")

        # 连接数据库
        if not self.connect_postgres():
            return False
        if not self.connect_clickhouse():
            return False

        try:
            # 1. 从 PG 获取融资数据
            margin_data = self.get_margin_data(trade_date)

            # 2. 创建临时表
            if not self.create_margin_temp_table(trade_date):
                return False

            # 3. 导入融资数据
            if margin_data:
                self.import_margin_data(margin_data, trade_date)

            # 4. 计算基础独立强度分数
            if not self.calculate_weighted_score(trade_date):
                return False

            # 5. 应用融资加权（如果有融资数据）
            if margin_data:
                self.apply_margin_weight(trade_date, weight_factor)

            # 6. 输出结果
            top_scores = self.get_top_scores(trade_date, 20)
            logger.info(f"\nTop 20 Margin-Weighted Independence Scores for {trade_date}:")
            logger.info(f"{'Symbol':<12} {'Score':>8} {'Raw':>8} {'Weight':>8} {'Sector':<20} {'Contra':>6}")
            logger.info("-" * 70)
            for row in top_scores:
                symbol, score, raw, weight, sector, contra = row
                logger.info(f"{symbol:<12} {score:>8.2f} {raw:>8} {weight:>8.2f} {sector:<20} {contra:>6}")

            return True

        except Exception as e:
            logger.error(f"Error in run: {e}")
            return False
        finally:
            self.cleanup()


def main():
    parser = argparse.ArgumentParser(
        description='Calculate margin-weighted independence score'
    )
    parser.add_argument(
        'date',
        nargs='?',
        default=datetime.now().strftime('%Y-%m-%d'),
        help='Trade date (YYYY-MM-DD), default: today'
    )
    parser.add_argument(
        '--weight-factor', '-w',
        type=float,
        default=0.1,
        help='Weight factor for margin change (default: 0.1)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    calculator = MarginWeightedIndependenceScore()
    success = calculator.run(args.date, args.weight_factor)

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
