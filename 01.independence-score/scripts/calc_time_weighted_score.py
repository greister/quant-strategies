#!/usr/bin/env python3
"""
时间加权独立强度因子计算脚本

基于 ClickHouse 的分时独立强度因子计算，支持多种时间权重配置。
权重配置用于对不同交易时间段赋予不同重要性，以反映不同的交易逻辑。
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from clickhouse_driver import Client

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TimeWeightedScoreCalculator:
    """时间加权独立强度因子计算器"""

    PRESETS = [
        'evening_focus', 'conservative', 'trending_market',
        'ranging_market', 'rotating_market', 'morning_focus'
    ]

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """
        初始化计算器

        Args:
            host: ClickHouse 主机地址
            port: ClickHouse 端口
            database: 数据库名称
            user: 用户名
            password: 密码
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.client: Optional[Client] = None

    def _connect(self) -> Client:
        """
        建立 ClickHouse 连接

        Returns:
            Client: ClickHouse 客户端实例

        Raises:
            Exception: 连接失败时抛出异常
        """
        try:
            client = Client(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            # 测试连接
            client.execute('SELECT 1')
            logger.debug(f"Connected to ClickHouse at {self.host}:{self.port}")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def _get_client(self) -> Client:
        """获取或创建客户端连接"""
        if self.client is None:
            self.client = self._connect()
        return self.client

    def ensure_tables(self) -> bool:
        """
        创建表结构并初始化配置

        读取并执行 SQL 文件：
        - ../sql/create_time_weighted_tables.sql
        - ../sql/init_weight_configs.sql

        Returns:
            bool: 成功返回 True，失败返回 False
        """
        script_dir = Path(__file__).parent
        sql_dir = script_dir.parent / 'sql'

        sql_files = [
            sql_dir / 'create_time_weighted_tables.sql',
            sql_dir / 'init_weight_configs.sql'
        ]

        client = self._get_client()

        for sql_file in sql_files:
            if not sql_file.exists():
                logger.error(f"SQL file not found: {sql_file}")
                return False

            try:
                sql_content = sql_file.read_text(encoding='utf-8')

                # ClickHouse 不支持多语句，需要按 ; 拆分执行
                statements = []
                current_statement = []

                for line in sql_content.split('\n'):
                    line = line.strip()
                    # 跳过注释和空行
                    if not line or line.startswith('--'):
                        continue
                    current_statement.append(line)
                    if line.endswith(';'):
                        stmt = ' '.join(current_statement).strip()
                        if stmt and len(stmt) > 1:  # 不只是 ';'
                            statements.append(stmt)
                        current_statement = []

                # 处理最后一条没有分号的语句
                if current_statement:
                    stmt = ' '.join(current_statement).strip()
                    if stmt:
                        statements.append(stmt)

                # 执行每条语句
                for stmt in statements:
                    if stmt:
                        logger.debug(f"Executing: {stmt[:100]}...")
                        client.execute(stmt)

                logger.info(f"Executed SQL file: {sql_file.name}")

            except Exception as e:
                logger.error(f"Failed to execute {sql_file.name}: {e}")
                return False

        return True

    def calc(self, trade_date: str, config_name: str) -> int:
        """
        执行时间加权独立强度因子计算

        Args:
            trade_date: 交易日期 (YYYY-MM-DD 格式)
            config_name: 权重配置名称

        Returns:
            int: 计算的股票数量，失败返回 -1
        """
        script_dir = Path(__file__).parent
        sql_file = script_dir.parent / 'sql' / 'calc_time_weighted_score.sql'

        if not sql_file.exists():
            logger.error(f"SQL file not found: {sql_file}")
            return -1

        try:
            calc_sql = sql_file.read_text(encoding='utf-8')

            # 参数替换
            calc_sql = calc_sql.replace('{trade_date:Date}', f"'{trade_date}'")
            calc_sql = calc_sql.replace('{config_name:String}', f"'{config_name}'")

            client = self._get_client()

            # 先验证配置存在
            config_check = client.execute(
                "SELECT count() FROM score_weight_configs WHERE config_name = %(name)s",
                {'name': config_name}
            )
            if config_check[0][0] == 0:
                logger.error(f"Weight config '{config_name}' not found")
                return -1

            logger.info(f"Calculating time-weighted scores for {trade_date} with config '{config_name}'")

            # 执行计算 SQL
            client.execute(calc_sql)

            # 获取计算结果数量
            result = client.execute(
                """
                SELECT count() 
                FROM independence_score_time_weighted 
                WHERE date = %(date)s AND config_name = %(config)s
                """,
                {'date': trade_date, 'config': config_name}
            )
            count = result[0][0]

            logger.info(f"Calculated {count} time-weighted scores for {trade_date}")
            return count

        except Exception as e:
            logger.error(f"Failed to calculate time-weighted scores: {e}")
            return -1

    def create_custom_config(
        self,
        name: str,
        weights: List[float],
        config_type: str = 'custom',
        description: str = ''
    ) -> bool:
        """
        创建自定义权重配置

        Args:
            name: 配置名称
            weights: 权重数组（48 个元素，总和应为 1.0）
            config_type: 配置类型，默认 'custom'
            description: 配置描述

        Returns:
            bool: 成功返回 True，失败返回 False
        """
        # 权重验证
        if len(weights) != 48:
            logger.error(f"Weights array must have exactly 48 elements, got {len(weights)}")
            return False

        weight_sum = sum(weights)
        if not (0.999 <= weight_sum <= 1.001):
            logger.error(f"Weights sum must be approximately 1.0 (0.999-1.001), got {weight_sum:.6f}")
            return False

        if any(w < 0 for w in weights):
            logger.error("All weights must be non-negative")
            return False

        try:
            client = self._get_client()

            # 检查配置是否已存在
            existing = client.execute(
                "SELECT count() FROM score_weight_configs WHERE config_name = %(name)s",
                {'name': name}
            )
            if existing[0][0] > 0:
                logger.warning(f"Config '{name}' already exists, will be updated")

            # 插入配置
            client.execute(
                """
                INSERT INTO score_weight_configs (config_name, config_type, granularity, weights, description)
                VALUES
                """,
                [{
                    'config_name': name,
                    'config_type': config_type,
                    'granularity': 'interval',
                    'weights': weights,
                    'description': description or f'Custom config with sum={weight_sum:.4f}'
                }]
            )

            logger.info(f"Created custom config '{name}' with {len(weights)} weights (sum={weight_sum:.6f})")
            return True

        except Exception as e:
            logger.error(f"Failed to create custom config: {e}")
            return False

    def list_presets(self) -> List[dict]:
        """
        列出所有可用的权重配置

        Returns:
            List[dict]: 配置列表
        """
        try:
            client = self._get_client()
            results = client.execute(
                """
                SELECT config_name, config_type, description, is_default
                FROM score_weight_configs
                ORDER BY config_name
                """
            )

            presets = []
            for row in results:
                presets.append({
                    'name': row[0],
                    'type': row[1],
                    'description': row[2],
                    'is_default': bool(row[3])
                })

            return presets

        except Exception as e:
            logger.error(f"Failed to list presets: {e}")
            return []

    def get_top_scores(
        self,
        trade_date: str,
        config_name: str,
        limit: int = 20
    ) -> List[tuple]:
        """
        获取 Top 加权分数

        Args:
            trade_date: 交易日期
            config_name: 配置名称
            limit: 返回数量限制

        Returns:
            List[tuple]: Top 分数列表
        """
        try:
            client = self._get_client()
            results = client.execute(
                """
                SELECT
                    symbol,
                    name,
                    sector,
                    raw_score,
                    weighted_score,
                    contra_count
                FROM independence_score_time_weighted
                WHERE date = %(date)s AND config_name = %(config)s
                ORDER BY weighted_score DESC
                LIMIT %(limit)s
                """,
                {'date': trade_date, 'config': config_name, 'limit': limit}
            )
            return results

        except Exception as e:
            logger.error(f"Failed to get top scores: {e}")
            return []

    def close(self):
        """关闭连接"""
        if self.client:
            # clickhouse-driver 不需要显式关闭
            self.client = None
            logger.debug("Closed ClickHouse connection")


def parse_weights(weights_str: str) -> List[float]:
    """
    解析逗号分隔的权重字符串

    Args:
        weights_str: 逗号分隔的权重值（如 "0.02,0.02,..."）

    Returns:
        List[float]: 权重列表

    Raises:
        ValueError: 解析失败时抛出
    """
    try:
        weights = [float(w.strip()) for w in weights_str.split(',')]
        return weights
    except ValueError as e:
        raise ValueError(f"Invalid weights format: {e}")


def main():
    """CLI 入口"""
    parser = argparse.ArgumentParser(
        description='Calculate time-weighted independence score',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 计算今天的默认配置分数
  %(prog)s

  # 计算指定日期的分数
  %(prog)s 2024-01-15

  # 使用趋势市配置
  %(prog)s --preset trending_market

  # 初始化表结构
  %(prog)s --init

  # 列出所有预设配置
  %(prog)s --list-presets

  # 使用自定义权重（48 个逗号分隔的值）
  %(prog)s --custom-name my_config --custom-weights "0.02,0.02,..."
        """
    )

    parser.add_argument(
        'date',
        nargs='?',
        default=datetime.now().strftime('%Y-%m-%d'),
        help='Trade date (YYYY-MM-DD), default: today'
    )

    parser.add_argument(
        '--preset',
        choices=TimeWeightedScoreCalculator.PRESETS,
        default='evening_focus',
        help='Preset weight configuration (default: evening_focus)'
    )

    parser.add_argument(
        '--custom-weights',
        type=str,
        help='Custom weights as comma-separated 48 values (e.g., "0.02,0.02,...")'
    )

    parser.add_argument(
        '--custom-name',
        type=str,
        help='Name for custom configuration'
    )

    parser.add_argument(
        '--init',
        action='store_true',
        help='Initialize table structures and preset configs'
    )

    parser.add_argument(
        '--list-presets',
        action='store_true',
        help='List all available preset configurations'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # 从环境变量获取连接信息
    host = os.getenv('CH_HOST', 'localhost')
    port = int(os.getenv('CH_PORT', '9000'))
    database = os.getenv('CH_DB', 'tdx2db_rust')
    user = os.getenv('CH_USER', 'default')
    password = os.getenv('CH_PASSWORD', '')

    # 创建计算器实例
    try:
        calculator = TimeWeightedScoreCalculator(host, port, database, user, password)
    except Exception:
        sys.exit(1)

    try:
        # 处理 --list-presets
        if args.list_presets:
            presets = calculator.list_presets()
            if presets:
                print("\nAvailable weight configurations:")
                print(f"{'Name':<20} {'Type':<15} {'Default':<8} Description")
                print("-" * 80)
                for p in presets:
                    default_mark = "*" if p['is_default'] else ""
                    print(f"{p['name']:<20} {p['type']:<15} {default_mark:<8} {p['description']}")
                print("\n* = default configuration")
            else:
                print("No configurations found. Run with --init to initialize.")
            return

        # 处理 --init
        if args.init:
            logger.info("Initializing table structures...")
            if calculator.ensure_tables():
                logger.info("Initialization completed successfully")
                presets = calculator.list_presets()
                print(f"\nInitialized {len(presets)} preset configurations:")
                for p in presets:
                    print(f"  - {p['name']}: {p['description']}")
            else:
                logger.error("Initialization failed")
                sys.exit(1)
            return

        # 处理自定义配置
        config_name = args.preset

        if args.custom_weights:
            if not args.custom_name:
                logger.error("--custom-name is required when using --custom-weights")
                sys.exit(1)

            try:
                weights = parse_weights(args.custom_weights)
            except ValueError as e:
                logger.error(str(e))
                sys.exit(1)

            if calculator.create_custom_config(
                args.custom_name,
                weights,
                description=f'Custom config created via CLI'
            ):
                config_name = args.custom_name
            else:
                sys.exit(1)

        # 执行计算
        logger.info(f"Calculating time-weighted independence scores for {args.date}")
        count = calculator.calc(args.date, config_name)

        if count < 0:
            sys.exit(1)

        if count == 0:
            logger.warning(f"No scores calculated for {args.date}")
        else:
            # 显示 Top 结果
            top_scores = calculator.get_top_scores(args.date, config_name, 20)
            print(f"\nTop 20 Time-Weighted Independence Scores for {args.date} (config: {config_name}):")
            print(f"{'Symbol':<12} {'Name':<12} {'Sector':<15} {'Raw':>8} {'Weighted':>10} {'Contra':>6}")
            print("-" * 75)
            for row in top_scores:
                symbol, name, sector, raw, weighted, contra = row
                name_display = (name or '')[:10]
                sector_display = (sector or '')[:14]
                print(f"{symbol:<12} {name_display:<12} {sector_display:<15} {raw:>8.2f} {weighted:>10.4f} {contra:>6}")

        logger.info("Calculation completed successfully")

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        calculator.close()


if __name__ == '__main__':
    main()
