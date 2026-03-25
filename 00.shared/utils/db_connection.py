"""Shared database connection utilities."""

import os
from typing import Optional
import psycopg2
from clickhouse_driver import Client


def get_clickhouse_client(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None
) -> Client:
    """Get ClickHouse client with environment fallback."""
    return Client(
        host=host or os.getenv('CH_HOST', 'localhost'),
        port=port or int(os.getenv('CH_PORT', '9000')),
        database=database or os.getenv('CH_DB', 'tdx2db_rust'),
        user=user or os.getenv('CH_USER', 'default'),
        password=password or os.getenv('CH_PASSWORD', ''),
    )


def get_postgres_connection(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None
):
    """Get PostgreSQL connection with environment fallback."""
    return psycopg2.connect(
        host=host or os.getenv('PG_HOST', 'localhost'),
        port=port or int(os.getenv('PG_PORT', '5432')),
        database=database or os.getenv('PG_DB', 'quantdb'),
        user=user or os.getenv('PG_USER', 'postgres'),
        password=password or os.getenv('PG_PASSWORD', ''),
    )
