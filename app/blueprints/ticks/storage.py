# app/blueprints/ticks/storage.py
from app.extensions import get_db
from typing import Literal

Market = Literal["binance_spot", "binance_futures"]

def table_name(market: Market, symbol: str) -> str:
    return f"{market}_{symbol.lower()}"

def ensure_table(market: Market, symbol: str) -> None:
    table = table_name(market, symbol)
    get_db().execute(f'''
        CREATE TABLE IF NOT EXISTS "{table}" (
            id  BIGINT PRIMARY KEY,
            price           DOUBLE,
            qty             DOUBLE,
            time         TIMESTAMP,
            is_buyer_maker  BOOLEAN
        )
    ''')

def insert_trades(market: Market, symbol: str, trades: list[dict]) -> None:
    if not trades:
        return
    ensure_table(market, symbol)
    table = table_name(market, symbol)

    get_db().execute(f'''
        INSERT INTO "{table}"
        SELECT
            a AS id,
            CAST(p AS DOUBLE) AS price,
            CAST(q AS DOUBLE) AS qty,
            T AS time,
            m AS is_buyer_maker
        FROM unnest(?)
        ON CONFLICT (id) DO NOTHING
    ''', [trades])