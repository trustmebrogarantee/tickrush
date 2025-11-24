import duckdb
from typing import Literal

def build_candles(
    con: duckdb.DuckDBPyConnection,
    ticks_table: str = "ethusdt_trades",
    candle_table: str = "candles_15m",
    timeframe: Literal["1m", "5m", "15m", "1h", "4h", "1d"] = "15m",
    tick_size: float = 0.01
):
    """
    Perfect OHLCV + volume profile (bid/ask) candles from Binance aggTrades.
    Works with `time` column in milliseconds since epoch.
    price_levels sorted descending (high to low).
    """

    bucket_seconds = {
        "1m": 60, "5m": 300, "15m": 900, "1h": 3600,
        "4h": 14400, "1d": 86400
    }[timeframe]

    # Use .format() instead of f-string to avoid any colon parsing issues
    sql = """
        CREATE OR REPLACE TABLE {candle_table} AS
        WITH buckets AS (
            SELECT
                time_bucket(INTERVAL '{bucket_seconds} seconds', time) AS ts,
                time,
                price,
                qty,
                is_buyer_maker,
                FLOOR(price / {tick_size}) * {tick_size} AS price_level
            FROM {ticks_table}
        ),
        ohlcv AS (
            SELECT
                ts,
                arg_min(price, time)                                   AS open,
                max(price)                                             AS high,
                min(price)                                             AS low,
                arg_max(price, time)                                   AS close,
                sum(qty)                                               AS volume,
                sum(qty * price)                                       AS quote_volume,
                sum(CASE WHEN is_buyer_maker THEN -qty ELSE qty END)   AS taker_buy_volume,
                sum(CASE WHEN is_buyer_maker THEN qty ELSE -qty END)   AS delta
            FROM buckets
            GROUP BY ts
        ),
        profile AS (
            SELECT
                ts,
                price_level,
                sum(CASE WHEN is_buyer_maker     THEN qty ELSE 0 END) AS bid_volume,
                sum(CASE WHEN NOT is_buyer_maker THEN qty ELSE 0 END) AS ask_volume,
                sum(qty) AS total_volume
            FROM buckets
            GROUP BY ts, price_level
        )
        SELECT
            o.ts,
            o.open,
            o.high,
            o.low,
            o.close,
            o.volume,
            o.quote_volume,
            o.taker_buy_volume,
            o.delta,
            list_sort(
                list(
                    struct_pack(
                        price        := price_level,
                        bid_volume   := bid_volume,
                        ask_volume   := ask_volume,
                        total_volume := total_volume
                    )
                ),
                (a, b) -> CASE WHEN a.price > b.price THEN -1 ELSE 1 END
            ) AS price_levels
        FROM ohlcv o
        LEFT JOIN profile p ON o.ts = p.ts
        GROUP BY 
            o.ts, o.open, o.high, o.low, o.close,
            o.volume, o.quote_volume, o.taker_buy_volume, o.delta
        ORDER BY o.ts
    """.format(
        candle_table=candle_table,
        ticks_table=ticks_table,
        bucket_seconds=bucket_seconds,
        tick_size=tick_size
    )

    con.execute(sql)
    return con.table(candle_table)