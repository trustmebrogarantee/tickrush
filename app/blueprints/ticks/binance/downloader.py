# app/blueprints/ticks/binance/downloader.py
import requests
import zipfile
from io import BytesIO
from datetime import date
from ..storage import insert_trades
from typing import Literal

Market = Literal["binance_spot", "binance_futures"]

BASE_URLS = {
    "binance_spot": "https://data.binance.vision/data/spot/daily/trades",
    "binance_futures": "https://data.binance.vision/data/futures/um/daily/trades",
}

def backfill_day(market: Market, symbol: str, dt: date) -> int:
    url = f"{BASE_URLS[market]}/{symbol}/{symbol}-trades-{dt.strftime('%Y-%m-%d')}.zip"
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        return 0

    with zipfile.ZipFile(BytesIO(resp.content)) as z:
        csv_file = z.namelist()[0]
        with z.open(csv_file) as f:
            import duckdb
            df = duckdb.read_csv(f, header=False) # type: ignore
            records = [
                {
                    "a": row[0],
                    "p": row[1],
                    "q": row[2],
                    "T": row[4] * 1000,   # to ms
                    "m": row[5]
                }
                for row in df.fetchall()
            ]
            insert_trades(market, symbol, records)
            return len(records)