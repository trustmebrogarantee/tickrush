# app/blueprints/ticks/tasks.py
import threading
import time
import json
import websocket
from flask import current_app
from app.extensions import get_db
from .storage import table_name, insert_trades
from .binance.downloader import backfill_day
from datetime import date, timedelta

SYMBOLS = {
    "binance_spot": ["BTCUSDT", "ETHUSDT"],
    "binance_futures": ["BTCUSDT", "ETHUSDT"],
}

def start_background_sync():
    threading.Thread(target=historical_worker, daemon=True).start()
    threading.Thread(target=websocket_worker, daemon=True).start()
    current_app.logger.info("Ticks background sync started")

def historical_worker():
    while True:
        for market, symbols in SYMBOLS.items():
            for symbol in symbols:
                table = table_name(market, symbol) # type: ignore
                result = get_db().execute(f'SELECT MAX(time) FROM "{table}"').fetchone()
                last_ts = result[0] if result and result[0] else None
                start_date = date(2024, 1, 1) if not last_ts else last_ts.date() + timedelta(days=1)
                today = date.today()

                current = start_date
                while current < today:
                    count = backfill_day(market, symbol, current) # type: ignore
                    if count:
                        current_app.logger.info(f"Backfilled {market} {symbol} {current}: {count} trades")
                    current += timedelta(days=1)
                    time.sleep(0.05)
        time.sleep(3600)  # run once per hour

def on_ws_message(ws, message, market: str, symbol: str):
    data = json.loads(message)
    if "data" in data:
        insert_trades(market, symbol, [data["data"]]) # type: ignore

def websocket_worker():
    def connect_one(market: str, symbol: str):
        stream = f"{symbol.lower()}@aggTrade"
        base = "wss://stream.binance.com:9443/ws" if market == "binance_spot" else "wss://fstream.binance.com/ws"
        url = f"{base}/{stream}"

        def on_msg(ws, msg): on_ws_message(ws, msg, market, symbol)
        ws = websocket.WebSocketApp(url, on_message=on_msg)
        ws.run_forever(ping_interval=30)

    for market, symbols in SYMBOLS.items():
        for sym in symbols:
            threading.Thread(target=connect_one, args=(market, sym), daemon=True).start()