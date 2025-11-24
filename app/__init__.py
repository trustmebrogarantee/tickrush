# app/__init__.py  (or wherever create_app lives)
import os
from flask import Flask
from config import config_map
from pathlib import Path
from .extensions import init_extensions, get_db, table_exists
from .blueprints.ticks.storage import table_name, ensure_table
from flask import jsonify

def create_app():
    app = Flask(__name__, instance_relative_config=True)

    config_name = os.getenv("FLASK_CONFIG", "development")
    app.config.from_object(config_map[config_name])

    # ------------------------------------------------------------------
    # 1. Ensure folders exist
    # ------------------------------------------------------------------
    os.makedirs(app.instance_path, exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    # ------------------------------------------------------------------
    # 2. Set DuckDB path in config (important!)
    # ------------------------------------------------------------------
    app.config.setdefault("DUCKDB_PATH", str(Path(app.instance_path) / "app.duckdb"))

    # ------------------------------------------------------------------
    # 3. Initialize extensions (your get_db() will create the file on first use)
    # ------------------------------------------------------------------
    import requests
    import zipfile
    from io import BytesIO
    init_extensions(app)        # ← does nothing now, but future-proof

    @app.route('/api/build-candles/<interval>')
    def build_candles(interval: str):
        """
        Build OHLCV candles from raw ticks with volume clusters.
        Supported intervals: 1m, 3m, 5m, 15m, 30m, 1h, 4h, 1d, etc.
        """
        db = get_db()
        ticks_table = table_name("binance_spot", "ETHUSDT")
        
        # Validate and convert interval to DuckDB syntax
        interval_map = {
            '1m': '1 minute',   '3m': '3 minutes',  '5m': '5 minutes',
            '15m': '15 minutes','30m': '30 minutes','1h': '1 hour',
            '4h': '4 hours',    '1d': '1 day',      '1w': '1 week'
        }
        if interval not in interval_map:
            return f"Unsupported interval: {interval}. Use: {', '.join(interval_map.keys())}", 400
        
        bucket_interval = interval_map[interval]
        candles_table = f"{ticks_table}_candles_{interval}"

        # Ensure ticks table exists
        if not table_exists(ticks_table):
            return f"Ticks table {ticks_table} not found. Load data first.", 404

        print(f"Building {interval} candles into table: {candles_table}")

        # For ETHUSDT, typical tick size is 0.01
        tick_size = 1

        db.execute(f'''
            CREATE OR REPLACE TABLE "{candles_table}" AS
            WITH candle_intervals AS (
                SELECT 
                    time_bucket(interval '{bucket_interval}', time) as open_time,
                    time_bucket(interval '{bucket_interval}', time) + interval '{bucket_interval}' as close_time
                FROM "{ticks_table}"
                GROUP BY open_time
            ),
            candle_ohlc AS (
                SELECT
                    time_bucket(interval '{bucket_interval}', time) as open_time,
                    ARG_MIN(price, time) AS open,
                    MAX(price) AS high,
                    MIN(price) AS low,
                    ARG_MAX(price, time) AS close,
                    SUM(qty) AS volume,
                    SUM(CASE WHEN is_buyer_maker THEN -qty ELSE qty END) AS delta,
                    SUM(delta) OVER (ORDER BY open_time) AS cvd
                FROM "{ticks_table}"
                GROUP BY open_time
            ),
            clusters_data AS (
                SELECT
                    time_bucket(interval '{bucket_interval}', t.time) as open_time,
                    ROUND(t.price / {tick_size}) * {tick_size} as cluster_price,
                    SUM(t.qty) as volume,
                    SUM(CASE WHEN NOT t.is_buyer_maker THEN t.qty ELSE 0 END) as ask,
                    SUM(CASE WHEN t.is_buyer_maker THEN t.qty ELSE 0 END) as bid,
                    SUM(CASE WHEN t.is_buyer_maker THEN -t.qty ELSE t.qty END) as delta
                FROM "{ticks_table}" t
                GROUP BY open_time, cluster_price
            ),
            clusters_aggregated AS (
                SELECT
                    open_time,
                    LIST([cluster_price, volume, ask, bid, delta] ORDER BY cluster_price DESC) as clusters_array
                FROM clusters_data
                GROUP BY open_time
            )
            SELECT 
                co.open_time,
                co.open,
                co.high,
                co.low,
                co.close,
                co.volume,
                co.delta,
                co.cvd,
                COALESCE(ca.clusters_array, []) as clusters
            FROM candle_ohlc co
            LEFT JOIN clusters_aggregated ca ON co.open_time = ca.open_time
            ORDER BY co.open_time;
        ''')

        result = db.execute(f'''
            SELECT 
                open_time,
                open,
                high, 
                low,
                close,
                volume,
                delta,
                clusters,
                cvd
            FROM "{candles_table}" 
            ORDER BY open_time
        ''').fetchall()

        # Convert to list of dictionaries
        candles = []
        for row in result:
            open_time, open_price, high, low, close, volume, delta, clusters, cvd = row
            
            # Convert clusters to Python list if it's not already
            if hasattr(clusters, '__iter__') and not isinstance(clusters, list):
                clusters_list = list(clusters)
            else:
                clusters_list = clusters or []
            
            candles.append({
                'time': open_time.isoformat() if hasattr(open_time, 'isoformat') else str(open_time),
                'open': float(open_price),
                'high': float(high),
                'low': float(low),
                'close': float(close),
                'volume': float(volume),
                'delta': float(delta),
                'cvd': float(cvd),
                'clusters': clusters_list
            })

        # Cluster map describing the array indices
        cluster_map = {
            'price': 0,
            'volume': 1,
            'ask': 2,
            'bid': 3,
            'delta': 4
        }

        return jsonify({
            'success': True,
            'interval': interval,
            'candles_count': len(candles),
            'cluster_map': cluster_map,
            'candles': candles
        })
    
     # a simple page that says hello
    @app.route('/api/update-ticks-data')
    def update_ticks ():
        db = get_db()
        table = table_name("binance_spot", "ETHUSDT")
        ensure_table("binance_spot", "ETHUSDT")

        def load_data():
            url = "https://data.binance.vision/data/spot/monthly/aggTrades/ETHUSDT/ETHUSDT-aggTrades-2025-10.zip"
            print("Strarting download...")
            resp = requests.get(url, timeout=360)
            print("Download complete.")
            if resp.status_code != 200:
                return "Failed to download data", 500        
            print("Starting extraction and insertion...")
            with zipfile.ZipFile(BytesIO(resp.content)) as z:
                csv_file = z.namelist()[0]
                with z.open(csv_file) as f:
                    import duckdb
                    df = duckdb.read_csv(f, header=False) # type: ignore
                    records = [
                        {"a": row[0], "p": row[1], "q": row[2], "T": row[5], "m": row[6]}
                        for row in df.fetchall()
                    ]

                    db.execute(f'''
                        INSERT INTO "{table}"
                        SELECT
                            rec.a AS id,
                            CAST(rec.p AS DOUBLE) AS price,
                            CAST(rec.q AS DOUBLE) AS qty,
                            make_timestamp(rec.t) as time,
                            rec.m AS is_buyer_maker
                        FROM unnest(?) AS t(rec)
                        ON CONFLICT (id) DO NOTHING
                ''', [records])
                print("Data insertion complete.")
        
        load_data()
        data = db.table(table).fetchall()

        return str(data)

    # ------------------------------------------------------------------
    # 4. Register blueprints (they will create tables when first accessed)
    # ------------------------------------------------------------------
    # from .blueprints.ticks import create_ticks_blueprint
    # app.register_blueprint(create_ticks_blueprint())



    return app