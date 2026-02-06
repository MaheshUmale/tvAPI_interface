"""
DuckDB-based Local Database implementation.
Provides an optimized columnar data store for high-frequency tick data.
"""
import duckdb
import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import threading
import pandas as pd

logger = logging.getLogger(__name__)

class LocalDBJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime): return obj.isoformat()
        return super().default(obj)

DB_PATH = os.getenv('DUCKDB_PATH', 'pro_trade.db')

class LocalDB:
    _instance = None
    _singleton_lock = threading.Lock()
    _execute_lock = threading.Lock()
    _batch_count = 0

    def __new__(cls):
        with cls._singleton_lock:
            if cls._instance is None:
                cls._instance = super(LocalDB, cls).__new__(cls)
                cls._instance._init_db()
        return cls._instance

    def _init_db(self):
        self.conn = duckdb.connect(DB_PATH)
        self.conn.execute("SET memory_limit = '1GB'")
        self.conn.execute("SET threads = 4")
        self.conn.execute("INSTALL json; LOAD json;")

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ticks (
                date DATE,
                instrumentKey VARCHAR,
                ts_ms BIGINT,
                price DOUBLE,
                qty BIGINT,
                source VARCHAR,
                full_feed JSON
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_ticks_key_ts ON ticks (instrumentKey, ts_ms)")

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                instrument_key VARCHAR PRIMARY KEY,
                hrn VARCHAR,
                meta JSON,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info(f"Local DuckDB initialized at {DB_PATH}")

    def insert_ticks(self, ticks: List[Dict[str, Any]]):
        if not ticks: return
        data = []
        for t in ticks:
            price = t.get('last_price', 0)
            qty = t.get('ltq', 0)
            data.append({
                'date': t.get('date', datetime.now().strftime('%Y-%m-%d')),
                'instrumentKey': t.get('instrumentKey'),
                'ts_ms': int(t.get('ts_ms', 0)),
                'price': float(price),
                'qty': int(qty),
                'source': t.get('source', 'live'),
                'full_feed': json.dumps(t, cls=LocalDBJSONEncoder)
            })

        df = pd.DataFrame(data)
        with self._execute_lock:
            self.conn.execute("INSERT INTO ticks SELECT * FROM df")
            self._batch_count += 1
            if self._batch_count >= 10:
                self.conn.execute("CHECKPOINT")
                self._batch_count = 0

    def update_metadata(self, instrument_key: str, hrn: str, meta: Dict[str, Any]):
        meta_json = json.dumps(meta)
        with self._execute_lock:
            self.conn.execute("""
                INSERT OR REPLACE INTO metadata (instrument_key, hrn, meta, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (instrument_key, hrn, meta_json))

    def get_metadata(self, instrument_key: str) -> Optional[Dict[str, Any]]:
        with self._execute_lock:
            res = self.conn.execute("SELECT hrn, meta FROM metadata WHERE instrument_key = ?", (instrument_key,)).fetchone()
        if res: return {'hrn': res[0], 'metadata': json.loads(res[1])}
        return None

    def query(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        with self._execute_lock:
            df = self.conn.execute(sql, params).fetch_df()
        return df.to_dict('records')

db = LocalDB()
