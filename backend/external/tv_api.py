import logging
try:
    from tvDatafeed import TvDatafeed, Interval
except ImportError:
    TvDatafeed = None
    Interval = None
from tradingview_scraper.symbols.stream import Streamer
import logging
import os
import contextlib
import io
import time

from datetime import datetime
import re

def convert_hrn_to_symbol(symbol_or_hrn):
    # Match pattern: "NIFTY 03 FEB 2026 CALL 25300"
    pattern = r"(NIFTY|BANKNIFTY|FINNIFTY)\s+(\d{1,2})\s+([A-Z]{3})\s+(\d{4})\s+(CALL|PUT)\s+(\d+)"
    match = re.search(pattern, symbol_or_hrn.upper())

    if match:
        base, day, month_str, year, opt_type, strike = match.groups()
        dt = datetime.strptime(f"{day} {month_str} {year}", "%d %b %Y")
        yy = dt.strftime("%y")
        mm = dt.strftime("%m")
        dd = dt.strftime("%d")
        cp = "C" if opt_type == "CALL" else "P"
        return f"{base}{yy}{mm}{dd}{cp}{strike}"
    return symbol_or_hrn

logger = logging.getLogger(__name__)

class TradingViewAPI:
    def __init__(self):
        username = os.getenv('TV_USERNAME')
        password = os.getenv('TV_PASSWORD')
        if TvDatafeed:
            self.tv = TvDatafeed(username, password) if username and password else TvDatafeed()
            logger.info("TradingViewAPI initialized with tvDatafeed")
        else:
            self.tv = None
            logger.warning("tvDatafeed not installed, falling back to Streamer only")

        self.streamer = Streamer()
        self.symbol_map = {
            'NIFTY': {'symbol': 'NIFTY', 'exchange': 'NSE'},
            'BANKNIFTY': {'symbol': 'BANKNIFTY', 'exchange': 'NSE'},
            'FINNIFTY': {'symbol': 'CNXFINANCE', 'exchange': 'NSE'},
            'INDIA VIX': {'symbol': 'INDIAVIX', 'exchange': 'NSE'}
        }

    def get_hist_candles(self, symbol_or_hrn, interval_min='1', n_bars=1000):
        try:
            logger.info(f"Fetching historical candles for {symbol_or_hrn}")
            if not symbol_or_hrn: return None

            tv_symbol = symbol_or_hrn
            tv_exchange = 'NSE'

            if ':' in symbol_or_hrn:
                parts = symbol_or_hrn.split(':')
                tv_exchange = parts[0].upper()
                tv_symbol = parts[1].upper()
                symbol_or_hrn = tv_symbol

            if symbol_or_hrn in self.symbol_map:
                meta = self.symbol_map[symbol_or_hrn]
                tv_symbol = meta['symbol']
                tv_exchange = meta['exchange']
            elif any(x in symbol_or_hrn.upper() for x in ['CALL', 'PUT']):
                tv_symbol = convert_hrn_to_symbol(symbol_or_hrn)
            elif 'NIFTY' in symbol_or_hrn.upper() and ' ' not in symbol_or_hrn:
                tv_symbol = 'NIFTY'
            elif 'BANK' in symbol_or_hrn.upper() and ' ' not in symbol_or_hrn:
                tv_symbol = 'BANKNIFTY'

            # Try Streamer first
            try:
                tf = f"{interval_min}m"
                if interval_min == 'D': tf = '1d'
                elif interval_min == 'W': tf = '1w'

                with contextlib.redirect_stdout(io.StringIO()):
                    stream_gen = self.streamer.stream(
                        exchange=tv_exchange,
                        symbol=tv_symbol,
                        timeframe=tf,
                        numb_price_candles=n_bars
                    )

                    # Consume generator to find OHLC data
                    data = None
                    start_time = time.time()
                    for update in stream_gen:
                        if isinstance(update, dict) and 'ohlc' in update:
                            data = update
                            break
                        if time.time() - start_time > 10: # Timeout
                            break

                if data and 'ohlc' in data:
                    candles = []
                    for row in data['ohlc']:
                        # Ensure we have a unix timestamp or ISO string that can be parsed
                        ts = row['datetime']
                        if isinstance(ts, (int, float)):
                            pass # already unix
                        else:
                            # If it's a string, try to parse it.
                            # Streamer usually returns timestamps or ISO
                            try:
                                ts = int(datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp())
                            except:
                                pass

                        candles.append([
                            ts,
                            float(row['open']), float(row['high']), float(row['low']), float(row['close']),
                            float(row['volume'])
                        ])
                    logger.info(f"Retrieved {len(candles)} candles via Streamer")
                    return candles[::-1] # Newest first
            except Exception as e:
                logger.warning(f"Streamer failed for {tv_symbol}: {e}")

            # Fallback to tvDatafeed
            if self.tv:
                tv_interval = Interval.in_1_minute
                if interval_min == '5': tv_interval = Interval.in_5_minute
                elif interval_min == '15': tv_interval = Interval.in_15_minute
                elif interval_min == '30': tv_interval = Interval.in_30_minute
                elif interval_min == '60': tv_interval = Interval.in_1_hour
                elif interval_min == 'D': tv_interval = Interval.in_daily
                elif interval_min == 'W': tv_interval = Interval.in_weekly

                df = self.tv.get_hist(symbol=tv_symbol, exchange=tv_exchange, interval=tv_interval, n_bars=n_bars)
                if df is not None and not df.empty:
                    candles = []
                    for ts, row in df.iterrows():
                        # tvDatafeed returns naive datetime in exchange timezone (usually IST for NSE)
                        # We need to treat it as IST and get UTC timestamp
                        import pytz
                        ist = pytz.timezone('Asia/Kolkata')
                        try:
                            ts_ist = ist.localize(ts) if ts.tzinfo is None else ts.astimezone(ist)
                            unix_ts = int(ts_ist.timestamp())
                        except:
                            unix_ts = int(ts.timestamp())

                        candles.append([
                            unix_ts,
                            float(row['open']), float(row['high']), float(row['low']), float(row['close']),
                            float(row['volume'])
                        ])
                    logger.info(f"Retrieved {len(candles)} candles via tvDatafeed")
                    return candles[::-1]

            return None
        except Exception as e:
            logger.error(f"Error fetching TradingView data: {e}")
            return None

tv_api = TradingViewAPI()
