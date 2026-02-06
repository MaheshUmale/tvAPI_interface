import websocket
import json
import random
import string
import threading
import time
import logging
import re

logger = logging.getLogger(__name__)

class TradingViewWSS:
    def __init__(self, on_message_callback):
        self.callback = on_message_callback
        self.ws = None
        self.session = self._generate_session()
        self.quote_session = self._generate_session("qs_")
        self.symbols = []
        self.symbol_map = {
            'NSE:NIFTY': 'NIFTY',
            'NSE:BANKNIFTY': 'BANKNIFTY',
            'NSE:CNXFINANCE': 'FINNIFTY',
            'NSE:INDIAVIX': 'INDIA VIX'
        }
        self.last_volumes = {} # track cumulative volume per symbol
        self.last_prices = {} # track latest price per symbol
        self.last_times = {} # track latest time per symbol
        self.stop_event = threading.Event()
        self.thread = None

    def _generate_session(self, prefix=""):
        return prefix + "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(12))

    def _prepend_header(self, message):
        return f"~m~{len(message)}~m~{message}"

    def _construct_message(self, func, param_list):
        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    def _send_message(self, func, param_list):
        if not self.ws or not self.ws.sock or not self.ws.sock.connected:
            return
        message = self._construct_message(func, param_list)
        payload = self._prepend_header(message)
        try:
            self.ws.send(payload)
        except Exception as e:
            logger.error(f"Error sending message to TV WSS: {e}")

    def subscribe(self, symbols):
        symbols = [s.upper() for s in symbols]
        new_symbols = [s for s in symbols if s not in self.symbols]
        self.symbols.extend(new_symbols)
        if self.ws and self.ws.sock and self.ws.sock.connected:
            self._subscribe_symbols(new_symbols)

    def _subscribe_symbols(self, symbols):
        if not symbols: return
        for symbol in symbols:
            # Simplified subscribe call
            self._send_message("quote_add_symbols", [self.quote_session, symbol])
        logger.info(f"Subscribed to {len(symbols)} symbols on TV WSS")

    def on_open(self, ws):
        logger.info("TV WSS Connection opened")
        self._send_message("set_auth_token", ["unauthorized_user_token"])
        self._send_message("quote_create_session", [self.quote_session])
        self._send_message("quote_set_fields", [self.quote_session, "lp", "lp_time", "volume"])
        if self.symbols:
            self._subscribe_symbols(self.symbols)

    def on_message(self, ws, message):
        if isinstance(message, bytes):
            message = message.decode('utf-8')

        # logger.debug(f"RAW TV WSS: {message[:100]}...")

        # Heartbeat check - TV WSS sends ~m~<len>~m~~h~<num>
        if "~h~" in message:
            try:
                # Reply with the exact same heartbeat message
                ws.send(message)
                logger.debug(f"TV WSS Heartbeat responded: {message}")
            except Exception as e:
                logger.error(f"Error sending heartbeat: {e}")
            return

        # Split multiple messages in one frame
        messages = re.split(r"~m~\d+~m~", message)
        for msg in messages:
            if not msg: continue
            try:
                data = json.loads(msg)
                if data.get("m") == "qsd" and len(data["p"]) > 1:
                    quote_data = data["p"][1]
                    symbol = quote_data["n"]
                    # Handle TradingView symbol prefix (e.g., =NSE:NIFTY)
                    clean_symbol = symbol[1:] if symbol.startswith('=') else symbol

                    logger.debug(f"TV WSS Tick for {clean_symbol}: {quote_data}")
                    values = quote_data.get("v", {})

                    hrn = self.symbol_map.get(clean_symbol, clean_symbol)

                    # Update tracked values
                    if 'lp' in values: self.last_prices[clean_symbol] = values['lp']
                    if 'last_price' in values: self.last_prices[clean_symbol] = values['last_price']
                    if 'lp_time' in values: self.last_times[clean_symbol] = values['lp_time']
                    if 'volume' in values: self.last_volumes[clean_symbol] = float(values['volume'])

                    price = self.last_prices.get(clean_symbol)
                    if price is not None:
                        ts_ms = int(self.last_times.get(clean_symbol, time.time()) * 1000)
                        current_cum_vol = self.last_volumes.get(clean_symbol)

                        # Determine if it's an index or market feed
                        feed_type = 'marketFF'
                        if 'VIX' in hrn.upper() or ('NIFTY' in hrn.upper() and ' ' not in hrn):
                            feed_type = 'indexFF'

                        feed_msg = {
                            'type': 'live_feed',
                            'feeds': {
                                hrn: {
                                    'fullFeed': {
                                        feed_type: {
                                            'ltpc': {
                                                'ltp': str(price),
                                                'ltt': str(ts_ms),
                                                'ltq': '0'
                                            },
                                            'oi': str(values.get('open_interest', 0))
                                        }
                                    },
                                    'tv_volume': current_cum_vol,
                                    'source': 'tradingview_wss'
                                }
                            }
                        }
                        self.callback(feed_msg)
            except Exception as e:
                pass

    def on_error(self, ws, error):
        logger.error(f"TV WSS Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.info(f"TV WSS Connection closed: {close_status_code} - {close_msg}")
        if not self.stop_event.is_set():
            logger.info("TV WSS reconnecting in 5 seconds...")
            time.sleep(5)
            self.start()

    def start(self):
        self.stop_event.clear()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Origin": "https://www.tradingview.com"
        }
        self.ws = websocket.WebSocketApp(
            "wss://data.tradingview.com/socket.io/websocket",
            header=headers,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        self.thread = threading.Thread(
            target=self.ws.run_forever,
            kwargs={
                "skip_utf8_validation": True,
                "ping_interval": 20,
                "ping_timeout": 10
            },
            daemon=True
        )
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.ws:
            self.ws.close()

tv_wss = None

def start_tv_wss(on_message_callback, symbols=None):
    global tv_wss
    if tv_wss is None:
        tv_wss = TradingViewWSS(on_message_callback)
        if symbols:
            tv_wss.subscribe(symbols)
        tv_wss.start()
    return tv_wss
