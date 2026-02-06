import json
import random
import string
import re
import requests
import websocket
import threading
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_session(prefix="cs_"):
    """Generates a random session ID."""
    return prefix + "".join(random.choices(string.ascii_lowercase + string.digits, k=12))

def prepend_header(st):
    """Prepends the protocol length header to a message."""
    return f"~m~{len(st)}~m~{st}"

def construct_message(m, p):
    """Constructs a JSON message with the required framing."""
    return prepend_header(json.dumps({"m": m, "p": p}))

def parse_messages(st):
    """Parses incoming WebSocket messages, handling heartbeats and multiple JSON packets."""
    if not st:
        return []
    if st.startswith("~h~"):
        return [{"type": "ping", "data": st}]

    # Split messages by the ~m~length~m~ delimiter
    messages = re.split(r"~m~\d+~m~", st)
    res = []
    for m in messages:
        if not m:
            continue
        try:
            res.append(json.loads(m))
        except json.JSONDecodeError:
            if m.startswith("~h~"):
                 res.append({"type": "ping", "data": m})
    return res

class TradingViewDataExtractor:
    def __init__(self, token="unauthorized_user_token"):
        self.ws_url = "wss://data.tradingview.com/socket.io/websocket?type=chart"
        self.ws = None
        self.token = token
        self.chart_session = generate_session("cs_")
        self.running = False
        self.ohlc = []
        self.indicator_data = {}
        self.error_occured = False

    def connect(self):
        """Establishes WebSocket connection and sends authentication token."""
        try:
            self.ws = websocket.create_connection(
                self.ws_url,
                header={"Origin": "https://www.tradingview.com"}
            )
            self.running = True
            self._send_auth()
            logger.info("Connected to TradingView WebSocket.")
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise

    def _send_auth(self):
        self.send("set_auth_token", [self.token])

    def create_chart_session(self):
        self.send("chart_create_session", [self.chart_session, ""])

    def resolve_symbol(self, symbol, series_id="s1"):
        symbol_payload = f"={json.dumps({'symbol': symbol, 'adjustment': 'splits'})}"
        self.send("resolve_symbol", [self.chart_session, series_id, symbol_payload])

    def create_series(self, series_id="s1", timeframe="1D", range=100):
        self.send("create_series", [self.chart_session, "$prices", "s1", series_id, timeframe, range])

    def create_study(self, study_id, indicator_metadata):
        """Adds an indicator (study) to the chart session."""
        inputs = {"text": indicator_metadata["script"]}
        if "pineId" in indicator_metadata:
            inputs["pineId"] = indicator_metadata["pineId"]
        if "pineVersion" in indicator_metadata:
            inputs["pineVersion"] = indicator_metadata["pineVersion"]

        for input_id, input_val in indicator_metadata.get("inputs", {}).items():
            inputs[input_id] = {
                "v": input_val.get("value"),
                "f": input_val.get("isFake", False),
                "t": input_val.get("type")
            }

        indicator_type = "Script@tv-scripting-101!"
        if indicator_metadata.get("type") == "strategy":
            indicator_type = "StrategyScript@tv-scripting-101!"

        self.send("create_study", [self.chart_session, study_id, "st1", "$prices", indicator_type, inputs])

    def get_indicator_metadata(self, indicator_id, version="last", session=None, signature=None):
        """Fetches indicator metadata from the Pine Facade API."""
        url = f"https://pine-facade.tradingview.com/pine-facade/translate/{indicator_id}/{version}"
        headers = {}
        if session:
            cookie = f"sessionid={session}"
            if signature:
                cookie += f";sessionid_sign={signature}"
            headers["Cookie"] = cookie

        response = requests.get(url, headers=headers)
        data = response.json()

        if not data.get("success"):
             raise Exception(f"Failed to get indicator metadata: {data.get('reason')}")

        result = data["result"]
        metaInfo = result["metaInfo"]

        inputs = {}
        for input_item in metaInfo.get("inputs", []):
            if input_item["id"] in ["text", "pineId", "pineVersion"]:
                continue
            inputs[input_item["id"]] = {
                "name": input_item["name"],
                "type": input_item["type"],
                "value": input_item.get("defval")
            }

        plots = {}
        for plot_id, style in metaInfo.get("styles", {}).items():
            plots[plot_id] = style["title"].replace(" ", "_")

        return {
            "pineId": metaInfo.get("scriptIdPart", indicator_id),
            "pineVersion": metaInfo.get("pine", {}).get("version", version),
            "description": metaInfo.get("description"),
            "inputs": inputs,
            "plots": plots,
            "script": result["ilTemplate"],
            "type": metaInfo.get("package", {}).get("type", "study")
        }

    def get_auth_token(self, session, signature=None):
        """Retrieves an auth token using session cookies."""
        url = "https://www.tradingview.com/"
        headers = {
            "Cookie": f"sessionid={session}" + (f";sessionid_sign={signature}" if signature else "")
        }
        try:
            response = requests.get(url, headers=headers, timeout=10)
            match = re.search(r'"auth_token":"(.*?)"', response.text)
            if match:
                return match.group(1)
        except Exception as e:
            logger.error(f"Auth token retrieval failed: {e}")
        return None

    def send(self, m, p):
        """Constructs and sends a message through the WebSocket."""
        msg = construct_message(m, p)
        if self.ws and self.ws.connected:
            self.ws.send(msg)

    def _handle_heartbeat(self, data):
        self.ws.send(prepend_header(data))

    def listen(self):
        """Main loop for listening to WebSocket messages."""
        while self.running:
            try:
                raw_data = self.ws.recv()
                msgs = parse_messages(raw_data)
                for msg in msgs:
                    if isinstance(msg, dict) and msg.get("type") == "ping":
                        self._handle_heartbeat(msg["data"])
                    else:
                        self.on_message(msg)
            except Exception as e:
                if self.running:
                    logger.error(f"WebSocket listening error: {e}")
                self.running = False

    def on_message(self, msg):
        """Dispatches incoming messages to appropriate data structures."""
        if not isinstance(msg, dict):
            return

        m_type = msg.get("m")
        p = msg.get("p", [])

        if m_type in ["timescale_update", "du"]:
            data = p[1]
            if "$prices" in data:
                prices = data["$prices"].get("s", [])
                for p_item in prices:
                    self.ohlc.append(p_item['v'])

            for key, val in data.items():
                if key.startswith("st"):
                    if "st" in val and val["st"]:
                        if key not in self.indicator_data:
                            self.indicator_data[key] = []
                        for st_item in val["st"]:
                            self.indicator_data[key].append(st_item["v"])

        elif m_type == "critical_error":
            logger.error(f"Critical error from server: {p}")
            self.error_occured = True
        elif m_type == "study_error":
            logger.error(f"Study error for {p[1]}: {p[3]}")
            self.error_occured = True

if __name__ == "__main__":
    # EXAMPLE USAGE
    # Replace these with actual credentials if available
    SESSION_ID = "YOUR_SESSION_ID"
    SESSION_SIGN = "YOUR_SESSION_SIGN"

    symbol = "BINANCE:BTCUSDT"
    indicator_id = "STD;RSI"

    extractor = TradingViewDataExtractor()

    try:
        # 1. Handle Authentication
        if SESSION_ID != "YOUR_SESSION_ID":
            logger.info("Attempting to login...")
            auth_token = extractor.get_auth_token(SESSION_ID, SESSION_SIGN)
            if auth_token:
                extractor.token = auth_token
                logger.info("Successfully authenticated.")
            else:
                logger.warning("Auth token retrieval failed. Proceeding with unauthorized token.")

        # 2. Fetch Metadata
        logger.info(f"Fetching metadata for {indicator_id}...")
        meta_session = SESSION_ID if SESSION_ID != "YOUR_SESSION_ID" else None
        meta_sign = SESSION_SIGN if SESSION_SIGN != "YOUR_SESSION_SIGN" else None
        meta = extractor.get_indicator_metadata(indicator_id, session=meta_session, signature=meta_sign)
        logger.info(f"Indicator Loaded: {meta['description']}")

        # 3. Connect and Start Listening
        extractor.connect()
        listener_thread = threading.Thread(target=extractor.listen, daemon=True)
        listener_thread.start()

        # 4. Set Up Chart
        logger.info(f"Setting up chart for {symbol}...")
        extractor.create_chart_session()
        time.sleep(1)
        extractor.resolve_symbol(symbol)
        time.sleep(1)
        extractor.create_series(timeframe="1D", range=50)
        time.sleep(2)

        # 5. Add Indicator
        study_id = generate_session("st_")
        logger.info(f"Adding indicator with ID {study_id}...")
        extractor.create_study(study_id, meta)

        # 6. Wait for Data
        logger.info("Awaiting data stream...")
        wait_start = time.time()
        while time.time() - wait_start < 15:
            if extractor.ohlc and study_id in extractor.indicator_data:
                break
            if extractor.error_occured:
                break
            time.sleep(1)

        # 7. Output Results
        if extractor.ohlc:
            logger.info(f"Extracted {len(extractor.ohlc)} OHLC bars.")
            print("\n--- OHLC (Last 5) ---")
            for bar in extractor.ohlc[-5:]:
                print(bar)

        if study_id in extractor.indicator_data:
            logger.info(f"Extracted {len(extractor.indicator_data[study_id])} indicator points.")
            print("\n--- Indicator Data (Last 5) ---")
            for point in extractor.indicator_data[study_id][-5:]:
                print(point)
        else:
            logger.warning("No indicator data received. This may require a valid session ID or different indicator.")

    except Exception as e:
        logger.error(f"Main execution failed: {e}")
    finally:
        extractor.running = False
        if extractor.ws:
            extractor.ws.close()
        logger.info("Process finished.")
