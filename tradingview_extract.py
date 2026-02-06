import json
import random
import string
import re
import requests
import websocket
import threading
import time
import logging

try:
    import rookiepy
except ImportError:
    rookiepy = None

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

def safe_get(data, keys, default=None):
    """Safely access nested dictionary keys."""
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return default
    return data if data is not None else default

GRAPHIC_TRANSLATOR = {
    'extend': { 'r': 'right', 'l': 'left', 'b': 'both', 'n': 'none' },
    'yLoc': { 'pr': 'price', 'ab': 'abovebar', 'bl': 'belowbar' },
    'labelStyle': {
        'n': 'none', 'xcr': 'xcross', 'cr': 'cross', 'tup': 'triangleup',
        'tdn': 'triangledown', 'flg': 'flag', 'cir': 'circle', 'aup': 'arrowup',
        'adn': 'arrowdown', 'lup': 'label_up', 'ldn': 'label_down', 'llf': 'label_left',
        'lrg': 'label_right', 'llwlf': 'label_lower_left', 'llwrg': 'label_lower_right',
        'luplf': 'label_upper_left', 'luprg': 'label_upper_right', 'lcn': 'label_center',
        'sq': 'square', 'dia': 'diamond',
    },
    'lineStyle': {
        'sol': 'solid', 'dot': 'dotted', 'dsh': 'dashed',
        'al': 'arrow_left', 'ar': 'arrow_right', 'ab': 'arrow_both',
    },
    'boxStyle': { 'sol': 'solid', 'dot': 'dotted', 'dsh': 'dashed' },
}

def parse_graphic_data(raw_graphic, indexes):
    """Parses raw graphical data into a readable format."""
    res = {
        'labels': [], 'lines': [], 'boxes': [], 'tables': [],
        'polygons': [], 'horizLines': [], 'horizHists': []
    }

    # Labels
    for l in raw_graphic.get('dwglabels', {}).values():
        x_pos = l.get('x')
        res['labels'].append({
            'id': l.get('id'),
            'x': indexes[x_pos] if isinstance(x_pos, int) and x_pos < len(indexes) else x_pos,
            'y': l.get('y'),
            'yLoc': GRAPHIC_TRANSLATOR['yLoc'].get(l.get('yl'), l.get('yl')),
            'text': l.get('t'),
            'style': GRAPHIC_TRANSLATOR['labelStyle'].get(l.get('st'), l.get('st')),
            'color': l.get('ci'),
            'textColor': l.get('tci'),
            'size': l.get('sz'),
            'textAlign': l.get('ta'),
            'toolTip': l.get('tt'),
        })

    # Lines
    for l in raw_graphic.get('dwglines', {}).values():
        x1_pos = l.get('x1')
        x2_pos = l.get('x2')
        res['lines'].append({
            'id': l.get('id'),
            'x1': indexes[x1_pos] if isinstance(x1_pos, int) and x1_pos < len(indexes) else x1_pos,
            'y1': l.get('y1'),
            'x2': indexes[x2_pos] if isinstance(x2_pos, int) and x2_pos < len(indexes) else x2_pos,
            'y2': l.get('y2'),
            'extend': GRAPHIC_TRANSLATOR['extend'].get(l.get('ex'), l.get('ex')),
            'style': GRAPHIC_TRANSLATOR['lineStyle'].get(l.get('st'), l.get('st')),
            'color': l.get('ci'),
            'width': l.get('w'),
        })

    # Boxes
    for b in raw_graphic.get('dwgboxes', {}).values():
        x1_pos = b.get('x1')
        x2_pos = b.get('x2')
        res['boxes'].append({
            'id': b.get('id'),
            'x1': indexes[x1_pos] if isinstance(x1_pos, int) and x1_pos < len(indexes) else x1_pos,
            'y1': b.get('y1'),
            'x2': indexes[x2_pos] if isinstance(x2_pos, int) and x2_pos < len(indexes) else x2_pos,
            'y2': b.get('y2'),
            'color': b.get('c'),
            'bgColor': b.get('bc'),
            'extend': GRAPHIC_TRANSLATOR['extend'].get(b.get('ex'), b.get('ex')),
            'style': GRAPHIC_TRANSLATOR['boxStyle'].get(b.get('st'), b.get('st')),
            'width': b.get('w'),
            'text': b.get('t'),
            'textSize': b.get('ts'),
            'textColor': b.get('tc'),
            'textVAlign': b.get('tva'),
            'textHAlign': b.get('tha'),
            'textWrap': b.get('tw'),
        })

    return res

def get_brave_cookies():
    """Extracts required cookies from Brave browser using rookiepy."""
    if not rookiepy:
        logger.error("rookiepy not installed. Cannot extract cookies from Brave automatically.")
        return None
    try:
        logger.info("Extracting cookies from Brave...")
        cookies = rookiepy.brave(['.tradingview.com'])
        return cookies
    except Exception as e:
        logger.error(f"Failed to extract cookies from Brave: {e}")
        return None

class TradingViewDataExtractor:
    def __init__(self, token="unauthorized_user_token"):
        self.ws_url = "wss://data.tradingview.com/socket.io/websocket?type=chart"
        self.ws = None
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        self.token = token
        self.chart_session = generate_session("cs_")
        self.running = False
        self.ohlc = []
        self.indicator_data = {}
        self.graphics_raw = {}
        self.graphics_indexes = []
        self.error_occurred = False

    def set_cookies(self, cookies):
        """Sets cookies for the session. Supports dict, list of dicts, or CookieJar."""
        if not cookies:
            return
        if isinstance(cookies, list):
            for c in cookies:
                self.session.cookies.set(c.get('name'), c.get('value'), domain=c.get('domain', '.tradingview.com'), path=c.get('path', '/'))
        elif isinstance(cookies, dict):
            for name, value in cookies.items():
                self.session.cookies.set(name, value, domain='.tradingview.com', path='/')
        else:
            # Assume it's a CookieJar or similar
            self.session.cookies.update(cookies)

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

    def create_study(self, study_id, indicator_metadata, custom_inputs=None):
        """Adds an indicator (study) to the chart session."""
        inputs = {"text": indicator_metadata["script"]}
        if "pineId" in indicator_metadata:
            inputs["pineId"] = indicator_metadata["pineId"]
        if "pineVersion" in indicator_metadata:
            inputs["pineVersion"] = indicator_metadata["pineVersion"]

        final_inputs = indicator_metadata.get("inputs", {}).copy()
        if custom_inputs:
            for k, v in custom_inputs.items():
                if k in final_inputs:
                    final_inputs[k]["value"] = v

        for input_id, input_val in final_inputs.items():
            inputs[input_id] = {
                "v": input_val.get("value"),
                "f": input_val.get("isFake", False),
                "t": input_val.get("type")
            }

        indicator_type = "Script@tv-scripting-101!"
        if indicator_metadata.get("type") == "strategy":
            indicator_type = "StrategyScript@tv-scripting-101!"

        self.send("create_study", [self.chart_session, study_id, "st1", "$prices", indicator_type, inputs])

    def get_indicator_metadata(self, indicator_id, version="last"):
        """Fetches indicator metadata from the Pine Facade API."""
        url = f"https://pine-facade.tradingview.com/pine-facade/translate/{indicator_id}/{version}"

        response = self.session.get(url)

        try:
            data = response.json()
        except Exception as e:
            logger.error(f"Failed to parse metadata JSON: {e}")
            raise

        if not isinstance(data, dict) or not data.get("success"):
             reason = data.get("reason") if isinstance(data, dict) else "Unknown error"
             raise Exception(f"Failed to get indicator metadata: {reason}")

        result = data.get("result", {})
        metaInfo = result.get("metaInfo", {})

        inputs = {}
        meta_inputs = metaInfo.get("inputs")
        if isinstance(meta_inputs, list):
            for input_item in meta_inputs:
                if not isinstance(input_item, dict): continue
                input_id = input_item.get("id")
                if input_id in ["text", "pineId", "pineVersion"]: continue
                inputs[input_id] = {
                    "name": input_item.get("name"),
                    "type": input_item.get("type"),
                    "value": input_item.get("defval"),
                    "isFake": input_item.get("isFake", False)
                }

        plots = {}
        meta_styles = metaInfo.get("styles")
        if isinstance(meta_styles, dict):
            for plot_id, style in meta_styles.items():
                if isinstance(style, dict) and "title" in style:
                    plots[plot_id] = style["title"].replace(" ", "_")

        package_type = safe_get(metaInfo, ["package", "type"])
        extra_kind = safe_get(metaInfo, ["extra", "kind"])
        indicator_type = extra_kind or package_type or "study"

        return {
            "pineId": metaInfo.get("scriptIdPart", indicator_id),
            "pineVersion": safe_get(metaInfo, ["pine", "version"], version),
            "description": metaInfo.get("description"),
            "inputs": inputs,
            "plots": plots,
            "script": result.get("ilTemplate"),
            "type": indicator_type
        }

    def get_user_data(self):
        """Retrieves user data including auth_token and user_id using session cookies."""
        url = "https://www.tradingview.com/"
        try:
            response = self.session.get(url, timeout=15)

            auth_token = re.search(r'"auth_token":"(.*?)"', response.text)
            user_id = re.search(r'"id":([0-9]{1,10}),', response.text)

            return {
                "auth_token": auth_token.group(1) if auth_token else None,
                "user_id": user_id.group(1) if user_id else None,
                "username": re.search(r'"username":"(.*?)"', response.text).group(1) if re.search(r'"username":"(.*?)"', response.text) else None
            }
        except Exception as e:
            logger.error(f"User data retrieval failed: {e}")
        return None

    def get_private_indicators(self):
        """Fetches all private (saved) indicators for the user."""
        url = "https://pine-facade.tradingview.com/pine-facade/list"
        params = {"filter": "saved"}
        try:
            response = self.session.get(url, params=params)
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch private indicators: {e}")
            return []

    def list_layouts(self):
        """Lists all chart layouts for the user."""
        url = "https://www.tradingview.com/chart-storage-v2/charts/"
        try:
            response = self.session.get(url)
            return response.json()
        except Exception as e:
            logger.error(f"Failed to list layouts: {e}")
            return []

    def get_chart_token(self, layout_id, user_id):
        """Retrieves a chart token for a specific layout."""
        url = "https://www.tradingview.com/chart-token"
        params = {"image_url": layout_id, "user_id": user_id}
        try:
            response = self.session.get(url, params=params)
            return response.json().get("token")
        except Exception as e:
            logger.error(f"Failed to get chart token: {e}")
            return None

    def get_layout_sources(self, layout_id, chart_token):
        """Fetches all sources (indicators/drawings) in a layout."""
        url = f"https://charts-storage.tradingview.com/charts-storage/get/layout/{layout_id}/sources"
        params = {"chart_id": "_shared", "jwt": chart_token}
        try:
            response = self.session.get(url, params=params)
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch layout sources: {e}")
            return {}

    def send(self, m, p):
        """Constructs and sends a message through the WebSocket."""
        msg = construct_message(m, p)
        if self.ws and self.ws.connected:
            self.ws.send(msg)

    def _handle_heartbeat(self, data):
        self.ws.send(prepend_header(data))

    def get_mapped_indicator_data(self, study_id, indicator_metadata):
        """Maps raw indicator data to plot names."""
        raw_data = self.indicator_data.get(study_id, [])
        if not raw_data: return []

        plot_names = ["timestamp"] + list(indicator_metadata.get("plots", {}).values())
        mapped_data = []
        for row in raw_data:
            mapped_row = {}
            for i, val in enumerate(row):
                if i < len(plot_names): mapped_row[plot_names[i]] = val
                else: mapped_row[f"plot_{i-1}"] = val
            mapped_data.append(mapped_row)
        return mapped_data

    def get_indicator_graphics(self, study_id):
        """Returns parsed graphical drawings for the specified study."""
        raw_graphic = self.graphics_raw.get(study_id, {})
        return parse_graphic_data(raw_graphic, self.graphics_indexes)

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
                if self.running: logger.error(f"WebSocket listening error: {e}")
                self.running = False

    def on_message(self, msg):
        """Dispatches incoming messages to appropriate data structures."""
        if not isinstance(msg, dict): return
        m_type = msg.get("m")
        p = msg.get("p", [])

        if m_type in ["timescale_update", "du"]:
            data = p[1]
            if "$prices" in data:
                prices = data["$prices"].get("s", [])
                for p_item in prices: self.ohlc.append(p_item['v'])

            for key, val in data.items():
                if not isinstance(val, dict): continue
                if key.startswith("st"):
                    if "st" in val and val["st"]:
                        if key not in self.indicator_data: self.indicator_data[key] = []
                        for st_item in val["st"]: self.indicator_data[key].append(st_item["v"])

                    ns = val.get("ns")
                    if isinstance(ns, dict):
                        if "indexes" in ns and ns["indexes"] != "nochange":
                            self.graphics_indexes = ns["indexes"]
                        if "d" in ns and ns["d"]:
                            try:
                                ns_data = json.loads(ns["d"])
                                graphics_cmds = ns_data.get("graphicsCmds")
                                if graphics_cmds:
                                    if key not in self.graphics_raw: self.graphics_raw[key] = {}
                                    for erase in graphics_cmds.get("erase", []):
                                        action = erase.get("action")
                                        draw_type = erase.get("type")
                                        if action == "all":
                                            if draw_type: self.graphics_raw[key][draw_type] = {}
                                            else: self.graphics_raw[key] = {}
                                        elif action == "one":
                                            if draw_type in self.graphics_raw[key]: self.graphics_raw[key][draw_type].pop(erase.get("id"), None)
                                    create = graphics_cmds.get("create", {})
                                    for draw_type, groups in create.items():
                                        if draw_type not in self.graphics_raw[key]: self.graphics_raw[key][draw_type] = {}
                                        for group in groups:
                                            for item in group.get("data", []): self.graphics_raw[key][draw_type][item["id"]] = item
                            except Exception as e: logger.error(f"Failed to parse graphical data: {e}")

        elif m_type == "critical_error":
            logger.error(f"Critical error from server: {p}")
            self.error_occurred = True
        elif m_type == "study_error":
            logger.error(f"Study error for {p[1]}: {p[3]}")
            self.error_occurred = True

if __name__ == "__main__":
    # 1. Setup Cookies (Automatically from Brave or manually)
    cookies = get_brave_cookies()
    if not cookies:
        cookies = {
            'sessionid': '04ldf2vb9uwfcayfs2mgtdwwli7jg82s',
            'sessionid_sign': 'v3:SHw6hycY6WFsEgbr5vKI6ISfkr331Go/ovj3kaQyG1o=',
        }

    extractor = TradingViewDataExtractor()
    extractor.set_cookies(cookies)

    try:
        # 2. Get User Data and Auth Token
        user_data = extractor.get_user_data() or {}
        if user_data.get("auth_token"):
            extractor.token = user_data["auth_token"]
            logger.info(f"Authenticated as {user_data.get('username')} (ID: {user_data.get('user_id')})")
        else:
            logger.warning("Authentication failed. Using unauthorized token.")

        # 3. Discover Indicators
        requested_indicator = {
            "id": "USER;f9c7fa68b382417ba34df4122c632dcf",
            "version": "1179.0",
            "name": "Target Study"
        }
        indicators_to_load = [requested_indicator]

        layouts = extractor.list_layouts()
        target_symbol = "BINANCE:BTCUSDT"
        if layouts and isinstance(layouts, list) and len(layouts) > 0:
            latest_layout = layouts[0]
            layout_id = latest_layout.get("url")
            target_symbol = latest_layout.get("symbol", target_symbol)
            logger.info(f"Found layout: {latest_layout.get('name')} ({layout_id}) on {target_symbol}")

            chart_token = extractor.get_chart_token(layout_id, user_data.get("user_id"))
            if chart_token:
                sources = extractor.get_layout_sources(layout_id, chart_token)
                for source_id, source in sources.get("payload", {}).get("sources", {}).items():
                    if source.get("type") in ["study", "pine_study"]:
                        meta_info = source.get("state", {}).get("metaInfo", {})
                        ind_id = meta_info.get("id") or source.get("pineId")
                        if ind_id and ind_id != requested_indicator["id"]:
                            indicators_to_load.append({
                                "id": ind_id,
                                "name": meta_info.get("description") or source.get("description"),
                                "inputs": source.get("state", {}).get("inputs")
                            })

        # 4. Connect to WebSocket
        extractor.connect()
        threading.Thread(target=extractor.listen, daemon=True).start()

        # 5. Set up Chart
        extractor.create_chart_session()
        time.sleep(1)
        extractor.resolve_symbol(target_symbol)
        time.sleep(1)
        extractor.create_series(timeframe="1D", range=100)
        time.sleep(2)

        # 6. Load all indicators
        loaded_indicators = []
        for i, ind_info in enumerate(indicators_to_load):
            try:
                ind_id = ind_info["id"]
                logger.info(f"Loading indicator {i+1}/{len(indicators_to_load)}: {ind_info['name']} ({ind_id})")
                meta = extractor.get_indicator_metadata(ind_id, version=ind_info.get("version", "last"))
                study_id = f"st{i+1}"
                extractor.create_study(study_id, meta, custom_inputs=ind_info.get("inputs"))
                loaded_indicators.append({"study_id": study_id, "meta": meta, "name": ind_info["name"]})
                time.sleep(1)
            except Exception as e:
                logger.error(f"Failed to load indicator {ind_info['name']}: {e}")

        # 7. Wait for data
        logger.info("Awaiting data for all indicators...")
        time.sleep(15)

        # 8. Output Results
        if extractor.ohlc:
            print(f"\n--- OHLC for {target_symbol} (Last 5) ---")
            for bar in extractor.ohlc[-5:]: print(bar)

        for ind in loaded_indicators:
            study_id = ind["study_id"]
            if study_id in extractor.indicator_data:
                print(f"\n--- Numerical Data for {ind['name']} ({study_id}) (Last 3) ---")
                mapped = extractor.get_mapped_indicator_data(study_id, ind["meta"])
                for row in mapped[-3:]: print(row)

            graphics = extractor.get_indicator_graphics(study_id)
            has_graphics = any(graphics[k] for k in graphics if k != 'raw')
            if has_graphics:
                print(f"\n--- Graphical Output for {ind['name']} ({study_id}) ---")
                for draw_type, items in graphics.items():
                    if items:
                        print(f"  {draw_type.capitalize()}: {len(items)} items")
                        for item in items[:5]: print(f"    {item}")

            if study_id not in extractor.indicator_data and not has_graphics:
                logger.warning(f"No data received for {ind['name']}")

    except Exception as e:
        logger.error(f"Execution failed: {e}")
    finally:
        extractor.running = False
        if extractor.ws: extractor.ws.close()
        logger.info("Process finished.")
