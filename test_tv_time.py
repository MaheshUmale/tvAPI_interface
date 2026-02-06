from backend.external.tv_api import tv_api
import pandas as pd
from datetime import datetime

candles = tv_api.get_hist_candles("NSE:NIFTY", "5", 10)
if candles:
    last_candle = candles[0] # Newest first according to the code
    print(f"Last Candle DT: {last_candle[0]}")
    print(f"Type: {type(last_candle[0])}")
