import logging
import threading
import time
from typing import Dict, Any

logger = logging.getLogger(__name__)

class StrategyEngine:
    def __init__(self, data_engine):
        self.data_engine = data_engine
        self.running = False
        self.thread = None

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        logger.info("Strategy Engine started.")

    def _run(self):
        # In a real scenario, we might use an async queue or event-driven approach
        # For this sample, we just poll the latest data or subscribe to updates
        pass

    def on_indicator_update(self, study_id: str, mapped_data: list):
        """Called when new indicator data is available."""
        if not mapped_data:
            return

        latest = mapped_data[-1]
        logger.info(f"Strategy Engine processing {study_id}: {latest}")

        # Sample logic: Log a trade signal if RSI (or similar) exceeds a threshold
        # Assuming the indicator has an 'RSI' plot or similar
        rsi_val = latest.get('RSI') or latest.get('plot_0')
        if isinstance(rsi_val, (int, float)):
            if rsi_val > 70:
                logger.warning(f"SIGNAL: Overbought detected for {study_id} (Value: {rsi_val})")
            elif rsi_val < 30:
                logger.warning(f"SIGNAL: Oversold detected for {study_id} (Value: {rsi_val})")

# Initialize global engine
strategy_engine = None

def start_strategy_engine(data_engine):
    global strategy_engine
    if strategy_engine is None:
        strategy_engine = StrategyEngine(data_engine)
        strategy_engine.start()
    return strategy_engine
