import os

# --- Global Configuration ---
# It is recommended to set these in environment variables for security
# TradingView Config
TV_USERNAME = os.getenv('TV_USERNAME', '')
TV_PASSWORD = os.getenv('TV_PASSWORD', '')
TV_COOKIE = os.getenv('TV_COOKIE', '')

# LocalDB Config
DUCKDB_PATH = os.getenv('DUCKDB_PATH', 'pro_trade.db')

# --- Strategy Configuration ---
INITIAL_INSTRUMENTS = ["NSE_INDEX|Nifty 50", "NSE_INDEX|Nifty Bank", "NSE_INDEX|India VIX"]

# --- Logging Configuration ---
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        },
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["default"],
            "level": "INFO",
            "propagate": True
        },
        "uvicorn.error": {
            "level": "INFO"
        },
        "uvicorn.access": {
            "level": "INFO"
        },
    }
}
