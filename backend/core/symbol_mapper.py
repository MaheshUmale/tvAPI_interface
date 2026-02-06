
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, Any
from db.local_db import db

logger = logging.getLogger(__name__)

class SymbolMapper:
    _instance = None
    _mapping_cache: Dict[str, str] = {
        "NSE_INDEX|Nifty 50": "NIFTY",
        "NSE_INDEX|Nifty Bank": "BANKNIFTY",
        "NSE_INDEX|Nifty Fin Service": "FINNIFTY",
        "NSE_INDEX|India VIX": "INDIA VIX"
    } # instrument_key -> HRN
    _reverse_cache: Dict[str, str] = {
        "NIFTY": "NSE_INDEX|Nifty 50",
        "BANKNIFTY": "NSE_INDEX|Nifty Bank",
        "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
        "INDIA VIX": "NSE_INDEX|India VIX"
    } # HRN -> instrument_key

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SymbolMapper, cls).__new__(cls)
        return cls._instance

    def get_hrn(self, instrument_key: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Converts an instrument key to a Human Readable Name.
        Format: NIFTY 03 OCT 2024 CALL 25000
        """
        if not instrument_key: return ""

        # Standardize input key
        key = instrument_key.replace(':', '|')

        if key in self._mapping_cache:
            return self._mapping_cache[key]

        # Try to find in Local DB
        try:
            res = db.get_metadata(key)
            if res:
                hrn = res['hrn']
                self._mapping_cache[key] = hrn
                self._reverse_cache[hrn] = key
                return hrn
        except:
            pass

        # If not found and metadata provided, generate and store
        if metadata:
            hrn = self._generate_hrn(key, metadata)
            if hrn:
                self._store_mapping(key, hrn, metadata)
                return hrn

        # Fallback to simple normalization if no metadata
        if '|' in key:
            parts = key.split('|')
            if len(parts) == 2 and parts[0] == 'NSE':
                return parts[1] # Return just RELIANCE for NSE|RELIANCE
        return key.replace('|', ':').replace('NSE INDEX', '').strip()

    def _generate_hrn(self, instrument_key: str, meta: Dict[str, Any]) -> str:
        """
        Generates HRN from metadata.
        meta keys: symbol, type, strike, expiry
        """
        symbol = meta.get('symbol', '').upper()
        if "NIFTY 50" in symbol: symbol = "NIFTY"
        if "NIFTY BANK" in symbol: symbol = "BANKNIFTY"
        if "NIFTY FIN SERVICE" in symbol: symbol = "FINNIFTY"

        itype = meta.get('type', '')
        strike = meta.get('strike')
        expiry = meta.get('expiry') # YYYY-MM-DD

        if itype == 'INDEX':
            return symbol

        if itype == 'FUT':
            if expiry:
                dt = datetime.strptime(expiry, "%Y-%m-%d")
                return f"{symbol} {dt.strftime('%d %b %Y').upper()} FUT"
            return f"{symbol} FUT"

        if itype in ['CE', 'PE', 'CALL', 'PUT']:
            option_type = 'CALL' if itype in ['CE', 'CALL'] else 'PUT'
            if expiry:
                dt = datetime.strptime(expiry, "%Y-%m-%d")
                expiry_str = dt.strftime('%d %b %Y').upper()
                return f"{symbol} {expiry_str} {option_type} {int(strike) if strike else ''}".strip()
            return f"{symbol} {option_type} {int(strike) if strike else ''}".strip()

        return instrument_key

    def _store_mapping(self, instrument_key: str, hrn: str, metadata: Dict[str, Any]):
        try:
            db.update_metadata(instrument_key, hrn, metadata)
        except:
            pass
        self._mapping_cache[instrument_key] = hrn
        self._reverse_cache[hrn] = instrument_key

    def resolve_to_key(self, hrn: str) -> Optional[str]:
        """Resolves a Human Readable Name back to an instrument key."""
        if not hrn: return None

        target = hrn.upper().strip()
        if target in self._reverse_cache:
            return self._reverse_cache[target]

        try:
            rows = db.query("SELECT instrument_key FROM metadata WHERE hrn = ?", (target,))
            if rows:
                key = rows[0]['instrument_key']
                self._mapping_cache[key] = target
                self._reverse_cache[target] = key
                return key
        except:
            pass

        return None

    def get_symbol(self, key_or_hrn: str) -> str:
        """Extracts the base symbol (NIFTY, BANKNIFTY, FINNIFTY) from a key or HRN."""
        if not key_or_hrn: return ""

        target = key_or_hrn.upper().replace(':', '|').strip()

        # 1. Handle Indices
        if "NIFTY BANK" in target or "BANKNIFTY" in target:
            return "BANKNIFTY"
        if "FIN SERVICE" in target or "FINNIFTY" in target:
            return "FINNIFTY"
        if "NIFTY" in target:
            return "NIFTY"
        if "INDIA VIX" in target or "INDIAVIX" in target:
            return "INDIA VIX"

        # 2. Handle technical keys with prefixes (e.g., NSE|RELIANCE)
        if "|" in target:
            return target.split("|")[-1]

        # 3. Handle HRN formats (e.g., RELIANCE 26 FEB 2026 CALL 2500)
        return target.split(" ")[0]

symbol_mapper = SymbolMapper()
