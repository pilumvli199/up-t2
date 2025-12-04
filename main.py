#!/usr/bin/env python3
"""
NIFTY50 STRIKE MASTER PRO v2.0 - COMPLETE ENHANCED VERSION
===========================================================
✅ FIXED: Historical OI data tracking (even after 4+ hours)
✅ NEW: Complete analysis data in Telegram alerts
✅ NEW: Better debugging and validation
✅ NEW: Auto-detect spot price key format
✅ NEW: Enhanced signal validation

Version: 2.1 - Professional Grade
Author: Enhanced by Claude Sonnet 4.5
"""

import os
import asyncio
import aiohttp
import urllib.parse
from datetime import datetime, timedelta, time
import pytz
import json
import logging
from dataclasses import dataclass, field
from typing import Optional, Tuple, Dict, List
import pandas as pd
import numpy as np
from collections import deque
import time as time_module
import gzip
from io import BytesIO

# Optional dependencies
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logging.warning("⚠️ Redis not available - using RAM mode")

try:
    from telegram import Bot
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    logging.warning("⚠️ Telegram not available")

# ==================== CONFIGURATION ====================
IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO  # Changed back to INFO for cleaner logs
)
logger = logging.getLogger("NIFTY-Pro-v2")

UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN', 'YOUR_TOKEN_HERE')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')

NIFTY_CONFIG = {
    'name': 'NIFTY 50',
    'spot_key': 'NSE_INDEX|Nifty 50',  # Will auto-detect correct format
    'strike_gap': 50,
    'lot_size': 25,
    'atr_fallback': 30,
    'expiry_day': 1,
    'expiry_type': 'weekly'
}

INSTRUMENTS_JSON_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

# Alert Settings
ALERT_ONLY_MODE = True
SCAN_INTERVAL = 60
SEND_ANALYSIS_DATA = True  # 🔥 NEW: Send complete analysis data in Telegram

# Signal Thresholds (v2.1 - Optimized based on research)
OI_THRESHOLD_STRONG = 4.0       # 4% = Strong (was 8.0)
OI_THRESHOLD_MEDIUM = 2.5       # 2.5% = Medium (was 5.0)
OI_THRESHOLD_WEAK = 1.5         # 1.5% = Weak signal
ATM_OI_THRESHOLD_STRONG = 5.0   # 5% ATM = Very strong
ATM_OI_THRESHOLD = 3.0          # 3% ATM = Medium (was 5.0)
OI_5M_THRESHOLD = 2.0           # 2% in 5m = Significant
VOL_SPIKE_MULTIPLIER = 2.0
PCR_BULLISH = 1.3   # Wider range (was 1.08)
PCR_BEARISH = 0.7   # Wider range (was 0.92)
MIN_CANDLE_SIZE = 8
VWAP_BUFFER = 5

# Time Restrictions
AVOID_OPENING = (time(9, 15), time(9, 45))
AVOID_CLOSING = (time(15, 15), time(15, 30))

# Technical Indicators
ATR_PERIOD = 14
ATR_SL_MULTIPLIER = 1.5
ATR_TARGET_MULTIPLIER = 2.5

# Rate Limiting
RATE_LIMIT_PER_SECOND = 50
RATE_LIMIT_PER_MINUTE = 500

# Memory & Signals
SIGNAL_COOLDOWN_SECONDS = 300
MEMORY_TTL_SECONDS = 7200  # 🔥 Increased to 2 hours
TELEGRAM_TIMEOUT = 5
# Hourly Updates (v2.1 NEW)
HOURLY_UPDATE_ENABLED = True    # Send hourly JSON to Telegram
HOURLY_UPDATE_INTERVAL = 3600   # 1 hour in seconds


@dataclass
class AnalysisData:
    """Complete analysis data for transparency"""
    # Market Data
    spot_price: float
    futures_price: float
    atm_strike: int
    vwap: float
    atr: float
    pcr: float
    
    # OI Data
    total_ce_oi: int
    total_pe_oi: int
    atm_ce_oi: int
    atm_pe_oi: int
    atm_ce_vol: int
    atm_pe_vol: int
    
    # OI Changes
    ce_oi_15m: float
    pe_oi_15m: float
    ce_oi_5m: float
    pe_oi_5m: float
    atm_ce_change_15m: float
    atm_pe_change_15m: float
    atm_ce_change_5m: float
    atm_pe_change_5m: float
    
    # Candle Data
    candle_color: str
    candle_size: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    current_volume: int
    
    # Technical Indicators
    vwap_distance: float
    volume_surge: float
    has_volume_spike: bool
    order_flow_imbalance: float
    max_pain_strike: int
    max_pain_distance: float
    gamma_zone: bool
    multi_tf_confirm: bool
    
    # Momentum
    bullish_momentum: bool
    bearish_momentum: bool
    
    # Strike Data (Top 5 strikes)
    strike_data_summary: Dict[int, dict] = field(default_factory=dict)
    
    # Data Quality
    data_quality: Dict[str, bool] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    
    timestamp: datetime = field(default_factory=lambda: datetime.now(IST))

@dataclass
class Signal:
    type: str
    reason: str
    confidence: int
    spot_price: float
    futures_price: float
    strike: int
    target_points: int
    stop_loss_points: int
    pcr: float
    candle_color: str
    volume_surge: float
    oi_5m: float
    oi_15m: float
    atm_ce_change: float
    atm_pe_change: float
    atr: float
    timestamp: datetime
    order_flow_imbalance: float = 0.0
    max_pain_distance: float = 0.0
    gamma_zone: bool = False
    multi_tf_confirm: bool = False
    lot_size: int = 25
    quantity: int = 1
    atm_ce_oi: int = 0
    atm_pe_oi: int = 0
    atm_ce_vol: int = 0
    atm_pe_vol: int = 0
    analysis: Optional[AnalysisData] = None  # 🔥 NEW: Complete analysis attached


class HourlyDataCollector:
    """Collects and sends hourly market data to Telegram"""
    def __init__(self):
        self.last_hourly_update = None
        self.hourly_data = []
    
    def should_send_hourly_update(self) -> bool:
        """Check if 1 hour has passed since last update"""
        now = datetime.now(IST)
        if self.last_hourly_update is None:
            return True
        elapsed = (now - self.last_hourly_update).total_seconds()
        return elapsed >= HOURLY_UPDATE_INTERVAL
    
    def collect_hourly_snapshot(self, spot, futures, atm_strike, vwap, atr, pcr,
                                total_ce, total_pe, ce_15m, pe_15m, atm_ce_oi, atm_pe_oi,
                                atm_ce_15m, atm_pe_15m, candle_data, volume, india_vix=0) -> dict:
        """Collect current market snapshot"""
        now = datetime.now(IST)
        
        snapshot = {
            "timestamp": now.strftime('%Y-%m-%d %H:%M:%S'),
            "hour": now.strftime('%I:%M %p'),
            
            "prices": {
                "spot": round(spot, 2),
                "futures": round(futures, 2),
                "vwap": round(vwap, 2),
                "vwap_distance": round(abs(futures - vwap), 2)
            },
            
            "candle": {
                "open": candle_data.get('open', 0),
                "high": candle_data.get('high', 0),
                "low": candle_data.get('low', 0),
                "close": candle_data.get('close', 0),
                "volume": candle_data.get('volume', 0)
            },
            
            "oi_data": {
                "total_ce_oi": total_ce,
                "total_pe_oi": total_pe,
                "pcr": round(pcr, 2),
                "ce_change_15m": round(ce_15m, 2),
                "pe_change_15m": round(pe_15m, 2)
            },
            
            "atm_data": {
                "strike": atm_strike,
                "ce_oi": atm_ce_oi,
                "pe_oi": atm_pe_oi,
                "ce_change_15m": round(atm_ce_15m, 2),
                "pe_change_15m": round(atm_pe_15m, 2)
            },
            
            "technical": {
                "atr": round(atr, 2),
                "india_vix": round(india_vix, 2) if india_vix else 0
            }
        }
        
        self.hourly_data.append(snapshot)
        return snapshot
    
    def generate_hourly_json_message(self, snapshot: dict) -> str:
        """Generate clean JSON for Telegram"""
        json_str = json.dumps(snapshot, indent=2)
        
        message = f"""
📊 NIFTY50 - Hourly Update

🕐 Time: {snapshot['hour']}

```json
{json_str}
```

💡 Quick View:
━━━━━━━━━━━━━━━━
💰 Spot: ₹{snapshot['prices']['spot']:.2f}
📊 PCR: {snapshot['oi_data']['pcr']}
🎯 ATM: {snapshot['atm_data']['strike']}

📈 OI Changes (15m):
   CE: {snapshot['oi_data']['ce_change_15m']:+.1f}%
   PE: {snapshot['oi_data']['pe_change_15m']:+.1f}%

⚡ ATM Changes (15m):
   CE: {snapshot['atm_data']['ce_change_15m']:+.1f}%
   PE: {snapshot['atm_data']['pe_change_15m']:+.1f}%
━━━━━━━━━━━━━━━━

💾 Data saved for backtesting
"""
        return message

class RateLimiter:
    def __init__(self):
        self.requests_per_second = deque(maxlen=RATE_LIMIT_PER_SECOND)
        self.requests_per_minute = deque(maxlen=RATE_LIMIT_PER_MINUTE)
        self.lock = asyncio.Lock()
    
    async def wait_if_needed(self):
        async with self.lock:
            now = time_module.time()
            while self.requests_per_second and now - self.requests_per_second[0] > 1.0:
                self.requests_per_second.popleft()
            while self.requests_per_minute and now - self.requests_per_minute[0] > 60.0:
                self.requests_per_minute.popleft()
            if len(self.requests_per_second) >= RATE_LIMIT_PER_SECOND:
                sleep_time = 1.0 - (now - self.requests_per_second[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    now = time_module.time()
            if len(self.requests_per_minute) >= RATE_LIMIT_PER_MINUTE:
                sleep_time = 60.0 - (now - self.requests_per_minute[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    now = time_module.time()
            self.requests_per_second.append(now)
            self.requests_per_minute.append(now)

rate_limiter = RateLimiter()

def get_next_tuesday_expiry() -> datetime:
    now = datetime.now(IST)
    days_until_tuesday = (1 - now.weekday()) % 7
    if days_until_tuesday == 0:
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if now > market_close:
            next_tuesday = now + timedelta(days=7)
        else:
            next_tuesday = now
    else:
        next_tuesday = now + timedelta(days=days_until_tuesday)
    return next_tuesday

def get_monthly_expiry() -> datetime:
    now = datetime.now(IST)
    year = now.year
    month = now.month
    if month == 12:
        next_month = datetime(year + 1, 1, 1, tzinfo=IST)
    else:
        next_month = datetime(year, month + 1, 1, tzinfo=IST)
    last_day = next_month - timedelta(days=1)
    while last_day.weekday() != 1:
        last_day -= timedelta(days=1)
    if last_day.date() < now.date() or (last_day.date() == now.date() and now.time() > time(15, 30)):
        if month == 12:
            next_next = datetime(year + 1, 1, 1, tzinfo=IST)
        else:
            next_next = datetime(year, month + 1, 1, tzinfo=IST)
        if next_next.month == 12:
            next_next_month = datetime(next_next.year + 1, 1, 1, tzinfo=IST)
        else:
            next_next_month = datetime(next_next.year, next_next.month + 1, 1, tzinfo=IST)
        last_day = next_next_month - timedelta(days=1)
        while last_day.weekday() != 1:
            last_day -= timedelta(days=1)
    return last_day

async def fetch_futures_instrument_key() -> Optional[str]:
    """Download instruments JSON and find correct futures key"""
    logger.info("📥 Downloading Upstox instruments...")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(INSTRUMENTS_JSON_URL, timeout=30) as resp:
                if resp.status == 200:
                    compressed = await resp.read()
                    decompressed = gzip.decompress(compressed)
                    instruments = json.loads(decompressed)
                    logger.info(f"✅ Loaded {len(instruments)} instruments")
                    
                    target_expiry = get_monthly_expiry()
                    target_date = target_expiry.date()
                    
                    logger.info(f"🎯 Looking for NIFTY futures expiry: {target_date.strftime('%d-%b-%Y')}")
                    
                    for instrument in instruments:
                        if instrument.get('segment') != 'NSE_FO':
                            continue
                        if instrument.get('instrument_type') != 'FUT':
                            continue
                        if instrument.get('name') != 'NIFTY':
                            continue
                        
                        expiry_ms = instrument.get('expiry', 0)
                        if not expiry_ms:
                            continue
                        
                        expiry_dt = datetime.fromtimestamp(expiry_ms / 1000, tz=IST)
                        expiry_date = expiry_dt.date()
                        
                        if expiry_date == target_date:
                            instrument_key = instrument.get('instrument_key')
                            trading_symbol = instrument.get('trading_symbol')
                            logger.info(f"✅ Found: {trading_symbol}")
                            logger.info(f"   Key: {instrument_key}")
                            return instrument_key
                    
                    logger.error("❌ No matching futures found")
                    return None
                else:
                    logger.error(f"❌ HTTP {resp.status}")
                    return None
    except Exception as e:
        logger.error(f"💥 Error: {e}")
        return None

def is_tradeable_time() -> bool:
    now = datetime.now(IST).time()
    if not (time(9, 15) <= now <= time(15, 30)):
        return False
    if AVOID_OPENING[0] <= now <= AVOID_OPENING[1]:
        return False
    if AVOID_CLOSING[0] <= now <= AVOID_CLOSING[1]:
        return False
    return True

class RedisBrain:
    """Enhanced Redis with better OI tracking"""
    def __init__(self):
        self.client = None
        self.memory = {}
        self.memory_timestamps = {}
        self.startup_time = datetime.now(IST)
        self.snapshot_count = 0  # Track how many snapshots saved
        
        if REDIS_AVAILABLE:
            try:
                self.client = redis.from_url(REDIS_URL, decode_responses=True)
                self.client.ping()
                logger.info("✅ Redis connected")
            except:
                self.client = None
        if not self.client:
            logger.info("💾 RAM-only mode")
    
    def is_warmed_up(self, minutes: int = 15) -> bool:
        """Check if enough snapshots have been saved"""
        elapsed = (datetime.now(IST) - self.startup_time).total_seconds() / 60
        has_enough_time = elapsed >= minutes
        has_enough_snapshots = self.snapshot_count >= (minutes / (SCAN_INTERVAL / 60))
        
        if not has_enough_time:
            logger.debug(f"⏳ Warmup: {elapsed:.1f}/{minutes} min elapsed")
        if not has_enough_snapshots:
            logger.debug(f"⏳ Snapshots: {self.snapshot_count}/{int(minutes / (SCAN_INTERVAL / 60))} saved")
        
        return has_enough_time and has_enough_snapshots
    
    def _cleanup_old_memory(self):
        """Clean expired memory in RAM mode"""
        if self.client:
            return
        now = time_module.time()
        expired = [k for k, ts in self.memory_timestamps.items() if now - ts > MEMORY_TTL_SECONDS]
        if expired:
            logger.debug(f"🧹 Cleaned {len(expired)} expired memory entries")
        for key in expired:
            del self.memory[key]
            del self.memory_timestamps[key]
    
    def save_strike_snapshot(self, strike: int, data: dict):
        """Save strike data with timestamp"""
        now = datetime.now(IST)
        timestamp = now.replace(second=0, microsecond=0)
        key = f"nifty:strike:{strike}:{timestamp.strftime('%Y%m%d_%H%M')}"
        value = json.dumps(data)
        
        if self.client:
            try:
                self.client.setex(key, MEMORY_TTL_SECONDS, value)
                logger.debug(f"💾 Redis saved: {key}")
            except Exception as e:
                logger.warning(f"⚠️ Redis save failed: {e}, using RAM")
                self.memory[key] = value
                self.memory_timestamps[key] = time_module.time()
        else:
            self.memory[key] = value
            self.memory_timestamps[key] = time_module.time()
            logger.debug(f"💾 RAM saved: {key}")
        
        self.snapshot_count += 1
        self._cleanup_old_memory()
    
    def get_strike_oi_change(self, strike: int, current_data: dict, minutes_ago: int = 15) -> Tuple[float, float, bool]:
        """Get OI change with data availability flag"""
        now = datetime.now(IST) - timedelta(minutes=minutes_ago)
        timestamp = now.replace(second=0, microsecond=0)
        key = f"nifty:strike:{strike}:{timestamp.strftime('%Y%m%d_%H%M')}"
        
        past_data_str = None
        if self.client:
            try:
                past_data_str = self.client.get(key)
            except:
                pass
        
        if not past_data_str:
            past_data_str = self.memory.get(key)
        
        if not past_data_str:
            logger.debug(f"📊 No historical data for strike {strike} @ {minutes_ago}m (key: {key})")
            return 0.0, 0.0, False  # Return False to indicate no data
        
        try:
            past = json.loads(past_data_str)
            ce_chg = ((current_data['ce_oi'] - past['ce_oi']) / past['ce_oi'] * 100 if past['ce_oi'] > 0 else 0)
            pe_chg = ((current_data['pe_oi'] - past['pe_oi']) / past['pe_oi'] * 100 if past['pe_oi'] > 0 else 0)
            logger.debug(f"✅ Found historical data for {strike} @ {minutes_ago}m: CE={ce_chg:+.1f}%, PE={pe_chg:+.1f}%")
            return ce_chg, pe_chg, True
        except Exception as e:
            logger.warning(f"⚠️ Error parsing OI data for {strike}: {e}")
            return 0.0, 0.0, False
    
    def save_total_oi_snapshot(self, ce_total: int, pe_total: int):
        """Save total OI snapshot"""
        now = datetime.now(IST)
        slot = now.replace(second=0, microsecond=0)
        key = f"nifty:total_oi:{slot.strftime('%Y%m%d_%H%M')}"
        data = json.dumps({"ce": ce_total, "pe": pe_total})
        
        if self.client:
            try:
                self.client.setex(key, MEMORY_TTL_SECONDS, data)
            except:
                self.memory[key] = data
                self.memory_timestamps[key] = time_module.time()
        else:
            self.memory[key] = data
            self.memory_timestamps[key] = time_module.time()
    
    def get_total_oi_change(self, current_ce: int, current_pe: int, minutes_ago: int = 15) -> Tuple[float, float, bool]:
        """Get total OI change with data availability flag"""
        now = datetime.now(IST) - timedelta(minutes=minutes_ago)
        slot = now.replace(second=0, microsecond=0)
        key = f"nifty:total_oi:{slot.strftime('%Y%m%d_%H%M')}"
        
        past_data = None
        if self.client:
            try:
                past_data = self.client.get(key)
            except:
                pass
        
        if not past_data:
            past_data = self.memory.get(key)
        
        if not past_data:
            return 0.0, 0.0, False
        
        try:
            past = json.loads(past_data)
            ce_chg = ((current_ce - past['ce']) / past['ce'] * 100 if past['ce'] > 0 else 0)
            pe_chg = ((current_pe - past['pe']) / past['pe'] * 100 if past['pe'] > 0 else 0)
            return ce_chg, pe_chg, True
        except:
            return 0.0, 0.0, False
    
    def get_memory_stats(self) -> Dict:
        """Get memory statistics for debugging"""
        return {
            'snapshot_count': self.snapshot_count,
            'ram_keys': len(self.memory) if not self.client else 0,
            'startup_time': self.startup_time.strftime('%H:%M:%S'),
            'elapsed_minutes': (datetime.now(IST) - self.startup_time).total_seconds() / 60,
            'warmed_up_15m': self.is_warmed_up(15),
            'warmed_up_5m': self.is_warmed_up(5)
        }

class NiftyDataFeed:
    def __init__(self, futures_key: str):
        self.headers = {
            "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
            "Accept": "application/json"
        }
        self.futures_symbol = futures_key
        self.detected_spot_key = None  # Will auto-detect
        logger.info(f"📊 Futures Key: {futures_key}")
    
    async def fetch_with_retry(self, url: str, session: aiohttp.ClientSession, max_retries: int = 3):
        for attempt in range(max_retries):
            try:
                await rate_limiter.wait_if_needed()
                async with session.get(url, headers=self.headers, timeout=15) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 429:
                        wait_time = 2 ** (attempt + 1)
                        logger.warning(f"⏳ Rate limited, waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                    elif resp.status == 400:
                        error_text = await resp.text()
                        logger.error(f"❌ Status 400: {error_text[:200]}")
                        return None
                    else:
                        logger.warning(f"⚠️ Status {resp.status}, retry {attempt + 1}/{max_retries}")
                        await asyncio.sleep(2)
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ Timeout, retry {attempt + 1}/{max_retries}")
                await asyncio.sleep(2)
            except Exception as e:
                logger.warning(f"❌ Error: {e}, retry {attempt + 1}/{max_retries}")
                await asyncio.sleep(2)
        return None
    
    async def get_market_data(self) -> Tuple[pd.DataFrame, Dict[int, dict], float, float, float]:
        """Fetch complete market data with auto-detection"""
        async with aiohttp.ClientSession() as session:
            spot_price = 0
            futures_price = 0
            df = pd.DataFrame()
            strike_data = {}
            total_options_volume = 0
            
            # 1. SPOT PRICE with auto-detection
            logger.info("🔍 Fetching NIFTY spot...")
            
            # Try original key first
            spot_keys_to_try = [
                NIFTY_CONFIG['spot_key'],
                'NSE_INDEX:Nifty 50',  # Alternative format
                'NSE_INDEX|Nifty%2050'  # URL encoded
            ]
            
            for spot_key in spot_keys_to_try:
                if self.detected_spot_key:
                    spot_key = self.detected_spot_key
                
                enc_key = urllib.parse.quote(spot_key)
                url = f"https://api.upstox.com/v2/market-quote/quotes?symbol={enc_key}"
                
                try:
                    async with session.get(url, headers=self.headers, timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get('status') == 'success':
                                data_dict = data.get('data', {})
                                
                                # Try to find the key in response
                                for key in data_dict.keys():
                                    if 'nifty' in key.lower() and '50' in key:
                                        quote = data_dict[key]
                                        spot_price = quote.get('last_price', 0)
                                        if spot_price > 0:
                                            self.detected_spot_key = key
                                            logger.info(f"✅ NIFTY Spot: ₹{spot_price:.2f} (key: {key})")
                                            break
                                
                                if spot_price > 0:
                                    break
                except Exception as e:
                    logger.debug(f"⚠️ Spot fetch attempt failed: {e}")
                
                if spot_price > 0:
                    break
            
            if spot_price == 0:
                logger.warning("⚠️ Spot price unavailable, will use futures")
            
            # 2. FUTURES CANDLES
            logger.info(f"🔍 Fetching futures: {self.futures_symbol}")
            enc_futures = urllib.parse.quote(self.futures_symbol)
            candle_url = f"https://api.upstox.com/v2/historical-candle/intraday/{enc_futures}/1minute"
            
            candle_data = await self.fetch_with_retry(candle_url, session)
            if candle_data and candle_data.get('status') == 'success':
                candles = candle_data.get('data', {}).get('candles', [])
                if candles:
                    candles_to_use = candles[:500]
                    df = pd.DataFrame(candles_to_use, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'oi'])
                    df['ts'] = pd.to_datetime(df['ts']).dt.tz_convert(IST)
                    df = df.sort_values('ts').set_index('ts')
                    if not df.empty:
                        futures_price = df['close'].iloc[-1]
                        logger.info(f"✅ Futures: {len(df)} candles | ₹{futures_price:.2f}")
                        if spot_price == 0 and futures_price > 0:
                            spot_price = futures_price
                            logger.warning(f"⚠️ Using futures as spot: ₹{spot_price:.2f}")
            
            if spot_price == 0:
                logger.error("❌ Both spot and futures fetch failed")
                return df, strike_data, 0, 0, 0
            
            # 3. OPTION CHAIN
            logger.info("🔍 Fetching option chain...")
            expiry = get_next_tuesday_expiry()
            expiry_str = expiry.strftime('%Y-%m-%d')
            
            # Use detected spot key or try alternatives
            index_key = self.detected_spot_key if self.detected_spot_key else NIFTY_CONFIG['spot_key']
            enc_index = urllib.parse.quote(index_key)
            chain_url = f"https://api.upstox.com/v2/option/chain?instrument_key={enc_index}&expiry_date={expiry_str}"
            
            strike_gap = NIFTY_CONFIG['strike_gap']
            atm_strike = round(spot_price / strike_gap) * strike_gap
            min_strike = atm_strike - (2 * strike_gap)
            max_strike = atm_strike + (2 * strike_gap)
            logger.info(f"📊 ATM: {atm_strike} | Range: {min_strike}-{max_strike}")
            
            chain_data = await self.fetch_with_retry(chain_url, session)
            if chain_data and chain_data.get('status') == 'success':
                for option in chain_data.get('data', []):
                    strike = option.get('strike_price', 0)
                    if min_strike <= strike <= max_strike:
                        call_data = option.get('call_options', {}).get('market_data', {})
                        put_data = option.get('put_options', {}).get('market_data', {})
                        strike_data[strike] = {
                            'ce_oi': call_data.get('oi', 0),
                            'pe_oi': put_data.get('oi', 0),
                            'ce_vol': call_data.get('volume', 0),
                            'pe_vol': put_data.get('volume', 0),
                            'ce_ltp': call_data.get('ltp', 0),
                            'pe_ltp': put_data.get('ltp', 0)
                        }
                        total_options_volume += (call_data.get('volume', 0) + put_data.get('volume', 0))
                logger.info(f"✅ Collected {len(strike_data)} strikes")
            
            return df, strike_data, spot_price, futures_price, total_options_volume

class NiftyAnalyzer:
    def __init__(self):
        self.volume_history = []
    
    def calculate_vwap(self, df: pd.DataFrame) -> float:
        if df.empty:
            return 0
        df_copy = df.copy()
        df_copy['tp'] = (df_copy['high'] + df_copy['low'] + df_copy['close']) / 3
        df_copy['vol_price'] = df_copy['tp'] * df_copy['vol']
        total_vol = df_copy['vol'].sum()
        if total_vol == 0:
            return df_copy['close'].iloc[-1]
        vwap = df_copy['vol_price'].cumsum() / df_copy['vol'].cumsum()
        return vwap.iloc[-1]
    
    def calculate_atr(self, df: pd.DataFrame, period: int = ATR_PERIOD) -> float:
        if len(df) < period:
            return NIFTY_CONFIG['atr_fallback']
        df_copy = df.tail(period).copy()
        df_copy['h-l'] = df_copy['high'] - df_copy['low']
        df_copy['h-pc'] = abs(df_copy['high'] - df_copy['close'].shift(1))
        df_copy['l-pc'] = abs(df_copy['low'] - df_copy['close'].shift(1))
        df_copy['tr'] = df_copy[['h-l', 'h-pc', 'l-pc']].max(axis=1)
        atr = df_copy['tr'].mean()
        if atr < 10:
            return NIFTY_CONFIG['atr_fallback']
        return atr
    
    def get_candle_info(self, df: pd.DataFrame) -> Tuple[str, float, dict]:
        if df.empty:
            return 'NEUTRAL', 0, {}
        last = df.iloc[-1]
        candle_size = abs(last['close'] - last['open'])
        if last['close'] > last['open']:
            color = 'GREEN'
        elif last['close'] < last['open']:
            color = 'RED'
        else:
            color = 'DOJI'
        
        candle_data = {
            'open': last['open'],
            'high': last['high'],
            'low': last['low'],
            'close': last['close'],
            'volume': int(last['vol'])
        }
        return color, candle_size, candle_data
    
    def check_volume_surge(self, current_vol: float) -> Tuple[bool, float]:
        now = datetime.now(IST)
        self.volume_history.append({'time': now, 'volume': current_vol})
        cutoff = now - timedelta(minutes=20)
        self.volume_history = [x for x in self.volume_history if x['time'] > cutoff]
        if len(self.volume_history) < 5:
            return False, 0
        past_volumes = [x['volume'] for x in self.volume_history[:-1]]
        avg_vol = sum(past_volumes) / len(past_volumes)
        if avg_vol == 0:
            return False, 0
        multiplier = current_vol / avg_vol
        return multiplier >= VOL_SPIKE_MULTIPLIER, multiplier
    
    def calculate_pcr(self, strike_data: Dict[int, dict]) -> float:
        total_ce = sum(data['ce_oi'] for data in strike_data.values())
        total_pe = sum(data['pe_oi'] for data in strike_data.values())
        return total_pe / total_ce if total_ce > 0 else 1.0
    
    def calculate_order_flow_imbalance(self, strike_data: Dict[int, dict]) -> float:
        ce_vol = sum(data['ce_vol'] for data in strike_data.values())
        pe_vol = sum(data['pe_vol'] for data in strike_data.values())
        if ce_vol == 0 and pe_vol == 0:
            return 1.0
        elif pe_vol == 0:
            return 999.0
        elif ce_vol == 0:
            return 0.001
        return ce_vol / pe_vol
    
    def calculate_max_pain(self, strike_data: Dict[int, dict], spot_price: float) -> Tuple[int, float]:
        max_pain_strike = 0
        min_pain_value = float('inf')
        for test_strike in strike_data.keys():
            pain = 0
            for strike, data in strike_data.items():
                if test_strike < strike:
                    pain += data['ce_oi'] * (strike - test_strike)
                if test_strike > strike:
                    pain += data['pe_oi'] * (test_strike - strike)
            if pain < min_pain_value:
                min_pain_value = pain
                max_pain_strike = test_strike
        distance = abs(spot_price - max_pain_strike)
        logger.info(f"🎯 Max Pain: {max_pain_strike} (Distance: {distance:.0f})")
        return max_pain_strike, distance
    
    def detect_gamma_zone(self, strike_data: Dict[int, dict], atm_strike: int) -> bool:
        if atm_strike not in strike_data:
            return False
        atm_data = strike_data[atm_strike]
        total_atm_oi = atm_data['ce_oi'] + atm_data['pe_oi']
        total_oi = sum(d['ce_oi'] + d['pe_oi'] for d in strike_data.values())
        if total_oi == 0:
            return False
        atm_concentration = (total_atm_oi / total_oi) * 100
        is_gamma_zone = atm_concentration > 30
        if is_gamma_zone:
            logger.info(f"⚡ Gamma Zone! ATM OI: {atm_concentration:.1f}%")
        return is_gamma_zone
    
    def check_multi_tf_confirmation(self, ce_5m: float, ce_15m: float, pe_5m: float, pe_15m: float,
                                   has_5m: bool, has_15m: bool) -> bool:
        """Enhanced multi-TF check with data availability"""
        if not (has_5m and has_15m):
            return False
        
        ce_aligned = (ce_5m < -3 and ce_15m < -5) or (ce_5m > 3 and ce_15m > 5)
        pe_aligned = (pe_5m < -3 and pe_15m < -5) or (pe_5m > 3 and pe_15m > 5)
        confirmed = ce_aligned or pe_aligned
        if confirmed:
            logger.info("✅ Multi-TF Confirmed")
        return confirmed
    
    def check_momentum(self, df: pd.DataFrame, direction: str = 'bullish') -> bool:
        if df.empty or len(df) < 3:
            return False
        last_3 = df.tail(3)
        if direction == 'bullish':
            return sum(last_3['close'] > last_3['open']) >= 2
        else:
            return sum(last_3['close'] < last_3['open']) >= 2

class NiftyStrikeMaster:
    def __init__(self, futures_key: str):
        self.feed = NiftyDataFeed(futures_key)
        self.redis = RedisBrain()
        self.analyzer = NiftyAnalyzer()
        self.telegram = None
        self.last_signal_time = {}
        self.hourly_collector = HourlyDataCollector()  # v2.1 NEW
        
        if TELEGRAM_AVAILABLE and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                self.telegram = Bot(token=TELEGRAM_BOT_TOKEN)
                logger.info("✅ Telegram ready")
            except Exception as e:
                logger.warning(f"⚠️ Telegram: {e}")
    
    def _can_send_signal(self, strike: int) -> bool:
        now = datetime.now(IST)
        key = f"nifty_{strike}"
        if key in self.last_signal_time:
            elapsed = (now - self.last_signal_time[key]).total_seconds()
            if elapsed < SIGNAL_COOLDOWN_SECONDS:
                logger.info(f"⏳ Signal cooldown: {int(SIGNAL_COOLDOWN_SECONDS - elapsed)}s")
                return False
        self.last_signal_time[key] = now
        return True
    
    async def run_cycle(self):
        """Main analysis cycle"""
        if not is_tradeable_time():
            return
        
        logger.info(f"\n{'='*80}")
        logger.info(f"🔍 NIFTY50 ANALYSIS SCAN")
        logger.info(f"{'='*80}")
        
        # Get market data
        df, strike_data, spot, futures, vol = await self.feed.get_market_data()
        if df.empty or not strike_data or spot == 0:
            logger.warning("⏳ Incomplete data, skipping")
            return
        
        # Calculate technical indicators
        vwap = self.analyzer.calculate_vwap(df)
        atr = self.analyzer.calculate_atr(df)
        pcr = self.analyzer.calculate_pcr(strike_data)
        candle_color, candle_size, candle_data = self.analyzer.get_candle_info(df)
        has_vol_spike, vol_mult = self.analyzer.check_volume_surge(vol)
        vwap_distance = abs(futures - vwap)
        order_flow = self.analyzer.calculate_order_flow_imbalance(strike_data)
        max_pain_strike, max_pain_dist = self.analyzer.calculate_max_pain(strike_data, spot)
        
        strike_gap = NIFTY_CONFIG['strike_gap']
        atm_strike = round(spot / strike_gap) * strike_gap
        gamma_zone = self.analyzer.detect_gamma_zone(strike_data, atm_strike)
        
        # Get total OI
        total_ce = sum(d['ce_oi'] for d in strike_data.values())
        total_pe = sum(d['pe_oi'] for d in strike_data.values())
        
        # Get OI changes with data availability flags
        ce_total_15m, pe_total_15m, has_15m_total = self.redis.get_total_oi_change(total_ce, total_pe, 15)
        ce_total_5m, pe_total_5m, has_5m_total = self.redis.get_total_oi_change(total_ce, total_pe, 5)
        
        # Get ATM OI changes
        atm_ce_15m, atm_pe_15m, has_15m_atm = 0, 0, False
        atm_ce_5m, atm_pe_5m, has_5m_atm = 0, 0, False
        atm_data = {}
        
        if atm_strike in strike_data:
            atm_data = strike_data[atm_strike]
            atm_ce_15m, atm_pe_15m, has_15m_atm = self.redis.get_strike_oi_change(atm_strike, atm_data, 15)
            atm_ce_5m, atm_pe_5m, has_5m_atm = self.redis.get_strike_oi_change(atm_strike, atm_data, 5)
            
            # Display with proper formatting
            ce_15m_display = f"{atm_ce_15m:+.1f}%" if has_15m_atm else "N/A"
            pe_15m_display = f"{atm_pe_15m:+.1f}%" if has_15m_atm else "N/A"
            ce_5m_display = f"{atm_ce_5m:+.1f}%" if has_5m_atm else "N/A"
            pe_5m_display = f"{atm_pe_5m:+.1f}%" if has_5m_atm else "N/A"
            
            logger.info(f"⚔️ ATM {atm_strike}: 15m CE={ce_15m_display} PE={pe_15m_display} | 5m CE={ce_5m_display} PE={pe_5m_display}")
        else:
            logger.info(f"⚠️ ATM strike {atm_strike} data not available")
        
        # Check multi-TF confirmation
        multi_tf = self.analyzer.check_multi_tf_confirmation(
            ce_total_5m, ce_total_15m, pe_total_5m, pe_total_15m,
            has_5m_total, has_15m_total
        )
        
        # Save current data
        for strike, data in strike_data.items():
            self.redis.save_strike_snapshot(strike, data)
        self.redis.save_total_oi_snapshot(total_ce, total_pe)
        
        # Check warmup status
        mem_stats = self.redis.get_memory_stats()
        if not mem_stats['warmed_up_15m']:
            logger.warning(f"⏳ Warmup: {mem_stats['elapsed_minutes']:.1f}/15 min | Snapshots: {mem_stats['snapshot_count']}")
        
        # Log current data
        logger.info(f"💰 Spot: {spot:.2f} | Futures: {futures:.2f}")
        logger.info(f"📊 VWAP: {vwap:.2f} | PCR: {pcr:.2f} | Candle: {candle_color}")
        logger.info(f"📉 Total OI 15m: CE={ce_total_15m:+.1f}% | PE={pe_total_15m:+.1f}% {'(N/A)' if not has_15m_total else ''}")
        logger.info(f"📉 Total OI 5m: CE={ce_total_5m:+.1f}% | PE={pe_total_5m:+.1f}% {'(N/A)' if not has_5m_total else ''}")
        
        # Check momentum
        bullish_momentum = self.analyzer.check_momentum(df, 'bullish')
        bearish_momentum = self.analyzer.check_momentum(df, 'bearish')
        
        # Create complete analysis data
        analysis = AnalysisData(
            spot_price=spot,
            futures_price=futures,
            atm_strike=atm_strike,
            vwap=vwap,
            atr=atr,
            pcr=pcr,
            total_ce_oi=total_ce,
            total_pe_oi=total_pe,
            atm_ce_oi=atm_data.get('ce_oi', 0),
            atm_pe_oi=atm_data.get('pe_oi', 0),
            atm_ce_vol=atm_data.get('ce_vol', 0),
            atm_pe_vol=atm_data.get('pe_vol', 0),
            ce_oi_15m=ce_total_15m,
            pe_oi_15m=pe_total_15m,
            ce_oi_5m=ce_total_5m,
            pe_oi_5m=pe_total_5m,
            atm_ce_change_15m=atm_ce_15m,
            atm_pe_change_15m=atm_pe_15m,
            atm_ce_change_5m=atm_ce_5m,
            atm_pe_change_5m=atm_pe_5m,
            candle_color=candle_color,
            candle_size=candle_size,
            open_price=candle_data['open'],
            high_price=candle_data['high'],
            low_price=candle_data['low'],
            close_price=candle_data['close'],
            current_volume=candle_data['volume'],
            vwap_distance=vwap_distance,
            volume_surge=vol_mult,
            has_volume_spike=has_vol_spike,
            order_flow_imbalance=order_flow,
            max_pain_strike=max_pain_strike,
            max_pain_distance=max_pain_dist,
            gamma_zone=gamma_zone,
            multi_tf_confirm=multi_tf,
            bullish_momentum=bullish_momentum,
            bearish_momentum=bearish_momentum,
            strike_data_summary=strike_data,
            data_quality={
                'has_15m_total_oi': has_15m_total,
                'has_5m_total_oi': has_5m_total,
                'has_15m_atm_oi': has_15m_atm,
                'has_5m_atm_oi': has_5m_atm,
                'warmed_up': mem_stats['warmed_up_15m']
            },
            warnings=[]
        )
        
        # Add warnings
        if not has_15m_total:
            analysis.warnings.append("15m total OI data not available")
        if not has_15m_atm:
            analysis.warnings.append("15m ATM OI data not available")
        
        # Send hourly update (v2.1 NEW)
        await self.send_hourly_update(
            spot, futures, atm_strike, vwap, atr, pcr,
            total_ce, total_pe, ce_total_15m, pe_total_15m,
            atm_data.get('ce_oi', 0), atm_data.get('pe_oi', 0),
            atm_ce_15m, atm_pe_15m,
            candle_data, vol
        )
        
        # Generate signal with complete analysis
        signal = self.generate_signal(
            spot, futures, vwap, vwap_distance, pcr, atr,
            ce_total_15m, pe_total_15m, ce_total_5m, pe_total_5m,
            atm_ce_15m, atm_pe_15m, candle_color, candle_size,
            has_vol_spike, vol_mult, df, order_flow, max_pain_dist, 
            gamma_zone, multi_tf, atm_strike, strike_data,
            has_15m_total, has_15m_atm, has_5m_total, has_5m_atm,
            analysis
        )
        
        if signal:
            if self._can_send_signal(signal.strike):
                await self.send_alert(signal)
            else:
                logger.info(f"✋ Duplicate signal blocked for strike {signal.strike}")
        else:
            logger.info("✋ No setup found")
        
        logger.info(f"{'='*80}\n")
    
    def generate_signal(self, spot_price, futures_price, vwap, vwap_distance, pcr, atr,
                       ce_total_15m, pe_total_15m, ce_total_5m, pe_total_5m,
                       atm_ce_change, atm_pe_change, candle_color, candle_size,
                       has_vol_spike, vol_mult, df, order_flow, max_pain_dist,
                       gamma_zone, multi_tf, atm_strike, strike_data,
                       has_15m_total, has_15m_atm, has_5m_total, has_5m_atm,
                       analysis: AnalysisData) -> Optional[Signal]:
        """Enhanced signal generation with data quality checks"""
        
        stop_loss_points = int(atr * ATR_SL_MULTIPLIER)
        target_points = int(atr * ATR_TARGET_MULTIPLIER)
        
        # Adjust targets based on OI strength
        if abs(ce_total_15m) >= OI_THRESHOLD_STRONG or abs(atm_ce_change) >= OI_THRESHOLD_STRONG:
            target_points = max(target_points, 80)
        elif abs(ce_total_15m) >= OI_THRESHOLD_MEDIUM or abs(atm_ce_change) >= OI_THRESHOLD_MEDIUM:
            target_points = max(target_points, 50)
        
        lot_size = NIFTY_CONFIG['lot_size']
        quantity = 1
        
        atm_data = strike_data.get(atm_strike, {})
        atm_ce_oi = atm_data.get('ce_oi', 0)
        atm_pe_oi = atm_data.get('pe_oi', 0)
        atm_ce_vol = atm_data.get('ce_vol', 0)
        atm_pe_vol = atm_data.get('pe_vol', 0)
        
        # CE BUY SIGNAL
        if (ce_total_15m < -OI_THRESHOLD_MEDIUM and has_15m_total) or \
           (atm_ce_change < -ATM_OI_THRESHOLD and has_15m_atm):
            
            checks = {
                "CE OI Unwinding (15m)": ce_total_15m < -OI_THRESHOLD_MEDIUM and has_15m_total,
                "ATM CE Unwinding (15m)": atm_ce_change < -ATM_OI_THRESHOLD and has_15m_atm,
                "Price > VWAP": futures_price > vwap,
                "GREEN Candle": candle_color == 'GREEN'
            }
            
            bonus = {
                "Strong 5m CE Unwinding": ce_total_5m < -5.0 and has_5m_total,
                "Big Candle": candle_size >= MIN_CANDLE_SIZE,
                "Far from VWAP": vwap_distance >= VWAP_BUFFER,
                "Bullish PCR": pcr > PCR_BULLISH,
                "Volume Spike": has_vol_spike,
                "Bullish Momentum": self.analyzer.check_momentum(df, 'bullish'),
                "Order Flow Bullish": order_flow < 1.0,
                "Multi-TF Confirmed": multi_tf,
                "Gamma Zone": gamma_zone,
                "ATM Strong Unwinding": atm_ce_change < -8.0 and has_15m_atm
            }
            
            passed = sum(checks.values())
            bonus_passed = sum(bonus.values())
            
            # Require at least 3 main checks AND good data quality
            if passed >= 3 and (has_15m_total or has_15m_atm):
                confidence = 70 + (passed * 5) + (bonus_passed * 3)
                confidence = min(confidence, 98)
                
                if confidence >= 90:
                    quantity = 2
                
                logger.info(f"🎯 CE BUY SIGNAL! Confidence: {confidence}%")
                logger.info(f"   Checks passed: {passed}/4 | Bonus: {bonus_passed}/10")
                
                return Signal(
                    type="CE_BUY",
                    reason=f"Call Unwinding (Total: {ce_total_15m:.1f}%, ATM: {atm_ce_change:.1f}%)",
                    confidence=confidence,
                    spot_price=spot_price,
                    futures_price=futures_price,
                    strike=atm_strike,
                    target_points=target_points,
                    stop_loss_points=stop_loss_points,
                    pcr=pcr,
                    candle_color=candle_color,
                    volume_surge=vol_mult,
                    oi_5m=ce_total_5m,
                    oi_15m=ce_total_15m,
                    atm_ce_change=atm_ce_change,
                    atm_pe_change=atm_pe_change,
                    atr=atr,
                    timestamp=datetime.now(IST),
                    order_flow_imbalance=order_flow,
                    max_pain_distance=max_pain_dist,
                    gamma_zone=gamma_zone,
                    multi_tf_confirm=multi_tf,
                    lot_size=lot_size,
                    quantity=quantity,
                    atm_ce_oi=atm_ce_oi,
                    atm_pe_oi=atm_pe_oi,
                    atm_ce_vol=atm_ce_vol,
                    atm_pe_vol=atm_pe_vol,
                    analysis=analysis
                )
        
        # PE BUY SIGNAL
        if (pe_total_15m < -OI_THRESHOLD_MEDIUM and has_15m_total) or \
           (atm_pe_change < -ATM_OI_THRESHOLD and has_15m_atm):
            
            if abs(pe_total_15m) >= OI_THRESHOLD_STRONG or abs(atm_pe_change) >= OI_THRESHOLD_STRONG:
                target_points = max(target_points, 80)
            
            checks = {
                "PE OI Unwinding (15m)": pe_total_15m < -OI_THRESHOLD_MEDIUM and has_15m_total,
                "ATM PE Unwinding (15m)": atm_pe_change < -ATM_OI_THRESHOLD and has_15m_atm,
                "Price < VWAP": futures_price < vwap,
                "RED Candle": candle_color == 'RED'
            }
            
            bonus = {
                "Strong 5m PE Unwinding": pe_total_5m < -5.0 and has_5m_total,
                "Big Candle": candle_size >= MIN_CANDLE_SIZE,
                "Far from VWAP": vwap_distance >= VWAP_BUFFER,
                "Bearish PCR": pcr < PCR_BEARISH,
                "Volume Spike": has_vol_spike,
                "Bearish Momentum": self.analyzer.check_momentum(df, 'bearish'),
                "Order Flow Bearish": order_flow > 1.5,
                "Multi-TF Confirmed": multi_tf,
                "Gamma Zone": gamma_zone
            }
            
            passed = sum(checks.values())
            bonus_passed = sum(bonus.values())
            
            if passed >= 3 and (has_15m_total or has_15m_atm):
                confidence = 70 + (passed * 5) + (bonus_passed * 3)
                confidence = min(confidence, 98)
                
                if confidence >= 90:
                    quantity = 2
                
                logger.info(f"🎯 PE BUY SIGNAL! Confidence: {confidence}%")
                logger.info(f"   Checks passed: {passed}/4 | Bonus: {bonus_passed}/9")
                
                return Signal(
                    type="PE_BUY",
                    reason=f"Put Unwinding (Total: {pe_total_15m:.1f}%, ATM: {atm_pe_change:.1f}%)",
                    confidence=confidence,
                    spot_price=spot_price,
                    futures_price=futures_price,
                    strike=atm_strike,
                    target_points=target_points,
                    stop_loss_points=stop_loss_points,
                    pcr=pcr,
                    candle_color=candle_color,
                    volume_surge=vol_mult,
                    oi_5m=pe_total_5m,
                    oi_15m=pe_total_15m,
                    atm_ce_change=atm_ce_change,
                    atm_pe_change=atm_pe_change,
                    atr=atr,
                    timestamp=datetime.now(IST),
                    order_flow_imbalance=order_flow,
                    max_pain_distance=max_pain_dist,
                    gamma_zone=gamma_zone,
                    multi_tf_confirm=multi_tf,
                    lot_size=lot_size,
                    quantity=quantity,
                    atm_ce_oi=atm_ce_oi,
                    atm_pe_oi=atm_pe_oi,
                    atm_ce_vol=atm_ce_vol,
                    atm_pe_vol=atm_pe_vol,
                    analysis=analysis
                )
        
        return None
    

    async def send_hourly_update(self, spot, futures, atm_strike, vwap, atr, pcr,
                                 total_ce, total_pe, ce_15m, pe_15m, 
                                 atm_ce_oi, atm_pe_oi, atm_ce_15m, atm_pe_15m,
                                 candle_data, volume):
        """Send hourly market data update to Telegram"""
        if not HOURLY_UPDATE_ENABLED:
            return
        
        if not self.hourly_collector.should_send_hourly_update():
            return
        
        # Collect snapshot
        snapshot = self.hourly_collector.collect_hourly_snapshot(
            spot, futures, atm_strike, vwap, atr, pcr,
            total_ce, total_pe, ce_15m, pe_15m,
            atm_ce_oi, atm_pe_oi, atm_ce_15m, atm_pe_15m,
            candle_data, volume
        )
        
        # Generate message
        message = self.hourly_collector.generate_hourly_json_message(snapshot)
        
        # Send to Telegram
        if self.telegram:
            try:
                await asyncio.wait_for(
                    self.telegram.send_message(chat_id=TELEGRAM_CHAT_ID, text=message),
                    timeout=TELEGRAM_TIMEOUT
                )
                logger.info(f"✅ Hourly update sent: {snapshot['hour']}")
                self.hourly_collector.last_hourly_update = datetime.now(IST)
            except asyncio.TimeoutError:
                logger.warning("⚠️ Hourly update timed out")
            except Exception as e:
                logger.error(f"❌ Hourly update error: {e}")
    
    async def send_alert(self, s: Signal):
        """Send enhanced alert with complete analysis data"""
        if s.type == "CE_BUY":
            entry = s.spot_price
            target = entry + s.target_points
            stop_loss = entry - s.stop_loss_points
            emoji = "🟢"
            target_direction = "+"
            sl_direction = "-"
        else:
            entry = s.spot_price
            target = entry - s.target_points
            stop_loss = entry + s.stop_loss_points
            emoji = "🔴"
            target_direction = "-"
            sl_direction = "+"
        
        mode = "🧪 ALERT ONLY" if ALERT_ONLY_MODE else "⚡ LIVE"
        timestamp_str = s.timestamp.strftime('%d-%b %I:%M %p')
        risk = abs(entry - stop_loss)
        reward = abs(target - entry)
        rr_ratio = reward / risk if risk > 0 else 0
        
        # Build main signal message
        msg = f"""
{emoji} NIFTY50 STRIKE MASTER PRO v2.0

{mode}

━━━━━━━━━━━━━━━━━━━━━━━━
⚡ SIGNAL: {s.type}
━━━━━━━━━━━━━━━━━━━━━━━━

📍 Entry: {entry:.1f}
🎯 Target: {target:.1f} ({target_direction}{s.target_points:.0f} pts)
🛑 Stop Loss: {stop_loss:.1f} ({sl_direction}{s.stop_loss_points:.0f} pts)
📊 Strike: {s.strike}
📦 Quantity: {s.quantity} lots ({s.quantity * s.lot_size} units)
💎 Risk:Reward = 1:{rr_ratio:.1f}

━━━━━━━━━━━━━━━━━━━━━━━━
📊 LOGIC & CONFIDENCE
━━━━━━━━━━━━━━━━━━━━━━━━

{s.reason}
Confidence: {s.confidence}%

━━━━━━━━━━━━━━━━━━━━━━━━
💰 MARKET DATA
━━━━━━━━━━━━━━━━━━━━━━━━

Spot: {s.spot_price:.1f}
Futures: {s.futures_price:.1f}
PCR: {s.pcr:.2f}
Candle: {s.candle_color}
Volume: {s.volume_surge:.1f}x
ATR: {s.atr:.1f}

━━━━━━━━━━━━━━━━━━━━━━━━
📉 OI ANALYSIS (Multi-TF)
━━━━━━━━━━━━━━━━━━━━━━━━

Total OI Change:
  5m:  CE={s.oi_5m:+.1f}% | PE={-s.oi_5m:+.1f}%
  15m: CE={s.oi_15m:+.1f}% | PE={-s.oi_15m:+.1f}%

ATM Strike {s.strike}:
  CE: {s.atm_ce_change:+.1f}% {"(N/A)" if s.atm_ce_change == 0 else ""}
  PE: {s.atm_pe_change:+.1f}% {"(N/A)" if s.atm_pe_change == 0 else ""}

ATM OI Levels:
  CE OI: {s.atm_ce_oi:,} | Vol: {s.atm_ce_vol:,}
  PE OI: {s.atm_pe_oi:,} | Vol: {s.atm_pe_vol:,}

━━━━━━━━━━━━━━━━━━━━━━━━
🔥 ADVANCED METRICS
━━━━━━━━━━━━━━━━━━━━━━━━

Order Flow: {s.order_flow_imbalance:.2f}
{"  (CE Buying)" if s.order_flow_imbalance < 1.0 else "  (PE Buying)"}

Max Pain: {s.max_pain_distance:.0f} pts away

{"⚡ Gamma Zone: ACTIVE" if s.gamma_zone else ""}
{"✅ Multi-TF: CONFIRMED" if s.multi_tf_confirm else ""}

━━━━━━━━━━━━━━━━━━━━━━━━

⏰ {timestamp_str}

✅ v2.0 - Complete Analysis
"""
        
        logger.info(f"🚨 {s.type} @ {entry:.1f} → Target: {target:.1f} | SL: {stop_loss:.1f}")
        
        # Send main signal
        if self.telegram:
            try:
                await asyncio.wait_for(
                    self.telegram.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg),
                    timeout=TELEGRAM_TIMEOUT
                )
                logger.info("✅ Alert sent to Telegram")
            except asyncio.TimeoutError:
                logger.warning("⚠️ Telegram alert timed out")
            except Exception as e:
                logger.error(f"❌ Telegram error: {e}")
        
        # Send detailed analysis if enabled
        if SEND_ANALYSIS_DATA and s.analysis and self.telegram:
            await self.send_analysis_data(s.analysis, s.type)
    
    async def send_analysis_data(self, analysis: AnalysisData, signal_type: str):
        """Send complete analysis data for transparency"""
        timestamp_str = analysis.timestamp.strftime('%d-%b %I:%M %p')
        
        # Format strike data
        strike_summary = []
        for strike, data in sorted(analysis.strike_data_summary.items()):
            ce_oi = data['ce_oi']
            pe_oi = data['pe_oi']
            ce_vol = data['ce_vol']
            pe_vol = data['pe_vol']
            mark = "🎯" if strike == analysis.atm_strike else "  "
            strike_summary.append(f"{mark}{strike}: CE OI={ce_oi:,} Vol={ce_vol:,} | PE OI={pe_oi:,} Vol={pe_vol:,}")
        
        strikes_text = "\n".join(strike_summary[:5])  # Top 5 strikes
        
        # Data quality status
        quality_items = []
        for key, value in analysis.data_quality.items():
            status = "✅" if value else "❌"
            quality_items.append(f"{status} {key.replace('_', ' ').title()}")
        quality_text = "\n".join(quality_items)
        
        # Warnings
        warnings_text = "\n".join([f"⚠️ {w}" for w in analysis.warnings]) if analysis.warnings else "✅ No warnings"
        
        analysis_msg = f"""
📊 COMPLETE ANALYSIS DATA
━━━━━━━━━━━━━━━━━━━━━━━━

🕐 Time: {timestamp_str}
📍 Signal: {signal_type}

━━━━━━━━━━━━━━━━━━━━━━━━
💰 PRICE DATA
━━━━━━━━━━━━━━━━━━━━━━━━

Spot: {analysis.spot_price:.2f}
Futures: {analysis.futures_price:.2f}
VWAP: {analysis.vwap:.2f}
Distance from VWAP: {analysis.vwap_distance:.2f}

━━━━━━━━━━━━━━━━━━━━━━━━
🕯️ CANDLE DATA
━━━━━━━━━━━━━━━━━━━━━━━━

Color: {analysis.candle_color}
Size: {analysis.candle_size:.2f}
Open: {analysis.open_price:.2f}
High: {analysis.high_price:.2f}
Low: {analysis.low_price:.2f}
Close: {analysis.close_price:.2f}
Volume: {analysis.current_volume:,}

━━━━━━━━━━━━━━━━━━━━━━━━
📈 TECHNICAL INDICATORS
━━━━━━━━━━━━━━━━━━━━━━━━

ATR: {analysis.atr:.2f}
PCR: {analysis.pcr:.2f}
Order Flow: {analysis.order_flow_imbalance:.2f}
Volume Surge: {analysis.volume_surge:.1f}x
Has Spike: {"Yes" if analysis.has_volume_spike else "No"}

━━━━━━━━━━━━━━━━━━━━━━━━
📊 OI DATA (Total)
━━━━━━━━━━━━━━━━━━━━━━━━

Total CE OI: {analysis.total_ce_oi:,}
Total PE OI: {analysis.total_pe_oi:,}

15m Changes:
  CE: {analysis.ce_oi_15m:+.1f}%
  PE: {analysis.pe_oi_15m:+.1f}%

5m Changes:
  CE: {analysis.ce_oi_5m:+.1f}%
  PE: {analysis.pe_oi_5m:+.1f}%

━━━━━━━━━━━━━━━━━━━━━━━━
🎯 ATM STRIKE {analysis.atm_strike}
━━━━━━━━━━━━━━━━━━━━━━━━

CE OI: {analysis.atm_ce_oi:,}
PE OI: {analysis.atm_pe_oi:,}
CE Vol: {analysis.atm_ce_vol:,}
PE Vol: {analysis.atm_pe_vol:,}

15m Changes:
  CE: {analysis.atm_ce_change_15m:+.1f}%
  PE: {analysis.atm_pe_change_15m:+.1f}%

5m Changes:
  CE: {analysis.atm_ce_change_5m:+.1f}%
  PE: {analysis.atm_pe_change_5m:+.1f}%

━━━━━━━━━━━━━━━━━━━━━━━━
🎲 ADVANCED ANALYSIS
━━━━━━━━━━━━━━━━━━━━━━━━

Max Pain: {analysis.max_pain_strike}
Distance: {analysis.max_pain_distance:.0f} pts

Gamma Zone: {"Yes ⚡" if analysis.gamma_zone else "No"}
Multi-TF: {"Confirmed ✅" if analysis.multi_tf_confirm else "Not Confirmed"}
Bullish Momentum: {"Yes" if analysis.bullish_momentum else "No"}
Bearish Momentum: {"Yes" if analysis.bearish_momentum else "No"}

━━━━━━━━━━━━━━━━━━━━━━━━
📋 STRIKE DATA
━━━━━━━━━━━━━━━━━━━━━━━━

{strikes_text}

━━━━━━━━━━━━━━━━━━━━━━━━
✅ DATA QUALITY
━━━━━━━━━━━━━━━━━━━━━━━━

{quality_text}

━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ WARNINGS
━━━━━━━━━━━━━━━━━━━━━━━━

{warnings_text}

━━━━━━━━━━━━━━━━━━━━━━━━

📊 Use this data to verify the signal
and make informed trading decisions!
"""
        
        if self.telegram:
            try:
                await asyncio.wait_for(
                    self.telegram.send_message(chat_id=TELEGRAM_CHAT_ID, text=analysis_msg),
                    timeout=TELEGRAM_TIMEOUT
                )
                logger.info("✅ Analysis data sent to Telegram")
            except asyncio.TimeoutError:
                logger.warning("⚠️ Analysis data timed out")
            except Exception as e:
                logger.error(f"❌ Analysis send error: {e}")
    
    async def send_startup_message(self):
        """Send startup notification"""
        now = datetime.now(IST)
        startup_time = now.strftime('%d-%b %I:%M %p')
        mode = "🧪 ALERT ONLY" if ALERT_ONLY_MODE else "⚡ LIVE TRADING"
        expiry_weekly = get_next_tuesday_expiry().strftime('%d-%b-%Y')
        expiry_monthly = get_monthly_expiry().strftime('%d-%b-%Y')
        
        msg = f"""
🚀 NIFTY50 STRIKE MASTER PRO v2.0

━━━━━━━━━━━━━━━━━━━━━━━━
✅ STATUS
━━━━━━━━━━━━━━━━━━━━━━━━

⏰ Started: {startup_time}
📊 Index: NIFTY 50
🔄 Mode: {mode}
✅ ALL SYSTEMS OPERATIONAL

━━━━━━━━━━━━━━━━━━━━━━━━
📅 CONFIGURATION
━━━━━━━━━━━━━━━━━━━━━━━━

Weekly Expiry: {expiry_weekly}
Monthly Expiry: {expiry_monthly}

Futures: {self.feed.futures_symbol}
Strikes: 5 (ATM ± 2 × 50)
Lot Size: {NIFTY_CONFIG['lot_size']}
Scan: Every {SCAN_INTERVAL}s

Memory TTL: {MEMORY_TTL_SECONDS/3600:.1f} hours
Signal Cooldown: {SIGNAL_COOLDOWN_SECONDS/60:.0f} min

━━━━━━━━━━━━━━━━━━━━━━━━
🔥 NEW FEATURES v2.1
━━━━━━━━━━━━━━━━━━━━━━━━

✅ Optimized Thresholds (4%, 2.5%, 3%)
✅ Hourly JSON Updates to Telegram
✅ Complete Analysis Data in Alerts
✅ Enhanced OI Tracking (2h TTL)
✅ Better Data Quality Checks
✅ Auto-detect Spot Price Format
✅ Multi-timeframe Validation
✅ Detailed Strike Data
✅ Data Quality Warnings
✅ Enhanced Debugging

━━━━━━━━━━━━━━━━━━━━━━━━
📊 ANALYSIS FEATURES
━━━━━━━━━━━━━━━━━━━━━━━━

✅ 500 Futures Candles (1-min)
✅ Multi-TF OI (5m + 15m)
✅ VWAP & ATR Analysis
✅ PCR & Order Flow
✅ Gamma Zone Detection
✅ Max Pain Calculation
✅ Volume Spike Detection
✅ Smart Rate Limiting
✅ Enhanced Redis Memory
✅ Duplicate Signal Filter
✅ Data Availability Checks

━━━━━━━━━━━━━━━━━━━━━━━━

🎯 System Active | Ready to Scan
📊 Detailed Analysis: {"ON" if SEND_ANALYSIS_DATA else "OFF"}
"""
        if self.telegram:
            try:
                await asyncio.wait_for(
                    self.telegram.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg),
                    timeout=TELEGRAM_TIMEOUT
                )
                logger.info("✅ Startup message sent")
            except:
                pass

async def main():
    logger.info("=" * 80)
    logger.info("🚀 NIFTY50 STRIKE MASTER PRO v2.0 - ENHANCED")
    logger.info("=" * 80)
    logger.info("")
    logger.info("📊 Index: NIFTY 50")
    logger.info(f"🔔 Mode: {'ALERT ONLY' if ALERT_ONLY_MODE else 'LIVE TRADING'}")
    logger.info(f"⏱️ Scan Interval: {SCAN_INTERVAL} seconds")
    logger.info(f"📊 Send Analysis: {'YES' if SEND_ANALYSIS_DATA else 'NO'}")
    logger.info("")
    
    # Fetch futures instrument key
    logger.info("🔍 Finding correct futures instrument...")
    futures_key = await fetch_futures_instrument_key()
    
    if not futures_key:
        logger.error("❌ Could not find futures instrument!")
        logger.error("   Check if NIFTY futures are available for current expiry")
        return
    
    logger.info("")
    
    try:
        bot = NiftyStrikeMaster(futures_key)
        logger.info("✅ Bot initialized")
    except Exception as e:
        logger.error(f"❌ Initialization failed: {e}")
        return
    
    logger.info("")
    logger.info("🔥 ENHANCED FEATURES v2.0:")
    logger.info("   ✅ Complete Analysis Data in Telegram")
    logger.info("   ✅ Fixed OI Tracking (2h TTL)")
    logger.info("   ✅ Better Data Quality Checks")
    logger.info("   ✅ Auto-detect Spot Price Format")
    logger.info("   ✅ Enhanced Multi-TF Validation")
    logger.info("   ✅ Detailed Strike Analysis")
    logger.info("   ✅ Data Availability Warnings")
    logger.info("   ✅ Professional Grade Signals")
    logger.info("")
    logger.info("=" * 80)
    
    await bot.send_startup_message()
    iteration = 0
    
    while True:
        try:
            now = datetime.now(IST).time()
            if time(9, 15) <= now <= time(15, 30):
                iteration += 1
                logger.info(f"\n{'='*80}")
                logger.info(f"🔄 SCAN #{iteration} - {datetime.now(IST).strftime('%I:%M:%S %p')}")
                logger.info(f"{'='*80}")
                await bot.run_cycle()
                await asyncio.sleep(SCAN_INTERVAL)
            else:
                logger.info("🌙 Market closed, waiting...")
                await asyncio.sleep(300)
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopped by user")
            break
        except Exception as e:
            logger.error(f"💥 Critical error: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(30)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n👋 Shutdown complete")
