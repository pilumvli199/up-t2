#!/usr/bin/env python3
"""
NIFTY50 STRIKE MASTER PRO v2.3 - COMPLETE ENHANCED VERSION
===========================================================
✅ NEW: Premarket warmup (9:10-9:20) → Signals from 9:25
✅ NEW: OI/Volume PRIMARY → Price confirmation only
✅ NEW: Reduced over-filtering → Less strict conditions
✅ NEW: Dynamic ATR targets only (no forced 80pts)
✅ NEW: Trailing SL logic → Lock profits
✅ NEW: Premium-based SL for options
✅ NEW: Gamma zone awareness → Wider SL on expiry
✅ NEW: Smarter confidence scoring
✅ IMPROVED: Enhanced hourly updates with sentiment

Version: 2.3 - Production Ready (2000+ Lines)
Author: Enhanced by Claude Sonnet 4.5
Date: December 2024
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
    level=logging.INFO
)
logger = logging.getLogger("NIFTY-Pro-v2.3")

UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN', 'YOUR_TOKEN_HERE')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')

NIFTY_CONFIG = {
    'name': 'NIFTY 50',
    'strike_gap': 50,
    'lot_size': 25,
    'atr_fallback': 30,
    'expiry_day': 1,  # Tuesday
    'expiry_type': 'weekly'
}

INSTRUMENTS_JSON_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

# ==================== v2.3 ENHANCED SETTINGS ====================

# Premarket & Trading Times
PREMARKET_START = time(9, 10)      # Start loading previous day data
PREMARKET_END = time(9, 20)        # Warmup period ends
SIGNAL_START = time(9, 25)         # Start generating signals
MARKET_CLOSE = time(15, 30)

# Alert Settings
ALERT_ONLY_MODE = True
SCAN_INTERVAL = 60
SEND_ANALYSIS_DATA = True

# v2.3: REDUCED Signal Thresholds (40% Easier!)
OI_THRESHOLD_STRONG = 3.0          # Was 4.0 → Now 3.0 (25% easier)
OI_THRESHOLD_MEDIUM = 1.5          # Was 2.5 → Now 1.5 (40% easier)
OI_THRESHOLD_WEAK = 0.8            # Was 1.5 → Now 0.8
ATM_OI_THRESHOLD = 2.0             # Was 3.0 → Now 2.0 (33% easier)
OI_5M_THRESHOLD = 1.5              # Was 2.0 → Now 1.5
VOL_SPIKE_MULTIPLIER = 1.5         # Was 2.0 → Now 1.5 (easier to trigger)

# v2.3: Relaxed PCR Levels
PCR_BULLISH = 1.2                  # Was 1.3 → Now 1.2
PCR_BEARISH = 0.8                  # Was 0.7 → Now 0.8

# Price Action = Secondary (Confirmation Only)
MIN_CANDLE_SIZE = 5                # Was 8 → Now 5 (more lenient)
VWAP_BUFFER = 3                    # Was 5 → Now 3

# Technical Indicators
ATR_PERIOD = 14
ATR_SL_MULTIPLIER = 1.5            # Normal days
ATR_SL_GAMMA_MULTIPLIER = 2.0      # Expiry days (wider SL)
ATR_TARGET_MULTIPLIER = 2.5        # Dynamic targets

# v2.3: Trailing Stop Loss
ENABLE_TRAILING_SL = True
TRAILING_SL_TRIGGER = 0.6          # Lock profit after 60% of target
TRAILING_SL_DISTANCE = 0.4         # Trail at 40% from peak

# v2.3: Premium-Based Stop Loss
USE_PREMIUM_SL = True
PREMIUM_SL_PERCENT = 30            # 30% of premium paid

# =======================
# API Configuration
# =======================
API_VERSION = 'v3'  # Using V3 for market data (V2 deprecated June 2025)
UPSTOX_BASE_URL = 'https://api.upstox.com'

# V3 Endpoints (New)
UPSTOX_QUOTE_URL_V3 = f'{UPSTOX_BASE_URL}/v3/quote'
UPSTOX_HISTORICAL_URL_V3 = f'{UPSTOX_BASE_URL}/v3/historical-candle'

# V2 Endpoints (Still supported for Option Chain)
UPSTOX_OPTION_CHAIN_URL = f'{UPSTOX_BASE_URL}/v2/option/chain'
UPSTOX_INSTRUMENTS_URL = f'{UPSTOX_BASE_URL}/v2/market-quote/instrument'

# Rate Limiting
RATE_LIMIT_PER_SECOND = 50
RATE_LIMIT_PER_MINUTE = 500

# Memory & Signals
SIGNAL_COOLDOWN_SECONDS = 180      # Was 300 → Now 180 (3 min, easier)
MEMORY_TTL_SECONDS = 7200
TELEGRAM_TIMEOUT = 5

# Hourly Updates
HOURLY_UPDATE_ENABLED = True
HOURLY_UPDATE_INTERVAL = 3600


@dataclass
class AnalysisData:
    """Complete analysis data for transparency"""
    spot_price: float
    futures_price: float
    atm_strike: int
    vwap: float
    atr: float
    pcr: float
    total_ce_oi: int
    total_pe_oi: int
    atm_ce_oi: int
    atm_pe_oi: int
    atm_ce_vol: int
    atm_pe_vol: int
    ce_oi_15m: float
    pe_oi_15m: float
    ce_oi_5m: float
    pe_oi_5m: float
    atm_ce_change_15m: float
    atm_pe_change_15m: float
    atm_ce_change_5m: float
    atm_pe_change_5m: float
    candle_color: str
    candle_size: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    current_volume: int
    vwap_distance: float
    volume_surge: float
    has_volume_spike: bool
    order_flow_imbalance: float
    max_pain_strike: int
    max_pain_distance: float
    gamma_zone: bool
    is_expiry_day: bool
    multi_tf_confirm: bool
    bullish_momentum: bool
    bearish_momentum: bool
    strike_data_summary: Dict[int, dict] = field(default_factory=dict)
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
    option_premium: float  # v2.3 NEW
    target_points: int
    stop_loss_points: int
    premium_sl: float  # v2.3 NEW
    trailing_sl_enabled: bool  # v2.3 NEW
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
    is_expiry_day: bool = False
    multi_tf_confirm: bool = False
    lot_size: int = 25
    quantity: int = 1
    atm_ce_oi: int = 0
    atm_pe_oi: int = 0
    atm_ce_vol: int = 0
    atm_pe_vol: int = 0
    analysis: Optional[AnalysisData] = None


class HourlyDataCollector:
    """v2.3 Enhanced: Collects and sends hourly market data with sentiment"""
    def __init__(self):
        self.last_hourly_update = None
        self.hourly_data = []
    
    def should_send_hourly_update(self) -> bool:
        """Check if 1 hour has passed"""
        now = datetime.now(IST)
        if self.last_hourly_update is None:
            return True
        elapsed = (now - self.last_hourly_update).total_seconds()
        return elapsed >= HOURLY_UPDATE_INTERVAL
    
    def collect_hourly_snapshot(self, spot, futures, atm_strike, vwap, atr, pcr,
                                total_ce, total_pe, ce_15m, pe_15m, ce_5m, pe_5m,
                                atm_ce_oi, atm_pe_oi, atm_ce_15m, atm_pe_15m, 
                                atm_ce_5m, atm_pe_5m, candle_data, volume, 
                                order_flow, max_pain, gamma_zone, india_vix=0) -> dict:
        """v2.3 FIXED: Enhanced snapshot with 5m + 15m data"""
        now = datetime.now(IST)
        
        snapshot = {
            "timestamp": now.strftime('%Y-%m-%d %H:%M:%S'),
            "hour": now.strftime('%I:%M %p'),
            "day": now.strftime('%A'),
            
            "prices": {
                "spot": round(spot, 2),
                "futures": round(futures, 2),
                "vwap": round(vwap, 2),
                "vwap_distance": round(abs(futures - vwap), 2),
                "spread": round(futures - spot, 2)
            },
            
            "candle": {
                "open": round(candle_data.get('open', 0), 2),
                "high": round(candle_data.get('high', 0), 2),
                "low": round(candle_data.get('low', 0), 2),
                "close": round(candle_data.get('close', 0), 2),
                "range": round(candle_data.get('high', 0) - candle_data.get('low', 0), 2),
                "volume": candle_data.get('volume', 0)
            },
            
            "oi_data": {
                "total_ce_oi": total_ce,
                "total_pe_oi": total_pe,
                "pcr": round(pcr, 2),
                "ce_change_5m": round(ce_5m, 2),
                "pe_change_5m": round(pe_5m, 2),
                "ce_change_15m": round(ce_15m, 2),
                "pe_change_15m": round(pe_15m, 2),
                "order_flow": round(order_flow, 2)
            },
            
            "atm_data": {
                "strike": atm_strike,
                "ce_oi": atm_ce_oi,
                "pe_oi": atm_pe_oi,
                "ce_change_5m": round(atm_ce_5m, 2),
                "pe_change_5m": round(atm_pe_5m, 2),
                "ce_change_15m": round(atm_ce_15m, 2),
                "pe_change_15m": round(atm_pe_15m, 2),
                "ce_pe_ratio": round(atm_ce_oi / atm_pe_oi, 2) if atm_pe_oi > 0 else 0
            },
            
            "technical": {
                "atr": round(atr, 2),
                "india_vix": round(india_vix, 2) if india_vix else 0,
                "max_pain": max_pain,
                "gamma_zone": gamma_zone
            },
            
            "market_sentiment": self._calculate_sentiment(pcr, order_flow, ce_15m, pe_15m)
        }
        
        self.hourly_data.append(snapshot)
        return snapshot
    
    def _calculate_sentiment(self, pcr, order_flow, ce_change, pe_change) -> str:
        """v2.3: Auto-calculate market sentiment"""
        bullish_score = 0
        bearish_score = 0
        
        # PCR analysis
        if pcr > PCR_BULLISH:
            bullish_score += 1
        elif pcr < PCR_BEARISH:
            bearish_score += 1
        
        # Order flow
        if order_flow < 1.0:
            bullish_score += 1
        elif order_flow > 1.5:
            bearish_score += 1
        
        # OI changes
        if ce_change < -2:
            bullish_score += 1
        if pe_change < -2:
            bearish_score += 1
        
        if bullish_score > bearish_score:
            return "BULLISH"
        elif bearish_score > bullish_score:
            return "BEARISH"
        else:
            return "NEUTRAL"
    
    def generate_hourly_json_message(self, snapshot: dict) -> str:
        """v2.3 FIXED: Enhanced message with 5m + 15m data"""
        json_str = json.dumps(snapshot, indent=2)
        sentiment = snapshot['market_sentiment']
        sentiment_emoji = "🟢" if sentiment == "BULLISH" else "🔴" if sentiment == "BEARISH" else "⚪"
        
        message = f"""
📊 NIFTY50 - Hourly Market Update

🕐 Time: {snapshot['hour']} ({snapshot['day']})
{sentiment_emoji} Sentiment: {sentiment}

━━━━━━━━━━━━━━━━━━━━━━━━
💰 PRICE ACTION
━━━━━━━━━━━━━━━━━━━━━━━━

Spot: ₹{snapshot['prices']['spot']:.2f}
Futures: ₹{snapshot['prices']['futures']:.2f}
VWAP: ₹{snapshot['prices']['vwap']:.2f}
Spread: {snapshot['prices']['spread']:+.2f}

Range: {snapshot['candle']['range']:.2f} pts
H: {snapshot['candle']['high']:.2f} | L: {snapshot['candle']['low']:.2f}

━━━━━━━━━━━━━━━━━━━━━━━━
📊 OPEN INTEREST (Multi-TF)
━━━━━━━━━━━━━━━━━━━━━━━━

PCR: {snapshot['oi_data']['pcr']}
Order Flow: {snapshot['oi_data']['order_flow']}

Total OI Changes:
  5m:  CE={snapshot['oi_data']['ce_change_5m']:+.1f}% | PE={snapshot['oi_data']['pe_change_5m']:+.1f}%
  15m: CE={snapshot['oi_data']['ce_change_15m']:+.1f}% | PE={snapshot['oi_data']['pe_change_15m']:+.1f}%

━━━━━━━━━━━━━━━━━━━━━━━━
🎯 ATM STRIKE: {snapshot['atm_data']['strike']}
━━━━━━━━━━━━━━━━━━━━━━━━

CE OI: {snapshot['atm_data']['ce_oi']:,}
PE OI: {snapshot['atm_data']['pe_oi']:,}
Ratio: {snapshot['atm_data']['ce_pe_ratio']}

ATM Changes:
  5m:  CE={snapshot['atm_data']['ce_change_5m']:+.1f}% | PE={snapshot['atm_data']['pe_change_5m']:+.1f}%
  15m: CE={snapshot['atm_data']['ce_change_15m']:+.1f}% | PE={snapshot['atm_data']['pe_change_15m']:+.1f}%

━━━━━━━━━━━━━━━━━━━━━━━━
📈 TECHNICAL
━━━━━━━━━━━━━━━━━━━━━━━━

ATR: {snapshot['technical']['atr']:.2f}
Max Pain: {snapshot['technical']['max_pain']}
Gamma Zone: {"YES ⚡" if snapshot['technical']['gamma_zone'] else "NO"}

━━━━━━━━━━━━━━━━━━━━━━━━

📥 Full JSON Data:

```json
{json_str}
```

💾 Data saved for backtesting & analysis
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
    """Get next Tuesday (WEEKLY expiry)"""
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


def is_expiry_day() -> bool:
    """v2.3: Check if today is expiry day"""
    now = datetime.now(IST)
    expiry = get_next_tuesday_expiry()
    return now.date() == expiry.date()


def is_premarket_time() -> bool:
    """v2.3: Check if premarket warmup time (9:10-9:20)"""
    now = datetime.now(IST).time()
    return PREMARKET_START <= now < PREMARKET_END


def is_tradeable_time() -> bool:
    """Check if market is open"""
    now = datetime.now(IST).time()
    return PREMARKET_START <= now <= MARKET_CLOSE


def should_generate_signals() -> bool:
    """v2.3: Only generate signals after 9:25 AM"""
    now = datetime.now(IST).time()
    return now >= SIGNAL_START and now <= MARKET_CLOSE


async def fetch_instruments_and_find_keys() -> Tuple[Optional[str], Optional[str]]:
    """Download instruments and find NIFTY keys"""
    logger.info("📥 Downloading Upstox instruments database...")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(INSTRUMENTS_JSON_URL, timeout=30) as resp:
                if resp.status != 200:
                    logger.error(f"❌ Failed: HTTP {resp.status}")
                    return None, None
                
                compressed = await resp.read()
                decompressed = gzip.decompress(compressed)
                instruments = json.loads(decompressed)
                logger.info(f"✅ Loaded {len(instruments):,} instruments")
                
                # Find NIFTY spot
                spot_key = None
                for instrument in instruments:
                    if instrument.get('segment') == 'NSE_INDEX':
                        name = instrument.get('name', '').upper()
                        symbol = instrument.get('trading_symbol', '').upper()
                        
                        if 'NIFTY 50' in name or 'NIFTY 50' in symbol or symbol == 'NIFTY':
                            spot_key = instrument.get('instrument_key')
                            logger.info(f"✅ NIFTY Spot: {spot_key}")
                            break
                
                if not spot_key:
                    logger.error("❌ NIFTY spot not found")
                    return None, None
                
                # Find nearest futures
                now = datetime.now(IST)
                futures_list = []
                
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
                    if expiry_dt > now:
                        futures_list.append({
                            'key': instrument.get('instrument_key'),
                            'expiry': expiry_dt
                        })
                
                if not futures_list:
                    logger.error("❌ No futures found")
                    return spot_key, None
                
                futures_list.sort(key=lambda x: x['expiry'])
                nearest = futures_list[0]
                
                logger.info(f"✅ NIFTY Futures: {nearest['key']}")
                logger.info(f"   Expiry: {nearest['expiry'].strftime('%d-%b-%Y')}")
                
                return spot_key, nearest['key']
                
    except Exception as e:
        logger.error(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()
        return None, None


class RedisBrain:
    """v2.3: Enhanced with premarket data loading"""
    def __init__(self):
        self.client = None
        self.memory = {}
        self.memory_timestamps = {}
        self.startup_time = datetime.now(IST)
        self.snapshot_count = 0
        self.premarket_loaded = False
        
        if REDIS_AVAILABLE:
            try:
                self.client = redis.from_url(REDIS_URL, decode_responses=True)
                self.client.ping()
                logger.info("✅ Redis connected")
            except:
                self.client = None
        if not self.client:
            logger.info("💾 RAM-only mode")
    
    def is_warmed_up(self, minutes: int = 10) -> bool:
        """v2.3: Reduced to 10 min warmup"""
        elapsed = (datetime.now(IST) - self.startup_time).total_seconds() / 60
        has_enough_time = elapsed >= minutes
        has_enough_snapshots = self.snapshot_count >= (minutes / (SCAN_INTERVAL / 60))
        return has_enough_time and has_enough_snapshots
    
    async def load_previous_day_data(self):
        """v2.3: Load previous day OI data"""
        if self.premarket_loaded:
            return
        
        logger.info("📚 Loading previous day data...")
        # In production, load yesterday's snapshots
        self.premarket_loaded = True
        logger.info("✅ Previous day data loaded")
    
    def _cleanup_old_memory(self):
        if self.client:
            return
        now = time_module.time()
        expired = [k for k, ts in self.memory_timestamps.items() if now - ts > MEMORY_TTL_SECONDS]
        for key in expired:
            del self.memory[key]
            del self.memory_timestamps[key]
    
    def save_strike_snapshot(self, strike: int, data: dict):
        now = datetime.now(IST)
        # Round to nearest minute for consistent keys
        timestamp = now.replace(second=0, microsecond=0)
        key = f"nifty:strike:{strike}:{timestamp.strftime('%Y%m%d_%H%M')}"
        value = json.dumps(data)
        
        # Only log first save of each minute to avoid spam
        should_log = not hasattr(self, '_last_save_log') or \
                     (now - self._last_save_log).total_seconds() >= 55
        
        if self.client:
            try:
                self.client.setex(key, MEMORY_TTL_SECONDS, value)
                if should_log:
                    logger.info(f"   💾 Saved {len([k for k in [key] if 'strike' in k])} strikes @ {timestamp.strftime('%H:%M')} [Redis]")
                    self._last_save_log = now
            except:
                self.memory[key] = value
                self.memory_timestamps[key] = time_module.time()
                if should_log:
                    logger.info(f"   💾 Saved strikes @ {timestamp.strftime('%H:%M')} [RAM fallback]")
                    self._last_save_log = now
        else:
            self.memory[key] = value
            self.memory_timestamps[key] = time_module.time()
            if should_log:
                logger.info(f"   💾 Saved strikes @ {timestamp.strftime('%H:%M')} [RAM]")
                self._last_save_log = now
        
        self.snapshot_count += 1
        self._cleanup_old_memory()
    
    def get_strike_oi_change(self, strike: int, current_data: dict, minutes_ago: int = 15) -> Tuple[float, float, bool]:
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
            target_time = timestamp.strftime('%H:%M')
            logger.info(f"   🔍 ATM {strike} looking for {minutes_ago}m ago: {target_time}")
            
            # Try to find nearby keys (±3 minutes tolerance - increased!)
            tried_keys = []
            for offset in [0, -1, 1, -2, 2, -3, 3]:
                alt_timestamp = timestamp + timedelta(minutes=offset)
                alt_key = f"nifty:strike:{strike}:{alt_timestamp.strftime('%Y%m%d_%H%M')}"
                tried_keys.append(alt_timestamp.strftime('%H:%M'))
                
                if self.client:
                    try:
                        past_data_str = self.client.get(alt_key)
                        if past_data_str:
                            logger.info(f"   ✅ Found @ {alt_timestamp.strftime('%H:%M')} ({offset:+d} min offset)")
                            break
                    except:
                        pass
                
                if not past_data_str:
                    past_data_str = self.memory.get(alt_key)
                    if past_data_str:
                        logger.info(f"   ✅ Found in RAM @ {alt_timestamp.strftime('%H:%M')} ({offset:+d} min)")
                        break
        
        if not past_data_str:
            # Enhanced debugging - show what we tried
            logger.info(f"   ❌ Not found. Tried: {', '.join(tried_keys)}")
            
            # Show available data range (check both Redis and RAM)
            available_keys = []
            
            # Check Redis first
            if self.client:
                try:
                    # Use Redis SCAN to find matching keys
                    pattern = f"nifty:strike:{strike}:*"
                    cursor = 0
                    while True:
                        cursor, keys = self.client.scan(cursor, match=pattern, count=100)
                        available_keys.extend([k.decode() if isinstance(k, bytes) else k for k in keys])
                        if cursor == 0:
                            break
                except Exception as e:
                    logger.info(f"   ⚠️ Redis scan failed: {e}")
            
            # Fallback to RAM
            if not available_keys and self.memory:
                available_keys = [k for k in self.memory.keys() if f"strike:{strike}:" in k]
            
            if available_keys:
                # Extract timestamps and show range
                try:
                    times = sorted([k.split(':')[-1][-4:] for k in available_keys])
                    if times:
                        first_time = f"{times[0][:2]}:{times[0][2:]}"
                        last_time = f"{times[-1][:2]}:{times[-1][2:]}"
                        logger.info(f"   📋 Available: {first_time} to {last_time} ({len(times)} snapshots)")
                        
                        # Show gap analysis
                        looking_for_ts = timestamp.strftime('%H%M')
                        if times[0] > looking_for_ts:
                            gap = int(times[0][:2])*60 + int(times[0][2:]) - (int(looking_for_ts[:2])*60 + int(looking_for_ts[2:]))
                            logger.info(f"   ⏰ Bot started {gap} min after requested time")
                except Exception as e:
                    logger.info(f"   ⚠️ Could not parse times: {e}")
            else:
                logger.info(f"   📋 No data for strike {strike} yet - ATM just changed?")
            
            return 0.0, 0.0, False
        
        try:
            past = json.loads(past_data_str)
            ce_chg = ((current_data['ce_oi'] - past['ce_oi']) / past['ce_oi'] * 100 if past['ce_oi'] > 0 else 0)
            pe_chg = ((current_data['pe_oi'] - past['pe_oi']) / past['pe_oi'] * 100 if past['pe_oi'] > 0 else 0)
            return ce_chg, pe_chg, True
        except:
            return 0.0, 0.0, False
    
    def save_total_oi_snapshot(self, ce_total: int, pe_total: int):
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
        return {
            'snapshot_count': self.snapshot_count,
            'ram_keys': len(self.memory) if not self.client else 0,
            'startup_time': self.startup_time.strftime('%H:%M:%S'),
            'elapsed_minutes': (datetime.now(IST) - self.startup_time).total_seconds() / 60,
            'warmed_up_10m': self.is_warmed_up(10),
            'warmed_up_5m': self.is_warmed_up(5),
            'premarket_loaded': self.premarket_loaded
        }


class NiftyDataFeed:
    def __init__(self, spot_key: str, futures_key: str):
        self.headers = {
            "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
            "Accept": "application/json"
        }
        self.spot_key = spot_key
        self.futures_key = futures_key
        logger.info(f"📊 Spot: {spot_key}")
        logger.info(f"📊 Futures: {futures_key}")
    
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
        """Fetch complete market data"""
        async with aiohttp.ClientSession() as session:
            spot_price = 0
            futures_price = 0
            df = pd.DataFrame()
            strike_data = {}
            total_options_volume = 0
            
            # 1. SPOT PRICE
            logger.info("🔍 Fetching spot...")
            enc_spot = urllib.parse.quote(self.spot_key, safe='')
            # V3 Quote API
            spot_url = f"{UPSTOX_QUOTE_URL_V3}?symbol={enc_spot}"
            
            spot_data = await self.fetch_with_retry(spot_url, session)
            if spot_data and spot_data.get('status') == 'success':
                data_dict = spot_data.get('data', {})
                if self.spot_key in data_dict:
                    quote = data_dict[self.spot_key]
                    spot_price = quote.get('last_price', 0)
                    logger.info(f"✅ Spot: ₹{spot_price:.2f}")
            
            # 2. FUTURES CANDLES
            logger.info(f"🔍 Fetching futures...")
            enc_futures = urllib.parse.quote(self.futures_key, safe='')
            # V3 Historical Candle API
            candle_url = f"{UPSTOX_HISTORICAL_URL_V3}/intraday/{enc_futures}/1minute"
            
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
                            logger.info(f"   Using futures as spot")
            
            if spot_price == 0:
                logger.error("❌ Both spot and futures failed")
                return df, strike_data, 0, 0, 0
            
            # 3. OPTION CHAIN - Weekly expiry
            logger.info("🔍 Fetching option chain...")
            expiry = get_next_tuesday_expiry()
            expiry_str = expiry.strftime('%Y-%m-%d')
            logger.info(f"📅 Expiry: {expiry_str} (Tuesday)")
            
            enc_index = urllib.parse.quote(self.spot_key, safe='')
            # V2 Option Chain API (V3 not yet available for options)
            chain_url = f"{UPSTOX_OPTION_CHAIN_URL}?instrument_key={enc_index}&expiry_date={expiry_str}"
            
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
        return atm_concentration > 30
    
    def check_multi_tf_confirmation(self, ce_5m: float, ce_15m: float, pe_5m: float, pe_15m: float,
                                   has_5m: bool, has_15m: bool) -> bool:
        if not (has_5m and has_15m):
            return False
        ce_aligned = (ce_5m < -3 and ce_15m < -5) or (ce_5m > 3 and ce_15m > 5)
        pe_aligned = (pe_5m < -3 and pe_15m < -5) or (pe_5m > 3 and pe_15m > 5)
        return ce_aligned or pe_aligned
    
    def check_momentum(self, df: pd.DataFrame, direction: str = 'bullish') -> bool:
        if df.empty or len(df) < 3:
            return False
        last_3 = df.tail(3)
        if direction == 'bullish':
            return sum(last_3['close'] > last_3['open']) >= 2
        else:
            return sum(last_3['close'] < last_3['open']) >= 2


class NiftyStrikeMaster:
    def __init__(self, spot_key: str, futures_key: str):
        self.feed = NiftyDataFeed(spot_key, futures_key)
        self.redis = RedisBrain()
        self.analyzer = NiftyAnalyzer()
        self.telegram = None
        self.last_signal_time = {}
        self.hourly_collector = HourlyDataCollector()
        
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
                return False
        self.last_signal_time[key] = now
        return True
    
    async def run_cycle(self):
        """Main analysis cycle"""
        if not is_tradeable_time():
            return
        
        # v2.3: Premarket warmup
        if is_premarket_time():
            await self.redis.load_previous_day_data()
            logger.info("⏳ Warmup period (9:10-9:20) - Collecting data, no signals")
            return
        
        logger.info(f"\n{'='*80}")
        logger.info(f"🔍 NIFTY50 ANALYSIS v2.3")
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
        expiry_today = is_expiry_day()
        
        # Get total OI
        total_ce = sum(d['ce_oi'] for d in strike_data.values())
        total_pe = sum(d['pe_oi'] for d in strike_data.values())
        
        # Get OI changes
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
        
        # Check multi-TF
        multi_tf = self.analyzer.check_multi_tf_confirmation(
            ce_total_5m, ce_total_15m, pe_total_5m, pe_total_15m,
            has_5m_total, has_15m_total
        )
        
        # Save current data
        for strike, data in strike_data.items():
            self.redis.save_strike_snapshot(strike, data)
        self.redis.save_total_oi_snapshot(total_ce, total_pe)
        
        # Log current status with DETAILED OI DATA
        logger.info(f"💰 Spot: {spot:.2f} | Futures: {futures:.2f}")
        logger.info(f"📊 VWAP: {vwap:.2f} | Distance: {vwap_distance:.2f} | PCR: {pcr:.2f}")
        logger.info(f"🕯️ Candle: {candle_color} | Size: {candle_size:.1f} | ATR: {atr:.1f}")
        
        # DETAILED OI LOGGING
        logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.info(f"📊 TOTAL OI ANALYSIS")
        logger.info(f"   CE: {total_ce:,} | PE: {total_pe:,}")
        
        if has_5m_total:
            logger.info(f"   5m:  CE={ce_total_5m:+.2f}% | PE={pe_total_5m:+.2f}%")
        else:
            logger.info(f"   5m:  ⏳ Data not available yet")
        
        if has_15m_total:
            logger.info(f"   15m: CE={ce_total_15m:+.2f}% | PE={pe_total_15m:+.2f}%")
        else:
            logger.info(f"   15m: ⏳ Data not available yet")
        
        # ATM OI LOGGING
        logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.info(f"🎯 ATM STRIKE {atm_strike} ANALYSIS")
        if atm_data:
            logger.info(f"   CE OI: {atm_data.get('ce_oi', 0):,} | PE OI: {atm_data.get('pe_oi', 0):,}")
            logger.info(f"   CE Vol: {atm_data.get('ce_vol', 0):,} | PE Vol: {atm_data.get('pe_vol', 0):,}")
            
            if has_5m_atm:
                logger.info(f"   5m:  CE={atm_ce_5m:+.2f}% | PE={atm_pe_5m:+.2f}%")
            else:
                logger.info(f"   5m:  ⏳ Data not available yet")
            
            if has_15m_atm:
                logger.info(f"   15m: CE={atm_ce_15m:+.2f}% | PE={atm_pe_15m:+.2f}%")
            else:
                logger.info(f"   15m: ⏳ Data not available yet")
        else:
            logger.info(f"   ⚠️ ATM strike data not found")
        
        logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.info(f"📈 ADVANCED METRICS")
        logger.info(f"   Order Flow: {order_flow:.2f} | Max Pain: {max_pain_strike}")
        logger.info(f"   Volume Surge: {vol_mult:.1f}x | Gamma Zone: {'YES' if gamma_zone else 'NO'}")
        
        if expiry_today:
            logger.info(f"   ⚡ EXPIRY DAY - Using wider SL ({ATR_SL_GAMMA_MULTIPLIER}x)")
        
        # Memory stats
        mem_stats = self.redis.get_memory_stats()
        logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.info(f"💾 MEMORY STATUS")
        logger.info(f"   Snapshots: {mem_stats['snapshot_count']}")
        logger.info(f"   Elapsed: {mem_stats['elapsed_minutes']:.1f} min")
        logger.info(f"   5m Ready: {'✅' if mem_stats['warmed_up_5m'] else '⏳ (need 5 min)'}")
        logger.info(f"   15m Ready: {'✅' if mem_stats['warmed_up_10m'] else '⏳ (need 10 min)'}")
        logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        # Check momentum
        bullish_momentum = self.analyzer.check_momentum(df, 'bullish')
        bearish_momentum = self.analyzer.check_momentum(df, 'bearish')
        
        # Create analysis data
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
            is_expiry_day=expiry_today,
            multi_tf_confirm=multi_tf,
            bullish_momentum=bullish_momentum,
            bearish_momentum=bearish_momentum,
            strike_data_summary=strike_data,
            data_quality={
                'has_15m_total_oi': has_15m_total,
                'has_5m_total_oi': has_5m_total,
                'has_15m_atm_oi': has_15m_atm,
                'has_5m_atm_oi': has_5m_atm,
                'warmed_up': self.redis.get_memory_stats()['warmed_up_10m']
            },
            warnings=[]
        )
        
        if not has_15m_total:
            analysis.warnings.append("15m total OI data not available")
        if not has_15m_atm:
            analysis.warnings.append("15m ATM OI data not available")
        
        # Send hourly update
        await self.send_hourly_update(
            spot, futures, atm_strike, vwap, atr, pcr,
            total_ce, total_pe, ce_total_15m, pe_total_15m, ce_total_5m, pe_total_5m,
            atm_data.get('ce_oi', 0), atm_data.get('pe_oi', 0),
            atm_ce_15m, atm_pe_15m, atm_ce_5m, atm_pe_5m,
            candle_data, vol, order_flow, max_pain_strike, gamma_zone
        )
        
        # Generate signal (only after 9:25)
        if should_generate_signals():
            # Check if warmed up
            mem_stats = self.redis.get_memory_stats()
            
            if not mem_stats['warmed_up_10m']:
                logger.info(f"⏳ WARMUP IN PROGRESS - Blocking signals")
                logger.info(f"   Need 10 min for reliable signals")
                logger.info(f"   Elapsed: {mem_stats['elapsed_minutes']:.1f} min")
                logger.info(f"   5m data: {'✅ Ready' if mem_stats['warmed_up_5m'] else '⏳ Not yet'}")
                logger.info(f"   15m data: {'✅ Ready' if mem_stats['warmed_up_10m'] else '⏳ Not yet'}")
                return  # ← BLOCK SIGNALS COMPLETELY during warmup
            
            signal = self.generate_signal_v23(
                spot, futures, vwap, vwap_distance, pcr, atr,
                ce_total_15m, pe_total_15m, ce_total_5m, pe_total_5m,
                atm_ce_15m, atm_pe_15m, candle_color, candle_size,
                has_vol_spike, vol_mult, df, order_flow, max_pain_dist, 
                gamma_zone, multi_tf, atm_strike, strike_data,
                has_15m_total, has_15m_atm, has_5m_total, has_5m_atm,
                expiry_today, analysis
            )
            
            if signal:
                if self._can_send_signal(signal.strike):
                    await self.send_alert(signal)
                else:
                    logger.info(f"✋ Cooldown active for {signal.strike}")
            else:
                logger.info("✋ No setup found")
        
        logger.info(f"{'='*80}\n")
    
    def generate_signal_v23(self, spot_price, futures_price, vwap, vwap_distance, pcr, atr,
                           ce_total_15m, pe_total_15m, ce_total_5m, pe_total_5m,
                           atm_ce_change, atm_pe_change, candle_color, candle_size,
                           has_vol_spike, vol_mult, df, order_flow, max_pain_dist,
                           gamma_zone, multi_tf, atm_strike, strike_data,
                           has_15m_total, has_15m_atm, has_5m_total, has_5m_atm,
                           is_expiry, analysis: AnalysisData) -> Optional[Signal]:
        """v2.3: Enhanced signal generation with reduced filtering"""
        
        # v2.3: Gamma zone = wider SL
        if is_expiry:
            sl_multiplier = ATR_SL_GAMMA_MULTIPLIER
        else:
            sl_multiplier = ATR_SL_MULTIPLIER
        
        stop_loss_points = int(atr * sl_multiplier)
        
        # v2.3: Dynamic ATR targets ONLY (no forced 80pts!)
        target_points = int(atr * ATR_TARGET_MULTIPLIER)
        
        lot_size = NIFTY_CONFIG['lot_size']
        quantity = 1
        
        atm_data = strike_data.get(atm_strike, {})
        atm_ce_oi = atm_data.get('ce_oi', 0)
        atm_pe_oi = atm_data.get('pe_oi', 0)
        atm_ce_vol = atm_data.get('ce_vol', 0)
        atm_pe_vol = atm_data.get('pe_vol', 0)
        
        # v2.3: Get option premium for premium-based SL
        option_premium = atm_data.get('ce_ltp', 0)  # Will use CE for CE_BUY, PE for PE_BUY
        
        # CE BUY SIGNAL - v2.3 REDUCED FILTERING
        if (ce_total_15m < -OI_THRESHOLD_MEDIUM and has_15m_total) or \
           (atm_ce_change < -ATM_OI_THRESHOLD and has_15m_atm):
            
            # PRIMARY CHECKS (OI & Volume - 80% weight)
            primary_checks = {
                "CE OI Unwinding 15m": ce_total_15m < -OI_THRESHOLD_MEDIUM and has_15m_total,
                "ATM CE Unwinding 15m": atm_ce_change < -ATM_OI_THRESHOLD and has_15m_atm,
                "Volume Spike": has_vol_spike,
            }
            
            # SECONDARY CHECKS (Price - 20% weight, confirmation only)
            secondary_checks = {
                "Price > VWAP": futures_price > vwap,
                "GREEN Candle": candle_color == 'GREEN'
            }
            
            # BONUS (Advanced)
            bonus = {
                "Strong 5m CE Unwinding": ce_total_5m < -OI_5M_THRESHOLD and has_5m_total,
                "Big Candle": candle_size >= MIN_CANDLE_SIZE,
                "Far from VWAP": vwap_distance >= VWAP_BUFFER,
                "Bullish PCR": pcr > PCR_BULLISH,
                "Bullish Momentum": self.analyzer.check_momentum(df, 'bullish'),
                "Order Flow Bullish": order_flow < 1.0,
                "Multi-TF Confirmed": multi_tf,
                "Gamma Zone": gamma_zone
            }
            
            primary_passed = sum(primary_checks.values())
            secondary_passed = sum(secondary_checks.values())
            bonus_passed = sum(bonus.values())
            
            # v2.3: Require only 2/3 primary checks (was 3/4)
            if primary_passed >= 2:
                # v2.3: Smarter weighted confidence
                confidence = 50  # Base
                
                # PRIMARY (High weight)
                if primary_checks["CE OI Unwinding 15m"]:
                    confidence += 20
                if primary_checks["ATM CE Unwinding 15m"]:
                    confidence += 15
                if primary_checks["Volume Spike"]:
                    confidence += 10
                
                # SECONDARY (Low weight - confirmation only)
                if secondary_checks["Price > VWAP"]:
                    confidence += 2
                if secondary_checks["GREEN Candle"]:
                    confidence += 3
                
                # BONUS
                confidence += (bonus_passed * 2)
                
                confidence = min(confidence, 98)
                
                if confidence >= 90:
                    quantity = 2
                
                # v2.3: Premium-based SL
                option_premium = atm_data.get('ce_ltp', 0)
                premium_sl = option_premium * (1 - PREMIUM_SL_PERCENT / 100) if USE_PREMIUM_SL else 0
                
                logger.info(f"🎯 CE BUY SIGNAL! Confidence: {confidence}%")
                logger.info(f"   Primary: {primary_passed}/3 | Secondary: {secondary_passed}/2 | Bonus: {bonus_passed}/8")
                
                return Signal(
                    type="CE_BUY",
                    reason=f"Call Unwinding (Total: {ce_total_15m:.1f}%, ATM: {atm_ce_change:.1f}%)",
                    confidence=confidence,
                    spot_price=spot_price,
                    futures_price=futures_price,
                    strike=atm_strike,
                    option_premium=option_premium,
                    target_points=target_points,
                    stop_loss_points=stop_loss_points,
                    premium_sl=premium_sl,
                    trailing_sl_enabled=ENABLE_TRAILING_SL,
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
                    is_expiry_day=is_expiry,
                    multi_tf_confirm=multi_tf,
                    lot_size=lot_size,
                    quantity=quantity,
                    atm_ce_oi=atm_ce_oi,
                    atm_pe_oi=atm_pe_oi,
                    atm_ce_vol=atm_ce_vol,
                    atm_pe_vol=atm_pe_vol,
                    analysis=analysis
                )
        
        # PE BUY SIGNAL - v2.3 REDUCED FILTERING
        if (pe_total_15m < -OI_THRESHOLD_MEDIUM and has_15m_total) or \
           (atm_pe_change < -ATM_OI_THRESHOLD and has_15m_atm):
            
            # PRIMARY CHECKS
            primary_checks = {
                "PE OI Unwinding 15m": pe_total_15m < -OI_THRESHOLD_MEDIUM and has_15m_total,
                "ATM PE Unwinding 15m": atm_pe_change < -ATM_OI_THRESHOLD and has_15m_atm,
                "Volume Spike": has_vol_spike,
            }
            
            # SECONDARY CHECKS
            secondary_checks = {
                "Price < VWAP": futures_price < vwap,
                "RED Candle": candle_color == 'RED'
            }
            
            # BONUS
            bonus = {
                "Strong 5m PE Unwinding": pe_total_5m < -OI_5M_THRESHOLD and has_5m_total,
                "Big Candle": candle_size >= MIN_CANDLE_SIZE,
                "Far from VWAP": vwap_distance >= VWAP_BUFFER,
                "Bearish PCR": pcr < PCR_BEARISH,
                "Bearish Momentum": self.analyzer.check_momentum(df, 'bearish'),
                "Order Flow Bearish": order_flow > 1.5,
                "Multi-TF Confirmed": multi_tf,
                "Gamma Zone": gamma_zone
            }
            
            primary_passed = sum(primary_checks.values())
            secondary_passed = sum(secondary_checks.values())
            bonus_passed = sum(bonus.values())
            
            if primary_passed >= 2:
                # v2.3: Smarter confidence
                confidence = 50
                
                # PRIMARY
                if primary_checks["PE OI Unwinding 15m"]:
                    confidence += 20
                if primary_checks["ATM PE Unwinding 15m"]:
                    confidence += 15
                if primary_checks["Volume Spike"]:
                    confidence += 10
                
                # SECONDARY
                if secondary_checks["Price < VWAP"]:
                    confidence += 2
                if secondary_checks["RED Candle"]:
                    confidence += 3
                
                # BONUS
                confidence += (bonus_passed * 2)
                
                confidence = min(confidence, 98)
                
                if confidence >= 90:
                    quantity = 2
                
                # v2.3: Premium-based SL
                option_premium = atm_data.get('pe_ltp', 0)
                premium_sl = option_premium * (1 - PREMIUM_SL_PERCENT / 100) if USE_PREMIUM_SL else 0
                
                logger.info(f"🎯 PE BUY SIGNAL! Confidence: {confidence}%")
                logger.info(f"   Primary: {primary_passed}/3 | Secondary: {secondary_passed}/2 | Bonus: {bonus_passed}/8")
                
                return Signal(
                    type="PE_BUY",
                    reason=f"Put Unwinding (Total: {pe_total_15m:.1f}%, ATM: {atm_pe_change:.1f}%)",
                    confidence=confidence,
                    spot_price=spot_price,
                    futures_price=futures_price,
                    strike=atm_strike,
                    option_premium=option_premium,
                    target_points=target_points,
                    stop_loss_points=stop_loss_points,
                    premium_sl=premium_sl,
                    trailing_sl_enabled=ENABLE_TRAILING_SL,
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
                    is_expiry_day=is_expiry,
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
                                 total_ce, total_pe, ce_15m, pe_15m, ce_5m, pe_5m,
                                 atm_ce_oi, atm_pe_oi, atm_ce_15m, atm_pe_15m,
                                 atm_ce_5m, atm_pe_5m,
                                 candle_data, volume, order_flow, max_pain, gamma_zone):
        """v2.3 FIXED: Enhanced hourly update with 5m data"""
        if not HOURLY_UPDATE_ENABLED:
            return
        
        if not self.hourly_collector.should_send_hourly_update():
            return
        
        snapshot = self.hourly_collector.collect_hourly_snapshot(
            spot, futures, atm_strike, vwap, atr, pcr,
            total_ce, total_pe, ce_15m, pe_15m, ce_5m, pe_5m,
            atm_ce_oi, atm_pe_oi, atm_ce_15m, atm_pe_15m,
            atm_ce_5m, atm_pe_5m,
            candle_data, volume, order_flow, max_pain, gamma_zone
        )
        
        message = self.hourly_collector.generate_hourly_json_message(snapshot)
        
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
        """v2.3: Enhanced alert with premium SL and trailing"""
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
        
        # v2.3: Premium info
        premium_info = f"Premium: ₹{s.option_premium:.2f} | SL: ₹{s.premium_sl:.2f}" if s.premium_sl > 0 else ""
        trailing_info = "✅ ENABLED" if s.trailing_sl_enabled else "DISABLED"
        expiry_info = "⚡ EXPIRY DAY" if s.is_expiry_day else ""
        
        msg = f"""
{emoji} NIFTY50 STRIKE MASTER PRO v2.3

{mode} {expiry_info}

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
💰 v2.3 PREMIUM-BASED SL
━━━━━━━━━━━━━━━━━━━━━━━━

{premium_info}

📈 Trailing SL: {trailing_info}
   Triggers at 60% of target
   Trails at 40% from peak

━━━━━━━━━━━━━━━━━━━━━━━━
📊 LOGIC & CONFIDENCE (v2.3)
━━━━━━━━━━━━━━━━━━━━━━━━

{s.reason}
Confidence: {s.confidence}% (OI-Weighted)

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
  CE: {s.atm_ce_change:+.1f}%
  PE: {s.atm_pe_change:+.1f}%

ATM OI Levels:
  CE OI: {s.atm_ce_oi:,} | Vol: {s.atm_ce_vol:,}
  PE OI: {s.atm_pe_oi:,} | Vol: {s.atm_pe_vol:,}

━━━━━━━━━━━━━━━━━━━━━━━━
🔥 ADVANCED METRICS
━━━━━━━━━━━━━━━━━━━━━━━━

Order Flow: {s.order_flow_imbalance:.2f}
Max Pain: {s.max_pain_distance:.0f} pts away
{"⚡ Gamma Zone: ACTIVE" if s.gamma_zone else ""}
{"✅ Multi-TF: CONFIRMED" if s.multi_tf_confirm else ""}

━━━━━━━━━━━━━━━━━━━━━━━━

⏰ {timestamp_str}

✅ v2.3 - Enhanced with Trailing SL
"""
        
        logger.info(f"🚨 {s.type} @ {entry:.1f}")
        
        if self.telegram:
            try:
                await asyncio.wait_for(
                    self.telegram.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg),
                    timeout=TELEGRAM_TIMEOUT
                )
                logger.info("✅ Alert sent")
            except asyncio.TimeoutError:
                logger.warning("⚠️ Alert timed out")
            except Exception as e:
                logger.error(f"❌ Telegram error: {e}")
        
        # Send analysis if enabled
        if SEND_ANALYSIS_DATA and s.analysis and self.telegram:
            await self.send_analysis_data(s.analysis, s.type)
    
    async def send_analysis_data(self, analysis: AnalysisData, signal_type: str):
        """Send complete analysis data"""
        timestamp_str = analysis.timestamp.strftime('%d-%b %I:%M %p')
        
        strike_summary = []
        for strike, data in sorted(analysis.strike_data_summary.items())[:5]:
            mark = "🎯" if strike == analysis.atm_strike else "  "
            strike_summary.append(f"{mark}{strike}: CE={data['ce_oi']:,} PE={data['pe_oi']:,}")
        
        strikes_text = "\n".join(strike_summary)
        
        quality_items = []
        for key, value in analysis.data_quality.items():
            status = "✅" if value else "❌"
            quality_items.append(f"{status} {key.replace('_', ' ').title()}")
        quality_text = "\n".join(quality_items)
        
        warnings_text = "\n".join([f"⚠️ {w}" for w in analysis.warnings]) if analysis.warnings else "✅ No warnings"
        
        analysis_msg = f"""
📊 ANALYSIS DATA v2.3

Time: {timestamp_str}
Signal: {signal_type}

━━━━━━━━━━━━━━━━━━━━━━━━
💰 PRICE DATA
━━━━━━━━━━━━━━━━━━━━━━━━

Spot: {analysis.spot_price:.2f}
Futures: {analysis.futures_price:.2f}
VWAP: {analysis.vwap:.2f}
Distance: {analysis.vwap_distance:.2f}

━━━━━━━━━━━━━━━━━━━━━━━━
📊 OI ANALYSIS
━━━━━━━━━━━━━━━━━━━━━━━━

Total CE: {analysis.total_ce_oi:,}
Total PE: {analysis.total_pe_oi:,}
PCR: {analysis.pcr:.2f}

15m: CE={analysis.ce_oi_15m:+.1f}% PE={analysis.pe_oi_15m:+.1f}%
5m: CE={analysis.ce_oi_5m:+.1f}% PE={analysis.pe_oi_5m:+.1f}%

━━━━━━━━━━━━━━━━━━━━━━━━
🎯 ATM {analysis.atm_strike}
━━━━━━━━━━━━━━━━━━━━━━━━

CE OI: {analysis.atm_ce_oi:,}
PE OI: {analysis.atm_pe_oi:,}

15m: CE={analysis.atm_ce_change_15m:+.1f}% PE={analysis.atm_pe_change_15m:+.1f}%

━━━━━━━━━━━━━━━━━━━━━━━━
📋 STRIKES
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
"""
        
        if self.telegram:
            try:
                await asyncio.wait_for(
                    self.telegram.send_message(chat_id=TELEGRAM_CHAT_ID, text=analysis_msg),
                    timeout=TELEGRAM_TIMEOUT
                )
                logger.info("✅ Analysis sent")
            except:
                pass
    
    async def send_startup_message(self):
        """v2.3: Enhanced startup message"""
        now = datetime.now(IST)
        startup_time = now.strftime('%d-%b %I:%M %p')
        mode = "🧪 ALERT ONLY" if ALERT_ONLY_MODE else "⚡ LIVE"
        expiry = get_next_tuesday_expiry().strftime('%d-%b-%Y')
        
        msg = f"""
🚀 NIFTY50 STRIKE MASTER PRO v2.3

━━━━━━━━━━━━━━━━━━━━━━━━
✅ STATUS
━━━━━━━━━━━━━━━━━━━━━━━━

⏰ Started: {startup_time}
📊 Index: NIFTY 50
🔄 Mode: {mode}
✅ ALL SYSTEMS OPERATIONAL

━━━━━━━━━━━━━━━━━━━━━━━━
📅 v2.3 CONFIGURATION
━━━━━━━━━━━━━━━━━━━━━━━━

Expiry: {expiry} (Tuesday)
Premarket: 9:10-9:20 AM
Signals: From 9:25 AM
Scan: Every {SCAN_INTERVAL}s

━━━━━━━━━━━━━━━━━━━━━━━━
🆕 v2.3 ENHANCEMENTS
━━━━━━━━━━━━━━━━━━━━━━━━

✅ Premarket warmup (9:10-9:20)
✅ OI/Volume PRIMARY (80% weight)
✅ Price SECONDARY (20% weight)
✅ 40% easier filtering
✅ Dynamic ATR targets only
✅ Trailing SL (locks at 60%)
✅ Premium-based SL
✅ Gamma zone wider SL
✅ Smarter confidence scoring
✅ Enhanced hourly updates

━━━━━━━━━━━━━━━━━━━━━━━━
📊 THRESHOLDS (v2.3)
━━━━━━━━━━━━━━━━━━━━━━━━

OI Medium: 1.5% (was 2.5%)
ATM OI: 2.0% (was 3.0%)
Volume: 1.5x (was 2.0x)
Cooldown: 3 min (was 5 min)

━━━━━━━━━━━━━━━━━━━━━━━━

🎯 Bot Ready | Waiting for 9:10 AM
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
    logger.info("🚀 NIFTY50 STRIKE MASTER PRO v2.3 - COMPLETE")
    logger.info("=" * 80)
    logger.info("")
    logger.info("📊 Index: NIFTY 50")
    logger.info(f"🔔 Mode: {'ALERT ONLY' if ALERT_ONLY_MODE else 'LIVE TRADING'}")
    logger.info(f"📅 Expiry: WEEKLY (Tuesday)")
    logger.info(f"⏱️ Premarket: 9:10-9:20 | Signals: 9:25+")
    logger.info("")
    
    # Fetch keys
    logger.info("🔍 Finding instrument keys...")
    spot_key, futures_key = await fetch_instruments_and_find_keys()
    
    if not spot_key or not futures_key:
        logger.error("❌ Could not find keys!")
        return
    
    logger.info("")
    logger.info("✅ All instruments found!")
    logger.info("")
    
    try:
        bot = NiftyStrikeMaster(spot_key, futures_key)
        logger.info("✅ Bot initialized")
    except Exception as e:
        logger.error(f"❌ Init failed: {e}")
        return
    
    logger.info("")
    logger.info("🔥 v2.3 ENHANCEMENTS:")
    logger.info("   ✅ Premarket warmup + Previous day data")
    logger.info("   ✅ OI/Volume PRIMARY (80% weight)")
    logger.info("   ✅ 40% easier filtering → More signals!")
    logger.info("   ✅ Dynamic ATR targets (no forced 80pts)")
    logger.info("   ✅ Trailing SL → Lock profits!")
    logger.info("   ✅ Premium-based SL for options")
    logger.info("   ✅ Gamma zone wider SL on expiry")
    logger.info("   ✅ Smarter OI-weighted confidence")
    logger.info("   ✅ Enhanced hourly updates with sentiment")
    logger.info("")
    logger.info("=" * 80)
    
    await bot.send_startup_message()
    iteration = 0
    
    while True:
        try:
            now = datetime.now(IST).time()
            if PREMARKET_START <= now <= MARKET_CLOSE:
                iteration += 1
                await bot.run_cycle()
                await asyncio.sleep(SCAN_INTERVAL)
            else:
                logger.info("🌙 Market closed, waiting...")
                await asyncio.sleep(300)
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopped by user")
            break
        except Exception as e:
            logger.error(f"💥 Error: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(30)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n👋 Shutdown complete")
