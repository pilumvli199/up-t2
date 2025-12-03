#!/usr/bin/env python3
"""
================================================================================
    NIFTY50 STRIKE MASTER PRO v2.1 - FULLY AUTONOMOUS TRADING SYSTEM
================================================================================

✅ FIXED IN v2.1:
   - Spot LTP for signals (not futures)
   - Options Volume for confirmation (CE+PE based)
   - OI Velocity tracking (movement strength)
   - OI vs Price Divergence detection
   - Cumulative OI Trend tracking
   - Movement Status Prediction (TRENDING/EXHAUSTING/SIDEWAYS/REVERSAL)
   - ATR-based Trailing SL (proper)
   - 7 strikes for analysis, OTM for trading
   - Better Railway.app logging
   - All endpoints verified

📊 TRADING LOGIC (90% OI/Volume, 10% Candle):
   - Entry: OI unwinding + Volume surge + Spot confirmation
   - Exit: OI support ends (velocity drop/reversal)
   - Hold: As long as OI supports the move

⚙️ SETTINGS:
   - Demo Capital: ₹5,000
   - Lot Size: 75 (NSE Dec 2025)
   - Analysis: 7 strikes (ATM ± 3)
   - Trading: OTM strikes (cheaper premium)

Author: Claude AI
Version: 2.1
Date: December 2025
================================================================================
"""

import os
import sys
import asyncio
import aiohttp
import urllib.parse
from datetime import datetime, timedelta, time
import pytz
import json
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from collections import deque
import time as time_module
import gzip
from enum import Enum

# ==================== LOGGING SETUP (Railway Compatible) ====================
# Force unbuffered output for Railway.app
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

logging.basicConfig(
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("NIFTY-v2.1")
logger.setLevel(logging.INFO)

# ==================== OPTIONAL DEPENDENCIES ====================
try:
    import redis
    REDIS_AVAILABLE = True
    logger.info("✅ Redis module available")
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("⚠️ Redis not available - using RAM mode")

try:
    from telegram import Bot
    TELEGRAM_AVAILABLE = True
    logger.info("✅ Telegram module available")
except ImportError:
    TELEGRAM_AVAILABLE = False
    logger.warning("⚠️ Telegram not available")

# ==================== TIMEZONE ====================
IST = pytz.timezone('Asia/Kolkata')

# ==================== ENVIRONMENT VARIABLES ====================
UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN', '')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')

# ==================== CONFIGURATION ====================
class Config:
    """All trading parameters in one place"""
    
    # Index
    INDEX_NAME = 'NIFTY 50'
    SPOT_KEY = 'NSE_INDEX|Nifty 50'
    STRIKE_GAP = 50
    LOT_SIZE = 75  # NSE Dec 2025
    
    # Demo Account
    DEMO_CAPITAL = 5000.0
    
    # Risk Management
    MAX_TRADES_PER_DAY = 5
    MAX_LOSS_PER_DAY = 250.0
    MAX_OPEN_POSITIONS = 2
    HARD_STOP_LOSS_POINTS = 5
    
    # Strike Configuration
    ANALYSIS_STRIKES = 3  # ATM ± 3 = 7 strikes for analysis
    OTM_STRIKES_AWAY = 2  # Trade 2 strikes OTM
    
    # OI Thresholds
    OI_ENTRY_THRESHOLD = 5.0
    OI_STRONG_THRESHOLD = 8.0
    ATM_OI_THRESHOLD = 5.0
    OI_EXIT_REVERSAL = 3.0
    OI_VELOCITY_STRONG = 0.5  # % per minute
    OI_VELOCITY_WEAK = 0.1
    
    # Volume
    VOLUME_SPIKE_MULTIPLIER = 2.0
    ORDER_FLOW_BULLISH = 0.8  # CE_vol/PE_vol < 0.8 = bullish
    ORDER_FLOW_BEARISH = 1.2  # CE_vol/PE_vol > 1.2 = bearish
    
    # PCR
    PCR_BULLISH = 1.08
    PCR_BEARISH = 0.92
    
    # Technical
    ATR_PERIOD = 14
    ATR_FALLBACK = 30
    ATR_TRAILING_MULTIPLIER = 1.0
    MIN_CANDLE_SIZE = 8
    
    # Timing
    MARKET_OPEN = time(9, 15)
    MARKET_CLOSE = time(15, 30)
    AVOID_OPEN_START = time(9, 15)
    AVOID_OPEN_END = time(9, 45)
    AVOID_CLOSE_START = time(15, 15)
    AVOID_CLOSE_END = time(15, 30)
    SUMMARY_TIME = time(15, 0)
    
    # Intervals
    SCAN_INTERVAL = 30
    
    # Memory
    REDIS_TTL_HOURS = 24
    SIGNAL_COOLDOWN = 300
    
    # API
    RATE_LIMIT_SEC = 50
    RATE_LIMIT_MIN = 500
    
    # Endpoints (Verified)
    INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    QUOTE_URL = "https://api.upstox.com/v2/market-quote/quotes"
    OHLC_URL = "https://api.upstox.com/v2/market-quote/ohlc"
    CANDLE_URL = "https://api.upstox.com/v2/historical-candle/intraday"
    OPTION_CHAIN_URL = "https://api.upstox.com/v2/option/chain"
    
    # Use 'symbol' parameter (some endpoints need this instead of instrument_key)
    USE_SYMBOL_PARAM = True

CFG = Config()

# ==================== ENUMS ====================
class TradeType(Enum):
    CE_BUY = "CE_BUY"
    PE_BUY = "PE_BUY"

class TradeStatus(Enum):
    OPEN = "OPEN"
    CLOSED_TARGET = "CLOSED_TARGET"
    CLOSED_SL = "CLOSED_SL"
    CLOSED_OI_EXIT = "CLOSED_OI_EXIT"
    CLOSED_TRAILING = "CLOSED_TRAILING"
    CLOSED_EOD = "CLOSED_EOD"

class ExitReason(Enum):
    TARGET_HIT = "Target Hit"
    STOP_LOSS = "Stop Loss"
    OI_REVERSAL = "OI Support Lost"
    TRAILING_SL = "Trailing SL Hit"
    EOD = "End of Day"
    MAX_LOSS = "Max Daily Loss"

class MovementStatus(Enum):
    TRENDING = "TRENDING"
    EXHAUSTING = "EXHAUSTING"
    SIDEWAYS = "SIDEWAYS"
    REVERSAL_SOON = "REVERSAL_SOON"
    UNKNOWN = "UNKNOWN"

# ==================== DATA CLASSES ====================
@dataclass
class DemoAccount:
    """Virtual trading account with full tracking"""
    initial_capital: float = CFG.DEMO_CAPITAL
    current_balance: float = CFG.DEMO_CAPITAL
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    trades_today: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_profit: float = 0.0
    total_loss: float = 0.0
    max_drawdown: float = 0.0
    peak_balance: float = CFG.DEMO_CAPITAL
    
    def reset_daily(self):
        self.trades_today = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_profit = 0.0
        self.total_loss = 0.0
        self.realized_pnl = 0.0
        logger.info("🔄 Daily counters reset")
    
    def can_trade(self) -> Tuple[bool, str]:
        if self.trades_today >= CFG.MAX_TRADES_PER_DAY:
            return False, f"Max trades ({CFG.MAX_TRADES_PER_DAY}) reached"
        if self.realized_pnl <= -CFG.MAX_LOSS_PER_DAY:
            return False, f"Max loss (₹{CFG.MAX_LOSS_PER_DAY}) reached"
        if self.current_balance <= 0:
            return False, "No balance"
        return True, "OK"
    
    def update_pnl(self, pnl: float, is_win: bool):
        self.realized_pnl += pnl
        self.current_balance += pnl
        if is_win:
            self.winning_trades += 1
            self.total_profit += pnl
        else:
            self.losing_trades += 1
            self.total_loss += abs(pnl)
        if self.current_balance > self.peak_balance:
            self.peak_balance = self.current_balance
        dd = self.peak_balance - self.current_balance
        if dd > self.max_drawdown:
            self.max_drawdown = dd
    
    def get_summary(self) -> dict:
        wr = (self.winning_trades / self.trades_today * 100) if self.trades_today > 0 else 0
        return {
            'initial': self.initial_capital,
            'balance': self.current_balance,
            'realized': self.realized_pnl,
            'unrealized': self.unrealized_pnl,
            'trades': self.trades_today,
            'wins': self.winning_trades,
            'losses': self.losing_trades,
            'win_rate': wr,
            'profit': self.total_profit,
            'loss': self.total_loss,
            'drawdown': self.max_drawdown,
            'return_pct': ((self.current_balance - self.initial_capital) / self.initial_capital) * 100
        }

@dataclass
class Position:
    """Open position with tracking"""
    id: str
    trade_type: TradeType
    strike: int
    instrument_key: str
    entry_price: float
    entry_time: datetime
    quantity: int
    units: int
    
    current_price: float = 0.0
    highest_price: float = 0.0
    lowest_price: float = 999999.0
    
    stop_loss: float = 0.0
    trailing_sl: float = 0.0
    trailing_active: bool = False
    
    entry_oi_data: dict = field(default_factory=dict)
    
    status: TradeStatus = TradeStatus.OPEN
    exit_price: float = 0.0
    exit_time: Optional[datetime] = None
    exit_reason: Optional[ExitReason] = None
    pnl: float = 0.0
    pnl_points: float = 0.0
    
    def update_price(self, price: float):
        self.current_price = price
        if price > self.highest_price:
            self.highest_price = price
        if price < self.lowest_price:
            self.lowest_price = price
        self.pnl_points = price - self.entry_price
        self.pnl = self.pnl_points * self.units

@dataclass
class OIData:
    """OI snapshot with velocity tracking"""
    timestamp: datetime
    strike: int
    ce_oi: int
    pe_oi: int
    ce_vol: int
    pe_vol: int
    ce_ltp: float
    pe_ltp: float
    spot_price: float
    
    # Calculated
    ce_oi_change_5m: float = 0.0
    pe_oi_change_5m: float = 0.0
    ce_oi_change_15m: float = 0.0
    pe_oi_change_15m: float = 0.0
    ce_velocity: float = 0.0  # % per minute
    pe_velocity: float = 0.0

@dataclass
class MarketState:
    """Complete market state for analysis"""
    timestamp: datetime
    spot_price: float
    futures_price: float
    atm_strike: int
    
    # OHLC for TWAP
    spot_open: float = 0.0
    spot_high: float = 0.0
    spot_low: float = 0.0
    
    # Strikes data (7 strikes)
    strikes: Dict[int, dict] = field(default_factory=dict)
    
    # Totals
    total_ce_oi: int = 0
    total_pe_oi: int = 0
    total_ce_vol: int = 0
    total_pe_vol: int = 0
    
    # Calculated
    pcr: float = 1.0
    order_flow_ratio: float = 1.0  # CE_vol / PE_vol
    twap: float = 0.0  # Time Weighted Average Price
    
    # OI Changes
    ce_oi_change_5m: float = 0.0
    pe_oi_change_5m: float = 0.0
    ce_oi_change_15m: float = 0.0
    pe_oi_change_15m: float = 0.0
    
    # Velocity
    ce_velocity: float = 0.0
    pe_velocity: float = 0.0
    
    # Movement
    movement_status: MovementStatus = MovementStatus.UNKNOWN
    divergence_detected: bool = False
    divergence_type: str = ""
    
    # Candle (minimal)
    candle_color: str = "NEUTRAL"
    candle_size: float = 0.0
    
    # ATR
    atr: float = CFG.ATR_FALLBACK

@dataclass
class Signal:
    """Trading signal"""
    type: TradeType
    reason: str
    confidence: int
    spot_price: float
    strike: int
    option_type: str
    premium: float
    instrument_key: str
    
    oi_data: dict = field(default_factory=dict)
    market_state: Optional[MarketState] = None
    
    stop_loss: float = 0.0
    quantity: int = 1
    timestamp: datetime = field(default_factory=lambda: datetime.now(IST))

# ==================== RATE LIMITER ====================
class RateLimiter:
    def __init__(self):
        self.per_sec = deque(maxlen=CFG.RATE_LIMIT_SEC)
        self.per_min = deque(maxlen=CFG.RATE_LIMIT_MIN)
        self.lock = asyncio.Lock()
    
    async def wait(self):
        async with self.lock:
            now = time_module.time()
            while self.per_sec and now - self.per_sec[0] > 1.0:
                self.per_sec.popleft()
            while self.per_min and now - self.per_min[0] > 60.0:
                self.per_min.popleft()
            if len(self.per_sec) >= CFG.RATE_LIMIT_SEC:
                wait = 1.0 - (now - self.per_sec[0])
                if wait > 0:
                    await asyncio.sleep(wait)
                    now = time_module.time()
            if len(self.per_min) >= CFG.RATE_LIMIT_MIN:
                wait = 60.0 - (now - self.per_min[0])
                if wait > 0:
                    await asyncio.sleep(wait)
                    now = time_module.time()
            self.per_sec.append(now)
            self.per_min.append(now)

# ==================== MEMORY (Redis/RAM) ====================
class Memory:
    """24-hour memory for OI tracking"""
    
    def __init__(self):
        self.client = None
        self.ram = {}
        self.ram_ts = {}
        self.startup = datetime.now(IST)
        self.ttl = CFG.REDIS_TTL_HOURS * 3600
        
        if REDIS_AVAILABLE:
            try:
                self.client = redis.from_url(REDIS_URL, decode_responses=True)
                self.client.ping()
                logger.info("✅ Redis connected (24h TTL)")
            except Exception as e:
                self.client = None
                logger.warning(f"⚠️ Redis failed: {e} - using RAM")
        else:
            logger.info("💾 Using RAM mode")
    
    def is_warmed_up(self, minutes: int = 15) -> bool:
        elapsed = (datetime.now(IST) - self.startup).total_seconds() / 60
        return elapsed >= minutes
    
    def _key(self, prefix: str, id: str, ts: datetime) -> str:
        slot = ts.replace(second=0, microsecond=0)
        return f"nifty:v21:{prefix}:{id}:{slot.strftime('%Y%m%d_%H%M')}"
    
    def _cleanup(self):
        if self.client:
            return
        now = time_module.time()
        expired = [k for k, t in self.ram_ts.items() if now - t > self.ttl]
        for k in expired:
            del self.ram[k]
            del self.ram_ts[k]
    
    def _set(self, key: str, data: str):
        if self.client:
            try:
                self.client.setex(key, self.ttl, data)
            except:
                self.ram[key] = data
                self.ram_ts[key] = time_module.time()
        else:
            self.ram[key] = data
            self.ram_ts[key] = time_module.time()
        self._cleanup()
    
    def _get(self, key: str) -> Optional[str]:
        if self.client:
            try:
                return self.client.get(key)
            except:
                return self.ram.get(key)
        return self.ram.get(key)
    
    def save_oi(self, strike: int, data: dict):
        key = self._key("oi", str(strike), datetime.now(IST))
        self._set(key, json.dumps(data))
    
    def get_oi_change(self, strike: int, current: dict, mins: int) -> Tuple[float, float]:
        past = datetime.now(IST) - timedelta(minutes=mins)
        key = self._key("oi", str(strike), past)
        data_str = self._get(key)
        if not data_str:
            return 0.0, 0.0
        try:
            past_data = json.loads(data_str)
            ce_chg = ((current['ce_oi'] - past_data['ce_oi']) / past_data['ce_oi'] * 100) if past_data.get('ce_oi', 0) > 0 else 0
            pe_chg = ((current['pe_oi'] - past_data['pe_oi']) / past_data['pe_oi'] * 100) if past_data.get('pe_oi', 0) > 0 else 0
            return ce_chg, pe_chg
        except:
            return 0.0, 0.0
    
    def save_total_oi(self, ce: int, pe: int):
        key = self._key("total", "all", datetime.now(IST))
        self._set(key, json.dumps({'ce': ce, 'pe': pe}))
    
    def get_total_oi_change(self, ce: int, pe: int, mins: int) -> Tuple[float, float]:
        past = datetime.now(IST) - timedelta(minutes=mins)
        key = self._key("total", "all", past)
        data_str = self._get(key)
        if not data_str:
            return 0.0, 0.0
        try:
            past_data = json.loads(data_str)
            ce_chg = ((ce - past_data['ce']) / past_data['ce'] * 100) if past_data.get('ce', 0) > 0 else 0
            pe_chg = ((pe - past_data['pe']) / past_data['pe'] * 100) if past_data.get('pe', 0) > 0 else 0
            return ce_chg, pe_chg
        except:
            return 0.0, 0.0
    
    def save_position_oi(self, pos_id: str, data: dict):
        key = f"nifty:v21:pos:{pos_id}"
        self._set(key, json.dumps(data))
    
    def get_position_oi(self, pos_id: str) -> Optional[dict]:
        key = f"nifty:v21:pos:{pos_id}"
        data_str = self._get(key)
        return json.loads(data_str) if data_str else None
    
    def save_spot_ohlc(self, open_p: float, high: float, low: float, close: float):
        """Save spot OHLC for TWAP calculation"""
        key = f"nifty:v21:ohlc:{datetime.now(IST).strftime('%Y%m%d')}"
        data = {'open': open_p, 'high': high, 'low': low, 'close': close}
        self._set(key, json.dumps(data))
    
    def get_spot_ohlc(self) -> dict:
        key = f"nifty:v21:ohlc:{datetime.now(IST).strftime('%Y%m%d')}"
        data_str = self._get(key)
        return json.loads(data_str) if data_str else {}

# ==================== EXPIRY CALCULATOR ====================
def get_weekly_expiry() -> datetime:
    now = datetime.now(IST)
    days = (1 - now.weekday()) % 7  # Tuesday = 1
    if days == 0 and now.time() > time(15, 30):
        days = 7
    return now + timedelta(days=days)

def get_monthly_expiry() -> datetime:
    now = datetime.now(IST)
    year, month = now.year, now.month
    if month == 12:
        next_m = datetime(year + 1, 1, 1, tzinfo=IST)
    else:
        next_m = datetime(year, month + 1, 1, tzinfo=IST)
    last_day = next_m - timedelta(days=1)
    while last_day.weekday() != 1:
        last_day -= timedelta(days=1)
    if last_day.date() < now.date():
        month = month + 1 if month < 12 else 1
        year = year if month > 1 else year + 1
        if month == 12:
            next_m = datetime(year + 1, 1, 1, tzinfo=IST)
        else:
            next_m = datetime(year, month + 1, 1, tzinfo=IST)
        last_day = next_m - timedelta(days=1)
        while last_day.weekday() != 1:
            last_day -= timedelta(days=1)
    return last_day

# ==================== DATA FEED ====================
class DataFeed:
    """Market data from Upstox API"""
    
    def __init__(self):
        self.headers = {
            "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
            "Accept": "application/json"
        }
        self.rate_limiter = RateLimiter()
        self.instruments = {}
        self.futures_key = None
        self.day_open = 0.0
        self.day_high = 0.0
        self.day_low = 999999.0
    
    async def initialize(self):
        logger.info("📥 Loading instruments...")
        await self._load_instruments()
        await self._find_futures()
        logger.info("✅ DataFeed initialized")
    
    async def _load_instruments(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(CFG.INSTRUMENTS_URL, timeout=30) as resp:
                    if resp.status == 200:
                        data = gzip.decompress(await resp.read())
                        instruments = json.loads(data)
                        for inst in instruments:
                            self.instruments[inst.get('instrument_key', '')] = inst
                        logger.info(f"✅ Loaded {len(instruments)} instruments")
        except Exception as e:
            logger.error(f"❌ Instruments load failed: {e}")
    
    async def _find_futures(self):
        target = get_monthly_expiry().date()
        for key, inst in self.instruments.items():
            if (inst.get('segment') == 'NSE_FO' and 
                inst.get('instrument_type') == 'FUT' and 
                inst.get('name') == 'NIFTY'):
                exp = inst.get('expiry', 0)
                if exp:
                    exp_date = datetime.fromtimestamp(exp / 1000, tz=IST).date()
                    if exp_date == target:
                        self.futures_key = key
                        logger.info(f"✅ Futures: {inst.get('trading_symbol')}")
                        return
        logger.warning("⚠️ Futures not found")
    
    async def _fetch(self, url: str, session: aiohttp.ClientSession) -> Optional[dict]:
        for attempt in range(3):
            try:
                await self.rate_limiter.wait()
                async with session.get(url, headers=self.headers, timeout=15) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 429:
                        wait = 2 ** (attempt + 1)
                        logger.warning(f"⏳ Rate limited, wait {wait}s")
                        await asyncio.sleep(wait)
                    elif resp.status == 401:
                        logger.error(f"❌ HTTP 401: Token expired or invalid!")
                        text = await resp.text()
                        logger.error(f"   Response: {text[:200]}")
                        return None
                    else:
                        text = await resp.text()
                        logger.warning(f"⚠️ HTTP {resp.status}: {text[:200]}")
                        await asyncio.sleep(2)
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ Timeout {attempt + 1}/3")
                await asyncio.sleep(2)
            except Exception as e:
                logger.warning(f"❌ Fetch error: {e}")
                await asyncio.sleep(2)
        return None
    
    async def get_spot_quote(self) -> dict:
        """Get NIFTY Spot LTP + OHLC"""
        async with aiohttp.ClientSession() as session:
            # Use 'symbol' parameter for quotes API
            key = urllib.parse.quote(CFG.SPOT_KEY)
            url = f"{CFG.QUOTE_URL}?symbol={key}"
            logger.info(f"🔍 Fetching spot from: {url[:80]}...")
            
            data = await self._fetch(url, session)
            
            if data:
                logger.debug(f"📥 Response status: {data.get('status')}")
                
                if data.get('status') == 'success':
                    data_dict = data.get('data', {})
                    
                    # Try multiple key formats (Upstox uses different formats)
                    possible_keys = [
                        CFG.SPOT_KEY,                    # NSE_INDEX|Nifty 50
                        'NSE_INDEX:Nifty 50',            # Colon format
                        'NSE_INDEX|NIFTY 50',            # Uppercase
                        'NSE_INDEX:NIFTY 50',            # Uppercase with colon
                    ]
                    
                    quote = None
                    for k in possible_keys:
                        if k in data_dict:
                            quote = data_dict[k]
                            logger.info(f"✅ Found spot with key: {k}")
                            break
                    
                    # If still not found, try first key
                    if not quote and data_dict:
                        first_key = list(data_dict.keys())[0]
                        quote = data_dict[first_key]
                        logger.info(f"✅ Using first key: {first_key}")
                    
                    if quote:
                        ltp = quote.get('last_price', 0)
                        ohlc = quote.get('ohlc', {})
                        
                        logger.info(f"✅ Spot LTP: ₹{ltp}")
                        
                        # Track day OHLC for TWAP
                        if ltp > 0:
                            if self.day_open == 0:
                                self.day_open = ohlc.get('open', ltp)
                            if ltp > self.day_high:
                                self.day_high = ltp
                            if ltp < self.day_low:
                                self.day_low = ltp
                        
                        return {
                            'ltp': ltp,
                            'open': ohlc.get('open', 0),
                            'high': ohlc.get('high', 0),
                            'low': ohlc.get('low', 0),
                            'close': ohlc.get('close', 0)
                        }
                    else:
                        logger.warning(f"⚠️ No quote data. Available keys: {list(data_dict.keys())[:3]}")
                else:
                    logger.warning(f"⚠️ API returned: {data.get('status')} - {data.get('message', 'No message')}")
            else:
                logger.warning("⚠️ No response from spot API")
                
        return {'ltp': 0, 'open': 0, 'high': 0, 'low': 0, 'close': 0}
    
    async def get_futures_candles(self, limit: int = 100) -> List:
        """Get futures candles for ATR calculation"""
        if not self.futures_key:
            return []
        async with aiohttp.ClientSession() as session:
            key = urllib.parse.quote(self.futures_key)
            url = f"{CFG.CANDLE_URL}/{key}/1minute"
            data = await self._fetch(url, session)
            if data and data.get('status') == 'success':
                candles = data.get('data', {}).get('candles', [])
                return candles[:limit]
        return []
    
    async def get_option_chain(self, expiry: str) -> List[dict]:
        """Get option chain for all strikes"""
        async with aiohttp.ClientSession() as session:
            key = urllib.parse.quote(CFG.SPOT_KEY)
            url = f"{CFG.OPTION_CHAIN_URL}?instrument_key={key}&expiry_date={expiry}"
            data = await self._fetch(url, session)
            if data and data.get('status') == 'success':
                return data.get('data', [])
        return []
    
    async def get_full_market_state(self, memory: Memory) -> MarketState:
        """Get complete market state for analysis"""
        now = datetime.now(IST)
        
        # 1. Spot Quote (LTP + OHLC)
        spot = await self.get_spot_quote()
        spot_ltp = spot['ltp']
        
        if spot_ltp == 0:
            logger.warning("⚠️ Spot price unavailable")
            return MarketState(timestamp=now, spot_price=0, futures_price=0, atm_strike=0)
        
        # 2. Calculate ATM
        atm = round(spot_ltp / CFG.STRIKE_GAP) * CFG.STRIKE_GAP
        
        # 3. Futures candles for ATR
        candles = await self.get_futures_candles(50)
        futures_price = candles[0][4] if candles else spot_ltp
        
        # 4. Option Chain
        expiry = get_weekly_expiry().strftime('%Y-%m-%d')
        chain = await self.get_option_chain(expiry)
        
        # 5. Process strikes (7 strikes: ATM ± 3)
        min_strike = atm - (CFG.ANALYSIS_STRIKES * CFG.STRIKE_GAP)
        max_strike = atm + (CFG.ANALYSIS_STRIKES * CFG.STRIKE_GAP)
        
        strikes = {}
        total_ce_oi = 0
        total_pe_oi = 0
        total_ce_vol = 0
        total_pe_vol = 0
        
        for opt in chain:
            strike = opt.get('strike_price', 0)
            if min_strike <= strike <= max_strike:
                ce_data = opt.get('call_options', {}).get('market_data', {})
                pe_data = opt.get('put_options', {}).get('market_data', {})
                
                strikes[strike] = {
                    'ce_oi': ce_data.get('oi', 0),
                    'pe_oi': pe_data.get('oi', 0),
                    'ce_vol': ce_data.get('volume', 0),
                    'pe_vol': pe_data.get('volume', 0),
                    'ce_ltp': ce_data.get('ltp', 0),
                    'pe_ltp': pe_data.get('ltp', 0),
                    'ce_key': opt.get('call_options', {}).get('instrument_key', ''),
                    'pe_key': opt.get('put_options', {}).get('instrument_key', ''),
                    'ce_prev_oi': ce_data.get('prev_oi', 0),
                    'pe_prev_oi': pe_data.get('prev_oi', 0)
                }
                
                total_ce_oi += ce_data.get('oi', 0)
                total_pe_oi += pe_data.get('oi', 0)
                total_ce_vol += ce_data.get('volume', 0)
                total_pe_vol += pe_data.get('volume', 0)
                
                # Save OI snapshot
                memory.save_oi(strike, {
                    'ce_oi': ce_data.get('oi', 0),
                    'pe_oi': pe_data.get('oi', 0),
                    'ce_vol': ce_data.get('volume', 0),
                    'pe_vol': pe_data.get('volume', 0),
                    'ts': now.isoformat()
                })
        
        # Save total OI
        memory.save_total_oi(total_ce_oi, total_pe_oi)
        
        # 6. Calculate indicators
        pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 1.0
        order_flow = total_ce_vol / total_pe_vol if total_pe_vol > 0 else 1.0
        
        # TWAP (for Index without volume)
        twap = (self.day_open + self.day_high + self.day_low + spot_ltp) / 4 if self.day_open > 0 else spot_ltp
        
        # ATR
        atr = self._calc_atr(candles)
        
        # Candle info (minimal - 10%)
        candle_color, candle_size = self._candle_info(candles)
        
        # 7. OI Changes
        ce_5m, pe_5m = memory.get_total_oi_change(total_ce_oi, total_pe_oi, 5)
        ce_15m, pe_15m = memory.get_total_oi_change(total_ce_oi, total_pe_oi, 15)
        
        # 8. OI Velocity (change per minute)
        ce_velocity = ce_15m / 15 if ce_15m != 0 else 0
        pe_velocity = pe_15m / 15 if pe_15m != 0 else 0
        
        # 9. Create state
        state = MarketState(
            timestamp=now,
            spot_price=spot_ltp,
            futures_price=futures_price,
            atm_strike=atm,
            spot_open=self.day_open,
            spot_high=self.day_high,
            spot_low=self.day_low,
            strikes=strikes,
            total_ce_oi=total_ce_oi,
            total_pe_oi=total_pe_oi,
            total_ce_vol=total_ce_vol,
            total_pe_vol=total_pe_vol,
            pcr=pcr,
            order_flow_ratio=order_flow,
            twap=twap,
            ce_oi_change_5m=ce_5m,
            pe_oi_change_5m=pe_5m,
            ce_oi_change_15m=ce_15m,
            pe_oi_change_15m=pe_15m,
            ce_velocity=ce_velocity,
            pe_velocity=pe_velocity,
            candle_color=candle_color,
            candle_size=candle_size,
            atr=atr
        )
        
        # 10. Movement Status & Divergence
        state.movement_status, state.divergence_detected, state.divergence_type = self._analyze_movement(state)
        
        return state
    
    def _calc_atr(self, candles: List, period: int = 14) -> float:
        if len(candles) < period:
            return CFG.ATR_FALLBACK
        tr_list = []
        for i in range(1, min(len(candles), period + 1)):
            h, l, pc = candles[i][2], candles[i][3], candles[i-1][4]
            tr = max(h - l, abs(h - pc), abs(l - pc))
            tr_list.append(tr)
        return max(sum(tr_list) / len(tr_list), 10) if tr_list else CFG.ATR_FALLBACK
    
    def _candle_info(self, candles: List) -> Tuple[str, float]:
        if not candles:
            return "NEUTRAL", 0
        c = candles[0]
        size = abs(c[4] - c[1])
        if c[4] > c[1]:
            return "GREEN", size
        elif c[4] < c[1]:
            return "RED", size
        return "DOJI", size
    
    def _analyze_movement(self, state: MarketState) -> Tuple[MovementStatus, bool, str]:
        """Analyze movement status and detect divergence"""
        
        # OI Velocity based movement status
        ce_vel = abs(state.ce_velocity)
        pe_vel = abs(state.pe_velocity)
        
        status = MovementStatus.UNKNOWN
        divergence = False
        div_type = ""
        
        # Strong velocity = trending
        if ce_vel > CFG.OI_VELOCITY_STRONG or pe_vel > CFG.OI_VELOCITY_STRONG:
            status = MovementStatus.TRENDING
        # Weak velocity = exhausting
        elif ce_vel < CFG.OI_VELOCITY_WEAK and pe_vel < CFG.OI_VELOCITY_WEAK:
            if abs(state.ce_oi_change_15m) < 2 and abs(state.pe_oi_change_15m) < 2:
                status = MovementStatus.SIDEWAYS
            else:
                status = MovementStatus.EXHAUSTING
        else:
            status = MovementStatus.EXHAUSTING
        
        # Divergence Detection
        # Price up + CE OI building = Bearish divergence
        price_change = ((state.spot_price - state.spot_open) / state.spot_open * 100) if state.spot_open > 0 else 0
        
        if price_change > 0.3 and state.ce_oi_change_15m > 3:
            divergence = True
            div_type = "BEARISH (Price↑ + CE OI↑)"
            status = MovementStatus.REVERSAL_SOON
        
        # Price down + PE OI building = Bullish divergence
        if price_change < -0.3 and state.pe_oi_change_15m > 3:
            divergence = True
            div_type = "BULLISH (Price↓ + PE OI↑)"
            status = MovementStatus.REVERSAL_SOON
        
        return status, divergence, div_type

# ==================== SIGNAL GENERATOR ====================
class SignalGenerator:
    """Generate signals based on OI + Volume (90%) + Candle (10%)"""
    
    def __init__(self, memory: Memory):
        self.memory = memory
        self.last_signal = {}
        self.volume_history = []
    
    def _can_signal(self, strike: int) -> bool:
        now = datetime.now(IST)
        key = f"sig_{strike}"
        if key in self.last_signal:
            if (now - self.last_signal[key]).total_seconds() < CFG.SIGNAL_COOLDOWN:
                return False
        self.last_signal[key] = now
        return True
    
    def _check_volume_surge(self, total_vol: int) -> Tuple[bool, float]:
        now = datetime.now(IST)
        self.volume_history.append({'time': now, 'vol': total_vol})
        cutoff = now - timedelta(minutes=20)
        self.volume_history = [v for v in self.volume_history if v['time'] > cutoff]
        
        if len(self.volume_history) < 5:
            return False, 1.0
        
        past = [v['vol'] for v in self.volume_history[:-1]]
        avg = sum(past) / len(past) if past else 1
        mult = total_vol / avg if avg > 0 else 1
        return mult >= CFG.VOLUME_SPIKE_MULTIPLIER, mult
    
    def generate(self, state: MarketState) -> Optional[Signal]:
        """Main signal generation - 90% OI/Volume based"""
        
        if not state.strikes or state.spot_price == 0:
            return None
        
        # Check warmup
        if not self.memory.is_warmed_up(15):
            elapsed = (datetime.now(IST) - self.memory.startup).total_seconds() / 60
            logger.info(f"⏳ Warmup: {elapsed:.0f}/15 min")
            return None
        
        # Volume surge check
        total_vol = state.total_ce_vol + state.total_pe_vol
        vol_surge, vol_mult = self._check_volume_surge(total_vol)
        
        # Log analysis
        logger.info(f"📊 Spot: ₹{state.spot_price:.2f} | ATM: {state.atm_strike}")
        logger.info(f"📊 OI 15m: CE={state.ce_oi_change_15m:+.1f}% | PE={state.pe_oi_change_15m:+.1f}%")
        logger.info(f"📊 Velocity: CE={state.ce_velocity:.2f}%/min | PE={state.pe_velocity:.2f}%/min")
        logger.info(f"📊 PCR: {state.pcr:.2f} | OrderFlow: {state.order_flow_ratio:.2f} | Vol: {vol_mult:.1f}x")
        logger.info(f"📊 Movement: {state.movement_status.value} | Divergence: {state.divergence_type if state.divergence_detected else 'None'}")
        logger.info(f"📊 TWAP: ₹{state.twap:.2f} | Spot vs TWAP: {state.spot_price - state.twap:+.1f}")
        
        # Don't trade during reversal
        if state.movement_status == MovementStatus.REVERSAL_SOON:
            logger.info(f"⚠️ Reversal detected - {state.divergence_type} - Skipping")
            return None
        
        # ========== CE BUY SIGNAL (Bullish) ==========
        # CE unwinding = Call writers covering = Bullish
        if state.ce_oi_change_15m < -CFG.OI_ENTRY_THRESHOLD:
            
            # OI/Volume checks (90%)
            oi_checks = {
                "CE Unwinding 15m": state.ce_oi_change_15m < -CFG.OI_ENTRY_THRESHOLD,
                "CE Unwinding 5m": state.ce_oi_change_5m < -3,
                "OI Velocity Strong": abs(state.ce_velocity) > CFG.OI_VELOCITY_WEAK,
                "Volume Surge": vol_surge,
                "Order Flow Bullish": state.order_flow_ratio < CFG.ORDER_FLOW_BULLISH,
                "PCR Bullish": state.pcr > CFG.PCR_BULLISH,
                "Spot > TWAP": state.spot_price > state.twap,
                "Movement OK": state.movement_status in [MovementStatus.TRENDING, MovementStatus.UNKNOWN]
            }
            
            # Candle check (10%)
            candle_ok = state.candle_color == 'GREEN'
            
            oi_passed = sum(oi_checks.values())
            
            if oi_passed >= 4:  # At least 4 OI/Volume conditions
                # OTM strike for trading
                otm_strike = state.atm_strike + (CFG.OTM_STRIKES_AWAY * CFG.STRIKE_GAP)
                
                # Check if OTM data available in wider range
                # First try from existing strikes
                strike_data = state.strikes.get(otm_strike)
                
                # If not in 7 strikes, we need to get it from chain
                if not strike_data:
                    # Use ATM+1 as fallback
                    otm_strike = state.atm_strike + CFG.STRIKE_GAP
                    strike_data = state.strikes.get(otm_strike)
                
                if not strike_data or strike_data.get('ce_ltp', 0) == 0:
                    logger.warning(f"⚠️ OTM strike {otm_strike} CE not available")
                    return None
                
                if not self._can_signal(otm_strike):
                    return None
                
                confidence = min(60 + (oi_passed * 5) + (5 if candle_ok else 0), 95)
                
                logger.info(f"🎯 CE BUY SIGNAL! Strike: {otm_strike} | Premium: ₹{strike_data['ce_ltp']:.2f} | Confidence: {confidence}%")
                logger.info(f"   Checks passed: {oi_passed}/8 OI + {'✓' if candle_ok else '✗'} Candle")
                
                return Signal(
                    type=TradeType.CE_BUY,
                    reason=f"CE Unwinding {state.ce_oi_change_15m:.1f}% | Vel: {state.ce_velocity:.2f}%/min",
                    confidence=confidence,
                    spot_price=state.spot_price,
                    strike=otm_strike,
                    option_type='CE',
                    premium=strike_data['ce_ltp'],
                    instrument_key=strike_data.get('ce_key', ''),
                    oi_data={
                        'ce_15m': state.ce_oi_change_15m,
                        'pe_15m': state.pe_oi_change_15m,
                        'ce_5m': state.ce_oi_change_5m,
                        'pe_5m': state.pe_oi_change_5m,
                        'ce_vel': state.ce_velocity,
                        'pe_vel': state.pe_velocity,
                        'vol_mult': vol_mult
                    },
                    market_state=state,
                    stop_loss=strike_data['ce_ltp'] - CFG.HARD_STOP_LOSS_POINTS
                )
        
        # ========== PE BUY SIGNAL (Bearish) ==========
        if state.pe_oi_change_15m < -CFG.OI_ENTRY_THRESHOLD:
            
            oi_checks = {
                "PE Unwinding 15m": state.pe_oi_change_15m < -CFG.OI_ENTRY_THRESHOLD,
                "PE Unwinding 5m": state.pe_oi_change_5m < -3,
                "OI Velocity Strong": abs(state.pe_velocity) > CFG.OI_VELOCITY_WEAK,
                "Volume Surge": vol_surge,
                "Order Flow Bearish": state.order_flow_ratio > CFG.ORDER_FLOW_BEARISH,
                "PCR Bearish": state.pcr < CFG.PCR_BEARISH,
                "Spot < TWAP": state.spot_price < state.twap,
                "Movement OK": state.movement_status in [MovementStatus.TRENDING, MovementStatus.UNKNOWN]
            }
            
            candle_ok = state.candle_color == 'RED'
            oi_passed = sum(oi_checks.values())
            
            if oi_passed >= 4:
                otm_strike = state.atm_strike - (CFG.OTM_STRIKES_AWAY * CFG.STRIKE_GAP)
                strike_data = state.strikes.get(otm_strike)
                
                if not strike_data:
                    otm_strike = state.atm_strike - CFG.STRIKE_GAP
                    strike_data = state.strikes.get(otm_strike)
                
                if not strike_data or strike_data.get('pe_ltp', 0) == 0:
                    logger.warning(f"⚠️ OTM strike {otm_strike} PE not available")
                    return None
                
                if not self._can_signal(otm_strike):
                    return None
                
                confidence = min(60 + (oi_passed * 5) + (5 if candle_ok else 0), 95)
                
                logger.info(f"🎯 PE BUY SIGNAL! Strike: {otm_strike} | Premium: ₹{strike_data['pe_ltp']:.2f} | Confidence: {confidence}%")
                
                return Signal(
                    type=TradeType.PE_BUY,
                    reason=f"PE Unwinding {state.pe_oi_change_15m:.1f}% | Vel: {state.pe_velocity:.2f}%/min",
                    confidence=confidence,
                    spot_price=state.spot_price,
                    strike=otm_strike,
                    option_type='PE',
                    premium=strike_data['pe_ltp'],
                    instrument_key=strike_data.get('pe_key', ''),
                    oi_data={
                        'ce_15m': state.ce_oi_change_15m,
                        'pe_15m': state.pe_oi_change_15m,
                        'ce_5m': state.ce_oi_change_5m,
                        'pe_5m': state.pe_oi_change_5m,
                        'ce_vel': state.ce_velocity,
                        'pe_vel': state.pe_velocity,
                        'vol_mult': vol_mult
                    },
                    market_state=state,
                    stop_loss=strike_data['pe_ltp'] - CFG.HARD_STOP_LOSS_POINTS
                )
        
        logger.info("✋ No valid setup")
        return None

# ==================== POSITION MANAGER ====================
class PositionManager:
    """Manage positions with OI-based exit"""
    
    def __init__(self, account: DemoAccount, memory: Memory):
        self.account = account
        self.memory = memory
        self.positions: Dict[str, Position] = {}
        self.closed: List[Position] = []
        self.counter = 0
    
    def can_open(self) -> Tuple[bool, str]:
        ok, reason = self.account.can_trade()
        if not ok:
            return False, reason
        if len(self.positions) >= CFG.MAX_OPEN_POSITIONS:
            return False, f"Max positions ({CFG.MAX_OPEN_POSITIONS})"
        return True, "OK"
    
    def open_position(self, signal: Signal) -> Optional[Position]:
        ok, reason = self.can_open()
        if not ok:
            logger.warning(f"❌ Cannot open: {reason}")
            return None
        
        self.counter += 1
        pos_id = f"P{datetime.now(IST).strftime('%Y%m%d')}_{self.counter:03d}"
        
        cost = signal.premium * CFG.LOT_SIZE
        if cost > self.account.current_balance:
            logger.warning(f"❌ Insufficient balance: need ₹{cost:.2f}, have ₹{self.account.current_balance:.2f}")
            return None
        
        pos = Position(
            id=pos_id,
            trade_type=signal.type,
            strike=signal.strike,
            instrument_key=signal.instrument_key,
            entry_price=signal.premium,
            entry_time=datetime.now(IST),
            quantity=1,
            units=CFG.LOT_SIZE,
            current_price=signal.premium,
            highest_price=signal.premium,
            stop_loss=signal.stop_loss,
            entry_oi_data=signal.oi_data
        )
        
        self.positions[pos_id] = pos
        self.account.trades_today += 1
        
        # Save entry OI
        self.memory.save_position_oi(pos_id, signal.oi_data)
        
        logger.info(f"✅ OPENED: {pos_id} | {signal.type.value} | Strike: {signal.strike} | Entry: ₹{signal.premium:.2f} | SL: ₹{signal.stop_loss:.2f}")
        
        return pos
    
    def close_position(self, pos_id: str, price: float, reason: ExitReason) -> Optional[Position]:
        if pos_id not in self.positions:
            return None
        
        pos = self.positions[pos_id]
        pos.exit_price = price
        pos.exit_time = datetime.now(IST)
        pos.exit_reason = reason
        pos.pnl_points = price - pos.entry_price
        pos.pnl = pos.pnl_points * pos.units
        
        status_map = {
            ExitReason.STOP_LOSS: TradeStatus.CLOSED_SL,
            ExitReason.OI_REVERSAL: TradeStatus.CLOSED_OI_EXIT,
            ExitReason.TRAILING_SL: TradeStatus.CLOSED_TRAILING,
            ExitReason.EOD: TradeStatus.CLOSED_EOD
        }
        pos.status = status_map.get(reason, TradeStatus.CLOSED_SL)
        
        is_win = pos.pnl > 0
        self.account.update_pnl(pos.pnl, is_win)
        
        del self.positions[pos_id]
        self.closed.append(pos)
        
        emoji = "🟢" if is_win else "🔴"
        logger.info(f"{emoji} CLOSED: {pos_id} | {reason.value} | P&L: ₹{pos.pnl:.2f} ({pos.pnl_points:+.1f} pts)")
        
        return pos
    
    def update_positions(self, state: MarketState) -> List[Tuple[str, ExitReason]]:
        """Update all positions and check exits"""
        exits = []
        
        for pos_id, pos in list(self.positions.items()):
            # Get current price from state
            # Note: Position might be on OTM strike outside 7-strike analysis range
            # Try to get from state, if not available, skip price update this cycle
            strike_data = state.strikes.get(pos.strike)
            
            if strike_data:
                if pos.trade_type == TradeType.CE_BUY:
                    price = strike_data.get('ce_ltp', pos.current_price)
                else:
                    price = strike_data.get('pe_ltp', pos.current_price)
                
                if price > 0:
                    pos.update_price(price)
            
            # Check exits
            reason = self._check_exit(pos, state)
            if reason:
                exits.append((pos_id, reason))
        
        return exits
    
    def _check_exit(self, pos: Position, state: MarketState) -> Optional[ExitReason]:
        """Check all exit conditions"""
        
        # 1. Hard Stop Loss
        if pos.current_price <= pos.stop_loss:
            logger.info(f"🛑 SL Hit: {pos.current_price:.2f} <= {pos.stop_loss:.2f}")
            return ExitReason.STOP_LOSS
        
        # 2. Trailing SL (ATR based)
        if pos.trailing_active and pos.current_price <= pos.trailing_sl:
            logger.info(f"🛑 Trailing SL: {pos.current_price:.2f} <= {pos.trailing_sl:.2f}")
            return ExitReason.TRAILING_SL
        
        # 3. OI-based Smart Exit
        if self._check_oi_exit(pos, state):
            return ExitReason.OI_REVERSAL
        
        # 4. Activate/Update Trailing SL if in profit
        if pos.pnl_points >= 10:  # 10 points profit
            pos.trailing_active = True
            # ATR-based trailing
            trail = pos.highest_price - (state.atr * CFG.ATR_TRAILING_MULTIPLIER)
            if trail > pos.trailing_sl:
                pos.trailing_sl = trail
                logger.info(f"📈 Trailing SL updated: ₹{pos.trailing_sl:.2f}")
        
        return None
    
    def _check_oi_exit(self, pos: Position, state: MarketState) -> bool:
        """OI-based exit - exit when OI support ends"""
        
        entry_oi = self.memory.get_position_oi(pos.id)
        if not entry_oi:
            return False
        
        # Get 5m OI change for quick exit
        ce_5m = state.ce_oi_change_5m
        pe_5m = state.pe_oi_change_5m
        
        if pos.trade_type == TradeType.CE_BUY:
            # Exit if CE starts building (resistance) or PE unwinding accelerates (bearish)
            if ce_5m > CFG.OI_EXIT_REVERSAL:
                logger.info(f"📊 OI Exit: CE building +{ce_5m:.1f}% = Resistance")
                return True
            if pe_5m < -CFG.OI_EXIT_REVERSAL and state.pe_velocity < -CFG.OI_VELOCITY_STRONG:
                logger.info(f"📊 OI Exit: PE unwinding {pe_5m:.1f}% with strong velocity = Bearish")
                return True
            # Exit if movement exhausting and we're in profit
            if state.movement_status == MovementStatus.EXHAUSTING and pos.pnl_points > 0:
                logger.info(f"📊 OI Exit: Movement exhausting, booking profit")
                return True
        
        else:  # PE_BUY
            if pe_5m > CFG.OI_EXIT_REVERSAL:
                logger.info(f"📊 OI Exit: PE building +{pe_5m:.1f}% = Support")
                return True
            if ce_5m < -CFG.OI_EXIT_REVERSAL and state.ce_velocity < -CFG.OI_VELOCITY_STRONG:
                logger.info(f"📊 OI Exit: CE unwinding {ce_5m:.1f}% = Bullish")
                return True
            if state.movement_status == MovementStatus.EXHAUSTING and pos.pnl_points > 0:
                logger.info(f"📊 OI Exit: Movement exhausting, booking profit")
                return True
        
        return False
    
    def get_positions_log(self) -> str:
        if not self.positions:
            return "No open positions"
        lines = []
        total = 0
        for p in self.positions.values():
            emoji = "🟢" if p.pnl >= 0 else "🔴"
            lines.append(f"  {p.id}: {p.trade_type.value} {p.strike} | ₹{p.entry_price:.2f}→₹{p.current_price:.2f} | {emoji} ₹{p.pnl:.2f}")
            total += p.pnl
        lines.append(f"  Total Unrealized: ₹{total:.2f}")
        return "\n".join(lines)

# ==================== TELEGRAM ====================
class Telegram:
    def __init__(self):
        self.bot = None
        if TELEGRAM_AVAILABLE and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
                logger.info("✅ Telegram ready")
            except Exception as e:
                logger.warning(f"⚠️ Telegram error: {e}")
    
    async def send(self, msg: str):
        if not self.bot:
            return
        try:
            await asyncio.wait_for(
                self.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg),
                timeout=5
            )
        except Exception as e:
            logger.warning(f"⚠️ Telegram send failed: {e}")
    
    async def send_trade(self, pos: Position, action: str):
        emoji = "🟢" if pos.trade_type == TradeType.CE_BUY else "🔴"
        if action == "ENTRY":
            msg = f"""
{emoji} TRADE ENTRY

{pos.trade_type.value} | Strike: {pos.strike}
Entry: ₹{pos.entry_price:.2f}
SL: ₹{pos.stop_loss:.2f}
Qty: {pos.quantity} lot ({pos.units} units)

⏰ {pos.entry_time.strftime('%I:%M:%S %p')}
"""
        else:
            pnl_emoji = "✅" if pos.pnl >= 0 else "❌"
            msg = f"""
{pnl_emoji} TRADE EXIT

{pos.trade_type.value} | Strike: {pos.strike}
Entry: ₹{pos.entry_price:.2f} → Exit: ₹{pos.exit_price:.2f}
P&L: ₹{pos.pnl:+.2f} ({pos.pnl_points:+.1f} pts)
Reason: {pos.exit_reason.value}

⏰ {pos.exit_time.strftime('%I:%M:%S %p')}
"""
        await self.send(msg)
    
    async def send_summary(self, account: DemoAccount, closed: List[Position]):
        s = account.get_summary()
        trades = ""
        for i, p in enumerate(closed, 1):
            e = "✅" if p.pnl >= 0 else "❌"
            trades += f"{i}. {e} {p.trade_type.value} {p.strike}: ₹{p.pnl:+.2f}\n"
        if not trades:
            trades = "No trades\n"
        
        pnl_e = "🟢" if s['realized'] >= 0 else "🔴"
        msg = f"""
📊 DAILY SUMMARY

{pnl_e} Day P&L: ₹{s['realized']:+.2f}
Balance: ₹{s['balance']:.2f}
Return: {s['return_pct']:+.2f}%

Trades: {s['trades']}
Wins: {s['wins']} | Losses: {s['losses']}
Win Rate: {s['win_rate']:.1f}%
Max DD: ₹{s['drawdown']:.2f}

{trades}
🤖 NIFTY Strike Master v2.1
"""
        await self.send(msg)

# ==================== MAIN BOT ====================
class NiftyBot:
    def __init__(self):
        self.account = DemoAccount()
        self.memory = Memory()
        self.feed = DataFeed()
        self.signal_gen = SignalGenerator(self.memory)
        self.pos_mgr = PositionManager(self.account, self.memory)
        self.telegram = Telegram()
        
        self.running = False
        self.scan_count = 0
        self.last_summary = None
    
    def is_market_hours(self) -> bool:
        now = datetime.now(IST).time()
        return CFG.MARKET_OPEN <= now <= CFG.MARKET_CLOSE
    
    def is_tradeable(self) -> bool:
        now = datetime.now(IST).time()
        if not self.is_market_hours():
            return False
        if CFG.AVOID_OPEN_START <= now <= CFG.AVOID_OPEN_END:
            return False
        if CFG.AVOID_CLOSE_START <= now <= CFG.AVOID_CLOSE_END:
            return False
        return True
    
    async def scan(self):
        self.scan_count += 1
        logger.info(f"\n{'='*70}")
        logger.info(f"🔍 SCAN #{self.scan_count} | {datetime.now(IST).strftime('%I:%M:%S %p')}")
        logger.info(f"{'='*70}")
        
        # Get market state
        state = await self.feed.get_full_market_state(self.memory)
        
        if state.spot_price == 0:
            logger.warning("⏳ No data, skipping...")
            return
        
        # Update positions
        exits = self.pos_mgr.update_positions(state)
        for pos_id, reason in exits:
            pos = self.pos_mgr.positions.get(pos_id)
            if pos:
                closed = self.pos_mgr.close_position(pos_id, pos.current_price, reason)
                if closed:
                    await self.telegram.send_trade(closed, "EXIT")
        
        # Log positions
        if self.pos_mgr.positions:
            logger.info("📍 Open Positions:")
            logger.info(self.pos_mgr.get_positions_log())
        
        # Generate signals
        if self.is_tradeable():
            signal = self.signal_gen.generate(state)
            if signal:
                pos = self.pos_mgr.open_position(signal)
                if pos:
                    await self.telegram.send_trade(pos, "ENTRY")
        else:
            if not self.is_market_hours():
                logger.info("🌙 Market closed")
            else:
                logger.info("⏳ Avoiding volatile period")
        
        # Account status
        s = self.account.get_summary()
        logger.info(f"💰 Balance: ₹{s['balance']:.2f} | Day P&L: ₹{s['realized']:+.2f} | Trades: {s['trades']}/{CFG.MAX_TRADES_PER_DAY}")
    
    async def check_summary(self):
        now = datetime.now(IST)
        if now.time() >= CFG.SUMMARY_TIME and self.last_summary != now.date():
            self.last_summary = now.date()
            
            # Close all positions EOD
            for pos_id in list(self.pos_mgr.positions.keys()):
                pos = self.pos_mgr.positions[pos_id]
                self.pos_mgr.close_position(pos_id, pos.current_price, ExitReason.EOD)
            
            await self.telegram.send_summary(self.account, self.pos_mgr.closed)
            logger.info("📊 Daily summary sent!")
    
    async def run(self):
        logger.info("="*70)
        logger.info("🚀 NIFTY50 STRIKE MASTER PRO v2.1")
        logger.info("   FULLY AUTONOMOUS TRADING SYSTEM")
        logger.info("="*70)
        logger.info("")
        logger.info(f"💰 Demo Capital: ₹{CFG.DEMO_CAPITAL}")
        logger.info(f"📦 Lot Size: {CFG.LOT_SIZE}")
        logger.info(f"📊 Analysis: {CFG.ANALYSIS_STRIKES*2+1} strikes | Trade: OTM ({CFG.OTM_STRIKES_AWAY} away)")
        logger.info(f"🎯 Logic: 90% OI/Volume + 10% Candle")
        logger.info(f"🛑 SL: {CFG.HARD_STOP_LOSS_POINTS} pts | Trailing: ATR×{CFG.ATR_TRAILING_MULTIPLIER}")
        logger.info(f"⚡ Exit: OI-based Smart Exit")
        logger.info("")
        
        # Check token
        if not UPSTOX_ACCESS_TOKEN or UPSTOX_ACCESS_TOKEN == '':
            logger.error("❌ UPSTOX_ACCESS_TOKEN not set!")
            logger.error("   Set environment variable and restart")
            return
        
        logger.info(f"🔑 Token: {UPSTOX_ACCESS_TOKEN[:20]}...")
        
        # Initialize
        await self.feed.initialize()
        
        # Startup message
        startup = f"""
🚀 NIFTY STRIKE MASTER v2.1 ONLINE

💰 Capital: ₹{CFG.DEMO_CAPITAL}
📦 Lot: {CFG.LOT_SIZE}
🎯 Strategy: OI+Volume Based (90%)
⚡ Exit: OI Support Based

📊 Analysis: 7 strikes
🎯 Trade: OTM ({CFG.OTM_STRIKES_AWAY} strikes away)
🛑 SL: {CFG.HARD_STOP_LOSS_POINTS} points

⏰ {datetime.now(IST).strftime('%d-%b %I:%M %p')}
"""
        await self.telegram.send(startup)
        
        self.running = True
        
        while self.running:
            try:
                if self.is_market_hours():
                    await self.scan()
                    await self.check_summary()
                    await asyncio.sleep(CFG.SCAN_INTERVAL)
                else:
                    # Reset at day start
                    now = datetime.now(IST)
                    if now.time() < CFG.MARKET_OPEN and now.date() != self.last_summary:
                        self.account.reset_daily()
                        self.pos_mgr.closed = []
                        self.feed.day_open = 0
                        self.feed.day_high = 0
                        self.feed.day_low = 999999
                    
                    logger.info(f"🌙 Market closed. Next check in 5 min...")
                    await asyncio.sleep(300)
                    
            except KeyboardInterrupt:
                logger.info("\n🛑 Shutdown requested")
                self.running = False
            except Exception as e:
                logger.error(f"💥 Error: {e}")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(30)
        
        logger.info("👋 Bot stopped")

# ==================== MAIN ====================
async def main():
    logger.info("="*70)
    logger.info("Starting NIFTY50 Strike Master PRO v2.1")
    logger.info("="*70)
    
    bot = NiftyBot()
    await bot.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
