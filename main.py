#!/usr/bin/env python3
"""
================================================================================
    NIFTY50 STRIKE MASTER PRO v2.2 - FULLY AUTONOMOUS TRADING SYSTEM
================================================================================

✅ NEW IN v2.2:
   - 30 strikes fetch (ATM ± 15) for wide coverage
   - 7 strikes for OI analysis (ATM ± 3)
   - Deep OTM trading (4-5 strikes away, ₹20-50 premium)
   - 9:10 AM start with Yesterday OI baseline
   - 5 sec polling during opening (9:20-9:45)
   - 30 sec polling normal hours
   - Max premium cap ₹60 (affordable with ₹5000)
   - Aggressive opening mode

📊 TRADING LOGIC (90% OI/Volume, 10% Candle):
   - Entry: OI unwinding + Volume surge + Spot confirmation
   - Exit: OI support ends (velocity drop/reversal)
   - Hold: As long as OI supports the move

⚙️ SETTINGS:
   - Demo Capital: ₹5,000
   - Lot Size: 75 (NSE Dec 2025)
   - Fetch: 30 strikes | Analysis: 7 strikes
   - Trade: Deep OTM (₹20-60 premium max)

Author: Claude AI
Version: 2.2
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
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

logging.basicConfig(
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("NIFTY-v2.2")
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
    HARD_STOP_LOSS_POINTS = 3  # Tight SL for opening
    NORMAL_STOP_LOSS_POINTS = 5  # Normal hours SL
    
    # Strike Configuration - UPDATED v2.2
    FETCH_STRIKES = 15      # ATM ± 15 = 31 strikes for fetching
    ANALYSIS_STRIKES = 3    # ATM ± 3 = 7 strikes for OI analysis
    OTM_STRIKES_AWAY = 4    # Trade 4-5 strikes OTM for cheap premium
    MAX_PREMIUM = 60.0      # Max ₹60 premium (₹60 × 75 = ₹4,500)
    MIN_PREMIUM = 15.0      # Min ₹15 premium (too cheap = illiquid)
    
    # OI Thresholds
    OI_ENTRY_THRESHOLD = 5.0
    OI_STRONG_THRESHOLD = 8.0
    OI_OPENING_THRESHOLD = 3.0  # Lower threshold for opening
    OI_EXIT_REVERSAL = 3.0
    OI_VELOCITY_STRONG = 0.5
    OI_VELOCITY_WEAK = 0.1
    
    # Volume
    VOLUME_SPIKE_MULTIPLIER = 1.5  # Lower for opening
    ORDER_FLOW_BULLISH = 0.8
    ORDER_FLOW_BEARISH = 1.2
    
    # PCR
    PCR_BULLISH = 1.08
    PCR_BEARISH = 0.92
    
    # Technical
    ATR_PERIOD = 14
    ATR_FALLBACK = 30
    ATR_TRAILING_MULTIPLIER = 1.0
    
    # Timing - UPDATED v2.2
    BOT_START = time(9, 10)       # Start at 9:10 for yesterday OI
    MARKET_OPEN = time(9, 15)
    MARKET_CLOSE = time(15, 30)
    TRADING_START = time(9, 20)   # Start trading at 9:20
    OPENING_END = time(9, 45)     # Opening period ends
    AVOID_CLOSE_START = time(15, 15)
    SUMMARY_TIME = time(15, 0)
    
    # Intervals - UPDATED v2.2
    OPENING_SCAN_INTERVAL = 5     # 5 sec during opening (9:20-9:45)
    NORMAL_SCAN_INTERVAL = 30     # 30 sec normal hours
    PREMARKET_SCAN_INTERVAL = 60  # 1 min pre-market (9:10-9:15)
    
    # Memory
    REDIS_TTL_HOURS = 24
    SIGNAL_COOLDOWN = 180  # 3 min cooldown (faster for opening)
    
    # API
    RATE_LIMIT_SEC = 50
    RATE_LIMIT_MIN = 500
    
    # Endpoints
    INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    QUOTE_URL = "https://api.upstox.com/v2/market-quote/quotes"
    OPTION_CHAIN_URL = "https://api.upstox.com/v2/option/chain"
    CANDLE_URL = "https://api.upstox.com/v2/historical-candle/intraday"

CFG = Config()

# ==================== ENUMS ====================
class TradeType(Enum):
    CE_BUY = "CE_BUY"
    PE_BUY = "PE_BUY"

class TradeStatus(Enum):
    OPEN = "OPEN"
    CLOSED_SL = "CLOSED_SL"
    CLOSED_OI_EXIT = "CLOSED_OI_EXIT"
    CLOSED_TRAILING = "CLOSED_TRAILING"
    CLOSED_EOD = "CLOSED_EOD"

class ExitReason(Enum):
    STOP_LOSS = "Stop Loss"
    OI_REVERSAL = "OI Support Lost"
    TRAILING_SL = "Trailing SL"
    EOD = "End of Day"
    MAX_LOSS = "Max Daily Loss"

class MovementStatus(Enum):
    TRENDING = "TRENDING"
    EXHAUSTING = "EXHAUSTING"
    SIDEWAYS = "SIDEWAYS"
    REVERSAL_SOON = "REVERSAL_SOON"
    UNKNOWN = "UNKNOWN"

class MarketPhase(Enum):
    PRE_MARKET = "PRE_MARKET"      # 9:10-9:15
    OPENING = "OPENING"            # 9:15-9:45
    NORMAL = "NORMAL"              # 9:45-15:15
    CLOSING = "CLOSING"            # 15:15-15:30
    CLOSED = "CLOSED"              # After hours

# ==================== DATA CLASSES ====================
@dataclass
class DemoAccount:
    """Virtual trading account"""
    initial_capital: float = CFG.DEMO_CAPITAL
    current_balance: float = CFG.DEMO_CAPITAL
    realized_pnl: float = 0.0
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
            'balance': self.current_balance,
            'realized': self.realized_pnl,
            'trades': self.trades_today,
            'wins': self.winning_trades,
            'losses': self.losing_trades,
            'win_rate': wr,
            'return_pct': ((self.current_balance - self.initial_capital) / self.initial_capital) * 100
        }

@dataclass
class Position:
    """Open position"""
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
        self.pnl_points = price - self.entry_price
        self.pnl = self.pnl_points * self.units

@dataclass
class MarketState:
    """Complete market state"""
    timestamp: datetime
    spot_price: float
    atm_strike: int
    phase: MarketPhase
    
    # All fetched strikes (30+)
    all_strikes: Dict[int, dict] = field(default_factory=dict)
    
    # Analysis strikes (7)
    analysis_strikes: Dict[int, dict] = field(default_factory=dict)
    
    # Totals (from analysis strikes)
    total_ce_oi: int = 0
    total_pe_oi: int = 0
    total_ce_vol: int = 0
    total_pe_vol: int = 0
    
    # Calculated
    pcr: float = 1.0
    order_flow_ratio: float = 1.0
    twap: float = 0.0
    
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
    
    # ATR
    atr: float = CFG.ATR_FALLBACK
    
    # Yesterday OI (for 9:10-9:15 baseline)
    yesterday_ce_oi: int = 0
    yesterday_pe_oi: int = 0

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
                await asyncio.sleep(1.0 - (now - self.per_sec[0]))
                now = time_module.time()
            if len(self.per_min) >= CFG.RATE_LIMIT_MIN:
                await asyncio.sleep(60.0 - (now - self.per_min[0]))
                now = time_module.time()
            self.per_sec.append(now)
            self.per_min.append(now)

# ==================== MEMORY ====================
class Memory:
    """24-hour memory for OI tracking"""
    
    def __init__(self):
        self.client = None
        self.ram = {}
        self.ram_ts = {}
        self.startup = datetime.now(IST)
        self.ttl = CFG.REDIS_TTL_HOURS * 3600
        self.yesterday_oi = {}  # Store yesterday's closing OI
        self.warmup_done = False
        
        if REDIS_AVAILABLE:
            try:
                self.client = redis.from_url(REDIS_URL, decode_responses=True)
                self.client.ping()
                logger.info("✅ Redis connected (24h TTL)")
            except Exception as e:
                self.client = None
                logger.warning(f"⚠️ Redis failed: {e} - using RAM")
    
    def is_warmed_up(self) -> bool:
        """Check if we have enough OI history"""
        # If we have yesterday's OI, we're ready immediately
        if self.yesterday_oi:
            return True
        # Otherwise wait 5 min
        elapsed = (datetime.now(IST) - self.startup).total_seconds() / 60
        return elapsed >= 5
    
    def _key(self, prefix: str, id: str, ts: datetime) -> str:
        slot = ts.replace(second=0, microsecond=0)
        return f"nifty:v22:{prefix}:{id}:{slot.strftime('%Y%m%d_%H%M')}"
    
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
        
        # If no recent data, try yesterday's data
        if not data_str and self.yesterday_oi.get(strike):
            past_data = self.yesterday_oi[strike]
            ce_chg = ((current['ce_oi'] - past_data['ce_oi']) / past_data['ce_oi'] * 100) if past_data.get('ce_oi', 0) > 0 else 0
            pe_chg = ((current['pe_oi'] - past_data['pe_oi']) / past_data['pe_oi'] * 100) if past_data.get('pe_oi', 0) > 0 else 0
            return ce_chg, pe_chg
        
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
    
    def save_yesterday_oi(self, strikes: Dict[int, dict]):
        """Save current OI as yesterday's baseline (call at 9:10-9:15)"""
        for strike, data in strikes.items():
            self.yesterday_oi[strike] = {
                'ce_oi': data.get('ce_oi', 0),
                'pe_oi': data.get('pe_oi', 0),
                'timestamp': datetime.now(IST).isoformat()
            }
        logger.info(f"📦 Saved {len(strikes)} strikes as yesterday OI baseline")
        self.warmup_done = True

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
        from datetime import datetime
        target_month = datetime.now(IST).month
        target_year = datetime.now(IST).year
        
        for key, inst in self.instruments.items():
            if (inst.get('segment') == 'NSE_FO' and 
                inst.get('instrument_type') == 'FUT' and 
                inst.get('name') == 'NIFTY'):
                exp = inst.get('expiry', 0)
                if exp:
                    exp_date = datetime.fromtimestamp(exp / 1000, tz=IST)
                    if exp_date.month == target_month or (target_month == 12 and exp_date.month == 1):
                        self.futures_key = key
                        logger.info(f"✅ Futures: {inst.get('trading_symbol')}")
                        return
    
    async def _fetch(self, url: str, session: aiohttp.ClientSession) -> Optional[dict]:
        for attempt in range(3):
            try:
                await self.rate_limiter.wait()
                async with session.get(url, headers=self.headers, timeout=15) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 401:
                        logger.error("❌ Token expired!")
                        return None
                    elif resp.status == 429:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        logger.warning(f"⚠️ HTTP {resp.status}")
                        await asyncio.sleep(1)
            except asyncio.TimeoutError:
                await asyncio.sleep(1)
            except Exception as e:
                logger.warning(f"❌ Fetch: {e}")
                await asyncio.sleep(1)
        return None
    
    async def get_spot_quote(self) -> dict:
        """Get NIFTY Spot LTP"""
        async with aiohttp.ClientSession() as session:
            key = urllib.parse.quote(CFG.SPOT_KEY)
            url = f"{CFG.QUOTE_URL}?symbol={key}"
            data = await self._fetch(url, session)
            
            if data and data.get('status') == 'success':
                data_dict = data.get('data', {})
                
                # Try multiple key formats
                for k in [CFG.SPOT_KEY, 'NSE_INDEX:Nifty 50', 'NSE_INDEX|NIFTY 50']:
                    if k in data_dict:
                        quote = data_dict[k]
                        ltp = quote.get('last_price', 0)
                        ohlc = quote.get('ohlc', {})
                        
                        if ltp > 0:
                            if self.day_open == 0:
                                self.day_open = ohlc.get('open', ltp)
                            self.day_high = max(self.day_high, ltp)
                            self.day_low = min(self.day_low, ltp)
                        
                        return {
                            'ltp': ltp,
                            'open': ohlc.get('open', 0),
                            'high': ohlc.get('high', 0),
                            'low': ohlc.get('low', 0)
                        }
                
                # Fallback: use first key
                if data_dict:
                    first_key = list(data_dict.keys())[0]
                    quote = data_dict[first_key]
                    return {'ltp': quote.get('last_price', 0), 'open': 0, 'high': 0, 'low': 0}
        
        return {'ltp': 0, 'open': 0, 'high': 0, 'low': 0, 'close': 0}
    
    def get_weekly_expiry(self) -> str:
        """Get current week's expiry date"""
        now = datetime.now(IST)
        days_ahead = 1 - now.weekday()  # Tuesday = 1
        if days_ahead < 0:
            days_ahead += 7
        if days_ahead == 0 and now.time() > time(15, 30):
            days_ahead = 7
        expiry = now + timedelta(days=days_ahead)
        return expiry.strftime('%Y-%m-%d')
    
    async def get_option_chain(self, expiry: str) -> List[dict]:
        """Get full option chain"""
        async with aiohttp.ClientSession() as session:
            key = urllib.parse.quote(CFG.SPOT_KEY)
            url = f"{CFG.OPTION_CHAIN_URL}?instrument_key={key}&expiry_date={expiry}"
            data = await self._fetch(url, session)
            if data and data.get('status') == 'success':
                return data.get('data', [])
        return []
    
    async def get_candles(self, limit: int = 50) -> List:
        """Get futures candles for ATR"""
        if not self.futures_key:
            return []
        async with aiohttp.ClientSession() as session:
            key = urllib.parse.quote(self.futures_key)
            url = f"{CFG.CANDLE_URL}/{key}/1minute"
            data = await self._fetch(url, session)
            if data and data.get('status') == 'success':
                return data.get('data', {}).get('candles', [])[:limit]
        return []
    
    def get_market_phase(self) -> MarketPhase:
        """Get current market phase"""
        now = datetime.now(IST).time()
        
        if now < CFG.MARKET_OPEN:
            if now >= CFG.BOT_START:
                return MarketPhase.PRE_MARKET
            return MarketPhase.CLOSED
        elif now < CFG.OPENING_END:
            return MarketPhase.OPENING
        elif now < CFG.AVOID_CLOSE_START:
            return MarketPhase.NORMAL
        elif now <= CFG.MARKET_CLOSE:
            return MarketPhase.CLOSING
        else:
            return MarketPhase.CLOSED
    
    async def get_market_state(self, memory: Memory) -> MarketState:
        """Get complete market state with 30 strikes"""
        now = datetime.now(IST)
        phase = self.get_market_phase()
        
        # Get spot
        spot = await self.get_spot_quote()
        spot_ltp = spot['ltp']
        
        if spot_ltp == 0:
            logger.warning("⚠️ Spot price unavailable")
            return MarketState(timestamp=now, spot_price=0, atm_strike=0, phase=phase)
        
        logger.info(f"✅ Spot: ₹{spot_ltp:.2f}")
        
        # ATM
        atm = round(spot_ltp / CFG.STRIKE_GAP) * CFG.STRIKE_GAP
        
        # Get option chain
        expiry = self.get_weekly_expiry()
        chain = await self.get_option_chain(expiry)
        
        if not chain:
            logger.warning("⚠️ Option chain unavailable")
            return MarketState(timestamp=now, spot_price=spot_ltp, atm_strike=atm, phase=phase)
        
        # Process ALL strikes (30+)
        min_fetch = atm - (CFG.FETCH_STRIKES * CFG.STRIKE_GAP)
        max_fetch = atm + (CFG.FETCH_STRIKES * CFG.STRIKE_GAP)
        
        # Analysis range (7 strikes)
        min_analysis = atm - (CFG.ANALYSIS_STRIKES * CFG.STRIKE_GAP)
        max_analysis = atm + (CFG.ANALYSIS_STRIKES * CFG.STRIKE_GAP)
        
        all_strikes = {}
        analysis_strikes = {}
        total_ce_oi = 0
        total_pe_oi = 0
        total_ce_vol = 0
        total_pe_vol = 0
        
        for opt in chain:
            strike = opt.get('strike_price', 0)
            
            # Skip if outside fetch range
            if strike < min_fetch or strike > max_fetch:
                continue
            
            ce_data = opt.get('call_options', {}).get('market_data', {})
            pe_data = opt.get('put_options', {}).get('market_data', {})
            
            strike_info = {
                'ce_oi': ce_data.get('oi', 0),
                'pe_oi': pe_data.get('oi', 0),
                'ce_vol': ce_data.get('volume', 0),
                'pe_vol': pe_data.get('volume', 0),
                'ce_ltp': ce_data.get('ltp', 0),
                'pe_ltp': pe_data.get('ltp', 0),
                'ce_key': opt.get('call_options', {}).get('instrument_key', ''),
                'pe_key': opt.get('put_options', {}).get('instrument_key', '')
            }
            
            all_strikes[strike] = strike_info
            
            # Save OI snapshot
            memory.save_oi(strike, {
                'ce_oi': strike_info['ce_oi'],
                'pe_oi': strike_info['pe_oi'],
                'ts': now.isoformat()
            })
            
            # Analysis strikes
            if min_analysis <= strike <= max_analysis:
                analysis_strikes[strike] = strike_info
                total_ce_oi += strike_info['ce_oi']
                total_pe_oi += strike_info['pe_oi']
                total_ce_vol += strike_info['ce_vol']
                total_pe_vol += strike_info['pe_vol']
        
        logger.info(f"📊 Fetched {len(all_strikes)} strikes | Analysis: {len(analysis_strikes)}")
        
        # Save baseline during pre-market
        if phase == MarketPhase.PRE_MARKET and not memory.warmup_done:
            memory.save_yesterday_oi(all_strikes)
        
        # Save total OI
        memory.save_total_oi(total_ce_oi, total_pe_oi)
        
        # Calculate metrics
        pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 1.0
        order_flow = total_ce_vol / total_pe_vol if total_pe_vol > 0 else 1.0
        twap = (self.day_open + self.day_high + self.day_low + spot_ltp) / 4 if self.day_open > 0 else spot_ltp
        
        # OI Changes
        ce_5m, pe_5m = memory.get_total_oi_change(total_ce_oi, total_pe_oi, 5)
        ce_15m, pe_15m = memory.get_total_oi_change(total_ce_oi, total_pe_oi, 15)
        
        # Velocity
        ce_vel = ce_15m / 15 if ce_15m != 0 else 0
        pe_vel = pe_15m / 15 if pe_15m != 0 else 0
        
        # ATR
        candles = await self.get_candles(20)
        atr = self._calc_atr(candles)
        
        # Create state
        state = MarketState(
            timestamp=now,
            spot_price=spot_ltp,
            atm_strike=atm,
            phase=phase,
            all_strikes=all_strikes,
            analysis_strikes=analysis_strikes,
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
            ce_velocity=ce_vel,
            pe_velocity=pe_vel,
            atr=atr
        )
        
        # Analyze movement
        state.movement_status, state.divergence_detected, state.divergence_type = self._analyze_movement(state)
        
        return state
    
    def _calc_atr(self, candles: List, period: int = 14) -> float:
        if len(candles) < 2:
            return CFG.ATR_FALLBACK
        tr_list = []
        for i in range(1, min(len(candles), period + 1)):
            h, l, pc = candles[i][2], candles[i][3], candles[i-1][4]
            tr = max(h - l, abs(h - pc), abs(l - pc))
            tr_list.append(tr)
        return max(sum(tr_list) / len(tr_list), 10) if tr_list else CFG.ATR_FALLBACK
    
    def _analyze_movement(self, state: MarketState) -> Tuple[MovementStatus, bool, str]:
        ce_vel = abs(state.ce_velocity)
        pe_vel = abs(state.pe_velocity)
        
        status = MovementStatus.UNKNOWN
        divergence = False
        div_type = ""
        
        if ce_vel > CFG.OI_VELOCITY_STRONG or pe_vel > CFG.OI_VELOCITY_STRONG:
            status = MovementStatus.TRENDING
        elif ce_vel < CFG.OI_VELOCITY_WEAK and pe_vel < CFG.OI_VELOCITY_WEAK:
            status = MovementStatus.SIDEWAYS if abs(state.ce_oi_change_15m) < 2 else MovementStatus.EXHAUSTING
        else:
            status = MovementStatus.EXHAUSTING
        
        # Divergence
        price_change = ((state.spot_price - state.twap) / state.twap * 100) if state.twap > 0 else 0
        
        if price_change > 0.3 and state.ce_oi_change_15m > 3:
            divergence = True
            div_type = "BEARISH (Price↑ + CE OI↑)"
            status = MovementStatus.REVERSAL_SOON
        elif price_change < -0.3 and state.pe_oi_change_15m > 3:
            divergence = True
            div_type = "BULLISH (Price↓ + PE OI↑)"
            status = MovementStatus.REVERSAL_SOON
        
        return status, divergence, div_type

# ==================== SIGNAL GENERATOR ====================
class SignalGenerator:
    """Generate signals - OI/Volume based (90%)"""
    
    def __init__(self, memory: Memory):
        self.memory = memory
        self.last_signal = {}
    
    def _can_signal(self, strike: int) -> bool:
        now = datetime.now(IST)
        key = f"sig_{strike}"
        if key in self.last_signal:
            if (now - self.last_signal[key]).total_seconds() < CFG.SIGNAL_COOLDOWN:
                return False
        self.last_signal[key] = now
        return True
    
    def _find_best_otm_strike(self, state: MarketState, direction: str) -> Optional[Tuple[int, dict]]:
        """Find best OTM strike within premium range"""
        
        if direction == "CE":
            # CE = strikes above ATM
            candidates = []
            for strike, data in state.all_strikes.items():
                if strike > state.atm_strike:  # OTM for CE
                    premium = data.get('ce_ltp', 0)
                    if CFG.MIN_PREMIUM <= premium <= CFG.MAX_PREMIUM:
                        candidates.append((strike, data, premium))
            
            # Sort by strike (closest first)
            candidates.sort(key=lambda x: x[0])
            
            if candidates:
                best = candidates[0]
                return best[0], best[1]
        
        else:  # PE
            # PE = strikes below ATM
            candidates = []
            for strike, data in state.all_strikes.items():
                if strike < state.atm_strike:  # OTM for PE
                    premium = data.get('pe_ltp', 0)
                    if CFG.MIN_PREMIUM <= premium <= CFG.MAX_PREMIUM:
                        candidates.append((strike, data, premium))
            
            # Sort by strike (closest first, descending)
            candidates.sort(key=lambda x: x[0], reverse=True)
            
            if candidates:
                best = candidates[0]
                return best[0], best[1]
        
        return None
    
    def generate(self, state: MarketState) -> Optional[Signal]:
        """Generate trading signal"""
        
        if state.spot_price == 0 or not state.analysis_strikes:
            return None
        
        # Check warmup
        if not self.memory.is_warmed_up():
            logger.info("⏳ Warming up...")
            return None
        
        # Skip during reversal
        if state.movement_status == MovementStatus.REVERSAL_SOON:
            logger.info(f"⚠️ Reversal detected: {state.divergence_type}")
            return None
        
        # Adjust thresholds based on phase
        oi_threshold = CFG.OI_OPENING_THRESHOLD if state.phase == MarketPhase.OPENING else CFG.OI_ENTRY_THRESHOLD
        
        # Log analysis
        logger.info(f"📊 ATM: {state.atm_strike} | Phase: {state.phase.value}")
        logger.info(f"📊 OI 15m: CE={state.ce_oi_change_15m:+.1f}% | PE={state.pe_oi_change_15m:+.1f}%")
        logger.info(f"📊 Velocity: CE={state.ce_velocity:.2f}/min | PE={state.pe_velocity:.2f}/min")
        logger.info(f"📊 PCR: {state.pcr:.2f} | OrderFlow: {state.order_flow_ratio:.2f}")
        logger.info(f"📊 Movement: {state.movement_status.value}")
        
        # ========== CE BUY (Bullish) ==========
        if state.ce_oi_change_15m < -oi_threshold:
            
            checks = {
                "CE Unwinding": state.ce_oi_change_15m < -oi_threshold,
                "CE 5m Down": state.ce_oi_change_5m < -2,
                "Velocity Strong": abs(state.ce_velocity) > CFG.OI_VELOCITY_WEAK,
                "OrderFlow Bullish": state.order_flow_ratio < CFG.ORDER_FLOW_BULLISH,
                "PCR Bullish": state.pcr > CFG.PCR_BULLISH,
                "Spot > TWAP": state.spot_price > state.twap,
                "Trending": state.movement_status in [MovementStatus.TRENDING, MovementStatus.UNKNOWN]
            }
            
            passed = sum(checks.values())
            
            # Opening needs 3 checks, normal needs 4
            required = 3 if state.phase == MarketPhase.OPENING else 4
            
            if passed >= required:
                result = self._find_best_otm_strike(state, "CE")
                
                if not result:
                    logger.warning("⚠️ No suitable CE OTM strike found")
                    return None
                
                strike, data = result
                premium = data['ce_ltp']
                
                if not self._can_signal(strike):
                    return None
                
                # Stop loss based on phase
                sl_points = CFG.HARD_STOP_LOSS_POINTS if state.phase == MarketPhase.OPENING else CFG.NORMAL_STOP_LOSS_POINTS
                
                confidence = min(60 + (passed * 5), 95)
                
                logger.info(f"🎯 CE BUY! Strike: {strike} | Premium: ₹{premium:.2f} | Conf: {confidence}%")
                
                return Signal(
                    type=TradeType.CE_BUY,
                    reason=f"CE Unwinding {state.ce_oi_change_15m:.1f}%",
                    confidence=confidence,
                    spot_price=state.spot_price,
                    strike=strike,
                    option_type='CE',
                    premium=premium,
                    instrument_key=data['ce_key'],
                    oi_data={
                        'ce_15m': state.ce_oi_change_15m,
                        'pe_15m': state.pe_oi_change_15m,
                        'ce_vel': state.ce_velocity,
                        'pe_vel': state.pe_velocity
                    },
                    stop_loss=premium - sl_points
                )
        
        # ========== PE BUY (Bearish) ==========
        if state.pe_oi_change_15m < -oi_threshold:
            
            checks = {
                "PE Unwinding": state.pe_oi_change_15m < -oi_threshold,
                "PE 5m Down": state.pe_oi_change_5m < -2,
                "Velocity Strong": abs(state.pe_velocity) > CFG.OI_VELOCITY_WEAK,
                "OrderFlow Bearish": state.order_flow_ratio > CFG.ORDER_FLOW_BEARISH,
                "PCR Bearish": state.pcr < CFG.PCR_BEARISH,
                "Spot < TWAP": state.spot_price < state.twap,
                "Trending": state.movement_status in [MovementStatus.TRENDING, MovementStatus.UNKNOWN]
            }
            
            passed = sum(checks.values())
            required = 3 if state.phase == MarketPhase.OPENING else 4
            
            if passed >= required:
                result = self._find_best_otm_strike(state, "PE")
                
                if not result:
                    logger.warning("⚠️ No suitable PE OTM strike found")
                    return None
                
                strike, data = result
                premium = data['pe_ltp']
                
                if not self._can_signal(strike):
                    return None
                
                sl_points = CFG.HARD_STOP_LOSS_POINTS if state.phase == MarketPhase.OPENING else CFG.NORMAL_STOP_LOSS_POINTS
                confidence = min(60 + (passed * 5), 95)
                
                logger.info(f"🎯 PE BUY! Strike: {strike} | Premium: ₹{premium:.2f} | Conf: {confidence}%")
                
                return Signal(
                    type=TradeType.PE_BUY,
                    reason=f"PE Unwinding {state.pe_oi_change_15m:.1f}%",
                    confidence=confidence,
                    spot_price=state.spot_price,
                    strike=strike,
                    option_type='PE',
                    premium=premium,
                    instrument_key=data['pe_key'],
                    oi_data={
                        'ce_15m': state.ce_oi_change_15m,
                        'pe_15m': state.pe_oi_change_15m,
                        'ce_vel': state.ce_velocity,
                        'pe_vel': state.pe_velocity
                    },
                    stop_loss=premium - sl_points
                )
        
        logger.info("✋ No setup")
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
        
        cost = signal.premium * CFG.LOT_SIZE
        if cost > self.account.current_balance:
            logger.warning(f"❌ Need ₹{cost:.0f}, have ₹{self.account.current_balance:.0f}")
            return None
        
        self.counter += 1
        pos_id = f"P{datetime.now(IST).strftime('%Y%m%d')}_{self.counter:03d}"
        
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
        
        logger.info(f"✅ OPENED: {pos_id} | {signal.type.value} {signal.strike} @ ₹{signal.premium:.2f} | SL: ₹{signal.stop_loss:.2f}")
        
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
        
        is_win = pos.pnl > 0
        self.account.update_pnl(pos.pnl, is_win)
        
        del self.positions[pos_id]
        self.closed.append(pos)
        
        emoji = "🟢" if is_win else "🔴"
        logger.info(f"{emoji} CLOSED: {pos_id} | {reason.value} | P&L: ₹{pos.pnl:.2f}")
        
        return pos
    
    def update_positions(self, state: MarketState) -> List[Tuple[str, ExitReason]]:
        """Update all positions"""
        exits = []
        
        for pos_id, pos in list(self.positions.items()):
            # Get current price from all_strikes (includes OTM)
            strike_data = state.all_strikes.get(pos.strike)
            
            if strike_data:
                if pos.trade_type == TradeType.CE_BUY:
                    price = strike_data.get('ce_ltp', 0)
                else:
                    price = strike_data.get('pe_ltp', 0)
                
                if price > 0:
                    pos.update_price(price)
            
            # Check exits
            reason = self._check_exit(pos, state)
            if reason:
                exits.append((pos_id, reason))
        
        return exits
    
    def _check_exit(self, pos: Position, state: MarketState) -> Optional[ExitReason]:
        """Check exit conditions"""
        
        # Hard SL
        if pos.current_price <= pos.stop_loss:
            return ExitReason.STOP_LOSS
        
        # Trailing SL
        if pos.trailing_active and pos.current_price <= pos.trailing_sl:
            return ExitReason.TRAILING_SL
        
        # OI-based exit
        if self._check_oi_exit(pos, state):
            return ExitReason.OI_REVERSAL
        
        # Activate trailing
        if pos.pnl_points >= 8:
            pos.trailing_active = True
            trail = pos.highest_price - (state.atr * CFG.ATR_TRAILING_MULTIPLIER)
            if trail > pos.trailing_sl:
                pos.trailing_sl = trail
        
        return None
    
    def _check_oi_exit(self, pos: Position, state: MarketState) -> bool:
        """OI-based exit"""
        ce_5m = state.ce_oi_change_5m
        pe_5m = state.pe_oi_change_5m
        
        if pos.trade_type == TradeType.CE_BUY:
            if ce_5m > CFG.OI_EXIT_REVERSAL:
                logger.info(f"📊 OI Exit: CE building +{ce_5m:.1f}%")
                return True
            if state.movement_status == MovementStatus.EXHAUSTING and pos.pnl_points > 0:
                logger.info("📊 OI Exit: Movement exhausting")
                return True
        else:
            if pe_5m > CFG.OI_EXIT_REVERSAL:
                logger.info(f"📊 OI Exit: PE building +{pe_5m:.1f}%")
                return True
            if state.movement_status == MovementStatus.EXHAUSTING and pos.pnl_points > 0:
                logger.info("📊 OI Exit: Movement exhausting")
                return True
        
        return False

# ==================== TELEGRAM ====================
class Telegram:
    def __init__(self):
        self.bot = None
        if TELEGRAM_AVAILABLE and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
                logger.info("✅ Telegram ready")
            except Exception as e:
                logger.warning(f"⚠️ Telegram: {e}")
    
    async def send(self, msg: str):
        if not self.bot:
            return
        try:
            await asyncio.wait_for(
                self.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg),
                timeout=5
            )
        except:
            pass
    
    async def send_trade(self, pos: Position, action: str):
        if action == "ENTRY":
            msg = f"🎯 ENTRY\n{pos.trade_type.value} {pos.strike}\n₹{pos.entry_price:.2f} | SL: ₹{pos.stop_loss:.2f}"
        else:
            emoji = "✅" if pos.pnl >= 0 else "❌"
            msg = f"{emoji} EXIT\n{pos.trade_type.value} {pos.strike}\n₹{pos.entry_price:.2f}→₹{pos.exit_price:.2f}\nP&L: ₹{pos.pnl:+.2f}"
        await self.send(msg)
    
    async def send_summary(self, account: DemoAccount, closed: List[Position]):
        s = account.get_summary()
        emoji = "🟢" if s['realized'] >= 0 else "🔴"
        msg = f"""
📊 DAILY SUMMARY

{emoji} P&L: ₹{s['realized']:+.2f}
Balance: ₹{s['balance']:.2f}
Trades: {s['trades']} | Win: {s['win_rate']:.0f}%

🤖 NIFTY v2.2
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
    
    def get_scan_interval(self, phase: MarketPhase) -> int:
        """Dynamic scan interval based on phase"""
        if phase == MarketPhase.PRE_MARKET:
            return CFG.PREMARKET_SCAN_INTERVAL
        elif phase == MarketPhase.OPENING:
            return CFG.OPENING_SCAN_INTERVAL
        else:
            return CFG.NORMAL_SCAN_INTERVAL
    
    async def scan(self):
        self.scan_count += 1
        now = datetime.now(IST)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🔍 SCAN #{self.scan_count} | {now.strftime('%I:%M:%S %p')}")
        logger.info(f"{'='*60}")
        
        # Get state
        state = await self.feed.get_market_state(self.memory)
        
        if state.spot_price == 0:
            logger.warning("⏳ No data")
            return state.phase
        
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
            for p in self.pos_mgr.positions.values():
                emoji = "🟢" if p.pnl >= 0 else "🔴"
                logger.info(f"📍 {p.id}: {p.trade_type.value} {p.strike} | {emoji} ₹{p.pnl:.2f}")
        
        # Generate signals (only during trading hours)
        if state.phase in [MarketPhase.OPENING, MarketPhase.NORMAL]:
            signal = self.signal_gen.generate(state)
            if signal:
                pos = self.pos_mgr.open_position(signal)
                if pos:
                    await self.telegram.send_trade(pos, "ENTRY")
        elif state.phase == MarketPhase.PRE_MARKET:
            logger.info("📦 Pre-market: Building OI baseline")
        
        # Account status
        s = self.account.get_summary()
        logger.info(f"💰 Balance: ₹{s['balance']:.2f} | P&L: ₹{s['realized']:+.2f} | Trades: {s['trades']}/{CFG.MAX_TRADES_PER_DAY}")
        
        return state.phase
    
    async def check_summary(self):
        now = datetime.now(IST)
        if now.time() >= CFG.SUMMARY_TIME and self.last_summary != now.date():
            self.last_summary = now.date()
            
            # Close all positions
            for pos_id in list(self.pos_mgr.positions.keys()):
                pos = self.pos_mgr.positions[pos_id]
                self.pos_mgr.close_position(pos_id, pos.current_price, ExitReason.EOD)
            
            await self.telegram.send_summary(self.account, self.pos_mgr.closed)
    
    async def run(self):
        logger.info("="*60)
        logger.info("🚀 NIFTY50 STRIKE MASTER PRO v2.2")
        logger.info("="*60)
        logger.info(f"💰 Capital: ₹{CFG.DEMO_CAPITAL} | Lot: {CFG.LOT_SIZE}")
        logger.info(f"📊 Fetch: {CFG.FETCH_STRIKES*2+1} strikes | Analysis: {CFG.ANALYSIS_STRIKES*2+1}")
        logger.info(f"💵 Premium: ₹{CFG.MIN_PREMIUM}-{CFG.MAX_PREMIUM}")
        logger.info(f"⏰ Start: {CFG.BOT_START} | Trade: {CFG.TRADING_START}")
        logger.info(f"⚡ Opening: {CFG.OPENING_SCAN_INTERVAL}s | Normal: {CFG.NORMAL_SCAN_INTERVAL}s")
        
        if not UPSTOX_ACCESS_TOKEN:
            logger.error("❌ UPSTOX_ACCESS_TOKEN not set!")
            return
        
        await self.feed.initialize()
        
        startup = f"""
🚀 NIFTY v2.2 ONLINE

💰 ₹{CFG.DEMO_CAPITAL} | Lot {CFG.LOT_SIZE}
📊 31 strikes fetch | 7 analysis
💵 Premium ₹{CFG.MIN_PREMIUM}-{CFG.MAX_PREMIUM}
⚡ Opening 5s | Normal 30s

⏰ {datetime.now(IST).strftime('%I:%M %p')}
"""
        await self.telegram.send(startup)
        
        self.running = True
        
        while self.running:
            try:
                now = datetime.now(IST).time()
                phase = self.feed.get_market_phase()
                
                if phase != MarketPhase.CLOSED:
                    current_phase = await self.scan()
                    await self.check_summary()
                    interval = self.get_scan_interval(current_phase)
                    await asyncio.sleep(interval)
                else:
                    # Reset at day start
                    if now < CFG.BOT_START:
                        self.account.reset_daily()
                        self.pos_mgr.closed = []
                        self.feed.day_open = 0
                        self.feed.day_high = 0
                        self.feed.day_low = 999999
                        self.memory.yesterday_oi = {}
                        self.memory.warmup_done = False
                    
                    logger.info("🌙 Market closed")
                    await asyncio.sleep(60)
                    
            except KeyboardInterrupt:
                self.running = False
            except Exception as e:
                logger.error(f"💥 Error: {e}")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(10)
        
        logger.info("👋 Bot stopped")

# ==================== MAIN ====================
async def main():
    bot = NiftyBot()
    await bot.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
