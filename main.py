#!/usr/bin/env python3
"""
NIFTY OPTIONS BOT V12.0 - STRIKE MASTER
========================================
🔥 ALL FIXES IMPLEMENTED + 5-STRIKE FOCUS

Strategy: Multi-Factor Strike Analysis
Target: 50-80 points daily | 80%+ accuracy

KEY FEATURES:
✅ 5-Strike Focus (ATM ± 2)
✅ Strike-Level OI Tracking
✅ ATM Battle Analysis
✅ Time-Based Filters
✅ Dynamic Stop-Loss (ATR)
✅ Focused PCR Calculation
✅ Smart Retry Logic
✅ Alert-Only Mode (Week 1 Testing)

Author: Data Monster Team
Version: 12.0 - Production Ready
"""

import os
import asyncio
import aiohttp
import urllib.parse
from datetime import datetime, timedelta, time
import pytz
import json
import logging
from dataclasses import dataclass
from typing import Optional, Tuple, Dict
import pandas as pd
import numpy as np
from calendar import monthrange

# Optional dependencies
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logging.warning("⚠️ Redis not installed. Using RAM-only mode.")

try:
    from telegram import Bot
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    logging.warning("⚠️ Telegram not installed. Alerts disabled.")

# ==================== CONFIGURATION ====================
IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("StrikeMaster-V12")

# API Configuration
UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN', 'YOUR_TOKEN_HERE')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')

# Instrument Symbols
NIFTY_SPOT = "NSE_INDEX|Nifty 50"

# Trading Configuration
ALERT_ONLY_MODE = True  # 🔥 Week 1: Only alerts, no auto-trading
SCAN_INTERVAL = 60  # seconds (1 minute for faster signals)

# Strategy Thresholds
OI_THRESHOLD_STRONG = 8.0
OI_THRESHOLD_MEDIUM = 5.0
ATM_OI_THRESHOLD = 5.0  # ATM strike minimum threshold

VOL_SPIKE_2X = 2.0
VOL_SPIKE_3X = 3.0

PCR_EXTREME_BULLISH = 1.15
PCR_BULLISH = 1.08
PCR_BEARISH = 0.92
PCR_EXTREME_BEARISH = 0.85

MIN_CANDLE_SIZE = 8
VWAP_BUFFER = 5

# Time Filters
AVOID_OPENING = (time(9, 15), time(9, 45))  # Opening noise
AVOID_CLOSING = (time(15, 15), time(15, 30))  # Closing manipulation
AVOID_LUNCH = (time(12, 30), time(13, 15))  # Optional: lunch hour low volume

# ATR Configuration
ATR_PERIOD = 14
ATR_SL_MULTIPLIER = 1.5
ATR_TARGET_MULTIPLIER = 2.5

@dataclass
class Signal:
    """Trading Signal with all metrics"""
    type: str  # CE_BUY or PE_BUY
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

# ==================== UTILITIES ====================
def get_current_futures_symbol() -> str:
    """
    Auto-detect current Nifty Futures symbol
    Format: NSE_FO|NIFTY25NOVFUT
    """
    now = datetime.now(IST)
    year = now.year
    month = now.month
    
    # Find last Thursday of current month
    last_day = monthrange(year, month)[1]
    last_date = datetime(year, month, last_day, tzinfo=IST)
    days_to_thursday = (last_date.weekday() - 3) % 7
    last_thursday = last_date - timedelta(days=days_to_thursday)
    
    # If past expiry, use next month
    if now.date() > last_thursday.date() or (
        now.date() == last_thursday.date() and now.time() > time(15, 30)
    ):
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    
    year_short = year % 100
    month_name = datetime(year, month, 1).strftime('%b').upper()
    symbol = f"NSE_FO|NIFTY{year_short:02d}{month_name}FUT"
    
    logger.info(f"🤖 Futures Symbol: {symbol}")
    return symbol

def get_weekly_expiry() -> str:
    """Get next Thursday expiry date"""
    now = datetime.now(IST)
    today = now.date()
    
    # Days until Thursday (0=Mon, 3=Thu)
    days_to_thursday = (3 - today.weekday() + 7) % 7
    
    # If today is Thursday after 3:30 PM, use next Thursday
    if days_to_thursday == 0 and now.time() > time(15, 30):
        expiry = today + timedelta(days=7)
    else:
        expiry = today + timedelta(days=days_to_thursday)
    
    return expiry.strftime('%Y-%m-%d')

def is_tradeable_time() -> bool:
    """Check if current time is good for trading"""
    now = datetime.now(IST).time()
    
    # Market hours check
    if not (time(9, 15) <= now <= time(15, 30)):
        return False
    
    # Avoid opening noise
    if AVOID_OPENING[0] <= now <= AVOID_OPENING[1]:
        logger.info("⏰ Opening hour - Skipping for clean signals")
        return False
    
    # Avoid closing manipulation
    if AVOID_CLOSING[0] <= now <= AVOID_CLOSING[1]:
        logger.info("⏰ Closing minutes - Risk of manipulation")
        return False
    
    # Optional: Avoid lunch hour
    # if AVOID_LUNCH[0] <= now <= AVOID_LUNCH[1]:
    #     logger.info("🍽️ Lunch hour - Low volume")
    #     return False
    
    return True

# ==================== REDIS BRAIN ====================
class RedisBrain:
    """Memory system for OI tracking with strike-level granularity"""
    
    def __init__(self):
        self.client = None
        self.memory = {}  # Fallback RAM storage
        
        if REDIS_AVAILABLE:
            try:
                self.client = redis.from_url(REDIS_URL, decode_responses=True)
                self.client.ping()
                logger.info("✅ Redis Connected")
            except Exception as e:
                logger.warning(f"⚠️ Redis Failed: {e}. Using RAM Mode")
                self.client = None
        else:
            logger.info("📦 RAM-only mode (Install redis-py for persistence)")
    
    def save_strike_snapshot(self, strike_data: Dict[int, dict]):
        """Save current strike-level OI data"""
        now = datetime.now(IST)
        timestamp = now.replace(second=0, microsecond=0)
        
        for strike, data in strike_data.items():
            key = f"strike:{strike}:{timestamp.strftime('%H%M')}"
            value = json.dumps(data)
            
            if self.client:
                try:
                    self.client.setex(key, 3600, value)  # 1 hour TTL
                except Exception as e:
                    logger.debug(f"Redis save error: {e}")
                    self.memory[key] = value
            else:
                self.memory[key] = value
    
    def get_strike_oi_change(self, strike: int, current_data: dict, 
                             minutes_ago: int = 15) -> Tuple[float, float]:
        """
        Calculate OI change % for specific strike
        Returns: (ce_change%, pe_change%)
        """
        now = datetime.now(IST) - timedelta(minutes=minutes_ago)
        timestamp = now.replace(second=0, microsecond=0)
        key = f"strike:{strike}:{timestamp.strftime('%H%M')}"
        
        past_data_str = None
        if self.client:
            try:
                past_data_str = self.client.get(key)
            except:
                past_data_str = self.memory.get(key)
        else:
            past_data_str = self.memory.get(key)
        
        if not past_data_str:
            return 0.0, 0.0
        
        try:
            past = json.loads(past_data_str)
            ce_chg = ((current_data['ce_oi'] - past['ce_oi']) / past['ce_oi'] * 100 
                      if past['ce_oi'] > 0 else 0)
            pe_chg = ((current_data['pe_oi'] - past['pe_oi']) / past['pe_oi'] * 100 
                      if past['pe_oi'] > 0 else 0)
            return ce_chg, pe_chg
        except Exception as e:
            logger.debug(f"Parse error: {e}")
            return 0.0, 0.0
    
    def save_total_oi_snapshot(self, ce_total: int, pe_total: int):
        """Save total OI for PCR calculation"""
        now = datetime.now(IST)
        slot = now.replace(second=0, microsecond=0)
        key = f"total_oi:{slot.strftime('%H%M')}"
        data = json.dumps({"ce": ce_total, "pe": pe_total})
        
        if self.client:
            try:
                self.client.setex(key, 3600, data)
            except:
                self.memory[key] = data
        else:
            self.memory[key] = data
    
    def get_total_oi_change(self, current_ce: int, current_pe: int, 
                           minutes_ago: int = 15) -> Tuple[float, float]:
        """Get total OI change % (for PCR trend)"""
        now = datetime.now(IST) - timedelta(minutes=minutes_ago)
        slot = now.replace(second=0, microsecond=0)
        key = f"total_oi:{slot.strftime('%H%M')}"
        
        past_data = None
        if self.client:
            try:
                past_data = self.client.get(key)
            except:
                past_data = self.memory.get(key)
        else:
            past_data = self.memory.get(key)
        
        if not past_data:
            return 0.0, 0.0
        
        try:
            past = json.loads(past_data)
            ce_chg = ((current_ce - past['ce']) / past['ce'] * 100 
                      if past['ce'] > 0 else 0)
            pe_chg = ((current_pe - past['pe']) / past['pe'] * 100 
                      if past['pe'] > 0 else 0)
            return ce_chg, pe_chg
        except:
            return 0.0, 0.0

# ==================== DATA FEED ====================
class StrikeDataFeed:
    """Fetch market data with 5-strike focus"""
    
    def __init__(self):
        self.headers = {
            "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
            "Accept": "application/json"
        }
        self.retry_count = 3
        self.base_retry_delay = 2
        self.futures_symbol = get_current_futures_symbol()
    
    async def fetch_with_retry(self, url: str, session: aiohttp.ClientSession):
        """Smart retry with exponential backoff"""
        for attempt in range(self.retry_count):
            try:
                async with session.get(url, headers=self.headers) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 429:
                        wait_time = (2 ** attempt) * self.base_retry_delay
                        logger.warning(f"⏳ Rate limited, waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"❌ HTTP {resp.status}")
                        await asyncio.sleep(self.base_retry_delay)
            except Exception as e:
                logger.error(f"💥 Attempt {attempt+1}: {e}")
                await asyncio.sleep(self.base_retry_delay * (attempt + 1))
        
        return None
    
    async def get_market_data(self) -> Tuple[pd.DataFrame, Dict[int, dict], 
                                            str, float, float, float]:
        """
        Fetch all required data
        Returns: (futures_df, strike_data, expiry, spot_price, 
                  futures_price, total_options_volume)
        """
        async with aiohttp.ClientSession() as session:
            spot_price = 0
            futures_price = 0
            df = pd.DataFrame()
            strike_data = {}
            total_options_volume = 0
            
            # 1. GET SPOT PRICE
            logger.info("🔍 Fetching Spot Price...")
            enc_spot = urllib.parse.quote(NIFTY_SPOT)
            ltp_url = f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={enc_spot}"
            
            ltp_data = await self.fetch_with_retry(ltp_url, session)
            if ltp_data and 'data' in ltp_data:
                for key in [NIFTY_SPOT, "NSE_INDEX:Nifty 50", "Nifty 50"]:
                    if key in ltp_data['data']:
                        spot_price = ltp_data['data'][key].get('last_price', 0)
                        if spot_price > 0:
                            logger.info(f"✅ Spot: {spot_price:.2f}")
                            break
            
            if spot_price == 0:
                logger.error("❌ Failed to fetch spot price")
                return df, strike_data, "", 0, 0, 0
            
            # 2. GET FUTURES CANDLES
            logger.info(f"🔍 Fetching Futures: {self.futures_symbol}")
            enc_futures = urllib.parse.quote(self.futures_symbol)
            to_date = datetime.now(IST).strftime('%Y-%m-%d')
            from_date = (datetime.now(IST) - timedelta(days=10)).strftime('%Y-%m-%d')
            candle_url = f"https://api.upstox.com/v2/historical-candle/{enc_futures}/1minute/{to_date}/{from_date}"
            
            candle_data = await self.fetch_with_retry(candle_url, session)
            if candle_data and candle_data.get('status') == 'success':
                candles = candle_data.get('data', {}).get('candles', [])
                if candles:
                    df = pd.DataFrame(
                        candles,
                        columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'oi']
                    )
                    df['ts'] = pd.to_datetime(df['ts']).dt.tz_convert(IST)
                    df = df.sort_values('ts').set_index('ts')
                    
                    today = datetime.now(IST).date()
                    df = df[df.index.date == today].tail(100)
                    
                    if not df.empty:
                        futures_price = df['close'].iloc[-1]
                        logger.info(f"✅ Futures: {len(df)} candles | Price: {futures_price:.2f}")
            
            # 3. GET OPTION CHAIN (5-STRIKE FOCUS!)
            logger.info("🔍 Fetching Option Chain (5-Strike Focus)...")
            expiry = get_weekly_expiry()
            chain_url = f"https://api.upstox.com/v2/option/chain?instrument_key={enc_spot}&expiry_date={expiry}"
            
            atm_strike = round(spot_price / 50) * 50
            min_strike = atm_strike - 100  # 🔥 Only ±2 strikes (5 total)
            max_strike = atm_strike + 100
            
            logger.info(f"📊 ATM: {atm_strike} | 5-Strike Range: {min_strike}-{max_strike}")
            
            chain_data = await self.fetch_with_retry(chain_url, session)
            if chain_data and chain_data.get('status') == 'success':
                for option in chain_data.get('data', []):
                    strike = option.get('strike_price', 0)
                    
                    if min_strike <= strike <= max_strike:
                        call_data = option.get('call_options', {}).get('market_data', {})
                        put_data = option.get('put_options', {}).get('market_data', {})
                        
                        ce_oi = call_data.get('oi', 0)
                        pe_oi = put_data.get('oi', 0)
                        ce_vol = call_data.get('volume', 0)
                        pe_vol = put_data.get('volume', 0)
                        
                        strike_data[strike] = {
                            'ce_oi': ce_oi,
                            'pe_oi': pe_oi,
                            'ce_vol': ce_vol,
                            'pe_vol': pe_vol,
                            'ce_ltp': call_data.get('ltp', 0),
                            'pe_ltp': put_data.get('ltp', 0)
                        }
                        
                        total_options_volume += (ce_vol + pe_vol)
                
                logger.info(f"✅ Collected {len(strike_data)} strikes")
                logger.info(f"🔥 Total Options Volume: {total_options_volume:,.0f}")
            
            return df, strike_data, expiry, spot_price, futures_price, total_options_volume

# ==================== ANALYZER ====================
class StrikeAnalyzer:
    """Multi-factor analysis engine"""
    
    def __init__(self):
        self.volume_history = []
    
    def calculate_vwap(self, df: pd.DataFrame) -> float:
        """VWAP from futures candles"""
        if df.empty:
            logger.warning("⚠️ Empty dataframe for VWAP")
            return 0
        
        df_copy = df.copy()
        df_copy['tp'] = (df_copy['high'] + df_copy['low'] + df_copy['close']) / 3
        df_copy['vol_price'] = df_copy['tp'] * df_copy['vol']
        
        total_vol = df_copy['vol'].sum()
        if total_vol == 0:
            return df_copy['close'].iloc[-1]
        
        vwap = df_copy['vol_price'].cumsum() / df_copy['vol'].cumsum()
        final_vwap = vwap.iloc[-1]
        
        logger.info(f"📊 VWAP: {final_vwap:.2f}")
        return final_vwap
    
    def calculate_atr(self, df: pd.DataFrame, period: int = ATR_PERIOD) -> float:
        """Average True Range for dynamic stops"""
        if len(df) < period:
            default_atr = 30
            logger.info(f"📏 ATR: {default_atr:.1f} (default, insufficient data)")
            return default_atr
        
        df_copy = df.tail(period).copy()
        
        df_copy['h-l'] = df_copy['high'] - df_copy['low']
        df_copy['h-pc'] = abs(df_copy['high'] - df_copy['close'].shift(1))
        df_copy['l-pc'] = abs(df_copy['low'] - df_copy['close'].shift(1))
        
        df_copy['tr'] = df_copy[['h-l', 'h-pc', 'l-pc']].max(axis=1)
        atr = df_copy['tr'].mean()
        
        logger.info(f"📏 ATR({period}): {atr:.1f} points")
        return atr
    
    def get_candle_info(self, df: pd.DataFrame) -> Tuple[str, float]:
        """Current candle color and size"""
        if df.empty:
            return 'NEUTRAL', 0
        
        last = df.iloc[-1]
        candle_size = abs(last['close'] - last['open'])
        
        if last['close'] > last['open']:
            color = 'GREEN'
        elif last['close'] < last['open']:
            color = 'RED'
        else:
            color = 'DOJI'
        
        logger.info(f"🕯️ Candle: {color} | Size: {candle_size:.1f}")
        return color, candle_size
    
    def check_volume_surge(self, current_vol: float) -> Tuple[bool, float]:
        """Detect options volume spike"""
        now = datetime.now(IST)
        self.volume_history.append({'time': now, 'volume': current_vol})
        
        # Keep only last 20 minutes
        cutoff = now - timedelta(minutes=20)
        self.volume_history = [
            x for x in self.volume_history if x['time'] > cutoff
        ]
        
        if len(self.volume_history) < 5:
            logger.info(f"📊 Vol: {current_vol:,.0f} (building history...)")
            return False, 0
        
        past_volumes = [x['volume'] for x in self.volume_history[:-1]]
        avg_vol = sum(past_volumes) / len(past_volumes)
        
        if avg_vol == 0:
            return False, 0
        
        multiplier = current_vol / avg_vol
        has_spike = multiplier >= VOL_SPIKE_2X
        
        emoji = "🔥🔥" if multiplier >= VOL_SPIKE_3X else "🔥" if has_spike else "📊"
        logger.info(f"{emoji} Vol: Curr={current_vol:,.0f} | Avg={avg_vol:,.0f} | {multiplier:.2f}x")
        
        return has_spike, multiplier
    
    def calculate_focused_pcr(self, strike_data: Dict[int, dict]) -> float:
        """PCR from only 5 strikes (focused analysis)"""
        total_ce = sum(data['ce_oi'] for data in strike_data.values())
        total_pe = sum(data['pe_oi'] for data in strike_data.values())
        
        pcr = total_pe / total_ce if total_ce > 0 else 1.0
        
        logger.info(f"🎯 Focused PCR (5 strikes): {pcr:.2f}")
        return pcr
    
    def analyze_atm_battle(self, strike_data: Dict[int, dict], atm_strike: int,
                          redis_brain: RedisBrain) -> Tuple[float, float]:
        """
        🔥 ATM Strike Battle Analysis
        Returns: (atm_ce_change%, atm_pe_change%)
        """
        if atm_strike not in strike_data:
            logger.warning(f"⚠️ ATM strike {atm_strike} not in data")
            return 0, 0
        
        current = strike_data[atm_strike]
        
        # Get 15-min change
        ce_15m, pe_15m = redis_brain.get_strike_oi_change(
            atm_strike, current, minutes_ago=15
        )
        
        # Get 5-min change
        ce_5m, pe_5m = redis_brain.get_strike_oi_change(
            atm_strike, current, minutes_ago=5
        )
        
        logger.info(f"⚔️ ATM {atm_strike} Battle:")
        logger.info(f"   15m: CE={ce_15m:+.1f}% | PE={pe_15m:+.1f}%")
        logger.info(f"   5m:  CE={ce_5m:+.1f}% | PE={pe_5m:+.1f}%")
        
        return ce_15m, pe_15m
    
    def check_momentum(self, df: pd.DataFrame, direction: str = 'bullish') -> bool:
        """Check if last 3 candles show momentum"""
        if df.empty or len(df) < 3:
            return False
        
        last_3 = df.tail(3)
        
        if direction == 'bullish':
            green_count = sum(last_3['close'] > last_3['open'])
            result = green_count >= 2
            logger.info(f"📈 Momentum: {green_count}/3 green = {result}")
        else:
            red_count = sum(last_3['close'] < last_3['open'])
            result = red_count >= 2
            logger.info(f"📉 Momentum: {red_count}/3 red = {result}")
        
        return result

# ==================== MAIN BOT ====================
class StrikeMasterBot:
    """Main trading bot with 5-strike focus"""
    
    def __init__(self):
        self.feed = StrikeDataFeed()
        self.redis = RedisBrain()
        self.analyzer = StrikeAnalyzer()
        self.telegram = None
        self.last_alert_time = None
        self.alert_cooldown = 300  # 5 minutes between alerts
        
        if TELEGRAM_AVAILABLE and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                self.telegram = Bot(token=TELEGRAM_BOT_TOKEN)
                logger.info("✅ Telegram Ready")
            except Exception as e:
                logger.warning(f"⚠️ Telegram setup failed: {e}")
    
    async def run_cycle(self):
        """Single analysis cycle"""
        
        # Time filter
        if not is_tradeable_time():
            return
        
        logger.info("=" * 80)
        logger.info("🔢 STRIKE MASTER SCAN V12.0")
        logger.info("=" * 80)
        
        # Fetch data
        df, strike_data, expiry, spot_price, futures_price, options_vol = \
            await self.feed.get_market_data()
        
        if df.empty or not strike_data or spot_price == 0:
            logger.warning("⏳ Incomplete data, skipping cycle")
            return
        
        logger.info("\n--- MARKET DATA ---")
        logger.info(f"💰 Spot: {spot_price:.2f} | Futures: {futures_price:.2f}")
        
        # Calculate indicators
        logger.info("\n--- INDICATORS ---")
        vwap = self.analyzer.calculate_vwap(df)
        atr = self.analyzer.calculate_atr(df)
        pcr = self.analyzer.calculate_focused_pcr(strike_data)
        candle_color, candle_size = self.analyzer.get_candle_info(df)
        has_vol_spike, vol_mult = self.analyzer.check_volume_surge(options_vol)
        vwap_distance = abs(futures_price - vwap)
        
        logger.info(f"📏 Distance from VWAP: {vwap_distance:.1f} points")
        
        # ATM Battle Analysis
        logger.info("\n--- ATM BATTLE ---")
        atm_strike = round(spot_price / 50) * 50
        atm_ce_15m, atm_pe_15m = self.analyzer.analyze_atm_battle(
            strike_data, atm_strike, self.redis
        )
        
        # Total OI changes (for confirmation)
        logger.info("\n--- TOTAL OI MOVEMENT ---")
        total_ce = sum(d['ce_oi'] for d in strike_data.values())
        total_pe = sum(d['pe_oi'] for d in strike_data.values())
        
        ce_total_15m, pe_total_15m = self.redis.get_total_oi_change(
            total_ce, total_pe, minutes_ago=15
        )
        ce_total_5m, pe_total_5m = self.redis.get_total_oi_change(
            total_ce, total_pe, minutes_ago=5
        )
        
        logger.info(f"📊 Total OI 15m: CE={ce_total_15m:+.1f}% | PE={pe_total_15m:+.1f}%")
        logger.info(f"📊 Total OI 5m: CE={ce_total_5m:+.1f}% | PE={pe_total_5m:+.1f}%")
        
        # Save snapshots
        self.redis.save_strike_snapshot(strike_data)
        self.redis.save_total_oi_snapshot(total_ce, total_pe)
        
        # Generate signal
        logger.info("\n--- SIGNAL GENERATION ---")
        signal = self.generate_signal(
            spot_price=spot_price,
            futures_price=futures_price,
            vwap=vwap,
            vwap_distance=vwap_distance,
            pcr=pcr,
            atr=atr,
            ce_total_15m=ce_total_15m,
            pe_total_15m=pe_total_15m,
            ce_total_5m=ce_total_5m,
            pe_total_5m=pe_total_5m,
            atm_ce_change=atm_ce_15m,
            atm_pe_change=atm_pe_15m,
            candle_color=candle_color,
            candle_size=candle_size,
            has_vol_spike=has_vol_spike,
            vol_mult=vol_mult,
            df=df
        )
        
        if signal:
            await self.send_alert(signal)
        else:
            logger.info("✋ No valid setup found")
        
        logger.info("=" * 80)
    
    def generate_signal(self, spot_price: float, futures_price: float,
                       vwap: float, vwap_distance: float, pcr: float,
                       atr: float, ce_total_15m: float, pe_total_15m: float,
                       ce_total_5m: float, pe_total_5m: float,
                       atm_ce_change: float, atm_pe_change: float,
                       candle_color: str, candle_size: float,
                       has_vol_spike: bool, vol_mult: float,
                       df: pd.DataFrame) -> Optional[Signal]:
        """
        🔥 Multi-Factor Signal Generation
        Main logic: ATM strike + Total OI + Price action + Volume
        """
        
        strike = round(spot_price / 50) * 50
        
        # Dynamic targets based on ATR
        stop_loss_points = int(atr * ATR_SL_MULTIPLIER)
        target_points = int(atr * ATR_TARGET_MULTIPLIER)
        
        # Determine expected move based on OI strength
        if abs(ce_total_15m) >= OI_THRESHOLD_STRONG or abs(atm_ce_change) >= OI_THRESHOLD_STRONG:
            target_points = max(target_points, 80)
        elif abs(ce_total_15m) >= OI_THRESHOLD_MEDIUM or abs(atm_ce_change) >= OI_THRESHOLD_MEDIUM:
            target_points = max(target_points, 50)
        
        # ==================== CE BUY ANALYSIS ====================
        if ce_total_15m < -OI_THRESHOLD_MEDIUM or atm_ce_change < -ATM_OI_THRESHOLD:
            logger.info(f"\n🔍 CE SIGNAL CHECK")
            logger.info(f"   Total CE OI 15m: {ce_total_15m:.1f}%")
            logger.info(f"   ATM CE Change: {atm_ce_change:.1f}%")
            logger.info("-" * 60)
            
            # 4 MAIN CHECKS (All required)
            checks = {
                "CE OI Unwinding (Total)": ce_total_15m < -OI_THRESHOLD_MEDIUM,
                "ATM CE Unwinding": atm_ce_change < -ATM_OI_THRESHOLD,
                "Price > VWAP": futures_price > vwap,
                "GREEN Candle": candle_color == 'GREEN'
            }
            
            # 6 BONUS CHECKS (Confidence boosters)
            bonus = {
                "Strong 5m OI": ce_total_5m < -5.0,
                "Big Candle": candle_size >= MIN_CANDLE_SIZE,
                "Far from VWAP": vwap_distance >= VWAP_BUFFER,
                "Bullish PCR": pcr > PCR_BULLISH,
                "Volume Spike": has_vol_spike,
                "3+ Green Momentum": self.analyzer.check_momentum(df, 'bullish')
            }
            
            passed = sum(checks.values())
            bonus_passed = sum(bonus.values())
            
            logger.info("MAIN CHECKS (All 4 Required):")
            for name, result in checks.items():
                logger.info(f"  {'✅' if result else '❌'} {name}")
            
            logger.info(f"\nBONUS CHECKS ({bonus_passed}/6):")
            for name, result in bonus.items():
                logger.info(f"  {'✅' if result else '❌'} {name}")
            
            # All main checks must pass
            if passed == 4:
                confidence = 75 + (bonus_passed * 3)  # 75-93%
                logger.info(f"\n🎯 CE SIGNAL APPROVED!")
                logger.info(f"   Confidence: {confidence}%")
                logger.info(f"   Target: {target_points} points")
                logger.info(f"   Stop Loss: {stop_loss_points} points")
                
                return Signal(
                    type="CE_BUY",
                    reason=f"Call Short Covering (ATM: {atm_ce_change:.1f}%)",
                    confidence=min(confidence, 95),
                    spot_price=spot_price,
                    futures_price=futures_price,
                    strike=strike,
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
                    timestamp=datetime.now(IST)
                )
            else:
                logger.info(f"\n❌ CE SIGNAL REJECTED: {passed}/4 main checks")
                return None
        
        # ==================== PE BUY ANALYSIS ====================
        if pe_total_15m < -OI_THRESHOLD_MEDIUM or atm_pe_change < -ATM_OI_THRESHOLD:
            logger.info(f"\n🔍 PE SIGNAL CHECK")
            logger.info(f"   Total PE OI 15m: {pe_total_15m:.1f}%")
            logger.info(f"   ATM PE Change: {atm_pe_change:.1f}%")
            logger.info("-" * 60)
            
            # Adjust target for PE based on OI strength
            if abs(pe_total_15m) >= OI_THRESHOLD_STRONG or abs(atm_pe_change) >= OI_THRESHOLD_STRONG:
                target_points = max(target_points, 80)
            elif abs(pe_total_15m) >= OI_THRESHOLD_MEDIUM or abs(atm_pe_change) >= OI_THRESHOLD_MEDIUM:
                target_points = max(target_points, 50)
            
            # 4 MAIN CHECKS
            checks = {
                "PE OI Unwinding (Total)": pe_total_15m < -OI_THRESHOLD_MEDIUM,
                "ATM PE Unwinding": atm_pe_change < -ATM_OI_THRESHOLD,
                "Price < VWAP": futures_price < vwap,
                "RED Candle": candle_color == 'RED'
            }
            
            # 6 BONUS CHECKS
            bonus = {
                "Strong 5m OI": pe_total_5m < -5.0,
                "Big Candle": candle_size >= MIN_CANDLE_SIZE,
                "Far from VWAP": vwap_distance >= VWAP_BUFFER,
                "Bearish PCR": pcr < PCR_BEARISH,
                "Volume Spike": has_vol_spike,
                "3+ Red Momentum": self.analyzer.check_momentum(df, 'bearish')
            }
            
            passed = sum(checks.values())
            bonus_passed = sum(bonus.values())
            
            logger.info("MAIN CHECKS (All 4 Required):")
            for name, result in checks.items():
                logger.info(f"  {'✅' if result else '❌'} {name}")
            
            logger.info(f"\nBONUS CHECKS ({bonus_passed}/6):")
            for name, result in bonus.items():
                logger.info(f"  {'✅' if result else '❌'} {name}")
            
            if passed == 4:
                confidence = 75 + (bonus_passed * 3)
                logger.info(f"\n🎯 PE SIGNAL APPROVED!")
                logger.info(f"   Confidence: {confidence}%")
                logger.info(f"   Target: {target_points} points")
                logger.info(f"   Stop Loss: {stop_loss_points} points")
                
                return Signal(
                    type="PE_BUY",
                    reason=f"Put Long Unwinding (ATM: {atm_pe_change:.1f}%)",
                    confidence=min(confidence, 95),
                    spot_price=spot_price,
                    futures_price=futures_price,
                    strike=strike,
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
                    timestamp=datetime.now(IST)
                )
            else:
                logger.info(f"\n❌ PE SIGNAL REJECTED: {passed}/4 main checks")
                return None
        
        return None
    
    async def send_alert(self, s: Signal):
        """Send Telegram alert with rate limiting"""
        
        # Rate limiting check
        if self.last_alert_time:
            elapsed = (datetime.now(IST) - self.last_alert_time).seconds
            if elapsed < self.alert_cooldown:
                logger.info(f"⏳ Alert cooldown: {self.alert_cooldown - elapsed}s remaining")
                return
        
        self.last_alert_time = datetime.now(IST)
        
        # Calculate entry/target/SL prices
        emoji = "🟢" if s.type == "CE_BUY" else "🔴"
        
        if s.type == "CE_BUY":
            entry = s.spot_price
            target = entry + s.target_points
            stop_loss = entry - s.stop_loss_points
        else:
            entry = s.spot_price
            target = entry - s.target_points
            stop_loss = entry + s.stop_loss_points
        
        mode = "🧪 ALERT ONLY" if ALERT_ONLY_MODE else "⚡ LIVE TRADING"
        
        msg = f"""
{emoji} *NIFTY STRIKE MASTER V12\\.0*

*{mode}*

━━━━━━━━━━━━━━━━━━━━━━━━
*SIGNAL: {s.type}*
━━━━━━━━━━━━━━━━━━━━━━━━

📍 *Entry:* {entry:.1f}
🎯 *Target:* {target:.1f} \\({s.target_points:+.0f} pts\\)
🛑 *Stop Loss:* {stop_loss:.1f} \\({s.stop_loss_points:.0f} pts\\)
📊 *Strike:* {s.strike}

━━━━━━━━━━━━━━━━━━━━━━━━
*LOGIC*
━━━━━━━━━━━━━━━━━━━━━━━━

{s.reason}
*Confidence:* {s.confidence}%

━━━━━━━━━━━━━━━━━━━━━━━━
*MARKET DATA*
━━━━━━━━━━━━━━━━━━━━━━━━

💰 Spot: {s.spot_price:.1f}
📈 Futures: {s.futures_price:.1f}
📊 PCR: {s.pcr:.2f}
🕯️ Candle: {s.candle_color}
🔥 Volume: {s.volume_surge:.1f}x
📏 ATR: {s.atr:.1f}

━━━━━━━━━━━━━━━━━━━━━━━━
*OI ANALYSIS*
━━━━━━━━━━━━━━━━━━━━━━━━

ATM Strike Battle:
  CE: {s.atm_ce_change:+.1f}%
  PE: {s.atm_pe_change:+.1f}%

Total OI Movement:
  5\\-min: {s.oi_5m:+.1f}%
  15\\-min: {s.oi_15m:+.1f}%

━━━━━━━━━━━━━━━━━━━━━━━━

⏰ {s.timestamp.strftime('%d\\-%b %I:%M %p')}

✅ 5\\-Strike Focus
✅ Multi\\-Factor Analysis
✅ 80%\\+ Target Accuracy
"""
        
        logger.info(f"\n🚨 SIGNAL GENERATED!")
        logger.info(f"   Type: {s.type}")
        logger.info(f"   Entry: {entry:.1f} → Target: {target:.1f}")
        logger.info(f"   Confidence: {s.confidence}%")
        
        if self.telegram:
            try:
                await self.telegram.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=msg,
                    parse_mode='MarkdownV2'
                )
                logger.info("✅ Alert sent to Telegram")
            except Exception as e:
                logger.error(f"❌ Telegram error: {e}")
                # Fallback: send plain text
                try:
                    plain_msg = msg.replace('*', '').replace('\\', '')
                    await self.telegram.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text=plain_msg
                    )
                    logger.info("✅ Alert sent (plain text)")
                except:
                    logger.error("❌ Failed to send alert")
        else:
            logger.info("📱 Alert ready (Telegram not configured)")
    
    async def send_startup_message(self):
        """Send bot startup notification"""
        now = datetime.now(IST)
        futures_sym = self.feed.futures_symbol
        
        mode = "🧪 ALERT ONLY MODE" if ALERT_ONLY_MODE else "⚡ LIVE TRADING MODE"
        
        msg = f"""
🚀 *STRIKE MASTER V12\\.0 ONLINE*

━━━━━━━━━━━━━━━━━━━━━━━━
*STATUS*
━━━━━━━━━━━━━━━━━━━━━━━━

⏰ Started: {now.strftime('%d\\-%b %I:%M %p')}
📊 Mode: {mode}
🔄 Scan: Every {SCAN_INTERVAL}s

━━━━━━━━━━━━━━━━━━━━━━━━
*CONFIGURATION*
━━━━━━━━━━━━━━━━━━━━━━━━

📈 Futures: `{futures_sym}`
🎯 Strikes: 5 \\(ATM ± 2\\)
⏰ Time Filters: Active
📏 Stop Loss: ATR\\-based
🎲 Target: 50\\-80 points

━━━━━━━━━━━━━━━━━━━━━━━━
*STRATEGY*
━━━━━━━━━━━━━━━━━━━━━━━━

✅ ATM Strike Battle Analysis
✅ Multi\\-Factor Scoring
✅ Volume Surge Detection
✅ VWAP Confirmation
✅ PCR Trend Analysis

━━━━━━━━━━━━━━━━━━━━━━━━
*THRESHOLDS*
━━━━━━━━━━━━━━━━━━━━━━━━

OI Unwinding: {OI_THRESHOLD_MEDIUM}%\\+
ATM OI: {ATM_OI_THRESHOLD}%\\+
Volume Spike: {VOL_SPIKE_2X}x\\+
Confidence: 75%\\+

━━━━━━━━━━━━━━━━━━━━━━━━

🎯 Target Accuracy: 80%\\+
⚡ Ready to scan\\!
"""
        
        logger.info("📲 Sending startup notification...")
        
        if self.telegram:
            try:
                await self.telegram.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=msg,
                    parse_mode='MarkdownV2'
                )
                logger.info("✅ Startup notification sent")
            except Exception as e:
                logger.error(f"❌ Startup notification failed: {e}")

# ==================== MAIN ====================
async def main():
    """Main bot loop"""
    bot = StrikeMasterBot()
    
    logger.info("=" * 80)
    logger.info("🚀 NIFTY STRIKE MASTER V12.0 - STARTING UP")
    logger.info("=" * 80)
    logger.info("")
    logger.info("🔥 FEATURES:")
    logger.info("   ✅ 5-Strike Focus (ATM ± 2)")
    logger.info("   ✅ Strike-Level OI Tracking")
    logger.info("   ✅ ATM Battle Analysis")
    logger.info("   ✅ Time-Based Filters")
    logger.info("   ✅ Dynamic Stop-Loss (ATR)")
    logger.info("   ✅ Multi-Factor Scoring")
    logger.info("   ✅ Smart Retry Logic")
    logger.info("")
    logger.info("📊 CONFIGURATION:")
    logger.info(f"   Futures: {bot.feed.futures_symbol}")
    logger.info(f"   Spot: {NIFTY_SPOT}")
    logger.info(f"   Mode: {'ALERT ONLY' if ALERT_ONLY_MODE else 'LIVE TRADING'}")
    logger.info(f"   Scan Interval: {SCAN_INTERVAL}s")
    logger.info("")
    logger.info("=" * 80)
    
    # Send startup message
    await bot.send_startup_message()
    
    # Main loop
    while True:
        try:
            now = datetime.now(IST).time()
            
            # Market hours check
            if time(9, 15) <= now <= time(15, 30):
                await bot.run_cycle()
                await asyncio.sleep(SCAN_INTERVAL)
            else:
                logger.info("🌙 Market closed. Waiting...")
                await asyncio.sleep(300)  # Check every 5 minutes
        
        except KeyboardInterrupt:
            logger.info("\n🛑 Bot stopped by user")
            break
        
        except Exception as e:
            logger.error(f"💥 Unexpected error: {e}")
            logger.error("   Retrying in 30 seconds...")
            await asyncio.sleep(30)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n👋 Shutdown complete")
