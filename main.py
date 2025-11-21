#!/usr/bin/env python3
"""
NIFTY50 DATA MONSTER V11.5 - REAL VOLUME FIX
=============================================
Strategy: "Follow the Smart Money (OI + Volume + VWAP)"

CRITICAL FIX IN V11.5:
🔥 Uses NIFTY FUTURES for Volume/VWAP (Real traded volume!)
✅ Enhanced logging (Shows all analysis steps)
✅ Volume spike now works correctly
✅ VWAP calculation accurate

Spot Index = No volume ❌
Nifty Futures = Real volume ✅

Target: 40-100 point moves | 75%+ accuracy
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
from typing import Optional, Tuple
import pandas as pd
import numpy as np

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logging.warning("Redis not installed. Using RAM-only mode.")

try:
    from telegram import Bot
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    logging.warning("python-telegram-bot not installed. Alerts disabled.")

# ==================== CONFIGURATION ====================
IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger("DataMonsterV11.5")

UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN', 'YOUR_TOKEN')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')

# --- INSTRUMENT SYMBOLS ---
NIFTY_SPOT = "NSE_INDEX|Nifty 50"  # For price only
NIFTY_FUTURES = "NSE_FO|NIFTY25DEC25FUT"  # For volume/VWAP (Current month)

# NOTE: Update NIFTY_FUTURES symbol monthly:
# November 2025 expiry: NIFTY25NOV25FUT
# December 2025 expiry: NIFTY25DEC25FUT
# January 2026 expiry: NIFTY26JAN26FUT

# --- STRATEGY CONSTANTS ---
OI_THRESHOLD_STRONG = 8.0
OI_THRESHOLD_MEDIUM = 5.0
OI_TEST_MODE = True
OI_TEST_THRESHOLD = 3.0

VOL_SPIKE_2X = 2.0
VOL_SPIKE_3X = 3.0

PCR_EXTREME_BULLISH = 1.15
PCR_BULLISH = 1.08
PCR_BEARISH = 0.92
PCR_EXTREME_BEARISH = 0.85

MIN_CANDLE_SIZE = 8
VWAP_BUFFER = 5

@dataclass
class Signal:
    type: str
    reason: str
    confidence: int
    price: float
    strike: int
    target_points: int
    pcr: float
    candle_color: str
    volume_surge: float
    oi_5m: float
    oi_15m: float

def escape_markdown_v2(text):
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

# ==================== REDIS BRAIN ====================
class RedisBrain:
    def __init__(self):
        self.client = None
        self.memory = {}
        
        if REDIS_AVAILABLE:
            try:
                self.client = redis.from_url(REDIS_URL, decode_responses=True)
                self.client.ping()
                logger.info("✅ Redis Connected")
            except Exception as e:
                logger.warning(f"⚠️ Redis Failed: Using RAM Mode")
                self.client = None
        else:
            logger.info("📦 RAM-only mode")

    def save_snapshot(self, ce_oi, pe_oi):
        now = datetime.now(IST)
        slot = now.replace(second=0, microsecond=0)
        key = f"oi:{slot.strftime('%H%M')}"
        data = json.dumps({"ce": ce_oi, "pe": pe_oi})
        
        if self.client:
            try:
                self.client.setex(key, 7200, data)
            except:
                self.memory[key] = data
        else:
            self.memory[key] = data

    def get_oi_delta(self, current_ce, current_pe, minutes_ago=15):
        now = datetime.now(IST) - timedelta(minutes=minutes_ago)
        slot = now.replace(second=0, microsecond=0)
        key = f"oi:{slot.strftime('%H%M')}"
        
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
            ce_chg = ((current_ce - past['ce']) / past['ce']) * 100 if past['ce'] > 0 else 0
            pe_chg = ((current_pe - past['pe']) / past['pe']) * 100 if past['pe'] > 0 else 0
            return ce_chg, pe_chg
        except:
            return 0.0, 0.0

# ==================== DATA FEED (ENHANCED) ====================
class DataFeed:
    def __init__(self):
        self.headers = {
            "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}", 
            "Accept": "application/json"
        }
        self.retry_count = 3
        self.retry_delay = 2

    async def get_market_data(self) -> Tuple[pd.DataFrame, int, int, str, float]:
        """
        🔥 V11.5: Gets FUTURES data for volume, SPOT for price
        """
        async with aiohttp.ClientSession() as session:
            enc_spot = urllib.parse.quote(NIFTY_SPOT)
            enc_futures = urllib.parse.quote(NIFTY_FUTURES)
            
            # SPOT LTP for accurate price
            ltp_url = f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={enc_spot}"
            
            # FUTURES candles for volume/VWAP
            to_date = datetime.now(IST).strftime('%Y-%m-%d')
            from_date = (datetime.now(IST) - timedelta(days=10)).strftime('%Y-%m-%d')
            candle_url = f"https://api.upstox.com/v2/historical-candle/{enc_futures}/1minute/{to_date}/{from_date}"
            
            expiry = self._get_weekly_expiry()
            chain_url = f"https://api.upstox.com/v2/option/chain?instrument_key={enc_spot}&expiry_date={expiry}"
            
            df = pd.DataFrame()
            total_ce, total_pe = 0, 0
            spot_price = 0
            
            try:
                # Get SPOT PRICE
                logger.info("🔍 Fetching Spot Price...")
                for attempt in range(self.retry_count):
                    try:
                        async with session.get(ltp_url, headers=self.headers) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                possible_keys = [
                                    NIFTY_SPOT,
                                    "NSE_INDEX:Nifty 50",
                                    "Nifty 50"
                                ]
                                
                                if 'data' in data:
                                    for key in possible_keys:
                                        if key in data['data']:
                                            spot_price = data['data'][key].get('last_price', 0)
                                            if spot_price > 0:
                                                logger.info(f"✅ Spot: {spot_price:.2f}")
                                                break
                                    
                                    if spot_price == 0 and data['data']:
                                        first_key = list(data['data'].keys())[0]
                                        spot_price = data['data'][first_key].get('last_price', 0)
                                
                                if spot_price > 0:
                                    break
                            elif resp.status == 429:
                                await asyncio.sleep(self.retry_delay * (attempt + 1))
                    except Exception as e:
                        logger.error(f"Spot fetch error: {e}")
                        await asyncio.sleep(self.retry_delay)
                
                # Get FUTURES CANDLES (With Real Volume!)
                logger.info("🔍 Fetching Futures Candles (for Volume/VWAP)...")
                for attempt in range(self.retry_count):
                    try:
                        async with session.get(candle_url, headers=self.headers) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                if data.get('status') == 'success' and 'data' in data:
                                    candles = data['data'].get('candles', [])
                                    if candles:
                                        df = pd.DataFrame(
                                            candles, 
                                            columns=['ts','open','high','low','close','vol','oi']
                                        )
                                        df['ts'] = pd.to_datetime(df['ts']).dt.tz_convert(IST)
                                        df = df.sort_values('ts').set_index('ts')
                                        
                                        today = datetime.now(IST).date()
                                        df = df[df.index.date == today].tail(100)
                                        
                                        # Log volume stats
                                        total_vol = df['vol'].sum()
                                        avg_vol = df['vol'].mean()
                                        last_vol = df['vol'].iloc[-1] if not df.empty else 0
                                        
                                        logger.info(f"✅ Futures Candles: {len(df)}")
                                        logger.info(f"📊 Volume Stats: Last={last_vol:,.0f} | Avg={avg_vol:,.0f} | Total={total_vol:,.0f}")
                                        break
                            elif resp.status == 429:
                                await asyncio.sleep(self.retry_delay * (attempt + 1))
                    except Exception as e:
                        logger.error(f"Futures candle error: {e}")
                        await asyncio.sleep(self.retry_delay)
                
                # Get Option Chain (OI data)
                logger.info("🔍 Fetching Option Chain (OI)...")
                atm_strike = round(spot_price / 50) * 50
                min_strike = atm_strike - 500
                max_strike = atm_strike + 500
                
                logger.info(f"📊 ATM: {atm_strike} | Range: {min_strike}-{max_strike}")
                
                for attempt in range(self.retry_count):
                    try:
                        async with session.get(chain_url, headers=self.headers) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                if data.get('status') == 'success' and 'data' in data:
                                    for option in data['data']:
                                        strike = option.get('strike_price', 0)
                                        if min_strike <= strike <= max_strike:
                                            ce_oi = option.get('call_options', {}).get('market_data', {}).get('oi', 0)
                                            pe_oi = option.get('put_options', {}).get('market_data', {}).get('oi', 0)
                                            total_ce += ce_oi
                                            total_pe += pe_oi
                                    
                                    pcr = total_pe / total_ce if total_ce > 0 else 1.0
                                    logger.info(f"✅ OI: CE={total_ce:,.0f} | PE={total_pe:,.0f} | PCR={pcr:.2f}")
                                    break
                            elif resp.status == 429:
                                await asyncio.sleep(self.retry_delay * (attempt + 1))
                    except:
                        await asyncio.sleep(self.retry_delay)
                
                return df, total_ce, total_pe, expiry, spot_price
                
            except Exception as e:
                logger.error(f"API Error: {e}")
                return pd.DataFrame(), 0, 0, expiry, 0

    def _get_weekly_expiry(self):
        now = datetime.now(IST)
        today = now.date()
        days_to_tuesday = (1 - today.weekday() + 7) % 7
        
        if days_to_tuesday == 0 and now.time() > time(15, 30):
            expiry = today + timedelta(days=7)
        else:
            expiry = today + timedelta(days=days_to_tuesday)
        
        return expiry.strftime('%Y-%m-%d')

# ==================== PURE ANALYZER (WITH LOGGING) ====================
class PureAnalyzer:
    
    @staticmethod
    def calculate_vwap(df):
        """VWAP from futures volume"""
        if df.empty:
            logger.warning("⚠️ Empty dataframe for VWAP")
            return 0
        
        df_copy = df.copy()
        df_copy['tp'] = (df_copy['high'] + df_copy['low'] + df_copy['close']) / 3
        df_copy['vol_price'] = df_copy['tp'] * df_copy['vol']
        
        total_vol = df_copy['vol'].sum()
        if total_vol == 0:
            logger.warning("⚠️ Zero volume - Cannot calculate VWAP")
            return df_copy['close'].iloc[-1]
        
        vwap = df_copy['vol_price'].cumsum() / df_copy['vol'].cumsum()
        final_vwap = vwap.iloc[-1]
        
        logger.info(f"📊 VWAP Calculated: {final_vwap:.2f}")
        return final_vwap

    @staticmethod
    def get_candle_info(df):
        """Current candle analysis"""
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
        
        logger.info(f"🕯️ Candle: {color} | Size: {candle_size:.1f} pts")
        return color, candle_size
    
    @staticmethod
    def check_volume_surge(df):
        """
        🔥 V11.5: Now works with REAL futures volume!
        """
        if df.empty or len(df) < 20:
            logger.warning("⚠️ Insufficient data for volume analysis")
            return False, 0
        
        current_vol = df['vol'].iloc[-1]
        avg_vol = df['vol'].iloc[-20:-1].mean()
        
        if avg_vol == 0:
            logger.warning("⚠️ Zero average volume")
            return False, 0
        
        multiplier = current_vol / avg_vol
        has_spike = multiplier >= VOL_SPIKE_2X
        
        surge_emoji = "🔥" if multiplier >= VOL_SPIKE_3X else "📈" if has_spike else "📊"
        logger.info(f"{surge_emoji} Volume: Current={current_vol:,.0f} | Avg={avg_vol:,.0f} | Ratio={multiplier:.2f}x")
        
        return has_spike, multiplier
    
    @staticmethod
    def get_price_distance_from_vwap(price, vwap):
        """Distance calculation"""
        distance = abs(price - vwap)
        direction = "above" if price > vwap else "below"
        logger.info(f"📏 Price {direction} VWAP by {distance:.1f} pts")
        return distance
    
    @staticmethod
    def check_momentum_candles(df, direction='bullish'):
        """Check last 3 candles"""
        if df.empty or len(df) < 3:
            return False
        
        last_3 = df.tail(3)
        
        if direction == 'bullish':
            green_count = sum(last_3['close'] > last_3['open'])
            result = green_count >= 2
            logger.info(f"📈 Bullish Momentum: {green_count}/3 green candles = {result}")
            return result
        else:
            red_count = sum(last_3['close'] < last_3['open'])
            result = red_count >= 2
            logger.info(f"📉 Bearish Momentum: {red_count}/3 red candles = {result}")
            return result

# ==================== MAIN BOT ====================
class DataMonsterBot:
    def __init__(self):
        self.feed = DataFeed()
        self.redis = RedisBrain()
        self.telegram = None
        self.last_alert_time = None
        
        if TELEGRAM_AVAILABLE and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                self.telegram = Bot(token=TELEGRAM_BOT_TOKEN)
                logger.info("✅ Telegram Ready")
            except Exception as e:
                logger.warning(f"Telegram failed: {e}")

    async def run_cycle(self):
        logger.info("=" * 80)
        logger.info("🔢 MARKET SCAN STARTED")
        logger.info("=" * 80)
        
        df, curr_ce, curr_pe, expiry, live_spot = await self.feed.get_market_data()
        
        if df.empty or curr_ce == 0 or curr_pe == 0 or live_spot == 0:
            logger.warning("⏳ Incomplete data - Waiting for next cycle...")
            return
        
        logger.info("\n--- CALCULATING INDICATORS ---")
        
        # Calculate indicators
        vwap = PureAnalyzer.calculate_vwap(df)
        pcr = curr_pe / curr_ce if curr_ce > 0 else 1.0
        candle_color, candle_size = PureAnalyzer.get_candle_info(df)
        has_volume_spike, vol_multiplier = PureAnalyzer.check_volume_surge(df)
        vwap_distance = PureAnalyzer.get_price_distance_from_vwap(live_spot, vwap)
        
        # OI Deltas
        logger.info("\n--- OI ANALYSIS ---")
        ce_5m, pe_5m = self.redis.get_oi_delta(curr_ce, curr_pe, 5)
        ce_15m, pe_15m = self.redis.get_oi_delta(curr_ce, curr_pe, 15)
        logger.info(f"📊 OI-5m: CE={ce_5m:+.1f}% | PE={pe_5m:+.1f}%")
        logger.info(f"📊 OI-15m: CE={ce_15m:+.1f}% | PE={pe_15m:+.1f}%")
        
        self.redis.save_snapshot(curr_ce, curr_pe)
        
        active_threshold = OI_TEST_THRESHOLD if OI_TEST_MODE else OI_THRESHOLD_MEDIUM
        mode = "🧪 TEST MODE" if OI_TEST_MODE else "📊 LIVE MODE"
        
        logger.info(f"\n--- MARKET SUMMARY ---")
        logger.info(f"📅 Expiry: {expiry} | {mode}")
        logger.info(f"💰 Spot: {live_spot:.1f} | VWAP: {vwap:.1f}")
        logger.info(f"📊 PCR: {pcr:.2f}")
        
        # Generate signal
        logger.info("\n--- SIGNAL GENERATION ---")
        signal = self.generate_pure_signal(
            live_spot, vwap, vwap_distance, pcr,
            ce_5m, pe_5m, ce_15m, pe_15m,
            active_threshold, candle_color, candle_size,
            has_volume_spike, vol_multiplier, df
        )
        
        if signal:
            await self.send_alert(signal)
        else:
            logger.info("✋ No signal - Waiting for setup...")
        
        logger.info("=" * 80)

    def generate_pure_signal(self, price, vwap, vwap_dist, pcr,
                            ce_5m, pe_5m, ce_15m, pe_15m,
                            threshold, candle_color, candle_size,
                            has_vol_spike, vol_mult, df):
        """Pure OI + Volume strategy with detailed logging"""
        
        strike = round(price/50)*50
        
        # CE_BUY Check
        if ce_15m < -threshold:
            logger.info(f"\n🔍 CE SIGNAL ANALYSIS (OI: {ce_15m:.1f}%)")
            logger.info("-" * 60)
            
            if abs(ce_15m) >= OI_THRESHOLD_STRONG:
                expected_move = 80
            else:
                expected_move = 50
            
            checks = {
                "CE OI Falling": ce_15m < -threshold,
                "Price > VWAP": price > vwap,
                "GREEN Candle": candle_color == 'GREEN',
                "Volume Surge": has_vol_spike
            }
            
            bonus = {
                "Strong OI (5m)": ce_5m < -5,
                "Big Candle": candle_size >= MIN_CANDLE_SIZE,
                "Far from VWAP": vwap_dist >= VWAP_BUFFER,
                "Bullish PCR": pcr > PCR_BULLISH,
                "3x Volume": vol_mult >= VOL_SPIKE_3X,
                "Momentum": PureAnalyzer.check_momentum_candles(df, 'bullish')
            }
            
            passed = sum(checks.values())
            bonus_passed = sum(bonus.values())
            
            logger.info("MAIN CHECKS (All 4 required):")
            for name, result in checks.items():
                logger.info(f"  {'✅' if result else '❌'} {name}")
            
            logger.info(f"\nBONUS CHECKS ({bonus_passed}/6):")
            for name, result in bonus.items():
                logger.info(f"  {'✅' if result else '❌'} {name}")
            
            if passed == 4:
                confidence = 75 + (bonus_passed * 3)
                logger.info(f"\n🎯 CE SIGNAL APPROVED!")
                logger.info(f"   Confidence: {confidence}% (4/4 main + {bonus_passed}/6 bonus)")
                logger.info(f"   Expected Move: {expected_move}+ points")
                
                return Signal(
                    "CE_BUY",
                    f"SHORT COVERING (OI: {ce_15m:.1f}%)",
                    min(confidence, 95),
                    price,
                    strike,
                    expected_move,
                    pcr,
                    candle_color,
                    vol_mult,
                    ce_5m,
                    ce_15m
                )
            else:
                logger.info(f"\n❌ CE SIGNAL REJECTED: Only {passed}/4 main checks passed")
                return None
        
        # PE_BUY Check
        if pe_15m < -threshold:
            logger.info(f"\n🔍 PE SIGNAL ANALYSIS (OI: {pe_15m:.1f}%)")
            logger.info("-" * 60)
            
            if abs(pe_15m) >= OI_THRESHOLD_STRONG:
                expected_move = 80
            else:
                expected_move = 50
            
            checks = {
                "PE OI Falling": pe_15m < -threshold,
                "Price < VWAP": price < vwap,
                "RED Candle": candle_color == 'RED',
                "Volume Surge": has_vol_spike
            }
            
            bonus = {
                "Strong OI (5m)": pe_5m < -5,
                "Big Candle": candle_size >= MIN_CANDLE_SIZE,
                "Far from VWAP": vwap_dist >= VWAP_BUFFER,
                "Bearish PCR": pcr < PCR_BEARISH,
                "3x Volume": vol_mult >= VOL_SPIKE_3X,
                "Momentum": PureAnalyzer.check_momentum_candles(df, 'bearish')
            }
            
            passed = sum(checks.values())
            bonus_passed = sum(bonus.values())
            
            logger.info("MAIN CHECKS (All 4 required):")
            for name, result in checks.items():
                logger.info(f"  {'✅' if result else '❌'} {name}")
            
            logger.info(f"\nBONUS CHECKS ({bonus_passed}/6):")
            for name, result in bonus.items():
                logger.info(f"  {'✅' if result else '❌'} {name}")
            
            if passed == 4:
                confidence = 75 + (bonus_passed * 3)
                logger.info(f"\n🎯 PE SIGNAL APPROVED!")
                logger.info(f"   Confidence: {confidence}% (4/4 main + {bonus_passed}/6 bonus)")
                logger.info(f"   Expected Move: {expected_move}+ points")
                
                return Signal(
                    "PE_BUY",
                    f"LONG UNWINDING (OI: {pe_15m:.1f}%)",
                    min(confidence, 95),
                    price,
                    strike,
                    expected_move,
                    pcr,
                    candle_color,
                    vol_mult,
                    pe_5m,
                    pe_15m
                )
            else:
                logger.info(f"\n❌ PE SIGNAL REJECTED: Only {passed}/4 main checks passed")
                return None
        
        logger.info("ℹ️ No OI trigger detected")
        return None

    async def send_alert(self, s: Signal):
        """Send alert"""
        if self.last_alert_time:
            diff = (datetime.now(IST) - self.last_alert_time).seconds
            if diff < 300:
                logger.info("⏳ Rate Limited (Last alert < 5 min)")
                return
        
        self.last_alert_time = datetime.now(IST)
        
        emoji = "🟢" if s.type == "CE_BUY" else "🔴"
        mode = "🧪 TEST" if OI_TEST_MODE else "📊 LIVE"
        
        # Calculate target
        if s.type == "CE_BUY":
            target = s.price + s.target_points
        else:
            target = s.price - s.target_points
        
        msg = f"""
{emoji} NIFTY SIGNAL V11.5

Action: {s.type}
Entry: {s.price:.1f}
Target: {target:.1f} ({s.target_points}+ points)
Strike: {s.strike}

Logic: {s.reason}
Confidence: {s.confidence}%

Market Data:
PCR: {s.pcr:.2f}
Candle: {s.candle_color}
Volume: {s.volume_surge:.1f}x

OI Movement:
5-min: {s.oi_5m:+.1f}%
15-min: {s.oi_15m:+.1f}%

{mode}
Real Futures Volume Strategy
"""
        
        logger.info(f"\n🚨 SIGNAL GENERATED!")
        logger.info(f"   {s.type} @ {s.price:.1f} → Target: {target:.1f}")
        logger.info(f"   Confidence: {s.confidence}%")
        
        if self.telegram:
            try:
                await self.telegram.send_message(TELEGRAM_CHAT_ID, msg)
                logger.info("✅ Telegram Alert Sent")
            except Exception as e:
                logger.error(f"Telegram error: {e}")

    async def send_startup_message(self):
        now = datetime.now(IST)
        msg = f"""
🚀 BOT V11.5 STARTED

Time: {now.strftime('%d-%b %I:%M %p')}

CRITICAL FIX:
✅ Using NIFTY FUTURES for volume
✅ Real traded volume data
✅ Accurate VWAP calculation
✅ Volume spike detection working

PURE STRATEGY:
✅ OI Delta (Smart money)
✅ Volume Surge (Confirmation)
✅ VWAP (Value zone)
✅ Candle (Price action)

Target: 40-100 point moves
Accuracy Goal: 75%+

{'🧪 TEST: 3% threshold' if OI_TEST_MODE else '📊 LIVE: 5% threshold'}

Scan: Every 90 seconds

NOTE: Update NIFTY_FUTURES symbol monthly
Current: {NIFTY_FUTURES}
"""
        
        logger.info("📲 Sending startup message...")
        
        if self.telegram:
            try:
                await self.telegram.send_message(TELEGRAM_CHAT_ID, msg)
                logger.info("✅ Startup sent")
            except Exception as e:
                logger.error(f"Startup error: {e}")

# ==================== MAIN ====================
async def main():
    bot = DataMonsterBot()
    logger.info("=" * 80)
    logger.info("🚀 DATA MONSTER V11.5 - REAL VOLUME FIX")
    logger.info("=" * 80)
    logger.info(f"📊 Using: {NIFTY_FUTURES} for Volume/VWAP")
    logger.info(f"🎯 Using: {NIFTY_SPOT} for Price/OI")
    logger.info(f"🎯 Target: 40-100 point moves | 75%+ accuracy")
    logger.info("=" * 80)
    
    await bot.send_startup_message()
    
    while True:
        try:
            now = datetime.now(IST).time()
            
            if time(9, 15) <= now <= time(15, 30):
                await bot.run_cycle()
                await asyncio.sleep(90)
            else:
                logger.info("🌙 Market Closed - Sleeping...")
                await asyncio.sleep(300)
                
        except KeyboardInterrupt:
            logger.info("🛑 Bot Stopped by User")
            break
        except Exception as e:
            logger.error(f"💥 Critical Error: {e}")
            logger.error(f"   Waiting 30s before retry...")
            await asyncio.sleep(30)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n👋 Shutdown Complete")
