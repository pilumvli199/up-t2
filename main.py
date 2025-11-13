#!/usr/bin/env python3
"""
SENSEX 5-MINUTE TRADING BOT - OPTIMIZED VERSION
================================================
✅ Intraday + Historical API (Live Candles Fixed)
✅ 420 Candles Analysis (5-Min Only)
✅ Ultra-Compressed Deepseek Format (Token Optimized)
✅ Improved Chart with Volume & Time Labels
✅ Option Chain Data in Alerts
✅ API Connection Status Notifications
"""

import os
import asyncio
import requests
import urllib.parse
from datetime import datetime, timedelta, time
import pytz
import time as time_sleep
from telegram import Bot
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import pandas as pd
import numpy as np
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import traceback
import re
import redis

# ==================== CONFIGURATION ====================
IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sensex_5min_bot.log')
    ]
)
logger = logging.getLogger(__name__)

# API Keys
UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN', 'your_token')
DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY', 'your_key')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', 'your_token')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', 'your_chat_id')

# Redis Connection
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
redis_client = redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)

# SENSEX Configuration
SENSEX_SYMBOL = "BSE_INDEX|SENSEX"
SENSEX_NAME = "SENSEX"
SENSEX_DISPLAY = "SENSEX"

# Analysis Configuration
CANDLE_COUNT = 420  # Total candles for analysis
TIMEFRAME = "5minute"  # Only 5-minute timeframe

# ==================== DATA CLASSES ====================
@dataclass
class StrikeData:
    strike: int
    ce_oi: int
    pe_oi: int
    ce_volume: int
    pe_volume: int
    ce_price: float
    pe_price: float
    ce_oi_change: int = 0
    pe_oi_change: int = 0

@dataclass
class OISnapshot:
    timestamp: datetime
    strikes: List[StrikeData]
    pcr: float
    max_pain: int
    support_strikes: List[int]
    resistance_strikes: List[int]
    total_ce_oi: int
    total_pe_oi: int

@dataclass
class TradeSignal:
    signal_type: str
    confidence: int
    entry_price: float
    stop_loss: float
    target_1: float
    target_2: float
    risk_reward: str
    recommended_strike: int
    reasoning: str
    price_analysis: str
    oi_analysis: str
    alignment_score: int
    risk_factors: List[str]
    support_levels: List[float]
    resistance_levels: List[float]
    pattern_detected: str

# ==================== EXPIRY CALCULATOR ====================
class ExpiryCalculator:
    @staticmethod
    def get_all_expiries_from_api(instrument_key: str, access_token: str) -> List[str]:
        """Fetch all available expiries from Upstox API"""
        try:
            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {access_token}"
            }
            
            encoded_key = urllib.parse.quote(instrument_key, safe='')
            url = f"https://api.upstox.com/v2/option/contract?instrument_key={encoded_key}"
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                contracts = response.json().get('data', [])
                expiries = sorted(list(set(c['expiry'] for c in contracts if 'expiry' in c)))
                logger.info(f"  📅 Found {len(expiries)} expiries from API")
                return expiries
            return []
        except Exception as e:
            logger.error(f"  ❌ Expiry API error: {e}")
            return []
    
    @staticmethod
    def get_next_thursday() -> str:
        """Calculate next Thursday for SENSEX weekly expiry"""
        today = datetime.now(IST).date()
        current_time = datetime.now(IST).time()
        
        # Thursday is weekday 3
        days_ahead = 3 - today.weekday()
        
        if days_ahead <= 0:
            # If today is Thursday and before 3:30 PM, use today
            if today.weekday() == 3 and current_time < time(15, 30):
                expiry = today
            else:
                # Otherwise, next Thursday
                days_ahead += 7
                expiry = today + timedelta(days=days_ahead)
        else:
            expiry = today + timedelta(days=days_ahead)
        
        return expiry.strftime('%Y-%m-%d')
    
    @staticmethod
    def get_weekly_expiry(access_token: str) -> str:
        """Get SENSEX weekly expiry (Thursday)"""
        expiries = ExpiryCalculator.get_all_expiries_from_api(SENSEX_SYMBOL, access_token)
        
        if expiries:
            today = datetime.now(IST).date()
            now_time = datetime.now(IST).time()
            
            future_expiries = []
            for exp_str in expiries:
                try:
                    exp_date = datetime.strptime(exp_str, '%Y-%m-%d').date()
                    if exp_date > today or (exp_date == today and now_time < time(15, 30)):
                        future_expiries.append(exp_str)
                except:
                    continue
            
            if future_expiries:
                nearest_expiry = min(future_expiries)
                logger.info(f"  ✅ Using API expiry: {nearest_expiry}")
                return nearest_expiry
        
        # Fallback to calculated Thursday expiry
        calculated_expiry = ExpiryCalculator.get_next_thursday()
        logger.info(f"  ⚠️ Using calculated expiry (Thursday): {calculated_expiry}")
        return calculated_expiry
    
    @staticmethod
    def days_to_expiry(expiry_str: str) -> int:
        """Calculate days remaining to expiry"""
        try:
            expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d').date()
            return (expiry_date - datetime.now(IST).date()).days
        except:
            return 0
    
    @staticmethod
    def format_for_display(expiry_str: str) -> str:
        """Format expiry date for display (e.g., 13NOV25)"""
        try:
            dt = datetime.strptime(expiry_str, '%Y-%m-%d')
            return dt.strftime('%d%b%y').upper()
        except:
            return expiry_str

# ==================== REDIS OI MANAGER ====================
class RedisOIManager:
    @staticmethod
    def save_oi_snapshot(snapshot: OISnapshot):
        """Save OI snapshot to Redis"""
        key = f"oi:sensex:{snapshot.timestamp.strftime('%Y-%m-%d_%H:%M')}"
        
        data = {
            "timestamp": snapshot.timestamp.isoformat(),
            "pcr": snapshot.pcr,
            "max_pain": snapshot.max_pain,
            "support_strikes": snapshot.support_strikes,
            "resistance_strikes": snapshot.resistance_strikes,
            "total_ce_oi": snapshot.total_ce_oi,
            "total_pe_oi": snapshot.total_pe_oi,
            "strikes": [
                {
                    "strike": s.strike,
                    "ce_oi": s.ce_oi,
                    "pe_oi": s.pe_oi,
                    "ce_volume": s.ce_volume,
                    "pe_volume": s.pe_volume,
                    "ce_price": s.ce_price,
                    "pe_price": s.pe_price,
                    "ce_oi_change": s.ce_oi_change,
                    "pe_oi_change": s.pe_oi_change
                }
                for s in snapshot.strikes
            ]
        }
        
        redis_client.setex(key, 259200, json.dumps(data))  # 3 days TTL
        logger.info(f"  💾 Saved OI snapshot: {key}")
    
    @staticmethod
    def get_oi_snapshot(minutes_ago: int) -> Optional[OISnapshot]:
        """Retrieve OI snapshot from Redis"""
        target_time = datetime.now(IST) - timedelta(minutes=minutes_ago)
        target_time = target_time.replace(
            minute=(target_time.minute // 5) * 5,
            second=0,
            microsecond=0
        )
        
        key = f"oi:sensex:{target_time.strftime('%Y-%m-%d_%H:%M')}"
        data = redis_client.get(key)
        
        if data:
            parsed = json.loads(data)
            return OISnapshot(
                timestamp=datetime.fromisoformat(parsed['timestamp']),
                strikes=[
                    StrikeData(
                        strike=s['strike'],
                        ce_oi=s['ce_oi'],
                        pe_oi=s['pe_oi'],
                        ce_volume=s['ce_volume'],
                        pe_volume=s['pe_volume'],
                        ce_price=s['ce_price'],
                        pe_price=s['pe_price'],
                        ce_oi_change=s.get('ce_oi_change', 0),
                        pe_oi_change=s.get('pe_oi_change', 0)
                    )
                    for s in parsed['strikes']
                ],
                pcr=parsed['pcr'],
                max_pain=parsed['max_pain'],
                support_strikes=parsed['support_strikes'],
                resistance_strikes=parsed['resistance_strikes'],
                total_ce_oi=parsed['total_ce_oi'],
                total_pe_oi=parsed['total_pe_oi']
            )
        
        return None
    
    @staticmethod
    def save_candle_data(df: pd.DataFrame):
        """Save candle data to Redis"""
        key = f"candles:sensex:5m"
        
        df_copy = df.copy()
        df_copy['timestamp'] = df_copy['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        data = df_copy.to_json(orient='records')
        
        now = datetime.now(IST)
        delete_time = now.replace(hour=15, minute=15, second=0, microsecond=0)
        
        if now.time() >= time(15, 15):
            delete_time += timedelta(days=1)
        
        ttl = int((delete_time - now).total_seconds())
        
        redis_client.setex(key, ttl, data)
        logger.info(f"  💾 Saved 5-min candles (expires at 3:15 PM)")
    
    @staticmethod
    def get_candle_data() -> Optional[pd.DataFrame]:
        """Retrieve candle data from Redis"""
        key = f"candles:sensex:5m"
        data = redis_client.get(key)
        
        if data:
            df = pd.DataFrame(json.loads(data))
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        
        return None

# ==================== UPSTOX DATA FETCHER (FIXED) ====================
class UpstoxDataFetcher:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
    
    def get_intraday_data(self) -> pd.DataFrame:
        """
        Fetch TODAY's live intraday 1-minute candles (V2 API)
        Then convert to 5-minute candles
        """
        try:
            encoded_symbol = urllib.parse.quote(SENSEX_SYMBOL, safe='')
            # Use 1-minute interval as 5-minute is not supported in intraday
            url = f"https://api.upstox.com/v2/historical-candle/intraday/{encoded_symbol}/1minute"
            
            logger.info(f"  📡 Fetching intraday 1-min data...")
            response = requests.get(url, headers=self.headers, timeout=30)
            
            logger.info(f"  📡 Intraday API Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success' and 'data' in data and 'candles' in data['data']:
                    df = pd.DataFrame(
                        data['data']['candles'],
                        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
                    )
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df = df.sort_values('timestamp').reset_index(drop=True)
                    
                    # Convert 1-min to 5-min candles
                    df_5min = self.convert_to_5min(df)
                    logger.info(f"  ✅ Converted {len(df)} 1-min → {len(df_5min)} 5-min candles")
                    return df_5min
                else:
                    logger.warning(f"  ⚠️ No intraday candles: {data}")
            else:
                logger.warning(f"  ⚠️ Intraday API error {response.status_code}: {response.text[:200]}")
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"  ❌ Intraday data error: {e}")
            traceback.print_exc()
            return pd.DataFrame()
    
    def convert_to_5min(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert 1-minute candles to 5-minute candles"""
        if df.empty:
            return df
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        # Resample to 5-minute intervals
        df_5min = df.resample('5T').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'oi': 'last'
        }).dropna()
        
        df_5min.reset_index(inplace=True)
        return df_5min
    
    def get_historical_data(self, days: int = 10) -> pd.DataFrame:
        """
        Fetch historical 5-minute data using V3 API (FIXED)
        """
        try:
            to_date = (datetime.now(IST) - timedelta(days=1)).date()  # Yesterday
            from_date = to_date - timedelta(days=days)
            
            # V3 API endpoint with correct format
            url = f"https://api.upstox.com/v3/historical-candle/{SENSEX_SYMBOL}/minutes/5/{to_date.strftime('%Y-%m-%d')}/{from_date.strftime('%Y-%m-%d')}"
            
            logger.info(f"  📡 V3 API: {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            
            logger.info(f"  📡 Historical V3 Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success' and 'data' in data and 'candles' in data['data']:
                    df = pd.DataFrame(
                        data['data']['candles'],
                        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
                    )
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df = df.sort_values('timestamp').reset_index(drop=True)
                    logger.info(f"  ✅ Fetched {len(df)} historical 5-min candles (V3)")
                    return df
                else:
                    logger.warning(f"  ⚠️ No historical data: {data}")
            else:
                logger.warning(f"  ⚠️ V3 API error {response.status_code}: {response.text[:200]}")
                
                # Fallback to V2 API with 30-minute (then convert to 5-min)
                logger.info(f"  🔄 Trying V2 fallback with 30-minute...")
                return self.get_historical_v2_fallback(days)
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"  ❌ Historical data error: {e}")
            traceback.print_exc()
            return pd.DataFrame()
    
    def get_historical_v2_fallback(self, days: int = 10) -> pd.DataFrame:
        """
        V2 API fallback using 30-minute interval (then convert to 5-min approximation)
        """
        try:
            to_date = (datetime.now(IST) - timedelta(days=1)).date()
            from_date = to_date - timedelta(days=days)
            
            encoded_symbol = urllib.parse.quote(SENSEX_SYMBOL, safe='')
            url = f"https://api.upstox.com/v2/historical-candle/{encoded_symbol}/30minute/{to_date.strftime('%Y-%m-%d')}/{from_date.strftime('%Y-%m-%d')}"
            
            logger.info(f"  📡 V2 Fallback: 30-minute data")
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success' and 'data' in data and 'candles' in data['data']:
                    df = pd.DataFrame(
                        data['data']['candles'],
                        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
                    )
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df = df.sort_values('timestamp').reset_index(drop=True)
                    logger.info(f"  ✅ V2 Fallback: {len(df)} 30-min candles")
                    
                    # Note: Using 30-min as approximation for 5-min analysis
                    # This is less accurate but better than no data
                    return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"  ❌ V2 Fallback error: {e}")
            return pd.DataFrame()
    
    def get_combined_candles(self) -> pd.DataFrame:
        """Get combined historical + intraday candles for 420 candle analysis"""
        try:
            # Step 1: Get historical data (V3 API - 5 min)
            logger.info("  📥 Fetching historical 5-min data (V3 API)...")
            df_historical = self.get_historical_data(days=10)
            
            # Step 2: Get today's intraday data (1-min converted to 5-min)
            logger.info("  📥 Fetching intraday 1-min → 5-min data...")
            df_intraday = self.get_intraday_data()
            
            # Step 3: Combine both
            if not df_historical.empty and not df_intraday.empty:
                df_combined = pd.concat([df_historical, df_intraday]).drop_duplicates(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)
                logger.info(f"  ✅ Combined: {len(df_historical)} hist + {len(df_intraday)} intraday = {len(df_combined)} total")
                return df_combined
            elif not df_historical.empty:
                logger.warning(f"  ⚠️ No intraday data, using only historical ({len(df_historical)} candles)")
                return df_historical
            elif not df_intraday.empty:
                logger.warning(f"  ⚠️ No historical data, using only intraday ({len(df_intraday)} candles)")
                return df_intraday
            else:
                logger.error(f"  ❌ No candle data available from any source!")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"  ❌ Combined candles error: {e}")
            traceback.print_exc()
            return pd.DataFrame()
    
    def get_ltp(self) -> float:
        """Get Last Traded Price"""
        try:
            encoded_symbol = urllib.parse.quote(SENSEX_SYMBOL, safe='')
            url = f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={encoded_symbol}"
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                try:
                    # Format 1: Standard nested format
                    if 'data' in data and SENSEX_SYMBOL in data['data']:
                        ltp = float(data['data'][SENSEX_SYMBOL]['last_price'])
                        logger.info(f"  ✅ LTP: ₹{ltp:.2f}")
                        return ltp
                    
                    # Format 2: Direct last_price in data
                    if 'data' in data and isinstance(data['data'], dict) and 'last_price' in data['data']:
                        ltp = float(data['data']['last_price'])
                        logger.info(f"  ✅ LTP: ₹{ltp:.2f}")
                        return ltp
                    
                    # Format 3: Nested in first key
                    if 'data' in data and isinstance(data['data'], dict):
                        first_key = list(data['data'].keys())[0] if data['data'] else None
                        if first_key and 'last_price' in data['data'][first_key]:
                            ltp = float(data['data'][first_key]['last_price'])
                            logger.info(f"  ✅ LTP: ₹{ltp:.2f}")
                            return ltp
                    
                except (KeyError, TypeError, ValueError, IndexError) as parse_error:
                    logger.error(f"  ❌ LTP parsing error: {parse_error}")
            
            return 0.0
            
        except Exception as e:
            logger.error(f"  ❌ LTP error: {e}")
            return 0.0
    
    def get_option_chain(self, expiry: str) -> List[StrikeData]:
        """Fetch option chain data for SENSEX"""
        try:
            encoded_symbol = urllib.parse.quote(SENSEX_SYMBOL, safe='')
            url = f"https://api.upstox.com/v2/option/chain?instrument_key={encoded_symbol}&expiry_date={expiry}"
            
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'data' not in data:
                    logger.warning(f"  ⚠️ No 'data' in option chain response")
                    return []
                
                strikes = []
                for item in data['data']:
                    try:
                        strike_price = int(float(item.get('strike_price', 0)))
                        
                        call_data = item.get('call_options', {}).get('market_data', {})
                        put_data = item.get('put_options', {}).get('market_data', {})
                        
                        strikes.append(StrikeData(
                            strike=strike_price,
                            ce_oi=int(call_data.get('oi', 0)),
                            pe_oi=int(put_data.get('oi', 0)),
                            ce_volume=int(call_data.get('volume', 0)),
                            pe_volume=int(put_data.get('volume', 0)),
                            ce_price=float(call_data.get('ltp', 0)),
                            pe_price=float(put_data.get('ltp', 0))
                        ))
                    except Exception as item_error:
                        continue
                
                logger.info(f"  ✅ Fetched {len(strikes)} strikes")
                return strikes
            else:
                logger.error(f"  ❌ Option chain API error {response.status_code}")
                return []
            
        except Exception as e:
            logger.error(f"  ❌ Option chain error: {e}")
            return []

# ==================== OI ANALYZER ====================
class OIAnalyzer:
    @staticmethod
    def calculate_pcr(strikes: List[StrikeData]) -> float:
        """Calculate Put-Call Ratio"""
        total_ce = sum(s.ce_oi for s in strikes)
        total_pe = sum(s.pe_oi for s in strikes)
        return total_pe / total_ce if total_ce > 0 else 0
    
    @staticmethod
    def find_max_pain(strikes: List[StrikeData]) -> int:
        """Find Max Pain strike price"""
        max_pain_values = {}
        
        for strike_data in strikes:
            strike = strike_data.strike
            total_pain = 0
            
            for s in strikes:
                if s.strike < strike:
                    total_pain += (strike - s.strike) * s.pe_oi
                elif s.strike > strike:
                    total_pain += (s.strike - strike) * s.ce_oi
            
            max_pain_values[strike] = total_pain
        
        return min(max_pain_values, key=max_pain_values.get) if max_pain_values else 0
    
    @staticmethod
    def get_atm_strikes(strikes: List[StrikeData], spot_price: float, count: int = 21) -> List[StrikeData]:
        """Get ATM strikes (±10 strikes from current price)"""
        # SENSEX has 100-point strike intervals
        atm_strike = round(spot_price / 100) * 100
        strike_range = range(atm_strike - 1000, atm_strike + 1100, 100)
        relevant = [s for s in strikes if s.strike in strike_range]
        return sorted(relevant, key=lambda x: x.strike)[:count]
    
    @staticmethod
    def identify_support_resistance(strikes: List[StrikeData]) -> Tuple[List[int], List[int]]:
        """Identify support and resistance levels from OI"""
        pe_sorted = sorted(strikes, key=lambda x: x.pe_oi, reverse=True)
        support_strikes = [s.strike for s in pe_sorted[:3]]
        
        ce_sorted = sorted(strikes, key=lambda x: x.ce_oi, reverse=True)
        resistance_strikes = [s.strike for s in ce_sorted[:3]]
        
        return support_strikes, resistance_strikes
    
    @staticmethod
    def calculate_oi_changes(current: List[StrikeData], previous: Optional[OISnapshot]) -> List[StrikeData]:
        """Calculate OI changes compared to previous snapshot"""
        if not previous:
            return current
        
        prev_dict = {s.strike: s for s in previous.strikes}
        
        for strike in current:
            if strike.strike in prev_dict:
                prev_strike = prev_dict[strike.strike]
                strike.ce_oi_change = strike.ce_oi - prev_strike.ce_oi
                strike.pe_oi_change = strike.pe_oi - prev_strike.pe_oi
        
        return current
    
    @staticmethod
    def create_oi_snapshot(strikes: List[StrikeData], spot_price: float, prev_snapshot: Optional[OISnapshot] = None) -> OISnapshot:
        """Create comprehensive OI snapshot"""
        atm_strikes = OIAnalyzer.get_atm_strikes(strikes, spot_price)
        
        # Calculate OI changes
        atm_strikes = OIAnalyzer.calculate_oi_changes(atm_strikes, prev_snapshot)
        
        pcr = OIAnalyzer.calculate_pcr(atm_strikes)
        max_pain = OIAnalyzer.find_max_pain(atm_strikes)
        support, resistance = OIAnalyzer.identify_support_resistance(atm_strikes)
        
        total_ce = sum(s.ce_oi for s in atm_strikes)
        total_pe = sum(s.pe_oi for s in atm_strikes)
        
        return OISnapshot(
            timestamp=datetime.now(IST),
            strikes=atm_strikes,
            pcr=pcr,
            max_pain=max_pain,
            support_strikes=support,
            resistance_strikes=resistance,
            total_ce_oi=total_ce,
            total_pe_oi=total_pe
        )

# ==================== ULTRA COMPRESSOR ====================
class UltraCompressor:
    """Ultra-compressed format for DeepSeek API (Maximum Token Saving)"""
    
    @staticmethod
    def compress_candles(df: pd.DataFrame, show_count: int = 50) -> Tuple[str, int]:
        """
        Ultra-compressed candle format with summary:
        Shows first 25 + last 25 candles, mentions total in between
        """
        df_copy = df.tail(CANDLE_COUNT).copy()
        total_candles = len(df_copy)
        
        # Format timestamp
        df_copy['timestamp'] = df_copy['timestamp'].dt.strftime('%H:%M')
        
        # Format volume with K/M suffix
        def format_volume(vol):
            if vol >= 1000000:
                return f"{vol/1000000:.1f}M"
            elif vol >= 1000:
                return f"{vol/1000:.0f}K"
            return str(int(vol))
        
        df_copy['volume'] = df_copy['volume'].apply(format_volume)
        
        # Take first 25 and last 25 candles
        df_first = df_copy.head(25)
        df_last = df_copy.tail(25)
        
        lines = []
        
        # First 25 candles
        for _, row in df_first.iterrows():
            lines.append(f"{row['timestamp']}|{int(row['open'])}|{int(row['high'])}|{int(row['low'])}|{int(row['close'])}|{row['volume']}")
        
        # Middle summary
        if total_candles > 50:
            lines.append(f"[... {total_candles - 50} more candles ...]")
        
        # Last 25 candles
        for _, row in df_last.iterrows():
            lines.append(f"{row['timestamp']}|{int(row['open'])}|{int(row['high'])}|{int(row['low'])}|{int(row['close'])}|{row['volume']}")
        
        return '\n'.join(lines), total_candles
    
    @staticmethod
    def compress_oi(strikes: List[StrikeData]) -> str:
        """
        Ultra-compressed OI format:
        Strike|C_OI|C_Δ|P_OI|P_Δ
        """
        def format_num(num):
            if abs(num) >= 1000000:
                return f"{num/1000000:.1f}M"
            elif abs(num) >= 1000:
                return f"{num/1000:.0f}K"
            return str(int(num))
        
        def format_change(change):
            if change > 0:
                return f"+{format_num(change)}"
            elif change < 0:
                return format_num(change)
            return "0"
        
        lines = []
        for s in strikes:
            lines.append(
                f"{s.strike}|{format_num(s.ce_oi)}|{format_change(s.ce_oi_change)}|"
                f"{format_num(s.pe_oi)}|{format_change(s.pe_oi_change)}"
            )
        
        return '\n'.join(lines)

# ==================== AI ANALYZER ====================
class AIAnalyzer:
    @staticmethod
    def extract_json(content: str) -> Optional[Dict]:
        """Extract JSON from AI response"""
        try:
            content = re.sub(r'```json\s*|\s*```', '', content)
            return json.loads(content)
        except:
            match = re.search(r'\{(?:[^{}]|(?:\{[^{}]*\}))*\}', content, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))
                except:
                    pass
        return None
    
    @staticmethod
    def analyze_with_deepseek(
        df_5m: pd.DataFrame,
        current_price: float,
        current_oi: OISnapshot
    ) -> Optional[TradeSignal]:
        """Send ultra-compressed analysis request to DeepSeek"""
        
        try:
            # Ultra-compressed formats
            candles_compressed, total_candles = UltraCompressor.compress_candles(df_5m)
            oi_compressed = UltraCompressor.compress_oi(current_oi.strikes)
            
            # Calculate simple momentum indicators
            df_tail = df_5m.tail(50)
            sma_20 = df_tail['close'].tail(20).mean()
            price_momentum = ((current_price - sma_20) / sma_20) * 100
            
            # Recent candle bias
            recent_closes = df_tail['close'].tail(10).values
            bullish_candles = sum(1 for i in range(1, len(recent_closes)) if recent_closes[i] > recent_closes[i-1])
            
            # Ultra-compact prompt
            prompt = f"""SENSEX 5-MIN ANALYSIS

PRICE: ₹{current_price:.2f} | TIME: {datetime.now(IST).strftime('%H:%M')}

**5-MIN CANDLES (Total: {total_candles}):**
Time |Open |High |Low  |Close|Vol
{candles_compressed}

**OPTION CHAIN (ATM ±10):**
Strike|C_OI|C_Δ|P_OI|P_Δ
{oi_compressed}

**METRICS:**
PCR: {current_oi.pcr:.2f} | MaxPain: {current_oi.max_pain}
Support: {','.join(map(str, current_oi.support_strikes[:2]))}
Resistance: {','.join(map(str, current_oi.resistance_strikes[:2]))}
Momentum: {price_momentum:+.1f}% | SMA20: ₹{sma_20:.0f}
Bullish/10: {bullish_candles}

**TASK:** Analyze 5-min price + OI. Output JSON with signal.

**JSON:**
{{
  "signal_type": "CE_BUY/PE_BUY/NO_TRADE",
  "confidence": 85,
  "entry_price": {current_price:.2f},
  "stop_loss": 0.0,
  "target_1": 0.0,
  "target_2": 0.0,
  "risk_reward": "1:2.5",
  "recommended_strike": {round(current_price/100)*100},
  "reasoning": "Brief (max 120 chars)",
  "price_analysis": "Summary (max 150 chars)",
  "oi_analysis": "Summary (max 150 chars)",
  "alignment_score": 8,
  "risk_factors": ["Risk1", "Risk2"],
  "support_levels": [0.0, 0.0],
  "resistance_levels": [0.0, 0.0],
  "pattern_detected": "Pattern or None"
}}"""
            
            response = requests.post(
                "https://api.deepseek.com/v1/chat/completions",
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {
                            "role": "system",
                            "content": "Elite F&O trader. Analyze 5-min data. Respond ONLY in JSON."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "temperature": 0.2,
                    "max_tokens": 2000
                },
                headers={
                    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                    "Content-Type": "application/json"
                },
                timeout=120
            )
            
            if response.status_code != 200:
                logger.error(f"  ❌ DeepSeek API error: {response.status_code}")
                return None
            
            ai_content = response.json()['choices'][0]['message']['content']
            analysis_dict = AIAnalyzer.extract_json(ai_content)
            
            if not analysis_dict:
                logger.error(f"  ❌ Failed to parse AI response")
                return None
            
            logger.info(f"  🧠 AI Signal: {analysis_dict.get('signal_type')} | Confidence: {analysis_dict.get('confidence')}%")
            
            return TradeSignal(**analysis_dict)
            
        except Exception as e:
            logger.error(f"  ❌ AI analysis error: {e}")
            traceback.print_exc()
            return None

# ==================== CHART GENERATOR ====================
class ChartGenerator:
    """Creates professional trading charts with 5-min data"""
    
    @staticmethod
    def create_chart(
        df_5m: pd.DataFrame,
        signal: TradeSignal,
        spot_price: float,
        save_path: str
    ):
        """Generate professional chart with signal visualization"""
        
        BG = '#0d1117'
        GRID = '#1e2835'
        TEXT = '#c9d1d9'
        GREEN = '#26a69a'
        RED = '#ef5350'
        YELLOW = '#ffd700'
        BLUE = '#2962ff'
        
        fig, (ax1, ax2) = plt.subplots(
            2, 1,
            figsize=(20, 12),
            gridspec_kw={'height_ratios': [4, 1]},
            facecolor=BG
        )
        
        ax1.set_facecolor(BG)
        df_plot = df_5m.tail(200).copy()
        
        # Create time labels for X-axis
        time_labels = df_plot['timestamp'].dt.strftime('%H:%M').tolist()
        x_positions = list(range(len(df_plot)))
        
        # Draw candlesticks
        for idx in range(len(df_plot)):
            row = df_plot.iloc[idx]
            color = GREEN if row['close'] > row['open'] else RED
            
            # Candle body
            ax1.add_patch(Rectangle(
                (x_positions[idx] - 0.3, min(row['open'], row['close'])),
                0.6,
                abs(row['close'] - row['open']),
                facecolor=color,
                edgecolor=color,
                alpha=0.9
            ))
            
            # Wick
            ax1.plot(
                [x_positions[idx], x_positions[idx]],
                [row['low'], row['high']],
                color=color,
                linewidth=1.2,
                alpha=0.7
            )
        
        # Support levels
        for support in signal.support_levels[:2]:
            ax1.axhline(support, color=GREEN, linestyle='--', linewidth=2, alpha=0.6)
            ax1.text(
                len(x_positions) - 5, support,
                f'  S: ₹{support:.0f}',
                color=GREEN,
                fontsize=10,
                va='bottom',
                bbox=dict(boxstyle='round,pad=0.3', facecolor=BG, edgecolor=GREEN, alpha=0.8)
            )
        
        # Resistance levels
        for resistance in signal.resistance_levels[:2]:
            ax1.axhline(resistance, color=RED, linestyle='--', linewidth=2, alpha=0.6)
            ax1.text(
                len(x_positions) - 5, resistance,
                f'  R: ₹{resistance:.0f}',
                color=RED,
                fontsize=10,
                va='top',
                bbox=dict(boxstyle='round,pad=0.3', facecolor=BG, edgecolor=RED, alpha=0.8)
            )
        
        # Stop loss and targets
        ax1.axhline(signal.stop_loss, color=RED, linewidth=2.5, linestyle=':', alpha=0.8)
        ax1.axhline(signal.target_1, color=GREEN, linewidth=2, linestyle=':', alpha=0.7)
        ax1.axhline(signal.target_2, color=GREEN, linewidth=2, linestyle=':', alpha=0.7)
        
        # Labels for SL and Targets
        ax1.text(2, signal.stop_loss, f' SL: ₹{signal.stop_loss:.0f} ', 
                color='white', fontsize=9, va='center',
                bbox=dict(boxstyle='round', facecolor=RED, alpha=0.9))
        ax1.text(2, signal.target_1, f' T1: ₹{signal.target_1:.0f} ', 
                color='white', fontsize=9, va='center',
                bbox=dict(boxstyle='round', facecolor=GREEN, alpha=0.9))
        ax1.text(2, signal.target_2, f' T2: ₹{signal.target_2:.0f} ', 
                color='white', fontsize=9, va='center',
                bbox=dict(boxstyle='round', facecolor=GREEN, alpha=0.9))
        
        # Current market price
        ax1.text(
            len(x_positions) - 1, spot_price,
            f' CMP: ₹{spot_price:.1f} ',
            fontsize=11,
            color='white',
            fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.4', facecolor=BLUE, edgecolor='white', linewidth=2),
            va='center'
        )
        
        # Signal info box - MOVED TO BOTTOM LEFT
        signal_emoji = "🟢" if signal.signal_type == "CE_BUY" else "🔴" if signal.signal_type == "PE_BUY" else "⚪"
        
        info_text = f"""{signal_emoji} {signal.signal_type} | Conf: {signal.confidence}% | Score: {signal.alignment_score}/10

Entry: ₹{signal.entry_price:.0f} | SL: ₹{signal.stop_loss:.0f}
T1: ₹{signal.target_1:.0f} | T2: ₹{signal.target_2:.0f} | RR: {signal.risk_reward}

Strike: {signal.recommended_strike} | Pattern: {signal.pattern_detected}"""
        
        ax1.text(
            0.01, 0.02,
            info_text,
            transform=ax1.transAxes,
            fontsize=10,
            va='bottom',
            bbox=dict(boxstyle='round,pad=0.8', facecolor=GRID, alpha=0.95, 
                     edgecolor=YELLOW if signal.signal_type != "NO_TRADE" else TEXT, linewidth=2),
            color=TEXT,
            family='monospace'
        )
        
        title = f"SENSEX | 5-Minute | {signal.signal_type} | Score: {signal.alignment_score}/10 | {datetime.now(IST).strftime('%d-%b-%Y %H:%M')}"
        ax1.set_title(title, color=TEXT, fontsize=15, fontweight='bold', pad=20)
        
        # Set X-axis with time labels
        tick_interval = max(len(x_positions) // 12, 1)
        ax1.set_xticks([x_positions[i] for i in range(0, len(x_positions), tick_interval)])
        ax1.set_xticklabels([time_labels[i] for i in range(0, len(time_labels), tick_interval)], 
                           rotation=45, ha='right', fontsize=9)
        
        ax1.grid(True, color=GRID, alpha=0.4, linestyle='--')
        ax1.tick_params(colors=TEXT, labelsize=10)
        ax1.set_ylabel('Price (₹)', color=TEXT, fontsize=12, fontweight='bold')
        
        # Volume subplot
        ax2.set_facecolor(BG)
        colors = [GREEN if df_plot.iloc[i]['close'] >= df_plot.iloc[i]['open'] else RED for i in range(len(df_plot))]
        
        # Plot volume bars
        ax2.bar(x_positions, df_plot['volume'].values, color=colors, alpha=0.7, width=0.8)
        
        ax2.set_ylabel('Volume', color=TEXT, fontsize=12, fontweight='bold')
        ax2.tick_params(colors=TEXT, labelsize=10)
        ax2.grid(True, color=GRID, alpha=0.3, linestyle='--')
        
        # Set X-axis for volume chart
        ax2.set_xticks([x_positions[i] for i in range(0, len(x_positions), tick_interval)])
        ax2.set_xticklabels([time_labels[i] for i in range(0, len(time_labels), tick_interval)], 
                           rotation=45, ha='right', fontsize=9)
        ax2.set_xlabel('Time (IST)', color=TEXT, fontsize=12, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=150, facecolor=BG, edgecolor='none')
        plt.close()
        
        logger.info(f"  📊 Chart saved: {save_path}")

# ==================== MAIN BOT ====================
class SensexBot:
    """Main bot orchestrator - 5-minute single timeframe"""
    
    def __init__(self):
        self.data_fetcher = UpstoxDataFetcher(UPSTOX_ACCESS_TOKEN)
        self.telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.scan_count = 0
        self.last_signal_time = None
    
    def check_api_connections(self) -> Dict[str, bool]:
        """Check if all APIs are connected"""
        status = {
            "upstox": False,
            "deepseek": False,
            "redis": False,
            "telegram": False
        }
        
        # Check Upstox
        try:
            ltp = self.data_fetcher.get_ltp()
            status["upstox"] = ltp > 0
        except:
            pass
        
        # Check DeepSeek
        try:
            test_response = requests.get(
                "https://api.deepseek.com/v1/models",
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}"},
                timeout=5
            )
            status["deepseek"] = test_response.status_code == 200
        except:
            pass
        
        # Check Redis
        try:
            redis_client.ping()
            status["redis"] = True
        except:
            pass
        
        # Check Telegram
        try:
            status["telegram"] = self.telegram_bot.get_me() is not None
        except:
            pass
        
        return status
    
    async def send_startup_message(self):
        """Send bot startup notification with API status"""
        expiry = ExpiryCalculator.get_weekly_expiry(UPSTOX_ACCESS_TOKEN)
        expiry_display = ExpiryCalculator.format_for_display(expiry)
        days_left = ExpiryCalculator.days_to_expiry(expiry)
        
        # Check API connections
        api_status = self.check_api_connections()
        
        upstox_icon = "✅" if api_status["upstox"] else "❌"
        deepseek_icon = "✅" if api_status["deepseek"] else "❌"
        redis_icon = "✅" if api_status["redis"] else "❌"
        telegram_icon = "✅" if api_status["telegram"] else "❌"
        
        message = f"""
🚀 SENSEX 5-MIN BOT STARTED

⏰ Time: {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S')}

🔌 API CONNECTION STATUS:
{upstox_icon} Upstox API: {"Connected" if api_status["upstox"] else "Failed"}
{deepseek_icon} DeepSeek API: {"Connected" if api_status["deepseek"] else "Failed"}
{redis_icon} Redis Cache: {"Connected" if api_status["redis"] else "Failed"}
{telegram_icon} Telegram Bot: {"Connected" if api_status["telegram"] else "Failed"}

📊 Configuration:
✅ Symbol: SENSEX Index (BSE)
✅ Timeframe: 5-Minute ONLY
✅ Analysis: 420 Candles
✅ Data: Historical + Intraday (Live)
✅ Scan: Every 5 minutes
✅ Market: 9:15 AM - 3:30 PM
✅ Expiry: {expiry_display} ({expiry}) - {days_left} days
✅ Expiry Day: Thursday

🧠 AI Features:
✅ Ultra-compressed data format
✅ Token-optimized prompts
✅ Entry/Exit analysis
✅ OI buildup tracking
✅ Pattern detection

🎯 Alert Criteria:
✅ Confidence: 75%+
✅ Score: 7+/10
✅ Cooldown: 30 min

🔄 Status: Active & Running
"""
        
        await self.telegram_bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message
        )
        logger.info("✅ Startup message sent with API status")
    
    async def send_telegram_alert(self, signal: TradeSignal, chart_path: str, current_oi: OISnapshot):
        """Send trading signal to Telegram with option chain data"""
        try:
            # Send chart
            with open(chart_path, 'rb') as photo:
                await self.telegram_bot.send_photo(
                    chat_id=TELEGRAM_CHAT_ID,
                    photo=photo
                )
            
            signal_emoji = "🟢" if signal.signal_type == "CE_BUY" else "🔴"
            
            # Get ATM strike data
            atm_strike = signal.recommended_strike
            atm_data = next((s for s in current_oi.strikes if s.strike == atm_strike), None)
            
            # Format option chain data
            oi_table = "📊 OPTION CHAIN (ATM ±5 Strikes):\n```\n"
            oi_table += "Strike | CE_OI  | CE_Δ   | PE_OI  | PE_Δ\n"
            oi_table += "-------+--------+--------+--------+-------\n"
            
            # Show 5 strikes above and below ATM
            strikes_to_show = [s for s in current_oi.strikes 
                             if abs(s.strike - atm_strike) <= 500][:11]
            
            for s in strikes_to_show:
                ce_oi_k = f"{s.ce_oi/1000:.0f}K" if s.ce_oi >= 1000 else str(s.ce_oi)
                pe_oi_k = f"{s.pe_oi/1000:.0f}K" if s.pe_oi >= 1000 else str(s.pe_oi)
                
                # Handle delta display
                if s.ce_oi_change == 0:
                    ce_delta = "-"
                elif s.ce_oi_change > 0:
                    ce_delta = f"+{abs(s.ce_oi_change)/1000:.0f}K"
                else:
                    ce_delta = f"-{abs(s.ce_oi_change)/1000:.0f}K"
                
                if s.pe_oi_change == 0:
                    pe_delta = "-"
                elif s.pe_oi_change > 0:
                    pe_delta = f"+{abs(s.pe_oi_change)/1000:.0f}K"
                else:
                    pe_delta = f"-{abs(s.pe_oi_change)/1000:.0f}K"
                
                marker = "→" if s.strike == atm_strike else " "
                oi_table += f"{marker}{s.strike:5d} | {ce_oi_k:6s} | {ce_delta:6s} | {pe_oi_k:6s} | {pe_delta:6s}\n"
            
            oi_table += "```\n"
            
            # ATM Strike Details
            atm_info = ""
            if atm_data:
                atm_info = f"""
📍 ATM STRIKE: {atm_strike}
CE Price: ₹{atm_data.ce_price:.2f} | PE Price: ₹{atm_data.pe_price:.2f}
CE OI: {atm_data.ce_oi:,} | PE OI: {atm_data.pe_oi:,}
"""
            
            reasoning = signal.reasoning[:150].replace('_', ' ').replace('*', ' ')
            price_analysis = signal.price_analysis[:200].replace('_', ' ').replace('*', ' ')
            oi_analysis = signal.oi_analysis[:200].replace('_', ' ').replace('*', ' ')
            pattern = signal.pattern_detected.replace('_', ' ').replace('*', ' ')
            
            message = f"""
{signal_emoji} SENSEX {signal.signal_type} SIGNAL

🎯 Confidence: {signal.confidence}% | Score: {signal.alignment_score}/10

💰 TRADE SETUP:
Entry: ₹{signal.entry_price:.2f}
Stop Loss: ₹{signal.stop_loss:.2f}
Target 1: ₹{signal.target_1:.2f}
Target 2: ₹{signal.target_2:.2f}
Risk:Reward → {signal.risk_reward}
{atm_info}
💡 REASONING:
{reasoning}

📈 PRICE ANALYSIS:
{price_analysis}

📊 OI ANALYSIS:
{oi_analysis}

🎨 PATTERN: {pattern}

{oi_table}
📊 PCR: {current_oi.pcr:.2f} | Max Pain: {current_oi.max_pain}
📊 Support: {', '.join([f'₹{s}' for s in signal.support_levels[:2]])}
📊 Resistance: {', '.join([f'₹{r}' for r in signal.resistance_levels[:2]])}

⚠️ RISKS:
{chr(10).join(['• ' + rf.replace('_', ' ')[:60] for rf in signal.risk_factors[:3]])}

🕐 {datetime.now(IST).strftime('%d-%b %H:%M:%S')}
"""
            
            await self.telegram_bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message
            )
            
            logger.info(f"  ✅ Alert sent: {signal.signal_type}")
            
        except Exception as e:
            logger.error(f"  ❌ Telegram error: {e}")
            traceback.print_exc()
    
    async def run_analysis(self):
        """Run complete analysis cycle - 5-minute timeframe only"""
        try:
            self.scan_count += 1
            logger.info(f"\n{'='*70}")
            logger.info(f"🔍 SCAN #{self.scan_count} - {datetime.now(IST).strftime('%H:%M:%S')}")
            logger.info(f"{'='*70}")
            
            # Step 1: Get combined historical + intraday candles
            df_5m = self.data_fetcher.get_combined_candles()
            
            if df_5m.empty or len(df_5m) < 100:
                logger.error(f"  ❌ Insufficient candle data: {len(df_5m)} candles")
                return
            
            # Save to cache
            RedisOIManager.save_candle_data(df_5m)
            
            logger.info(f"  📊 Total 5-min candles: {len(df_5m)}")
            
            # Step 2: Get current price
            spot_price = self.data_fetcher.get_ltp()
            if spot_price == 0:
                spot_price = df_5m['close'].iloc[-1]
                logger.info(f"  💹 Using last close: ₹{spot_price:.2f}")
            
            # Step 3: Get option chain data
            expiry = ExpiryCalculator.get_weekly_expiry(UPSTOX_ACCESS_TOKEN)
            expiry_display = ExpiryCalculator.format_for_display(expiry)
            logger.info(f"  📅 Expiry: {expiry_display} ({expiry})")
            
            all_strikes = self.data_fetcher.get_option_chain(expiry)
            if not all_strikes:
                logger.warning("  ⚠️ No option chain data available")
                return
            
            # Step 4: Create OI snapshot with changes
            prev_oi = RedisOIManager.get_oi_snapshot(5)
            current_oi = OIAnalyzer.create_oi_snapshot(all_strikes, spot_price, prev_oi)
            logger.info(f"  📊 PCR: {current_oi.pcr:.2f} | Max Pain: {current_oi.max_pain}")
            
            # Save current OI snapshot
            RedisOIManager.save_oi_snapshot(current_oi)
            
            if prev_oi:
                logger.info(f"  ✅ OI changes from 5 min ago (PCR: {prev_oi.pcr:.2f})")
            
            # Step 5: Send to DeepSeek AI
            logger.info("  🧠 Sending to DeepSeek AI (Ultra-Compressed)...")
            signal = AIAnalyzer.analyze_with_deepseek(
                df_5m=df_5m,
                current_price=spot_price,
                current_oi=current_oi
            )
            
            if not signal:
                logger.info("  ⏸️ No valid signal generated")
                return
            
            logger.info(f"  🧠 AI Signal: {signal.signal_type} | Conf: {signal.confidence}% | Score: {signal.alignment_score}/10")
            
            # Step 6: Check signal validity
            if signal.signal_type == "NO_TRADE":
                logger.info(f"  ⏸️ NO_TRADE (Confidence: {signal.confidence}%)")
                return
            
            if signal.confidence < 75 or signal.alignment_score < 7:
                logger.info(f"  ⏸️ Below threshold (Conf: {signal.confidence}% | Score: {signal.alignment_score}/10)")
                return
            
            # Step 7: Check cooldown
            if self.last_signal_time:
                time_since_last = (datetime.now(IST) - self.last_signal_time).total_seconds() / 60
                if time_since_last < 30:
                    logger.info(f"  ⏸️ Cooldown active ({time_since_last:.0f} min since last)")
                    return
            
            # Step 8: Generate alert
            logger.info(f"  🚨 ALERT! {signal.signal_type} | Conf: {signal.confidence}% | Score: {signal.alignment_score}/10")
            
            chart_path = f"/tmp/sensex_5min_{datetime.now(IST).strftime('%H%M')}.png"
            ChartGenerator.create_chart(df_5m, signal, spot_price, chart_path)
            
            await self.send_telegram_alert(signal, chart_path, current_oi)
            self.last_signal_time = datetime.now(IST)
            
        except Exception as e:
            logger.error(f"  ❌ Analysis error: {e}")
            traceback.print_exc()
    
    async def run_scanner(self):
        """Main scanner loop"""
        logger.info("\n" + "="*80)
        logger.info("🚀 SENSEX 5-MIN BOT - OPTIMIZED VERSION")
        logger.info("="*80)
        
        await self.send_startup_message()
        
        while True:
            try:
                now = datetime.now(IST)
                current_time = now.time()
                
                # Check market hours (9:15 AM - 3:30 PM)
                if current_time < time(9, 15) or current_time > time(15, 30):
                    logger.info(f"⏸️ Market closed. Waiting... (Current: {current_time.strftime('%H:%M')})")
                    await asyncio.sleep(300)
                    continue
                
                # Check weekends
                if now.weekday() >= 5:
                    logger.info(f"📅 Weekend. Pausing...")
                    await asyncio.sleep(3600)
                    continue
                
                # Run analysis
                await self.run_analysis()
                
                # Calculate next scan time (every 5 minutes)
                current_minute = now.minute
                next_scan_minute = ((current_minute // 5) + 1) * 5
                if next_scan_minute >= 60:
                    next_scan_minute = 0
                
                next_scan = now.replace(minute=next_scan_minute % 60, second=0, microsecond=0)
                if next_scan_minute == 0:
                    next_scan += timedelta(hours=1)
                
                wait_seconds = (next_scan - now).total_seconds()
                
                logger.info(f"\n✅ Scan complete. Next at {next_scan.strftime('%H:%M')} ({wait_seconds:.0f}s)")
                await asyncio.sleep(wait_seconds)
                
            except Exception as e:
                logger.error(f"❌ Scanner error: {e}")
                traceback.print_exc()
                await asyncio.sleep(60)

# ==================== ENTRY POINT ====================
if __name__ == "__main__":
    logger.info("="*80)
    logger.info("STARTING SENSEX 5-MIN TRADING BOT")
    logger.info("="*80)
    
    bot = SensexBot()
    asyncio.run(bot.run_scanner())
