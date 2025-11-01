#!/usr/bin/env python3
"""
HYBRID TRADING BOT v22.0 - PRODUCTION READY
============================================
✅ Charts (50) + OI (50) = 100 Score
✅ News optional (Telegram only)
✅ ATM ±7 Strike Selection (Top 15)
✅ Redis 3-day expiry (Railway.app)
✅ Monthly expiry for all symbols
✅ TradingView style professional charts
✅ Order Blocks + Pattern annotations
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
import io
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import traceback
import re
import redis

# ==================== CONFIGURATION ====================
IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# API Keys (Set as environment variables)
UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN', 'your_token')
DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY', 'your_key')
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY', 'your_key')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', 'your_token')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', 'your_chat_id')

# Redis Connection (supports both local and cloud URLs)
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')

redis_client = redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5
)

# ==================== SYMBOLS CONFIG ====================
INDICES = {
    "NSE_INDEX|Nifty Bank": {"name": "BANKNIFTY", "display_name": "BANK NIFTY", "type": "index", "has_options": True, "expiry_type": "monthly"},
    "NSE_INDEX|Nifty Midcap Select": {"name": "MIDCPNIFTY", "display_name": "MIDCAP NIFTY", "type": "index", "has_options": True, "expiry_type": "monthly"}
}

FO_STOCKS = {
    "NSE_EQ|INE467B01029": {"name": "TATAMOTORS", "display_name": "TATA MOTORS", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE585B01010": {"name": "MARUTI", "display_name": "MARUTI SUZUKI", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE208A01029": {"name": "ASHOKLEY", "display_name": "ASHOK LEYLAND", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE494B01023": {"name": "TVSMOTOR", "display_name": "TVS MOTOR", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE101A01026": {"name": "M&M", "display_name": "M&M", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE917I01010": {"name": "BAJAJ-AUTO", "display_name": "BAJAJ AUTO", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE040A01034": {"name": "HDFCBANK", "display_name": "HDFC BANK", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE090A01021": {"name": "ICICIBANK", "display_name": "ICICI BANK", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE062A01020": {"name": "SBIN", "display_name": "STATE BANK", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE028A01039": {"name": "BANKBARODA", "display_name": "BANK OF BARODA", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE238A01034": {"name": "AXISBANK", "display_name": "AXIS BANK", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE237A01028": {"name": "KOTAKBANK", "display_name": "KOTAK BANK", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE155A01022": {"name": "TATASTEEL", "display_name": "TATA STEEL", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE205A01025": {"name": "HINDALCO", "display_name": "HINDALCO", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE019A01038": {"name": "JSWSTEEL", "display_name": "JSW STEEL", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE002A01018": {"name": "RELIANCE", "display_name": "RELIANCE IND", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE213A01029": {"name": "ONGC", "display_name": "ONGC", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE242A01010": {"name": "IOC", "display_name": "INDIAN OIL", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE009A01021": {"name": "INFY", "display_name": "INFOSYS", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE075A01022": {"name": "WIPRO", "display_name": "WIPRO", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE854D01024": {"name": "TCS", "display_name": "TCS", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE047A01021": {"name": "HCLTECH", "display_name": "HCL TECH", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE044A01036": {"name": "SUNPHARMA", "display_name": "SUN PHARMA", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE361B01024": {"name": "DIVISLAB", "display_name": "DIVI'S LAB", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE089A01023": {"name": "DRREDDY", "display_name": "DR REDDY", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE154A01025": {"name": "ITC", "display_name": "ITC LTD", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE030A01027": {"name": "HUL", "display_name": "HINDUSTAN UNILEVER", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE216A01030": {"name": "BRITANNIA", "display_name": "BRITANNIA", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE742F01042": {"name": "ADANIPORTS", "display_name": "ADANI PORTS", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE733E01010": {"name": "NTPC", "display_name": "NTPC", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE018A01030": {"name": "LT", "display_name": "L&T", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE280A01028": {"name": "TITAN", "display_name": "TITAN", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE849A01020": {"name": "TRENT", "display_name": "TRENT", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE021A01026": {"name": "ASIANPAINT", "display_name": "ASIAN PAINTS", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE397D01024": {"name": "BHARTIARTL", "display_name": "BHARTI AIRTEL", "type": "stock", "has_options": True, "expiry_type": "monthly"},
    "NSE_EQ|INE296A01024": {"name": "BAJFINANCE", "display_name": "BAJAJ FINANCE", "type": "stock", "has_options": True, "expiry_type": "monthly"}
}

ALL_SYMBOLS = {**INDICES, **FO_STOCKS}

NSE_HOLIDAYS_2025 = [
    '2025-01-26', '2025-03-14', '2025-03-31', '2025-04-10', '2025-04-14', '2025-04-18',
    '2025-05-01', '2025-06-07', '2025-07-07', '2025-08-15', '2025-08-27', '2025-10-02',
    '2025-10-21', '2025-11-01', '2025-11-03', '2025-11-05', '2025-12-25'
]

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

@dataclass
class OIData:
    pcr: float
    support_strike: int
    resistance_strike: int
    strikes_data: List[StrikeData]
    timestamp: datetime

@dataclass
class NewsData:
    headline: str
    sentiment: str
    impact_score: int
    source: str

@dataclass
class MultiTimeframeData:
    df_15m: pd.DataFrame
    spot_price: float
    atr: float

@dataclass
class DeepAnalysis:
    chart_bias: str
    market_structure: str
    pattern_signal: str
    oi_flow_signal: str
    opportunity: str
    confidence: int
    chart_score: int
    oi_score: int
    total_score: int
    entry_price: float
    stop_loss: float
    target_1: float
    target_2: float
    risk_reward: str
    recommended_strike: int
    support_levels: List[float]
    resistance_levels: List[float]
    risk_factors: List[str]
    order_blocks: List[Dict] = field(default_factory=list)
    pcr_value: float = 0.0

# ==================== EXPIRY CALCULATOR ====================
class ExpiryCalculator:
    @staticmethod
    def get_monthly_expiry(symbol_name: str) -> str:
        """Get nearest monthly expiry (Last Thursday for stocks, specific days for indices)"""
        today = datetime.now(IST).date()
        current_time = datetime.now(IST).time()
        
        # Expiry day mapping
        EXPIRY_DAY = {
            "BANKNIFTY": 2,     # Wednesday
            "MIDCPNIFTY": 0,    # Monday
        }
        
        # Get target weekday (default Thursday=3 for stocks)
        target_weekday = EXPIRY_DAY.get(symbol_name, 3)
        
        # Find last occurrence of target day in current month
        last_day = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        days_to_subtract = (last_day.weekday() - target_weekday) % 7
        expiry = last_day - timedelta(days=days_to_subtract)
        
        # If expiry passed, get next month's expiry
        if expiry < today or (expiry == today and current_time >= time(15, 30)):
            next_month = (today.replace(day=28) + timedelta(days=4))
            last_day = (next_month.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            days_to_subtract = (last_day.weekday() - target_weekday) % 7
            expiry = last_day - timedelta(days=days_to_subtract)
        
        return expiry.strftime('%d%b%y').upper()

# ==================== REDIS OI MANAGER ====================
class RedisOIManager:
    @staticmethod
    def save_oi(symbol: str, expiry: str, oi_data: OIData):
        """Save OI data with 3-day expiry"""
        key = f"oi:{symbol}:{expiry}:{oi_data.timestamp.strftime('%Y-%m-%d_%H:%M')}"
        data = {
            "pcr": oi_data.pcr,
            "support": oi_data.support_strike,
            "resistance": oi_data.resistance_strike,
            "strikes": [
                {
                    "strike": s.strike,
                    "ce_oi": s.ce_oi,
                    "pe_oi": s.pe_oi,
                    "ce_volume": s.ce_volume,
                    "pe_volume": s.pe_volume
                } for s in oi_data.strikes_data
            ]
        }
        redis_client.setex(key, 259200, json.dumps(data))  # 3 days
        logger.info(f"  💾 Saved OI to Redis: {key}")
    
    @staticmethod
    def get_comparison_oi(symbol: str, expiry: str, current_time: datetime) -> Optional[OIData]:
        """Get OI from 2 hours ago (or market open if < 2 hours)"""
        # Calculate comparison time
        two_hours_ago = current_time - timedelta(hours=2)
        
        if current_time.time() < time(11, 15):
            # Use market open data
            comparison_time = current_time.replace(hour=9, minute=15)
        else:
            # Round to nearest 15-min interval
            comparison_time = two_hours_ago.replace(
                minute=(two_hours_ago.minute // 15) * 15,
                second=0,
                microsecond=0
            )
        
        # Try current day first
        key = f"oi:{symbol}:{expiry}:{comparison_time.strftime('%Y-%m-%d_%H:%M')}"
        data = redis_client.get(key)
        
        if not data:
            # Try previous trading day (for morning comparisons)
            prev_day = comparison_time - timedelta(days=1)
            while prev_day.strftime('%Y-%m-%d') in NSE_HOLIDAYS_2025 or prev_day.weekday() >= 5:
                prev_day -= timedelta(days=1)
            
            key = f"oi:{symbol}:{expiry}:{prev_day.replace(hour=13, minute=15).strftime('%Y-%m-%d_%H:%M')}"
            data = redis_client.get(key)
        
        if data:
            parsed = json.loads(data)
            return OIData(
                pcr=parsed['pcr'],
                support_strike=parsed['support'],
                resistance_strike=parsed['resistance'],
                strikes_data=[StrikeData(**s) for s in parsed['strikes']],
                timestamp=comparison_time
            )
        
        logger.warning(f"  ⚠️ No comparison OI found for {symbol}")
        return None

# ==================== STRIKE SELECTOR ====================
class StrikeSelector:
    @staticmethod
    def get_top_15_atm_strikes(strikes_data: List[StrikeData], spot_price: float) -> List[StrikeData]:
        """Get top 15 strikes: ATM ±7, sorted by OI+Volume"""
        atm_strike = round(spot_price / 100) * 100
        atm_range = range(atm_strike - 700, atm_strike + 800, 100)
        
        # Filter strikes in ATM range
        relevant = [s for s in strikes_data if s.strike in atm_range]
        
        # Sort by total liquidity
        sorted_strikes = sorted(
            relevant,
            key=lambda x: (x.ce_oi + x.pe_oi + x.ce_volume + x.pe_volume),
            reverse=True
        )
        
        return sorted_strikes[:15]

# ==================== NEWS FETCHER ====================
class NewsFetcher:
    @staticmethod
    def fetch_finnhub_news(symbol_name: str) -> Optional[NewsData]:
        """Fetch latest news from Finnhub"""
        try:
            today = datetime.now(IST).date()
            yesterday = today - timedelta(days=1)
            
            url = f"https://finnhub.io/api/v1/company-news"
            params = {
                "symbol": symbol_name,
                "from": yesterday.strftime('%Y-%m-%d'),
                "to": today.strftime('%Y-%m-%d'),
                "token": FINNHUB_API_KEY
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                news_list = response.json()
                if news_list:
                    latest = news_list[0]
                    return NewsData(
                        headline=latest.get('headline', 'No headline'),
                        sentiment=latest.get('sentiment', 'NEUTRAL'),
                        impact_score=0,  # Not used in scoring
                        source='Finnhub'
                    )
        except Exception as e:
            logger.error(f"  📰 Finnhub error: {e}")
        
        return None

# ==================== CHART GENERATOR ====================
class ChartGenerator:
    @staticmethod
    def create_tradingview_chart(symbol: str, df: pd.DataFrame, analysis: DeepAnalysis, save_path: str):
        """Generate professional TradingView-style chart"""
        # Colors
        BG = '#131722'
        GRID = '#1e222d'
        TEXT = '#d1d4dc'
        GREEN = '#26a69a'
        RED = '#ef5350'
        YELLOW = '#ffd700'
        
        fig, (ax1, ax2) = plt.subplots(
            2, 1, figsize=(16, 10),
            gridspec_kw={'height_ratios': [3, 1]},
            facecolor=BG
        )
        
        # === PRICE CHART ===
        ax1.set_facecolor(BG)
        df_plot = df.tail(150).reset_index(drop=True)
        
        # Candlesticks
        for idx, row in df_plot.iterrows():
            color = GREEN if row['close'] > row['open'] else RED
            
            # Body
            ax1.add_patch(Rectangle(
                (idx, min(row['open'], row['close'])),
                0.6,
                abs(row['close'] - row['open']),
                facecolor=color,
                edgecolor=color,
                alpha=0.8
            ))
            
            # Wicks
            ax1.plot([idx+0.3, idx+0.3], [row['low'], row['high']], 
                     color=color, linewidth=1, alpha=0.6)
        
        # Order Blocks
        for ob in analysis.order_blocks:
            ax1.add_patch(Rectangle(
                (ob['start_idx'], ob['low']),
                ob['end_idx'] - ob['start_idx'],
                ob['high'] - ob['low'],
                facecolor=GREEN if ob['type'] == 'bullish' else RED,
                alpha=0.15,
                edgecolor=GREEN if ob['type'] == 'bullish' else RED,
                linewidth=1.5
            ))
        
        # Support/Resistance
        for support in analysis.support_levels:
            ax1.axhline(support, color=GREEN, linestyle='--', linewidth=1.5, alpha=0.7)
            ax1.text(len(df_plot)*0.02, support, f'  S: ₹{support:.0f}', 
                     color=GREEN, fontsize=10, verticalalignment='bottom')
        
        for resistance in analysis.resistance_levels:
            ax1.axhline(resistance, color=RED, linestyle='--', linewidth=1.5, alpha=0.7)
            ax1.text(len(df_plot)*0.02, resistance, f'  R: ₹{resistance:.0f}', 
                     color=RED, fontsize=10, verticalalignment='top')
        
        # Entry/SL/Targets
        ax1.scatter([len(df_plot)-1], [analysis.entry_price], 
                    color=YELLOW, s=250, marker='D', zorder=5, edgecolors='white', linewidths=2)
        
        ax1.axhline(analysis.stop_loss, color=RED, linewidth=2.5, linestyle=':')
        ax1.axhline(analysis.target_1, color=GREEN, linewidth=2, linestyle=':')
        ax1.axhline(analysis.target_2, color=GREEN, linewidth=2, linestyle=':')
        
        # Annotations
        ax1.text(len(df_plot)*0.98, analysis.entry_price, 
                 f'ENTRY: ₹{analysis.entry_price:.2f}  ', 
                 color=YELLOW, fontsize=11, fontweight='bold', 
                 horizontalalignment='right', verticalalignment='center',
                 bbox=dict(boxstyle='round', facecolor=BG, edgecolor=YELLOW, linewidth=2))
        
        ax1.text(len(df_plot)*0.98, analysis.stop_loss, 
                 f'SL: ₹{analysis.stop_loss:.2f}  ', 
                 color=RED, fontsize=10, horizontalalignment='right')
        
        ax1.text(len(df_plot)*0.98, analysis.target_1, 
                 f'T1: ₹{analysis.target_1:.2f}  ', 
                 color=GREEN, fontsize=10, horizontalalignment='right')
        
        # Pattern name annotation
        if analysis.pattern_signal:
            ax1.text(len(df_plot)*0.5, df_plot['high'].max() * 0.98, 
                     analysis.pattern_signal.upper(),
                     color=YELLOW, fontsize=12, fontweight='bold',
                     horizontalalignment='center',
                     bbox=dict(boxstyle='round', facecolor=BG, edgecolor=YELLOW, alpha=0.8))
        
        # PCR value (top-right)
        ax1.text(len(df_plot)*0.98, df_plot['high'].max() * 0.98,
                 f'PCR: {analysis.pcr_value:.2f}',
                 color=TEXT, fontsize=11, fontweight='bold',
                 horizontalalignment='right', verticalalignment='top',
                 bbox=dict(boxstyle='round', facecolor=GRID, alpha=0.9))
        
        # Title
        ax1.set_title(
            f'{symbol} | 15-Min | {analysis.chart_bias.upper()} | Score: {analysis.total_score}/100',
            color=TEXT, fontsize=16, fontweight='bold', pad=20
        )
        
        ax1.grid(True, color=GRID, alpha=0.3, linestyle='-', linewidth=0.5)
        ax1.tick_params(colors=TEXT, labelsize=10)
        ax1.set_ylabel('Price (₹)', color=TEXT, fontsize=12)
        
        # === VOLUME CHART ===
        ax2.set_facecolor(BG)
        colors = [GREEN if df_plot.iloc[i]['close'] > df_plot.iloc[i]['open'] else RED 
                  for i in range(len(df_plot))]
        ax2.bar(range(len(df_plot)), df_plot['volume'], color=colors, alpha=0.6, width=0.8)
        ax2.set_ylabel('Volume', color=TEXT, fontsize=12)
        ax2.tick_params(colors=TEXT, labelsize=10)
        ax2.grid(True, color=GRID, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=150, facecolor=BG, edgecolor='none')
        plt.close()
        logger.info(f"  📊 Chart saved: {save_path}")

# ==================== AI ANALYZER ====================
class AIAnalyzer:
    @staticmethod
    def extract_json(content: str) -> Optional[Dict]:
        """Extract JSON from AI response"""
        try:
            content = re.sub(r'```json\s*|\s*```', '', content)
            return json.loads(content)
        except json.JSONDecodeError:
            match = re.search(r'\{(?:[^{}]|(?:\{[^{}]*\}))*\}', content, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))
                except:
                    pass
        logger.error("Failed to extract JSON")
        return None
    
    @staticmethod
    def validate_targets(opportunity: str, entry: float, sl: float, t1: float, t2: float) -> bool:
        """Validate R:R ratio"""
        if opportunity == "WAIT":
            return True
        
        if opportunity == "CE_BUY":
            if not (t2 > t1 > entry > sl):
                return False
            risk = entry - sl
            reward = t1 - entry
        elif opportunity == "PE_BUY":
            if not (t2 < t1 < entry < sl):
                return False
            risk = sl - entry
            reward = entry - t1
        else:
            return False
        
        rr = reward / risk if risk > 0 else 0
        if rr < 1.5:
            logger.warning(f"  ⚠️ Poor R:R: {rr:.2f}")
            return False
        
        logger.info(f"  ✅ R:R: {rr:.2f}")
        return True
    
    @staticmethod
    def deep_analysis(symbol: str, mtf_data: MultiTimeframeData, 
                     current_oi: OIData, prev_oi: Optional[OIData],
                     news_data: Optional[NewsData]) -> Optional[DeepAnalysis]:
        """Main AI analysis with Charts + OI hybrid scoring"""
        try:
            # Prepare candle data (last 150)
            candle_df = mtf_data.df_15m.tail(150).reset_index()
            candle_df['timestamp'] = candle_df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
            candles_json = candle_df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].to_json(orient='records')
            
            # OI comparison table
            oi_section = f"""
**OI ANALYSIS (Score: 0-50)**
- PCR: {current_oi.pcr:.2f}
- Support: {current_oi.support_strike}
- Resistance: {current_oi.resistance_strike}
"""
            
            if prev_oi and prev_oi.strikes_data:
                oi_section += "\n**Top 15 Strikes OI Change:**\n| Strike | CE OI Δ | PE OI Δ |\n|:---:|:---:|:---:|\n"
                
                prev_map = {s.strike: s for s in prev_oi.strikes_data}
                for strike_data in current_oi.strikes_data:
                    prev_strike = prev_map.get(strike_data.strike)
                    if prev_strike:
                        ce_change = strike_data.ce_oi - prev_strike.ce_oi
                        pe_change = strike_data.pe_oi - prev_strike.pe_oi
                        ce_str = f"+{ce_change:,}" if ce_change > 0 else f"{ce_change:,}"
                        pe_str = f"+{pe_change:,}" if pe_change > 0 else f"{pe_change:,}"
                        oi_section += f"| {strike_data.strike} | {ce_str} | {pe_str} |\n"
            
            # News section (optional)
            news_text = ""
            if news_data:
                news_text = f"\n**NEWS (For context only, not scored):**\n- {news_data.headline}\n"
            
            # AI Prompt
            prompt = f"""You are an institutional F&O trader. Analyze {symbol} using Charts + OI hybrid model.

**DATA INPUT:**
**1. PRICE DATA (150 candles, 15-min):**
{candles_json}

**2. CURRENT STATUS:**
- Spot: ₹{mtf_data.spot_price:.2f}
- ATR: {mtf_data.atr:.2f}
- Time: {datetime.now(IST).strftime('%d-%b %H:%M')}

{oi_section}
{news_text}

**ANALYSIS FRAMEWORK:**

**STEP 1: CHART ANALYSIS (50 points)**
- Trend: Uptrend/Downtrend/Sideways
- Market Structure: HH/HL or LH/LL?
- Key Support/Resistance levels
- BOS (Break of Structure) or CHoCH (Change of Character)?
- Chart Patterns: Triangle, H&S, Range, etc.
- Candlestick patterns at key levels
- Volume confirmation
- Order Blocks (bullish/bearish zones)
- Fair Value Gaps (FVG)
- **Score out of 50** based on clarity + confluence

**STEP 2: OI ANALYSIS (50 points)**
- PCR interpretation (bullish/bearish?)
- Strike-wise OI changes (building/unwinding?)
- Support/Resistance from max OI
- Does OI confirm chart direction?
- **Score out of 50** based on OI strength + alignment

**STEP 3: HYBRID CONFLUENCE**
- Do Charts + OI agree or diverge?
- Example: Chart shows breakout + CE OI increasing at resistance = Strong signal
- Example: Chart bullish but PE unwinding = Weak signal

**STEP 4: TRADE SETUP**
- If total score ≥75, recommend CE_BUY or PE_BUY
- If < 75, recommend WAIT
- Entry, SL, T1, T2 with R:R ≥ 1:2
- Identify Order Blocks for chart annotation

**OUTPUT (JSON ONLY):**
```json
{{
  "chart_bias": "Bullish/Bearish/Neutral",
  "market_structure": "Making HH/HL in uptrend",
  "pattern_signal": "Bullish Flag Breakout",
  "oi_flow_signal": "Support building at 48000 PE",
  "opportunity": "CE_BUY/PE_BUY/WAIT",
  "confidence": 85,
  "chart_score": 42,
  "oi_score": 38,
  "total_score": 80,
  "entry_price": {mtf_data.spot_price:.2f},
  "stop_loss": 0.0,
  "target_1": 0.0,
  "target_2": 0.0,
  "risk_reward": "1:2.5",
  "recommended_strike": {round(mtf_data.spot_price/100)*100},
  "support_levels": [0.0, 0.0],
  "resistance_levels": [0.0, 0.0],
  "risk_factors": ["Risk 1", "Risk 2"],
  "order_blocks": [
    {{"type": "bullish", "start_idx": 120, "end_idx": 125, "low": 48000, "high": 48100}},
    {{"type": "bearish", "start_idx": 140, "end_idx": 145, "low": 48400, "high": 48500}}
  ]
}}
```"""

            # Call DeepSeek API
            url = "https://api.deepseek.com/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": "You are an expert F&O trader. Respond ONLY in JSON format."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.2,
                "max_tokens": 2500
            }
            
            logger.info("  🧠 Analyzing with AI...")
            response = requests.post(url, json=payload, headers=headers, timeout=120)
            
            if response.status_code != 200:
                logger.error(f"AI API error: {response.status_code}")
                return None
            
            ai_content = response.json()['choices'][0]['message']['content']
            analysis_dict = AIAnalyzer.extract_json(ai_content)
            
            if not analysis_dict:
                return None
            
            logger.info(f"  🧠 AI Result: {analysis_dict.get('opportunity')} | Chart: {analysis_dict.get('chart_score')}/50 | OI: {analysis_dict.get('oi_score')}/50 | Total: {analysis_dict.get('total_score')}/100")
            
            # Validate
            opportunity = analysis_dict.get('opportunity', 'WAIT')
            if opportunity != "WAIT":
                if not AIAnalyzer.validate_targets(
                    opportunity,
                    analysis_dict.get('entry_price', 0),
                    analysis_dict.get('stop_loss', 0),
                    analysis_dict.get('target_1', 0),
                    analysis_dict.get('target_2', 0)
                ):
                    return None
            
            # Add PCR to analysis
            analysis_dict['pcr_value'] = current_oi.pcr
            
            return DeepAnalysis(**analysis_dict)
            
        except Exception as e:
            logger.error(f"AI analysis error: {e}")
            traceback.print_exc()
            return None

# ==================== DATA FETCHER ====================
class UpstoxDataFetcher:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
    
    def get_historical_data(self, instrument_key: str, interval: str, days: int = 5) -> pd.DataFrame:
        """Fetch historical candle data"""
        try:
            to_date = datetime.now(IST)
            from_date = to_date - timedelta(days=days)
            
            url = "https://api.upstox.com/v2/historical-candle"
            full_url = f"{url}/{instrument_key}/{interval}/{to_date.strftime('%Y-%m-%d')}/{from_date.strftime('%Y-%m-%d')}"
            
            response = requests.get(full_url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'candles' in data['data']:
                    df = pd.DataFrame(
                        data['data']['candles'],
                        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
                    )
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df = df.sort_values('timestamp').reset_index(drop=True)
                    return df
            
            logger.error(f"Historical data fetch failed: {response.status_code}")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Historical data error: {e}")
            return pd.DataFrame()
    
    def get_ltp(self, instrument_key: str) -> float:
        """Get Last Traded Price"""
        try:
            url = "https://api.upstox.com/v2/market-quote/ltp"
            params = {"instrument_key": instrument_key}
            
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return data['data'][instrument_key]['last_price']
            
            return 0.0
            
        except Exception as e:
            logger.error(f"LTP error: {e}")
            return 0.0
    
    def get_option_chain(self, instrument_key: str, expiry: str) -> List[StrikeData]:
        """Fetch option chain data"""
        try:
            url = "https://api.upstox.com/v2/option/chain"
            params = {
                "instrument_key": instrument_key,
                "expiry_date": expiry
            }
            
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                strikes = []
                
                for item in data.get('data', []):
                    strike_price = item.get('strike_price', 0)
                    call_data = item.get('call_options', {})
                    put_data = item.get('put_options', {})
                    
                    strikes.append(StrikeData(
                        strike=int(strike_price),
                        ce_oi=call_data.get('market_data', {}).get('oi', 0),
                        pe_oi=put_data.get('market_data', {}).get('oi', 0),
                        ce_volume=call_data.get('market_data', {}).get('volume', 0),
                        pe_volume=put_data.get('market_data', {}).get('volume', 0),
                        ce_price=call_data.get('market_data', {}).get('ltp', 0),
                        pe_price=put_data.get('market_data', {}).get('ltp', 0)
                    ))
                
                return strikes
            
            logger.error(f"Option chain fetch failed: {response.status_code}")
            return []
            
        except Exception as e:
            logger.error(f"Option chain error: {e}")
            return []

# ==================== MAIN BOT ====================
class HybridBot:
    def __init__(self):
        self.data_fetcher = UpstoxDataFetcher(UPSTOX_ACCESS_TOKEN)
        self.telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.processed_signals = set()
    
    async def send_telegram_alert(self, symbol: str, analysis: DeepAnalysis, 
                                  chart_path: str, news_headline: Optional[str] = None):
        """Send chart + text alert to Telegram"""
        try:
            # 1. Send Chart first
            with open(chart_path, 'rb') as photo:
                await self.telegram_bot.send_photo(
                    chat_id=TELEGRAM_CHAT_ID,
                    photo=photo
                )
            
            # 2. Send Text alert
            message = f"""
🚨 **{symbol} {analysis.opportunity} SIGNAL**

📊 **Total Score:** {analysis.total_score}/100
🎯 **Confidence:** {analysis.confidence}%

📈 **Chart Analysis:** {analysis.chart_score}/50
   └─ {analysis.chart_bias} | {analysis.pattern_signal}

📊 **OI Analysis:** {analysis.oi_score}/50
   └─ {analysis.oi_flow_signal}
   └─ PCR: {analysis.pcr_value:.2f}
"""
            
            if news_headline:
                message += f"\n📰 **Latest News:** {news_headline}\n"
            
            message += f"""
💰 **Trade Setup:**
Entry: ₹{analysis.entry_price:.2f}
Stop Loss: ₹{analysis.stop_loss:.2f}
Target 1: ₹{analysis.target_1:.2f}
Target 2: ₹{analysis.target_2:.2f}
Risk:Reward → {analysis.risk_reward}

📍 **Strike:** {analysis.recommended_strike}
⚠️ **Risks:** {', '.join(analysis.risk_factors[:2])}

🕐 Time: {datetime.now(IST).strftime('%d-%b %H:%M:%S')}
"""
            
            await self.telegram_bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
                parse_mode='Markdown'
            )
            
            logger.info(f"  ✅ Telegram alert sent for {symbol}")
            
        except Exception as e:
            logger.error(f"Telegram error: {e}")
    
    async def analyze_symbol(self, instrument_key: str, symbol_info: Dict):
        """Main analysis pipeline for a symbol"""
        try:
            symbol_name = symbol_info['name']
            display_name = symbol_info['display_name']
            
            logger.info(f"\n{'='*60}")
            logger.info(f"🔍 Analyzing: {display_name} ({symbol_name})")
            logger.info(f"{'='*60}")
            
            # 1. Get expiry
            expiry = ExpiryCalculator.get_monthly_expiry(symbol_name)
            logger.info(f"  📅 Expiry: {expiry}")
            
            # 2. Fetch candle data
            df_15m = self.data_fetcher.get_historical_data(instrument_key, "15minute", days=5)
            if df_15m.empty:
                logger.warning(f"  ⚠️ No candle data for {symbol_name}")
                return
            
            # 3. Calculate ATR
            df_15m['tr'] = df_15m[['high', 'low', 'close']].apply(
                lambda x: max(x['high'] - x['low'], 
                            abs(x['high'] - x['close']), 
                            abs(x['low'] - x['close'])), axis=1
            )
            atr = df_15m['tr'].rolling(14).mean().iloc[-1]
            
            # 4. Get spot price
            spot_price = self.data_fetcher.get_ltp(instrument_key)
            if spot_price == 0:
                spot_price = df_15m['close'].iloc[-1]
            
            logger.info(f"  💹 Spot: ₹{spot_price:.2f} | ATR: {atr:.2f}")
            
            # 5. Fetch option chain
            all_strikes = self.data_fetcher.get_option_chain(instrument_key, expiry)
            if not all_strikes:
                logger.warning(f"  ⚠️ No option chain data")
                return
            
            # 6. Select top 15 ATM strikes
            top_15_strikes = StrikeSelector.get_top_15_atm_strikes(all_strikes, spot_price)
            logger.info(f"  📊 Selected {len(top_15_strikes)} strikes (ATM ±7)")
            
            # 7. Calculate PCR and key levels
            total_ce_oi = sum(s.ce_oi for s in top_15_strikes)
            total_pe_oi = sum(s.pe_oi for s in top_15_strikes)
            pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
            
            max_ce_strike = max(top_15_strikes, key=lambda x: x.ce_oi).strike
            max_pe_strike = max(top_15_strikes, key=lambda x: x.pe_oi).strike
            
            current_oi = OIData(
                pcr=pcr,
                support_strike=max_pe_strike,
                resistance_strike=max_ce_strike,
                strikes_data=top_15_strikes,
                timestamp=datetime.now(IST)
            )
            
            logger.info(f"  📊 PCR: {pcr:.2f} | Support: {max_pe_strike} | Resistance: {max_ce_strike}")
            
            # 8. Save current OI to Redis
            RedisOIManager.save_oi(symbol_name, expiry, current_oi)
            
            # 9. Get comparison OI (2 hours ago)
            prev_oi = RedisOIManager.get_comparison_oi(symbol_name, expiry, datetime.now(IST))
            
            # 10. Fetch news (optional)
            news_data = NewsFetcher.fetch_finnhub_news(symbol_name)
            if news_data:
                logger.info(f"  📰 News: {news_data.headline[:60]}...")
            
            # 11. AI Analysis
            mtf_data = MultiTimeframeData(
                df_15m=df_15m,
                spot_price=spot_price,
                atr=atr
            )
            
            analysis = AIAnalyzer.deep_analysis(
                symbol_name, mtf_data, current_oi, prev_oi, news_data
            )
            
            if not analysis:
                logger.info(f"  ⏸️ No valid analysis for {symbol_name}")
                return
            
            # 12. Check alert threshold
            if analysis.total_score >= 75 and analysis.confidence >= 75:
                signal_key = f"{symbol_name}_{analysis.opportunity}_{datetime.now(IST).strftime('%Y%m%d_%H')}"
                
                if signal_key not in self.processed_signals:
                    logger.info(f"  🚨 ALERT TRIGGERED! Score: {analysis.total_score}, Conf: {analysis.confidence}%")
                    
                    # 13. Generate chart
                    chart_path = f"/tmp/{symbol_name}_chart.png"
                    ChartGenerator.create_tradingview_chart(
                        display_name, df_15m, analysis, chart_path
                    )
                    
                    # 14. Send Telegram alert
                    await self.send_telegram_alert(
                        display_name, 
                        analysis, 
                        chart_path,
                        news_data.headline if news_data else None
                    )
                    
                    self.processed_signals.add(signal_key)
                else:
                    logger.info(f"  ⏭️ Signal already sent this hour")
            else:
                logger.info(f"  ⏸️ Score {analysis.total_score} or Confidence {analysis.confidence}% below threshold")
            
        except Exception as e:
            logger.error(f"Analysis error for {symbol_info.get('name')}: {e}")
            traceback.print_exc()
    
    async def run_scanner(self):
        """Main scanner loop"""
        logger.info("\n" + "="*80)
        logger.info("🚀 HYBRID BOT v22.0 STARTED")
        logger.info("="*80)
        
        while True:
            try:
                now = datetime.now(IST)
                current_time = now.time()
                
                # Check if market is open
                if current_time < time(9, 15) or current_time > time(15, 30):
                    logger.info(f"⏸️ Market closed. Next scan at 9:15 AM")
                    await asyncio.sleep(300)  # 5 min
                    continue
                
                # Check if today is holiday
                if now.strftime('%Y-%m-%d') in NSE_HOLIDAYS_2025 or now.weekday() >= 5:
                    logger.info(f"📅 Market holiday. Pausing...")
                    await asyncio.sleep(3600)  # 1 hour
                    continue
                
                logger.info(f"\n🔄 Starting scan cycle at {now.strftime('%H:%M:%S')}")
                
                # Scan all symbols
                for instrument_key, symbol_info in ALL_SYMBOLS.items():
                    if symbol_info.get('has_options'):
                        await self.analyze_symbol(instrument_key, symbol_info)
                        await asyncio.sleep(2)  # Rate limiting
                
                logger.info(f"\n✅ Scan cycle completed. Waiting 15 minutes...")
                await asyncio.sleep(900)  # 15 min
                
            except Exception as e:
                logger.error(f"Scanner error: {e}")
                traceback.print_exc()
                await asyncio.sleep(60)

# ==================== ENTRY POINT ====================
if __name__ == "__main__":
    bot = HybridBot()
    asyncio.run(bot.run_scanner())
