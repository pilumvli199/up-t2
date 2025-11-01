#!/usr/bin/env python3
"""
HYBRID TRADING BOT v23.0 - COMPLETE PROFESSIONAL
=================================================
✅ Multi-Timeframe Analysis (1H + 15M + 5M)
✅ Enhanced TradingView Charts with Info Box
✅ CMP Label + Professional Styling
✅ News Integration (Finnhub)
✅ Redis OI Comparison (3-day expiry)
✅ Startup Status + Alert Format
"""

import os
import asyncio
import requests
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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('hybrid_bot.log')
    ]
)
logger = logging.getLogger(__name__)

# API Keys
UPSTOX_ACCESS_TOKEN = os.getenv('UPSTOX_ACCESS_TOKEN', 'your_token')
DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY', 'your_key')
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY', 'your_key')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', 'your_token')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', 'your_chat_id')

# Redis Connection
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
redis_client = redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)

# ==================== SYMBOLS CONFIG ====================
INDICES = {
    "NSE_INDEX|Nifty Bank": {"name": "BANKNIFTY", "display_name": "BANK NIFTY", "type": "index"},
    "NSE_INDEX|Nifty Midcap Select": {"name": "MIDCPNIFTY", "display_name": "MIDCAP NIFTY", "type": "index"}
}

FO_STOCKS = {
    "NSE_EQ|INE467B01029": {"name": "TATAMOTORS", "display_name": "TATA MOTORS", "type": "stock"},
    "NSE_EQ|INE585B01010": {"name": "MARUTI", "display_name": "MARUTI SUZUKI", "type": "stock"},
    "NSE_EQ|INE208A01029": {"name": "ASHOKLEY", "display_name": "ASHOK LEYLAND", "type": "stock"},
    "NSE_EQ|INE494B01023": {"name": "TVSMOTOR", "display_name": "TVS MOTOR", "type": "stock"},
    "NSE_EQ|INE101A01026": {"name": "M&M", "display_name": "M&M", "type": "stock"},
    "NSE_EQ|INE917I01010": {"name": "BAJAJ-AUTO", "display_name": "BAJAJ AUTO", "type": "stock"},
    "NSE_EQ|INE040A01034": {"name": "HDFCBANK", "display_name": "HDFC BANK", "type": "stock"},
    "NSE_EQ|INE090A01021": {"name": "ICICIBANK", "display_name": "ICICI BANK", "type": "stock"},
    "NSE_EQ|INE062A01020": {"name": "SBIN", "display_name": "STATE BANK", "type": "stock"},
    "NSE_EQ|INE028A01039": {"name": "BANKBARODA", "display_name": "BANK OF BARODA", "type": "stock"},
    "NSE_EQ|INE238A01034": {"name": "AXISBANK", "display_name": "AXIS BANK", "type": "stock"},
    "NSE_EQ|INE237A01028": {"name": "KOTAKBANK", "display_name": "KOTAK BANK", "type": "stock"},
    "NSE_EQ|INE155A01022": {"name": "TATASTEEL", "display_name": "TATA STEEL", "type": "stock"},
    "NSE_EQ|INE205A01025": {"name": "HINDALCO", "display_name": "HINDALCO", "type": "stock"},
    "NSE_EQ|INE019A01038": {"name": "JSWSTEEL", "display_name": "JSW STEEL", "type": "stock"},
    "NSE_EQ|INE002A01018": {"name": "RELIANCE", "display_name": "RELIANCE IND", "type": "stock"},
    "NSE_EQ|INE213A01029": {"name": "ONGC", "display_name": "ONGC", "type": "stock"},
    "NSE_EQ|INE242A01010": {"name": "IOC", "display_name": "INDIAN OIL", "type": "stock"},
    "NSE_EQ|INE009A01021": {"name": "INFY", "display_name": "INFOSYS", "type": "stock"},
    "NSE_EQ|INE075A01022": {"name": "WIPRO", "display_name": "WIPRO", "type": "stock"},
    "NSE_EQ|INE854D01024": {"name": "TCS", "display_name": "TCS", "type": "stock"},
    "NSE_EQ|INE047A01021": {"name": "HCLTECH", "display_name": "HCL TECH", "type": "stock"},
    "NSE_EQ|INE044A01036": {"name": "SUNPHARMA", "display_name": "SUN PHARMA", "type": "stock"},
    "NSE_EQ|INE361B01024": {"name": "DIVISLAB", "display_name": "DIVI'S LAB", "type": "stock"},
    "NSE_EQ|INE089A01023": {"name": "DRREDDY", "display_name": "DR REDDY", "type": "stock"},
    "NSE_EQ|INE154A01025": {"name": "ITC", "display_name": "ITC LTD", "type": "stock"},
    "NSE_EQ|INE030A01027": {"name": "HUL", "display_name": "HINDUSTAN UNILEVER", "type": "stock"},
    "NSE_EQ|INE216A01030": {"name": "BRITANNIA", "display_name": "BRITANNIA", "type": "stock"},
    "NSE_EQ|INE742F01042": {"name": "ADANIPORTS", "display_name": "ADANI PORTS", "type": "stock"},
    "NSE_EQ|INE733E01010": {"name": "NTPC", "display_name": "NTPC", "type": "stock"},
    "NSE_EQ|INE018A01030": {"name": "LT", "display_name": "L&T", "type": "stock"},
    "NSE_EQ|INE280A01028": {"name": "TITAN", "display_name": "TITAN", "type": "stock"},
    "NSE_EQ|INE849A01020": {"name": "TRENT", "display_name": "TRENT", "type": "stock"},
    "NSE_EQ|INE021A01026": {"name": "ASIANPAINT", "display_name": "ASIAN PAINTS", "type": "stock"},
    "NSE_EQ|INE397D01024": {"name": "BHARTIARTL", "display_name": "BHARTI AIRTEL", "type": "stock"},
    "NSE_EQ|INE296A01024": {"name": "BAJFINANCE", "display_name": "BAJAJ FINANCE", "type": "stock"}
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
    ce_oi_change_pct: float = 0.0
    pe_oi_change_pct: float = 0.0

@dataclass
class NewsData:
    headline: str
    sentiment: str
    impact_score: int
    source: str

@dataclass
class MultiTimeframeData:
    df_1h: pd.DataFrame
    df_15m: pd.DataFrame
    df_5m: pd.DataFrame
    spot_price: float
    atr: float

@dataclass
class TimeframeBias:
    tf_1h: str
    tf_15m: str
    tf_5m: str
    tf_1h_confidence: int
    tf_15m_confidence: int
    alignment: str

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
    timeframe_bias: Optional[TimeframeBias] = None
    news_headline: Optional[str] = None
    oi_sentiment: str = "NEUTRAL"

# ==================== EXPIRY CALCULATOR ====================
class ExpiryCalculator:
    @staticmethod
    def get_monthly_expiry(symbol_name: str) -> str:
        today = datetime.now(IST).date()
        current_time = datetime.now(IST).time()
        
        EXPIRY_DAY = {"BANKNIFTY": 2, "MIDCPNIFTY": 0}
        target_weekday = EXPIRY_DAY.get(symbol_name, 3)
        
        last_day = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        days_to_subtract = (last_day.weekday() - target_weekday) % 7
        expiry = last_day - timedelta(days=days_to_subtract)
        
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
        key = f"oi:{symbol}:{expiry}:{oi_data.timestamp.strftime('%Y-%m-%d_%H:%M')}"
        data = {
            "pcr": oi_data.pcr,
            "support": oi_data.support_strike,
            "resistance": oi_data.resistance_strike,
            "ce_oi_change_pct": oi_data.ce_oi_change_pct,
            "pe_oi_change_pct": oi_data.pe_oi_change_pct,
            "strikes": [{"strike": s.strike, "ce_oi": s.ce_oi, "pe_oi": s.pe_oi, 
                        "ce_volume": s.ce_volume, "pe_volume": s.pe_volume} 
                       for s in oi_data.strikes_data]
        }
        redis_client.setex(key, 259200, json.dumps(data))
        logger.info(f"  💾 Redis saved: {key}")
    
    @staticmethod
    def get_comparison_oi(symbol: str, expiry: str, current_time: datetime) -> Optional[OIData]:
        two_hours_ago = current_time - timedelta(hours=2)
        
        if current_time.time() < time(11, 15):
            comparison_time = current_time.replace(hour=9, minute=15)
        else:
            comparison_time = two_hours_ago.replace(
                minute=(two_hours_ago.minute // 15) * 15, second=0, microsecond=0
            )
        
        key = f"oi:{symbol}:{expiry}:{comparison_time.strftime('%Y-%m-%d_%H:%M')}"
        data = redis_client.get(key)
        
        if not data:
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
                ce_oi_change_pct=parsed.get('ce_oi_change_pct', 0),
                pe_oi_change_pct=parsed.get('pe_oi_change_pct', 0),
                strikes_data=[StrikeData(
                    strike=s['strike'], ce_oi=s['ce_oi'], pe_oi=s['pe_oi'],
                    ce_volume=s['ce_volume'], pe_volume=s['pe_volume'],
                    ce_price=0, pe_price=0
                ) for s in parsed['strikes']],
                timestamp=comparison_time
            )
        
        return None

# ==================== STRIKE SELECTOR ====================
class StrikeSelector:
    @staticmethod
    def get_top_15_atm_strikes(strikes_data: List[StrikeData], spot_price: float) -> List[StrikeData]:
        atm_strike = round(spot_price / 100) * 100
        atm_range = range(atm_strike - 700, atm_strike + 800, 100)
        relevant = [s for s in strikes_data if s.strike in atm_range]
        return sorted(relevant, key=lambda x: (x.ce_oi + x.pe_oi + x.ce_volume + x.pe_volume), reverse=True)[:15]

# ==================== NEWS FETCHER ====================
class NewsFetcher:
    @staticmethod
    def fetch_finnhub_news(symbol_name: str) -> Optional[NewsData]:
        try:
            today = datetime.now(IST).date()
            yesterday = today - timedelta(days=1)
            
            response = requests.get(
                "https://finnhub.io/api/v1/company-news",
                params={"symbol": symbol_name, "from": yesterday.strftime('%Y-%m-%d'),
                       "to": today.strftime('%Y-%m-%d'), "token": FINNHUB_API_KEY},
                timeout=10
            )
            
            if response.status_code == 200:
                news_list = response.json()
                if news_list:
                    latest = news_list[0]
                    sentiment = latest.get('sentiment', 'neutral').upper()
                    impact = 25 if sentiment == 'POSITIVE' else (10 if sentiment == 'NEUTRAL' else 0)
                    return NewsData(
                        headline=latest.get('headline', 'No headline')[:100],
                        sentiment=sentiment,
                        impact_score=impact,
                        source='Finnhub'
                    )
        except Exception as e:
            logger.error(f"  📰 News fetch error: {e}")
        return None

# ==================== MULTI-TIMEFRAME PROCESSOR ====================
class MultiTimeframeProcessor:
    @staticmethod
    def resample_to_timeframe(df_1m: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """Resample 1-min data to different timeframes"""
        df = df_1m.copy()
        df.set_index('timestamp', inplace=True)
        
        resampled = df.resample(timeframe).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna().reset_index()
        
        return resampled
    
    @staticmethod
    def get_timeframe_bias(df: pd.DataFrame) -> Tuple[str, int]:
        """Determine bias and confidence for a timeframe"""
        if len(df) < 20:
            return "NEUTRAL", 50
        
        df_tail = df.tail(20)
        closes = df_tail['close'].values
        
        # Calculate trend
        sma_20 = closes.mean()
        current_price = closes[-1]
        price_vs_sma = ((current_price - sma_20) / sma_20) * 100
        
        # Higher highs / Lower lows
        recent_highs = df_tail['high'].tail(10).values
        hh_count = sum(1 for i in range(1, len(recent_highs)) if recent_highs[i] > recent_highs[i-1])
        
        recent_lows = df_tail['low'].tail(10).values
        ll_count = sum(1 for i in range(1, len(recent_lows)) if recent_lows[i] < recent_lows[i-1])
        
        # Determine bias
        if price_vs_sma > 1 and hh_count >= 6:
            bias = "BULLISH"
            confidence = min(95, 60 + int(price_vs_sma * 5))
        elif price_vs_sma < -1 and ll_count >= 6:
            bias = "BEARISH"
            confidence = min(95, 60 + int(abs(price_vs_sma) * 5))
        else:
            bias = "NEUTRAL"
            confidence = 50
        
        return bias, confidence

# ==================== CHART GENERATOR (ENHANCED) ====================
class ChartGenerator:
    @staticmethod
    def create_professional_chart(symbol: str, df: pd.DataFrame, analysis: DeepAnalysis, 
                                 spot_price: float, save_path: str):
        """Generate professional TradingView-style chart with info box"""
        BG = '#131722'
        GRID = '#1e222d'
        TEXT = '#d1d4dc'
        GREEN = '#26a69a'
        RED = '#ef5350'
        YELLOW = '#ffd700'
        BLUE = '#2962ff'
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 10), gridspec_kw={'height_ratios': [3, 1]}, facecolor=BG)
        
        # === PRICE CHART ===
        ax1.set_facecolor(BG)
        df_plot = df.tail(150).reset_index(drop=True)
        
        # Candlesticks
        for idx, row in df_plot.iterrows():
            color = GREEN if row['close'] > row['open'] else RED
            ax1.add_patch(Rectangle((idx, min(row['open'], row['close'])), 0.6,
                                   abs(row['close'] - row['open']), facecolor=color, edgecolor=color, alpha=0.8))
            ax1.plot([idx+0.3, idx+0.3], [row['low'], row['high']], color=color, linewidth=1, alpha=0.6)
        
        # Order Blocks
        for ob in analysis.order_blocks:
            ax1.add_patch(Rectangle((ob['start_idx'], ob['low']), ob['end_idx'] - ob['start_idx'],
                                   ob['high'] - ob['low'], facecolor=GREEN if ob['type'] == 'bullish' else RED,
                                   alpha=0.15, edgecolor=GREEN if ob['type'] == 'bullish' else RED, linewidth=1.5))
        
        # Support/Resistance
        for support in analysis.support_levels:
            ax1.axhline(support, color=GREEN, linestyle='--', linewidth=1.5, alpha=0.7)
            ax1.text(len(df_plot)*0.98, support, f'S:₹{support:.1f}  ', color=GREEN, fontsize=10, 
                    ha='right', va='bottom', bbox=dict(boxstyle='round', facecolor=BG, alpha=0.7))
        
        for resistance in analysis.resistance_levels:
            ax1.axhline(resistance, color=RED, linestyle='--', linewidth=1.5, alpha=0.7)
            ax1.text(len(df_plot)*0.98, resistance, f'R:₹{resistance:.1f}  ', color=RED, fontsize=10,
                    ha='right', va='top', bbox=dict(boxstyle='round', facecolor=BG, alpha=0.7))
        
        # Entry/SL/Targets
        ax1.scatter([len(df_plot)-1], [analysis.entry_price], color=YELLOW, s=300, marker='D', 
                   zorder=5, edgecolors='white', linewidths=2.5)
        ax1.axhline(analysis.stop_loss, color=RED, linewidth=2.5, linestyle=':')
        ax1.axhline(analysis.target_1, color=GREEN, linewidth=2, linestyle=':')
        ax1.axhline(analysis.target_2, color=GREEN, linewidth=2, linestyle=':')
        
        # Entry annotation
        ax1.text(len(df_plot)*0.97, analysis.entry_price, f'ENTRY: ₹{analysis.entry_price:.2f}  ',
                color=YELLOW, fontsize=11, fontweight='bold', ha='right', va='center',
                bbox=dict(boxstyle='round', facecolor=BG, edgecolor=YELLOW, linewidth=2))
        
        # Pattern annotation
        if analysis.pattern_signal:
            ax1.text(len(df_plot)*0.5, df_plot['high'].max() * 0.995, analysis.pattern_signal.upper(),
                    color=YELLOW, fontsize=12, fontweight='bold', ha='center',
                    bbox=dict(boxstyle='round', facecolor=BG, edgecolor=YELLOW, alpha=0.9))
        
        # CMP Label (right side)
        ax1.text(len(df_plot)-1, spot_price, f'  CMP: ₹{spot_price:.1f}', fontsize=11, color='white',
                fontweight='bold', bbox=dict(boxstyle='round', facecolor=BLUE, edgecolor='white', linewidth=2),
                va='center')
        
        # INFO BOX (Left side)
        tf_bias = analysis.timeframe_bias
        info_text = f"""{'🔴 PE_BUY' if analysis.opportunity == 'PE_BUY' else '🟢 CE_BUY'} | Conf: {analysis.confidence}%
Reason: {tf_bias.tf_1h} ({tf_bias.tf_1h_confidence}%) + {tf_bias.tf_15m}
TF: {tf_bias.alignment}
{'📰' if analysis.news_headline else '📰'} News: {analysis.oi_sentiment} ({analysis.oi_score})

1H: {tf_bias.tf_1h}
15M: {tf_bias.tf_15m}
5M: ₹{spot_price:.1f}

OI (30min):
PCR: {analysis.pcr_value:.2f}
Sentiment: {analysis.oi_sentiment}
CE: {analysis.chart_score/50*100:.1f}%
PE: {analysis.oi_score/50*100:.1f}%

Entry: ₹{analysis.entry_price:.1f}
SL: ₹{analysis.stop_loss:.1f}
T1: ₹{analysis.target_1:.1f}
T2: ₹{analysis.target_2:.1f}
RR: {analysis.risk_reward}"""
        
        ax1.text(0.01, 0.99, info_text, transform=ax1.transAxes, fontsize=9, va='top',
                bbox=dict(boxstyle='round', facecolor=GRID, alpha=0.95, edgecolor=TEXT, linewidth=1),
                color=TEXT, family='monospace')
        
        # Title
        title = f"{symbol} | 15min | Score:{analysis.total_score}/100 | "
        title += f"{tf_bias.tf_1h} ({tf_bias.tf_1h_confidence}%) + {tf_bias.tf_15m}"
        if analysis.pattern_signal:
            title += f" {analysis.pattern_signal.upper()} alignment confirms {analysis.chart_bias.lower()} bias per strict rules"
        
        ax1.set_title(title, color=TEXT, fontsize=13, fontweight='bold', pad=15)
        ax1.grid(True, color=GRID, alpha=0.3)
        ax1.tick_params(colors=TEXT)
        ax1.set_ylabel('Price (₹)', color=TEXT, fontsize=11)
        
        # === VOLUME CHART ===
        ax2.set_facecolor(BG)
        colors = [GREEN if df_plot.iloc[i]['close'] > df_plot.iloc[i]['open'] else RED for i in range(len(df_plot))]
        ax2.bar(range(len(df_plot)), df_plot['volume'], color=colors, alpha=0.6, width=0.8)
        ax2.set_ylabel('Volume', color=TEXT, fontsize=11)
        ax2.tick_params(colors=TEXT)
        ax2.grid(True, color=GRID, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=150, facecolor=BG)
        plt.close()
        logger.info(f"  📊 Chart saved: {save_path}")

# ==================== AI ANALYZER (MULTI-TIMEFRAME) ====================
class AIAnalyzer:
    @staticmethod
    def extract_json(content: str) -> Optional[Dict]:
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
    def validate_targets(opportunity: str, entry: float, sl: float, t1: float, t2: float) -> bool:
        if opportunity == "WAIT":
            return True
        if opportunity == "CE_BUY":
            if not (t2 > t1 > entry > sl):
                return False
            risk, reward = entry - sl, t1 - entry
        elif opportunity == "PE_BUY":
            if not (t2 < t1 < entry < sl):
                return False
            risk, reward = sl - entry, entry - t1
        else:
            return False
        
        rr = reward / risk if risk > 0 else 0
        return rr >= 1.5
    
    @staticmethod
    def deep_analysis(symbol: str, mtf_data: MultiTimeframeData, current_oi: OIData,
                     prev_oi: Optional[OIData], news_data: Optional[NewsData],
                     tf_bias: TimeframeBias) -> Optional[DeepAnalysis]:
        try:
            # Prepare data
            candle_df = mtf_data.df_15m.tail(150).reset_index()
            candle_df['timestamp'] = candle_df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%S')
            candles_json = candle_df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].to_json(orient='records')
            
            # OI section
            oi_section = f"""
**OI ANALYSIS (Score: 0-50)**
- PCR: {current_oi.pcr:.2f}
- Support: {current_oi.support_strike}
- Resistance: {current_oi.resistance_strike}
- CE OI Change: {current_oi.ce_oi_change_pct:.1f}%
- PE OI Change: {current_oi.pe_oi_change_pct:.1f}%
"""
            
            if prev_oi:
                oi_section += "\n**Top 15 Strikes OI Change:**\n| Strike | CE OI Δ | PE OI Δ |\n|:---:|:---:|:---:|\n"
                prev_map = {s.strike: s for s in prev_oi.strikes_data}
                for strike_data in current_oi.strikes_data[:15]:
                    prev_strike = prev_map.get(strike_data.strike)
                    if prev_strike:
                        ce_change = strike_data.ce_oi - prev_strike.ce_oi
                        pe_change = strike_data.pe_oi - prev_strike.pe_oi
                        ce_str = f"+{ce_change:,}" if ce_change > 0 else f"{ce_change:,}"
                        pe_str = f"+{pe_change:,}" if pe_change > 0 else f"{pe_change:,}"
                        oi_section += f"| {strike_data.strike} | {ce_str} | {pe_str} |\n"
            
            # News section
            news_text = ""
            if news_data:
                news_text = f"\n**NEWS (Context only, not scored):**\n- {news_data.headline}\n- Sentiment: {news_data.sentiment}\n"
            
            # Multi-timeframe context
            mtf_context = f"""
**MULTI-TIMEFRAME ANALYSIS:**
- 1H Bias: {tf_bias.tf_1h} (Confidence: {tf_bias.tf_1h_confidence}%)
- 15M Bias: {tf_bias.tf_15m} (Confidence: {tf_bias.tf_15m_confidence}%)
- 5M Bias: {tf_bias.tf_5m}
- Alignment: {tf_bias.alignment}
"""
            
            # AI Prompt
            prompt = f"""You are an institutional F&O trader. Analyze {symbol} using Multi-Timeframe + OI hybrid model.

**DATA INPUT:**

**1. PRICE DATA (150 candles, 15-min):**
{candles_json}

**2. CURRENT STATUS:**
- Spot: ₹{mtf_data.spot_price:.2f}
- ATR: {mtf_data.atr:.2f}
- Time: {datetime.now(IST).strftime('%d-%b %H:%M')}

{mtf_context}

{oi_section}

{news_text}

**ANALYSIS FRAMEWORK:**

**STEP 1: CHART ANALYSIS (50 points)**
- Consider the multi-timeframe alignment
- If 1H and 15M both BULLISH = Strong trend (higher score)
- If 1H BEARISH but 15M BULLISH = Divergence (lower score)
- Trend, structure, patterns, volume, order blocks
- Score 0-50 based on clarity

**STEP 2: OI ANALYSIS (50 points)**
- PCR interpretation
- Strike OI changes (building/unwinding)
- Does OI confirm chart direction?
- Score 0-50 based on strength

**STEP 3: HYBRID CONFLUENCE**
- Multi-timeframe + Chart + OI all agree = High score
- Example: 1H BULLISH + 15M BULLISH + PE OI building = Strong BUY

**STEP 4: TRADE SETUP**
- If total ≥75, recommend CE_BUY/PE_BUY
- Otherwise WAIT
- Entry, SL, T1, T2 with R:R ≥1:2

**OUTPUT (JSON ONLY):**
```json
{{
  "chart_bias": "Bullish/Bearish/Neutral",
  "market_structure": "Making HH/HL",
  "pattern_signal": "Bullish Flag Breakout",
  "oi_flow_signal": "Support building at {current_oi.support_strike}",
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
    {{"type": "bullish", "start_idx": 120, "end_idx": 125, "low": 48000, "high": 48100}}
  ]
}}
```"""
            
            # Call AI
            response = requests.post(
                "https://api.deepseek.com/v1/chat/completions",
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "Expert F&O trader. Respond in JSON only."},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.2,
                    "max_tokens": 2500
                },
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                timeout=120
            )
            
            if response.status_code != 200:
                logger.error(f"AI error: {response.status_code}")
                return None
            
            ai_content = response.json()['choices'][0]['message']['content']
            analysis_dict = AIAnalyzer.extract_json(ai_content)
            
            if not analysis_dict:
                return None
            
            logger.info(f"  🧠 AI: {analysis_dict.get('opportunity')} | Chart: {analysis_dict.get('chart_score')}/50 | OI: {analysis_dict.get('oi_score')}/50 | Total: {analysis_dict.get('total_score')}/100")
            
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
            
            # Add additional data
            analysis_dict['pcr_value'] = current_oi.pcr
            analysis_dict['timeframe_bias'] = tf_bias
            analysis_dict['news_headline'] = news_data.headline if news_data else None
            
            # OI Sentiment
            if current_oi.pcr > 1.2:
                oi_sentiment = "BULLISH"
            elif current_oi.pcr < 0.8:
                oi_sentiment = "BEARISH"
            else:
                oi_sentiment = "NEUTRAL"
            analysis_dict['oi_sentiment'] = oi_sentiment
            
            return DeepAnalysis(**analysis_dict)
            
        except Exception as e:
            logger.error(f"AI analysis error: {e}")
            traceback.print_exc()
            return None

# ==================== DATA FETCHER ====================
class UpstoxDataFetcher:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    
    def get_historical_data(self, instrument_key: str, interval: str, days: int = 10) -> pd.DataFrame:
        try:
            to_date = datetime.now(IST)
            from_date = to_date - timedelta(days=days)
            
            url = f"https://api.upstox.com/v2/historical-candle/{instrument_key}/{interval}/{to_date.strftime('%Y-%m-%d')}/{from_date.strftime('%Y-%m-%d')}"
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'candles' in data['data']:
                    df = pd.DataFrame(
                        data['data']['candles'],
                        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
                    )
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    return df.sort_values('timestamp').reset_index(drop=True)
            
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Historical data error: {e}")
            return pd.DataFrame()
    
    def get_ltp(self, instrument_key: str) -> float:
        try:
            response = requests.get(
                "https://api.upstox.com/v2/market-quote/ltp",
                headers=self.headers,
                params={"instrument_key": instrument_key},
                timeout=10
            )
            if response.status_code == 200:
                return response.json()['data'][instrument_key]['last_price']
            return 0.0
        except Exception as e:
            logger.error(f"LTP error: {e}")
            return 0.0
    
    def get_option_chain(self, instrument_key: str, expiry: str) -> List[StrikeData]:
        try:
            response = requests.get(
                "https://api.upstox.com/v2/option/chain",
                headers=self.headers,
                params={"instrument_key": instrument_key, "expiry_date": expiry},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                strikes = []
                for item in data.get('data', []):
                    call_data = item.get('call_options', {}).get('market_data', {})
                    put_data = item.get('put_options', {}).get('market_data', {})
                    strikes.append(StrikeData(
                        strike=int(item.get('strike_price', 0)),
                        ce_oi=call_data.get('oi', 0),
                        pe_oi=put_data.get('oi', 0),
                        ce_volume=call_data.get('volume', 0),
                        pe_volume=put_data.get('volume', 0),
                        ce_price=call_data.get('ltp', 0),
                        pe_price=put_data.get('ltp', 0)
                    ))
                return strikes
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
    
    async def send_startup_message(self):
        """Send bot startup status"""
        message = f"""
🚀 **HYBRID BOT v23.0 STARTED**

⏰ **Time:** {datetime.now(IST).strftime('%d-%b-%Y %H:%M:%S')}

📊 **Features:**
✅ Multi-Timeframe Analysis (1H+15M+5M)
✅ OI Comparison (2-hour window)
✅ Redis Storage (3-day expiry)
✅ Professional Charts
✅ News Integration

📈 **Monitoring:**
- 2 Indices (BANKNIFTY, MIDCPNIFTY)
- 37 F&O Stocks

🎯 **Alert Threshold:**
- Total Score ≥ 75/100
- Confidence ≥ 75%

📡 **Scan Interval:** 15 minutes
🔄 **Status:** Active & Running
"""
        await self.telegram_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='Markdown')
        logger.info("✅ Startup message sent")
    
    async def send_telegram_alert(self, symbol: str, analysis: DeepAnalysis, chart_path: str):
        """Send chart + text alert"""
        try:
            # 1. Send Chart
            with open(chart_path, 'rb') as photo:
                await self.telegram_bot.send_photo(chat_id=TELEGRAM_CHAT_ID, photo=photo)
            
            # 2. Send Text
            tf = analysis.timeframe_bias
            message = f"""
🚨 **{symbol} {analysis.opportunity} SIGNAL**

📊 **Total Score:** {analysis.total_score}/100
🎯 **Confidence:** {analysis.confidence}%

📈 **Multi-Timeframe Analysis:**
   └─ 1H: {tf.tf_1h} ({tf.tf_1h_confidence}%)
   └─ 15M: {tf.tf_15m} ({tf.tf_15m_confidence}%)
   └─ 5M: {tf.tf_5m}
   └─ Alignment: {tf.alignment}

📈 **Chart:** {analysis.chart_score}/50
   └─ {analysis.chart_bias} | {analysis.pattern_signal}

📊 **OI:** {analysis.oi_score}/50
   └─ {analysis.oi_flow_signal}
   └─ PCR: {analysis.pcr_value:.2f}
   └─ Sentiment: {analysis.oi_sentiment}
"""
            
            if analysis.news_headline:
                message += f"\n📰 **News:** {analysis.news_headline}\n"
            
            message += f"""
💰 **Trade Setup:**
Entry: ₹{analysis.entry_price:.2f}
Stop Loss: ₹{analysis.stop_loss:.2f}
Target 1: ₹{analysis.target_1:.2f}
Target 2: ₹{analysis.target_2:.2f}
Risk:Reward → {analysis.risk_reward}

📍 **Strike:** {analysis.recommended_strike}
⚠️ **Risks:** {', '.join(analysis.risk_factors[:2])}

🕐 {datetime.now(IST).strftime('%d-%b %H:%M:%S')}
"""
            
            await self.telegram_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='Markdown')
            logger.info(f"  ✅ Alert sent for {symbol}")
            
        except Exception as e:
            logger.error(f"Telegram error: {e}")
    
    async def analyze_symbol(self, instrument_key: str, symbol_info: Dict):
        """Main analysis pipeline"""
        try:
            symbol_name = symbol_info['name']
            display_name = symbol_info['display_name']
            
            logger.info(f"\n{'='*70}")
            logger.info(f"🔍 {display_name} ({symbol_name})")
            logger.info(f"{'='*70}")
            
            # 1. Expiry
            expiry = ExpiryCalculator.get_monthly_expiry(symbol_name)
            logger.info(f"  📅 Expiry: {expiry}")
            
            # 2. Fetch 1-min data (10 days for resampling)
            df_1m = self.data_fetcher.get_historical_data(instrument_key, "1minute", days=10)
            if df_1m.empty:
                logger.warning(f"  ⚠️ No data")
                return
            
            # 3. Resample to multiple timeframes
            df_1h = MultiTimeframeProcessor.resample_to_timeframe(df_1m, '1H')
            df_15m = MultiTimeframeProcessor.resample_to_timeframe(df_1m, '15T')
            df_5m = MultiTimeframeProcessor.resample_to_timeframe(df_1m, '5T')
            
            logger.info(f"  📊 Data: 1H({len(df_1h)}) | 15M({len(df_15m)}) | 5M({len(df_5m)})")
            
            # 4. Get timeframe biases
            bias_1h, conf_1h = MultiTimeframeProcessor.get_timeframe_bias(df_1h)
            bias_15m, conf_15m = MultiTimeframeProcessor.get_timeframe_bias(df_15m)
            bias_5m, conf_5m = MultiTimeframeProcessor.get_timeframe_bias(df_5m)
            
            # Alignment
            if bias_1h == bias_15m == bias_5m:
                alignment = "STRONG"
            elif bias_1h == bias_15m or bias_15m == bias_5m:
                alignment = "MODERATE"
            else:
                alignment = "WEAK"
            
            tf_bias = TimeframeBias(
                tf_1h=bias_1h, tf_15m=bias_15m, tf_5m=bias_5m,
                tf_1h_confidence=conf_1h, tf_15m_confidence=conf_15m,
                alignment=alignment
            )
            
            logger.info(f"  📊 TF: 1H {bias_1h}({conf_1h}%) | 15M {bias_15m}({conf_15m}%) | 5M {bias_5m} | {alignment}")
            
            # 5. ATR & Spot
            df_15m['tr'] = df_15m[['high', 'low', 'close']].apply(
                lambda x: max(x['high']-x['low'], abs(x['high']-x['close']), abs(x['low']-x['close'])), axis=1
            )
            atr = df_15m['tr'].rolling(14).mean().iloc[-1]
            spot_price = self.data_fetcher.get_ltp(instrument_key)
            if spot_price == 0:
                spot_price = df_15m['close'].iloc[-1]
            
            logger.info(f"  💹 Spot: ₹{spot_price:.2f} | ATR: {atr:.2f}")
            
            # 6. Option Chain
            all_strikes = self.data_fetcher.get_option_chain(instrument_key, expiry)
            if not all_strikes:
                logger.warning(f"  ⚠️ No OI data")
                return
            
            top_15 = StrikeSelector.get_top_15_atm_strikes(all_strikes, spot_price)
            logger.info(f"  📊 Selected {len(top_15)} strikes")
            
            # 7. OI Analysis
            total_ce = sum(s.ce_oi for s in top_15)
            total_pe = sum(s.pe_oi for s in top_15)
            pcr = total_pe / total_ce if total_ce > 0 else 0
            
            max_ce_strike = max(top_15, key=lambda x: x.ce_oi).strike
            max_pe_strike = max(top_15, key=lambda x: x.pe_oi).strike
            
            # Get previous OI for comparison
            prev_oi = RedisOIManager.get_comparison_oi(symbol_name, expiry, datetime.now(IST))
            
            # Calculate OI change %
            ce_change_pct = 0.0
            pe_change_pct = 0.0
            if prev_oi:
                prev_ce_total = sum(s.ce_oi for s in prev_oi.strikes_data)
                prev_pe_total = sum(s.pe_oi for s in prev_oi.strikes_data)
                if prev_ce_total > 0:
                    ce_change_pct = ((total_ce - prev_ce_total) / prev_ce_total) * 100
                if prev_pe_total > 0:
                    pe_change_pct = ((total_pe - prev_pe_total) / prev_pe_total) * 100
            
            current_oi = OIData(
                pcr=pcr, support_strike=max_pe_strike, resistance_strike=max_ce_strike,
                strikes_data=top_15, timestamp=datetime.now(IST),
                ce_oi_change_pct=ce_change_pct, pe_oi_change_pct=pe_change_pct
            )
            
            logger.info(f"  📊 PCR: {pcr:.2f} | S: {max_pe_strike} | R: {max_ce_strike} | CE: {ce_change_pct:+.1f}% | PE: {pe_change_pct:+.1f}%")
            
            # 8. Save OI
            RedisOIManager.save_oi(symbol_name, expiry, current_oi)
            
            # 9. News
            news_data = NewsFetcher.fetch_finnhub_news(symbol_name)
            if news_data:
                logger.info(f"  📰 {news_data.headline[:60]}...")
            
            # 10. AI Analysis
            mtf_data = MultiTimeframeData(df_1h=df_1h, df_15m=df_15m, df_5m=df_5m, spot_price=spot_price, atr=atr)
            analysis = AIAnalyzer.deep_analysis(symbol_name, mtf_data, current_oi, prev_oi, news_data, tf_bias)
            
            if not analysis:
                logger.info(f"  ⏸️ No valid analysis")
                return
            
            # 11. Check threshold
            if analysis.total_score >= 75 and analysis.confidence >= 75:
                signal_key = f"{symbol_name}_{analysis.opportunity}_{datetime.now(IST).strftime('%Y%m%d_%H')}"
                
                if signal_key not in self.processed_signals:
                    logger.info(f"  🚨 ALERT! Score: {analysis.total_score} | Conf: {analysis.confidence}%")
                    
                    # Generate chart
                    chart_path = f"/tmp/{symbol_name}_chart.png"
                    ChartGenerator.create_professional_chart(display_name, df_15m, analysis, spot_price, chart_path)
                    
                    # Send alert
                    await self.send_telegram_alert(display_name, analysis, chart_path)
                    self.processed_signals.add(signal_key)
                else:
                    logger.info(f"  ⏭️ Already sent")
            else:
                logger.info(f"  ⏸️ Score {analysis.total_score} or Conf {analysis.confidence}% below threshold")
            
        except Exception as e:
            logger.error(f"Analysis error: {e}")
            traceback.print_exc()
    
    async def run_scanner(self):
        """Main loop"""
        logger.info("\n" + "="*80)
        logger.info("🚀 HYBRID BOT v23.0 - MULTI-TIMEFRAME PROFESSIONAL")
        logger.info("="*80)
        
        await self.send_startup_message()
        
        while True:
            try:
                now = datetime.now(IST)
                current_time = now.time()
                
                # Market hours check
                if current_time < time(9, 15) or current_time > time(15, 30):
                    logger.info(f"⏸️ Market closed. Waiting...")
                    await asyncio.sleep(300)
                    continue
                
                # Holiday check
                if now.strftime('%Y-%m-%d') in NSE_HOLIDAYS_2025 or now.weekday() >= 5:
                    logger.info(f"📅 Holiday. Pausing...")
                    await asyncio.sleep(3600)
                    continue
                
                logger.info(f"\n🔄 Scan started: {now.strftime('%H:%M:%S')}")
                
                # Scan all symbols
                for instrument_key, symbol_info in ALL_SYMBOLS.items():
                    await self.analyze_symbol(instrument_key, symbol_info)
                    await asyncio.sleep(2)
                
                logger.info(f"\n✅ Scan complete. Next in 15 min...")
                await asyncio.sleep(900)
                
            except Exception as e:
                logger.error(f"Scanner error: {e}")
                traceback.print_exc()
                await asyncio.sleep(60)

# ==================== ENTRY POINT ====================
if __name__ == "__main__":
    bot = HybridBot()
    asyncio.run(bot.run_scanner())
