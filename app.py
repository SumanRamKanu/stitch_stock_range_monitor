import os
import sys
import json
import time
import logging
import threading
import requests
import pandas as pd
from io import StringIO
from flask import Flask, jsonify, render_template
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logging.getLogger('urllib3').setLevel(logging.WARNING)
log = logging.getLogger(__name__)

def safe_log(level, msg, *args, **kwargs):
    try:
        getattr(log, level)(msg, *args, **kwargs)
    except Exception:
        try:
            print(msg % args if args else msg, flush=True)
        except Exception:
            pass

app = Flask(__name__)

NSE_EQUITY_CSV = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
NSE_EQUITY_CSV_ALT = "https://archives.nseindia.com/content/historical/EQUITIES/2024/JAN/cm11JAN24.csv"
NIFTY_50_CSV = "https://archives.nseindia.com/content/indices/ind_nifty50list.csv"
NIFTY_100_CSV = "https://archives.nseindia.com/content/indices/ind_nifty100list.csv"

FALLBACK_NSE_SYMBOLS = [
    "20MICRONS", "21STCentury", "3MINDIA", "AARTIDRUGS", "AARTIIND", "ABB", "ABBOTINDIA",
    "ABCAPITAL", "ABFRL", "ACC", "ADANIENT", "ADANIPORTS", "ALKEM", "AMARAJABAT", "AMBUJACEM",
    "ANGELONE", "APLAPOLLO", "APOLLOHOSP", "APOLLOTYRE", "ASHOKLEY", "ASIANPAINT", "ASTRAL",
    "AUBANK", "AUROPHARMA", "AXISBANK", "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BALKRISIND",
    "BALRAMCHIN", "BANDHANBNK", "BANKBARODA", "BANKINDIA", "BATAINDIA", "BEL", "BHARATFORG",
    "BHARTIARTL", "BIOCON", "BOSCHLTD", "BPCL", "BRITANNIA", "BSOFT", "CANBK", "CANFINHOME",
    "CHAMBLFERT", "CHOLAFIN", "CIPLA", "COALINDIA", "COFORGE", "COLPAL", "CONCOR", "COROMANDEL",
    "CROMPTON", "CUB", "CUMMINSIND", "DABUR", "DALBHARAT", "DEEPAKNTR", "DIVISLAB", "DIXON",
    "DLF", "DRREDDY", "EICHERMOT", "ELGIEQUIP", "EMAMILTD", "ENDURANCE", "ESCORTS", "EXIDEIND",
    "FEDERALBNK", "GAIL", "GLENMARK", "GMRINFRA", "GODREJAGRO", "GODREJCP", "GODREJIND",
    "GOODYEAR", "GRASIM", "GSFC", "GSPL", "HAL", "HAVELLS", "HCLTECH", "HDFC", "HDFCBANK",
    "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDCOPPER", "HINDUNILVR", "ICICIBANK", "IDFCFIRSTB",
    "IEXINDIA", "INDHOTEL", "INDIACEM", "INDIAMART", "INDIANB", "INDUSTINB", "INFY", "IOC",
    "IPCALAB", "IRCTC", "ITC", "JUBLFOOD", "KAJARIACER", "KANSAINER", "KOTAKBANK", "LALPATHLAB",
    "LAOPALA", "LICHSGFIN", "LT", "LTF", "LUPIN", "M&M", "M&MFIN", "MANAPPURAM", "MARICO",
    "MARUTI", "MCX", "METROPOLIS", "MFSL", "MGL", "MIDHANI", "MMTC", "MOTHERSON", "MPHASIS",
    "MRF", "MUTHOOTFIN", "NAM-INDIA", "NATIONALUM", "NAVINFLUOR", "NESTLEIND", "NEWGEN",
    "NTPC", "OBEROIRLTY", "OFSS", "OIL", "ONGC", "PAGEIND", "PERSISTENT", "PETRONET",
    "PFIZER", "PIDILITIND", "PIIND", "PNB", "POLYCAB", "POWERGRID", "PVRINOX", "RAMCOCEM",
    "RBLBANK", "RECLTD", "RELIANCE", "RENUKA", "SAIL", "SBICARD", "SBILIFE", "SBIN",
    "SHREECEM", "SHRIRAMFIN", "SIEMENS", "SONACOMS", "SRF", "SUNPHARMA", "SUNTV",
    "TATACHEM", "TATACOMM", "TATAELXSI", "TATAMOTORS", "TATAPOWER", "TATASTEEL",
    "TCS", "TECHM", "TORNTPHARM", "TRENT", "TRIDENT", "TVSMOTOR", "UBL", "UCO",
    "UJJIVANSFB", "ULTRACEMCO", "UNIONBANK", "UPL", "VEDL", "VOLTAS", "WHIRLPOOL",
    "WIPRO", "ZEEL", "ZYDUSLIFE"
]

FALLBACK_NIFTY50 = [
    "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJAJ-AUTO",
    "BAJFINANCE", "BAJAJFINSV", "BPCL", "BRITANNIA", "CIPLA",
    "COALINDIA", "DRREDDY", "EICHERMOT", "GRASIM", "HCLTECH",
    "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDUNILVR",
    "ICICIBANK", "INFY", "ITC", "KOTAKBANK", "LT",
    "M&M", "MARUTI", "NESTLEIND", "NTPC", "ONGC",
    "POWERGRID", "RELIANCE", "SBIN", "SUNPHARMA", "TATACONSUM",
    "TATAMOTORS", "TATASTEEL", "TCS", "TECHM", "TITAN",
    "ULTRACEMCO", "WIPRO", "SBILIFE", "TRENT", "ADANIENT", "BEL",
]

FALLBACK_NIFTY100 = FALLBACK_NIFTY50 + [
    "ABB", "ABCAPITAL", "ABFRL", "ACC", "ALKEM",
    "AMBUJACEM", "APLAPOLLO", "ASHOKLEY", "ASTRAL", "AUBANK",
    "AUROPHARMA", "BALKRISIND", "BANDHANBNK", "BANKBARODA", "BHARATFORG",
    "BHARTIARTL", "BIOCON", "BOSCHLTD", "CANBK", "CHOLAFIN",
    "COFORGE", "COLPAL", "CONCOR", "CROMPTON", "CUMMINSIND",
    "DABUR", "DALBHARAT", "DEEPAKNTR", "DIVISLAB", "DIXON",
    "DLF", "ELGIEQUIP", "ESCORTS", "EXIDEIND", "FEDERALBNK",
    "GLENMARK", "GODREJCP", "GODREJIND", "GSFC", "HAL",
    "HAVELLS", "IDFCFIRSTB", "INDHOTEL", "INDUSINDBK", "IOC",
    "IRCTC", "JUBLFOOD", "KAJARIACER", "LALPATHLAB", "LICHSGFIN",
    "LUPIN", "MANAPPURAM", "MARICO", "MFSL", "MGL",
    "MOTHERSON", "MPHASIS", "MUTHOOTFIN", "NATIONALUM", "OBEROIRLTY",
    "OIL", "PAGEIND", "PERSISTENT", "PETRONET", "PIDILITIND",
    "PIIND", "PNB", "POLYCAB", "PVRINOX", "RAMCOCEM",
    "RBLBANK", "RECLTD", "SAIL", "SBICARD", "SHREECEM",
    "SHRIRAMFIN", "SIEMENS", "SRF", "SUNTV", "TATACHEM",
    "TATACOMM", "TATAELXSI", "TATAPOWER", "TORNTPHARM", "TRIDENT",
    "TVSMOTOR", "UBL", "UCO", "UPL", "VEDL",
    "VOLTAS", "ZEEL", "ZYDUSLIFE"
]

STOCKS_CACHE_FILE = "stocks_cache.json"
DATA_CACHE_FILE = "data_cache.json"
NIFTY50_CACHE_FILE = "nifty50_cache.json"
NIFTY100_CACHE_FILE = "nifty100_cache.json"
SUGGESTIONS_CACHE_FILE = "suggestions_cache.json"
SUGGESTIONS_CACHE_TTL = 300

NSE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
}

YAHOO_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

_nse_session = requests.Session()
_nse_session.headers.update(NSE_HEADERS)

_http_session = requests.Session()
_http_session.headers.update(YAHOO_HEADERS)

_data_cache = []
_data_status = "idle"
_data_total = 0
_data_done = 0
_lock = threading.Lock()

_yahoo_semaphore = threading.Semaphore(15)

_suggestions_cache = []
_suggestions_status = "idle"
_suggestions_total = 0
_suggestions_done = 0
_suggestions_lock = threading.Lock()


def get_all_nse_symbols():
    if os.path.exists(STOCKS_CACHE_FILE):
        try:
            with open(STOCKS_CACHE_FILE) as f:
                data = json.load(f)
                if isinstance(data, list) and len(data) >= 50:
                    safe_log("info", "Loaded %d symbols from local cache", len(data))
                    return data
        except Exception as e:
            safe_log("warning", "Failed to read stocks cache: %s", e)

    safe_log("info", "Attempting to fetch NSE equity list from archives...")
    try:
        resp = _nse_session.get(NSE_EQUITY_CSV, timeout=(3, 5))
        if resp.status_code == 200:
            df = pd.read_csv(StringIO(resp.text))
            stocks = []
            for _, row in df.iterrows():
                symbol = str(row.get('SYMBOL', '')).strip()
                name = str(row.get('NAME OF COMPANY', '')).strip()
                if symbol and symbol != 'nan':
                    stocks.append({'symbol': symbol, 'name': name})
            if stocks:
                safe_log("info", "Fetched %d symbols from NSE archives", len(stocks))
                try:
                    with open(STOCKS_CACHE_FILE, 'w') as f:
                        json.dump(stocks, f)
                except Exception:
                    pass
                return stocks
        safe_log("warning", "NSE archives returned status %d", resp.status_code)
    except requests.exceptions.Timeout:
        safe_log("warning", "NSE archives timed out")
    except requests.exceptions.ConnectionError:
        safe_log("warning", "NSE archives connection failed")
    except Exception as e:
        safe_log("warning", "NSE CSV fetch failed: %s", e)

    safe_log("info", "Using fallback NSE stock list (%d symbols)", len(FALLBACK_NSE_SYMBOLS))
    stocks = [{'symbol': s, 'name': s} for s in FALLBACK_NSE_SYMBOLS]
    try:
        with open(STOCKS_CACHE_FILE, 'w') as f:
            json.dump(stocks, f)
    except Exception:
        pass
    return stocks


def fetch_index_symbols(index_name, csv_url, cache_file, fallback=None):
    if os.path.exists(cache_file):
        try:
            with open(cache_file) as f:
                data = json.load(f)
                if isinstance(data, list) and len(data) > 0:
                    safe_log("info", "Loaded %d %s symbols from cache", len(data), index_name)
                    return set(data)
        except Exception as e:
            safe_log("warning", "Failed to read %s cache: %s", index_name, e)

    safe_log("info", "Fetching %s symbols from NSE...", index_name)
    try:
        resp = _nse_session.get(csv_url, timeout=(3, 5))
        if resp.status_code == 200:
            df = pd.read_csv(StringIO(resp.text))
            symbols = []
            for _, row in df.iterrows():
                symbol = str(row.get('Symbol', '')).strip()
                if symbol and symbol != 'nan':
                    symbols.append(symbol)
            if symbols:
                safe_log("info", "Fetched %d %s symbols", len(symbols), index_name)
                try:
                    with open(cache_file, 'w') as f:
                        json.dump(symbols, f)
                except Exception:
                    pass
                return set(symbols)
        safe_log("warning", "%s CSV returned status %d", index_name, resp.status_code)
    except requests.exceptions.Timeout:
        safe_log("warning", "%s CSV timed out", index_name)
    except requests.exceptions.ConnectionError:
        safe_log("warning", "%s CSV connection failed", index_name)
    except Exception as e:
        safe_log("warning", "%s CSV fetch failed: %s", index_name, e)

    if fallback:
        safe_log("info", "Using fallback %s list (%d symbols)", index_name, len(fallback))
        return set(fallback)

    return set()


def get_nifty50_symbols():
    return fetch_index_symbols("NIFTY 50", NIFTY_50_CSV, NIFTY50_CACHE_FILE, fallback=FALLBACK_NIFTY50)


def get_nifty100_symbols():
    return fetch_index_symbols("NIFTY 100", NIFTY_100_CSV, NIFTY100_CACHE_FILE, fallback=FALLBACK_NIFTY100)


def fetch_yahoo_stock(symbol):
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS'
    params = {'interval': '1d', 'range': '1y'}
    with _yahoo_semaphore:
        try:
            r = _http_session.get(url, params=params, timeout=12)
            if r.status_code == 200:
                data = r.json()
                chart = data.get('chart', {})
                result_list = chart.get('result', [])
                if not result_list:
                    safe_log("debug", "Yahoo: no result for %s", symbol)
                    return None
                result = result_list[0]
                meta = result.get('meta', {})
                price = meta.get('regularMarketPrice', 0)
                high52 = meta.get('fiftyTwoWeekHigh', 0)
                name = meta.get('shortName', '') or meta.get('longName', '')
                vol = meta.get('regularMarketVolume', 0)
                if high52 and price and high52 > 0:
                    dd = ((high52 - price) / high52) * 100
                    return {
                        'symbol': symbol,
                        'name': name or '',
                        'currentPrice': round(float(price), 2),
                        'high52w': round(float(high52), 2),
                        'drawdown': round(dd, 2),
                        'volume': int(vol) if vol else 0,
                    }
            elif r.status_code == 429:
                safe_log("warning", "Yahoo rate-limited for %s", symbol)
                time.sleep(2)
            else:
                safe_log("debug", "Yahoo returned %d for %s", r.status_code, symbol)
        except requests.exceptions.Timeout:
            safe_log("debug", "Yahoo timeout for %s", symbol)
        except requests.exceptions.ConnectionError:
            safe_log("debug", "Yahoo connection error for %s", symbol)
        except Exception as e:
            safe_log("debug", "Yahoo fetch failed for %s: %s", symbol, e)
    return None


def fetch_stock_with_ohlcv(symbol):
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS'
    params = {'interval': '1d', 'range': '1y'}
    with _yahoo_semaphore:
        try:
            r = _http_session.get(url, params=params, timeout=12)
            if r.status_code == 200:
                data = r.json()
                chart = data.get('chart', {})
                result_list = chart.get('result', [])
                if not result_list:
                    return None
                result = result_list[0]
                meta = result.get('meta', {})
                quotes = result.get('indicators', {}).get('quote', [{}])[0]
                price = meta.get('regularMarketPrice', 0)
                high52 = meta.get('fiftyTwoWeekHigh', 0)
                name = meta.get('shortName', '') or meta.get('longName', '')
                vol = meta.get('regularMarketVolume', 0)
                if high52 and price and high52 > 0:
                    dd = ((high52 - price) / high52) * 100
                    closes = [c for c in quotes.get('close', []) if c is not None]
                    highs = [h for h in quotes.get('high', []) if h is not None]
                    lows = [l for l in quotes.get('low', []) if l is not None]
                    volumes = [v for v in quotes.get('volume', []) if v is not None]
                    return {
                        'symbol': symbol,
                        'name': name or '',
                        'currentPrice': round(float(price), 2),
                        'high52w': round(float(high52), 2),
                        'drawdown': round(dd, 2),
                        'volume': int(vol) if vol else 0,
                        'closes': closes,
                        'highs': highs,
                        'lows': lows,
                        'volumes': volumes,
                    }
            elif r.status_code == 429:
                time.sleep(2)
        except requests.exceptions.Timeout:
            safe_log("debug", "OHLCV timeout for %s", symbol)
        except requests.exceptions.ConnectionError:
            safe_log("debug", "OHLCV connection error for %s", symbol)
        except Exception as e:
            safe_log("debug", "OHLCV fetch failed for %s: %s", symbol, e)
    return None


def load_data_cache():
    global _data_cache, _data_status, _data_total, _data_done
    if os.path.exists(DATA_CACHE_FILE):
        try:
            with open(DATA_CACHE_FILE) as f:
                data = json.load(f)
                if isinstance(data, list) and len(data) > 0:
                    _data_cache = data
                    _data_status = "done"
                    _data_total = len(data)
                    _data_done = len(data)
                    return True
        except Exception:
            pass
    return False


def save_data_cache(results):
    try:
        with open(DATA_CACHE_FILE, 'w') as f:
            json.dump(results, f)
    except Exception:
        pass


def load_suggestions_cache():
    if os.path.exists(SUGGESTIONS_CACHE_FILE):
        try:
            with open(SUGGESTIONS_CACHE_FILE) as f:
                data = json.load(f)
                if isinstance(data, dict) and data.get('timestamp', 0) > time.time() - SUGGESTIONS_CACHE_TTL:
                    return data.get('results', [])
        except Exception:
            pass
    return None


def save_suggestions_cache(results):
    try:
        with open(SUGGESTIONS_CACHE_FILE, 'w') as f:
            json.dump({'timestamp': time.time(), 'results': results}, f)
    except Exception:
        pass


def compute_ema(closes, period):
    if not closes or len(closes) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
    return ema


def compute_rsi(closes, period=14):
    if not closes or len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_macd(closes, fast=12, slow=26, signal=9):
    if not closes or len(closes) < slow + signal:
        return None, None, None
    ema_fast = compute_ema(closes, fast)
    ema_slow = compute_ema(closes, slow)
    if ema_fast is None or ema_slow is None:
        return None, None, None
    macd_line = ema_fast - ema_slow
    macd_values = []
    for i in range(slow, len(closes)):
        ef = compute_ema(closes[:i + 1], fast)
        es = compute_ema(closes[:i + 1], slow)
        if ef is not None and es is not None:
            macd_values.append(ef - es)
    if len(macd_values) < signal:
        return macd_line, None, None
    signal_line = sum(macd_values[-signal:]) / signal
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def compute_volume_signal(closes, volumes):
    if not volumes or len(volumes) < 20:
        return 0, "Insufficient data"
    recent_vol = sum(volumes[-5:]) / 5
    avg_vol = sum(volumes[-20:]) / 20
    if avg_vol == 0:
        return 0, "No volume data"
    vol_ratio = recent_vol / avg_vol
    if vol_ratio > 2:
        return 60, "High volume surge"
    elif vol_ratio > 1.5:
        return 40, "Above average volume"
    elif vol_ratio > 1:
        return 20, "Normal volume"
    elif vol_ratio > 0.5:
        return -10, "Low volume"
    else:
        return -30, "Very low volume"


def compute_momentum(closes, period=20):
    if not closes or len(closes) < period:
        return 0, "Insufficient data"
    current = closes[-1]
    past = closes[-period]
    if past == 0:
        return 0, "No data"
    momentum = ((current - past) / past) * 100
    if momentum > 10:
        return 40, "Strong upward momentum"
    elif momentum > 0:
        return 20, "Mild upward momentum"
    elif momentum > -10:
        return -10, "Mild downward momentum"
    else:
        return -30, "Strong downward momentum"


def compute_buy_score(data):
    closes = data.get('closes', [])
    volumes = data.get('volumes', [])
    if not closes or len(closes) < 20:
        return {'score': 0, 'label': 'Avoid', 'signals': {}}

    rsi = compute_rsi(closes)
    macd_line, signal_line, histogram = compute_macd(closes)
    ma20 = compute_ema(closes, 20) if len(closes) >= 20 else None
    ma50 = compute_ema(closes, 50) if len(closes) >= 50 else None
    ma200 = compute_ema(closes, 200) if len(closes) >= 200 else None

    rsi_score = 0
    rsi_signal = "Neutral"
    if rsi is not None:
        if rsi < 30:
            rsi_score = 80
            rsi_signal = f"Oversold ({rsi:.1f})"
        elif rsi < 40:
            rsi_score = 50
            rsi_signal = f"Approaching oversold ({rsi:.1f})"
        elif rsi > 70:
            rsi_score = -60
            rsi_signal = f"Overbought ({rsi:.1f})"
        elif rsi > 60:
            rsi_score = -20
            rsi_signal = f"Approaching overbought ({rsi:.1f})"
        else:
            rsi_score = 10
            rsi_signal = f"Neutral ({rsi:.1f})"

    macd_score = 0
    macd_signal = "Neutral"
    if histogram is not None:
        if histogram > 0 and macd_line > signal_line:
            macd_score = 60
            macd_signal = "Bullish crossover"
        elif histogram > 0:
            macd_score = 30
            macd_signal = "Positive histogram"
        elif histogram < 0 and macd_line < signal_line:
            macd_score = -60
            macd_signal = "Bearish crossover"
        elif histogram < 0:
            macd_score = -30
            macd_signal = "Negative histogram"

    ma_score = 0
    ma_signal = "Neutral"
    ma_data = {
        'above_ma20': closes[-1] > ma20 if ma20 else False,
        'above_ma50': closes[-1] > ma50 if ma50 else False,
        'above_ma200': closes[-1] > ma200 if ma200 else False,
    }
    ma_above_count = sum([
        ma_data.get('above_ma20', False),
        ma_data.get('above_ma50', False),
        ma_data.get('above_ma200', False),
    ])
    if ma_above_count == 3:
        ma_score = 80
        ma_signal = "Above all MAs"
    elif ma_above_count == 2:
        ma_score = 50
        above = []
        if ma_data.get('above_ma20'): above.append('MA20')
        if ma_data.get('above_ma50'): above.append('MA50')
        if ma_data.get('above_ma200'): above.append('MA200')
        ma_signal = "Above " + ", ".join(above)
    elif ma_above_count == 1:
        ma_score = 10
        above = []
        if ma_data.get('above_ma20'): above.append('MA20')
        if ma_data.get('above_ma50'): above.append('MA50')
        if ma_data.get('above_ma200'): above.append('MA200')
        ma_signal = "Only above " + ", ".join(above)
    else:
        ma_score = -30
        ma_signal = "Below all MAs"

    vol_score, vol_signal = compute_volume_signal(closes, volumes)
    mom_score, mom_signal = compute_momentum(closes)

    rsi_score = max(-100, min(100, rsi_score))
    macd_score = max(-100, min(100, macd_score))
    ma_score = max(-100, min(100, ma_score))
    vol_score = max(-100, min(100, vol_score))
    mom_score = max(-100, min(100, mom_score))

    raw = (rsi_score * 0.20 + macd_score * 0.25 + ma_score * 0.25 +
           vol_score * 0.15 + mom_score * 0.15)
    final_score = max(0, min(100, (raw + 100) / 2))

    if final_score >= 75:
        label = "Strong Buy"
    elif final_score >= 60:
        label = "Buy"
    elif final_score >= 40:
        label = "Hold"
    else:
        label = "Avoid"

    return {
        'score': round(final_score, 1),
        'label': label,
        'signals': {
            'rsi': {'value': rsi, 'score': round(rsi_score, 1), 'signal': rsi_signal},
            'macd': {'value': f"{macd_line:.4f}" if macd_line else "N/A", 'score': round(macd_score, 1), 'signal': macd_signal},
            'ma': {'value': f"{ma_above_count}/3 above", 'score': round(ma_score, 1), 'signal': ma_signal},
            'volume': {'value': vol_signal, 'score': round(vol_score, 1), 'signal': vol_signal},
            'momentum': {'value': f"{mom_score:+.1f}", 'score': round(mom_score, 1), 'signal': mom_signal},
        }
    }


def compute_historical_metrics(closes, highs, timestamps=None):
    if not closes or len(closes) < 5:
        return {}

    current_price = closes[-1]
    result = {}

    high_52w = max(highs) if highs else max(closes)
    if high_52w > 0:
        result['pct_from_high'] = round(((high_52w - current_price) / high_52w) * 100, 1)
        if highs:
            high_idx = highs.index(high_52w)
            days_since_high = len(highs) - 1 - high_idx
            result['days_since_high'] = days_since_high
        else:
            result['days_since_high'] = 0

    low_52w = min(closes)
    if low_52w > 0:
        result['pct_from_low'] = round(((current_price - low_52w) / low_52w) * 100, 1)
        low_idx = closes.index(low_52w)
        result['days_since_low'] = len(closes) - 1 - low_idx

    result['price_position'] = round(((current_price - low_52w) / (high_52w - low_52w)) * 100, 1) if high_52w != low_52w else 50

    if len(closes) >= 200:
        ma200 = sum(closes[-200:]) / 200
        result['ma200'] = round(ma200, 2)
        result['above_ma200'] = current_price > ma200
        days_below = 0
        for c in reversed(closes[-200:]):
            if c < ma200:
                days_below += 1
            else:
                break
        result['days_below_ma200'] = days_below
        result['dist_from_ma200'] = round(((current_price - ma200) / ma200) * 100, 1)

    if len(closes) >= 50:
        ma50 = sum(closes[-50:]) / 50
        result['ma50'] = round(ma50, 2)
        result['above_ma50'] = current_price > ma50
        days_below_50 = 0
        for c in reversed(closes[-50:]):
            if c < ma50:
                days_below_50 += 1
            else:
                break
        result['days_below_ma50'] = days_below_50

    if len(closes) >= 20:
        ma20 = sum(closes[-20:]) / 20
        result['ma20'] = round(ma20, 2)
        result['above_ma20'] = current_price > ma20

    return result


def background_suggestions():
    global _suggestions_cache, _suggestions_status, _suggestions_total, _suggestions_done

    try:
        cached = load_suggestions_cache()
        if cached is not None:
            with _suggestions_lock:
                _suggestions_cache = cached
                _suggestions_status = "done"
            safe_log("info", "Loaded %d suggestions from cache", len(cached))
            return

        with _suggestions_lock:
            _suggestions_status = "computing"

        symbols_data = []
        with _lock:
            for s in _data_cache:
                symbols_data.append({'symbol': s['symbol'], 'name': s.get('name', ''),
                                     'drawdown': s['drawdown'], 'indices': s.get('indices', [])})

        if not symbols_data:
            safe_log("warning", "No stock data available for suggestions")
            with _suggestions_lock:
                _suggestions_status = "idle"
            return

        with _suggestions_lock:
            _suggestions_total = len(symbols_data)
            _suggestions_done = 0

        safe_log("info", "Computing suggestions for %d stocks...", len(symbols_data))

        results = []
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(fetch_stock_with_ohlcv, s['symbol']): s for s in symbols_data}
            for f in as_completed(futures):
                stock_info = futures[f]
                with _suggestions_lock:
                    _suggestions_done += 1
                try:
                    data = f.result(timeout=20)
                except Exception:
                    data = None
                if data and len(data.get('closes', [])) >= 20:
                    score_result = compute_buy_score(data)
                    hist_metrics = compute_historical_metrics(data['closes'], data.get('highs', []))
                    results.append({
                        'symbol': data['symbol'],
                        'name': data['name'],
                        'currentPrice': data['currentPrice'],
                        'drawdown': data['drawdown'],
                        'volume': data['volume'],
                        'indices': stock_info.get('indices', []),
                        'score': score_result['score'],
                        'label': score_result['label'],
                        'signals': score_result['signals'],
                        'history': hist_metrics,
                    })

        results.sort(key=lambda x: x['score'], reverse=True)

        with _suggestions_lock:
            _suggestions_cache = results
            _suggestions_status = "done"

        save_suggestions_cache(results)
        safe_log("info", "Suggestions complete: %d stocks scored", len(results))

    except Exception as e:
        safe_log("error", "background_suggestions CRASHED: %s", e)
        with _suggestions_lock:
            _suggestions_status = "done"


def background_refresh():
    global _data_cache, _data_status, _data_total, _data_done

    try:
        with _lock:
            _data_status = "fetching"
        safe_log("info", "Starting background refresh...")

        try:
            stocks = get_all_nse_symbols()
            safe_log("info", "Got %d NSE symbols", len(stocks))
        except Exception as e:
            safe_log("error", "Failed to get NSE symbols: %s", e)
            stocks = [{'symbol': s, 'name': s} for s in FALLBACK_NSE_SYMBOLS]

        symbols = [s['symbol'] for s in stocks]
        if not symbols:
            safe_log("warning", "No symbols, using fallback")
            symbols = list(FALLBACK_NSE_SYMBOLS)

        with _lock:
            _data_total = len(symbols)
            _data_done = 0
        safe_log("info", "Total symbols to scan: %d", _data_total)

        nifty50 = set()
        nifty100 = set()
        try:
            nifty50 = get_nifty50_symbols()
            safe_log("info", "Loaded %d NIFTY50 symbols", len(nifty50))
        except Exception as e:
            safe_log("warning", "NIFTY50 fetch failed: %s, using fallback", e)
            nifty50 = set(FALLBACK_NIFTY50)

        try:
            nifty100 = get_nifty100_symbols()
            safe_log("info", "Loaded %d NIFTY100 symbols", len(nifty100))
        except Exception as e:
            safe_log("warning", "NIFTY100 fetch failed: %s, using fallback", e)
            nifty100 = set(FALLBACK_NIFTY100)

        safe_log("info", "Index data: NIFTY50=%d, NIFTY100=%d", len(nifty50), len(nifty100))

        results = []
        failed_count = 0
        with ThreadPoolExecutor(max_workers=15) as ex:
            futures = {ex.submit(fetch_yahoo_stock, s): s for s in symbols}
            for f in as_completed(futures):
                try:
                    r = f.result(timeout=20)
                except Exception:
                    r = None
                with _lock:
                    _data_done += 1
                if r:
                    indices = []
                    if r['symbol'] in nifty50:
                        indices.append('NIFTY 50')
                    if r['symbol'] in nifty100:
                        indices.append('NIFTY 100')
                    r['indices'] = indices
                    results.append(r)
                else:
                    failed_count += 1

                done = _data_done
                if done % 100 == 0 or done == _data_total:
                    safe_log("info", "Progress: %d/%d scanned, %d matched, %d failed", done, _data_total, len(results), failed_count)

        results = [s for s in results if s.get('drawdown', 0) >= 25]
        results.sort(key=lambda x: x['drawdown'], reverse=True)

        with _lock:
            _data_cache = results
            _data_status = "done"

        save_data_cache(results)
        safe_log("info", "Refresh complete: %d stocks with >=25%% drawdown from %d total (Yahoo failures: %d)", len(results), len(symbols), failed_count)

    except Exception as e:
        safe_log("error", "background_refresh CRASHED: %s", e)
        with _lock:
            _data_status = "done"


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/stocks')
def api_stocks():
    with _lock:
        status = _data_status
        total = _data_total
        done = _data_done
        cache = list(_data_cache)

    if status == "done":
        if not cache:
            safe_log("info", "Status is done but cache is empty, triggering refresh")
            threading.Thread(target=background_refresh, daemon=True).start()
            return jsonify({'status': 'loading', 'total': 0, 'done': 0, 'percent': 0}), 202
        return jsonify(cache)

    if status == "idle":
        safe_log("info", "Status is idle, triggering background refresh")
        threading.Thread(target=background_refresh, daemon=True).start()

    return jsonify({
        'status': 'loading',
        'total': total,
        'done': done,
        'percent': round((done / total * 100) if total > 0 else 0),
    }), 202


@app.route('/api/status')
def api_status():
    with _lock:
        return jsonify({
            'status': _data_status,
            'total': _data_total,
            'done': _data_done,
            'results': len(_data_cache),
        })


@app.route('/api/debug')
def api_debug():
    return jsonify({
        'status': _data_status,
        'total': _data_total,
        'done': _data_done,
        'cache_size': len(_data_cache),
        'has_stocks_cache': os.path.exists(STOCKS_CACHE_FILE),
        'has_data_cache': os.path.exists(DATA_CACHE_FILE),
        'has_nifty50_cache': os.path.exists(NIFTY50_CACHE_FILE),
        'has_nifty100_cache': os.path.exists(NIFTY100_CACHE_FILE),
        'suggestions_status': _suggestions_status,
    })


@app.route('/api/stock/<symbol>')
def api_stock_detail(symbol):
    symbol = symbol.upper().replace('.NS', '')
    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS'
    params = {'interval': '1d', 'range': '1y'}
    try:
        r = _http_session.get(url, params=params, timeout=15)
        if r.status_code != 200:
            return jsonify({'error': 'Stock not found'}), 404

        data = r.json()
        result_list = data.get('chart', {}).get('result', [])
        if not result_list:
            return jsonify({'error': 'Stock not found'}), 404
        result = result_list[0]
        meta = result['meta']
        timestamps = result.get('timestamp', [])
        quotes = result['indicators']['quote'][0]

        price = meta.get('regularMarketPrice', 0)
        high52 = meta.get('fiftyTwoWeekHigh', 0)
        low52 = meta.get('fiftyTwoWeekLow', 0)
        prev_close = meta.get('chartPreviousClose', 0)
        day_high = meta.get('regularMarketDayHigh', 0)
        day_low = meta.get('regularMarketDayLow', 0)
        day_open = meta.get('regularMarketOpen', 0)
        vol = meta.get('regularMarketVolume', 0)
        name = meta.get('shortName', '') or meta.get('longName', '')
        exchange = meta.get('fullExchangeName', '')

        closes = [c for c in quotes.get('close', []) if c is not None]
        highs = [h for h in quotes.get('high', []) if h is not None]
        lows = [l for l in quotes.get('low', []) if l is not None]
        volumes = [v for v in quotes.get('volume', []) if v is not None]

        day_change = round(price - prev_close, 2) if prev_close and price else 0
        day_change_pct = round((day_change / prev_close) * 100, 2) if prev_close else 0
        dd = round(((high52 - price) / high52) * 100, 2) if high52 and price else 0

        ma20 = round(sum(closes[-20:]) / 20, 2) if len(closes) >= 20 else None
        ma50 = round(sum(closes[-50:]) / 50, 2) if len(closes) >= 50 else None
        ma200 = round(sum(closes[-200:]) / 200, 2) if len(closes) >= 200 else None

        avg_vol = round(sum(volumes) / len(volumes)) if volumes else 0

        chart_data = []
        for i in range(len(timestamps)):
            c = quotes.get('close', [None] * len(timestamps))[i]
            h = quotes.get('high', [None] * len(timestamps))[i]
            l = quotes.get('low', [None] * len(timestamps))[i]
            v = quotes.get('volume', [None] * len(timestamps))[i]
            if c is not None:
                chart_data.append({
                    't': timestamps[i],
                    'c': round(c, 2),
                    'h': round(h, 2) if h else None,
                    'l': round(l, 2) if l else None,
                    'v': v or 0,
                })

        hist_metrics = compute_historical_metrics(closes, highs, timestamps)

        return jsonify({
            'symbol': symbol,
            'name': name,
            'exchange': exchange,
            'currentPrice': round(float(price), 2),
            'high52w': round(float(high52), 2),
            'low52w': round(float(low52), 2),
            'drawdown': dd,
            'volume': int(vol) if vol else 0,
            'avgVolume': avg_vol,
            'dayOpen': round(float(day_open), 2) if day_open else None,
            'dayHigh': round(float(day_high), 2) if day_high else None,
            'dayLow': round(float(day_low), 2) if day_low else None,
            'prevClose': round(float(prev_close), 2) if prev_close else None,
            'dayChange': day_change,
            'dayChangePct': day_change_pct,
            'ma20': ma20,
            'ma50': ma50,
            'ma200': ma200,
            'history': hist_metrics,
            'chart': chart_data,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/suggestions')
def api_suggestions():
    with _suggestions_lock:
        status = _suggestions_status
        total = _suggestions_total
        done = _suggestions_done

    if status == "done":
        return jsonify({
            'status': 'done',
            'results': _suggestions_cache,
            'total': len(_suggestions_cache),
        })

    if status == "idle":
        if _data_status == "done":
            safe_log("info", "Triggering background suggestions computation")
            threading.Thread(target=background_suggestions, daemon=True).start()
            return jsonify({'status': 'computing', 'done': 0, 'total': 0}), 202
        return jsonify({'status': 'waiting', 'message': 'Stock data not ready'}), 202

    return jsonify({
        'status': 'computing',
        'done': done,
        'total': total,
        'percent': round((done / total * 100) if total > 0 else 0),
    }), 202


safe_log("info", "=== Starting Deep Pullback Terminal ===")
load_data_cache()
if _data_cache:
    safe_log("info", "Loaded %d stocks from cache", len(_data_cache))
else:
    safe_log("info", "No cached data, starting background refresh...")
    threading.Thread(target=background_refresh, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
