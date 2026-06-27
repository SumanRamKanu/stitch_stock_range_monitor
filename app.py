import os
import sys
import json
import time
import math
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
    "ULTRACEMCO", "WIPRO", "BAJAJFINSV", "HINDUNILVR", "ITC",
    "SBILIFE", "HDFCLIFE", "TRENT", "ADANIENT", "BEL",
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
    "VOLTAS", "ZEEL", "ZYDUSLIFE", "ALKEM", "AUROPHARMA",
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

_data_cache = []
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
        except Exception:
            pass
    return None


def background_refresh():
    global _data_cache, _data_status, _data_total, _data_done

    try:
        with _lock:
            _data_status = "fetching"
        safe_log("info", "Starting background refresh...")

        stocks = get_all_nse_symbols()
        symbols = [s['symbol'] for s in stocks]
        safe_log("info", "Got %d NSE symbols to scan", len(symbols))

        if not symbols:
            safe_log("error", "No symbols from cache, using fallback")
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
        safe_log("info", "Refresh complete: %d stocks with >=25% drawdown from %d total (Yahoo failures: %d)", len(results), len(symbols), failed_count)

    except Exception as e:
        safe_log("error", "background_refresh CRASHED: %s", e)
        with _lock:
            _data_status = "done"