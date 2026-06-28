import requests
import time
import gspread
from google.oauth2.service_account import Credentials

from config import SHEET_ID


# =====================================================
# GOOGLE SHEETS CONNECTION
# =====================================================

scope  = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds  = Credentials.from_service_account_file("service_account.json", scopes=scope)
client = gspread.authorize(creds)
sheet  = client.open_by_key(SHEET_ID).sheet1


# =====================================================
# COINDCX HELPERS
# =====================================================

def get_all_pairs():
    url = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT"
    return requests.get(url, timeout=15).json()


def pair_to_symbol(pair):
    return pair.replace("B-", "").replace("_", "")


def get_all_symbols():
    pairs = get_all_pairs()
    symbols = []
    for p in pairs:
        pair_name = p if isinstance(p, str) else p.get("pair")
        if not pair_name:
            continue
        symbols.append(pair_to_symbol(pair_name))
    print(f"[SYMBOLS] Fetched {len(symbols)} symbols")
    return symbols


# =====================================================
# CANDLE FETCH — CoinDCX futures candles
# =====================================================

CANDLE_URL = "https://public.coindcx.com/market_data/candles"

def fetch_candles(pair, interval, limit=500):
    """
    interval: '1d' or '3d'
    Returns list of candles, each normalised to a dict with key 'low'.
    """
    try:
        params = {"pair": pair, "interval": interval, "limit": limit}
        r = requests.get(CANDLE_URL, params=params, timeout=15)
        data = r.json()
        if not isinstance(data, list):
            return []
        normalised = []
        for c in data:
            if isinstance(c, dict):
                # dict format: keys may be 'low', 'l', or positional
                low = c.get("low") or c.get("l")
                normalised.append({"low": float(low)})
            elif isinstance(c, (list, tuple)) and len(c) >= 4:
                # list format: [timestamp, open, high, low, ...]
                normalised.append({"low": float(c[3])})
            else:
                print(f"[CANDLES] Unknown candle format: {c}")
        return normalised
    except Exception as e:
        print(f"[CANDLES] Error fetching {pair} {interval}: {e}")
        return []


def symbol_to_pair(symbol):
    """Convert e.g. BTCUSDT → B-BTC_USDT"""
    if symbol.endswith("USDT"):
        base = symbol[:-4]
        return f"B-{base}_USDT"
    return symbol


# =====================================================
# ATL DETECTION
# =====================================================

def check_atl(symbol):
    """
    Returns True if today's candle low <= all-time low on BOTH 1D and 3D candles.
    ATL is the minimum low across ALL historical candles EXCLUDING today's candle.
    """
    pair = symbol_to_pair(symbol)

    # --- 1D check ---
    candles_1d = fetch_candles(pair, "1d", limit=1000)
    if len(candles_1d) < 2:
        print(f"[ATL] {symbol}: not enough 1D candles ({len(candles_1d)})")
        return False

    # Last candle = today (may be forming), rest = history
    history_1d  = candles_1d[:-1]
    today_1d    = candles_1d[-1]
    today_low_1d = today_1d["low"]
    atl_1d       = min(c["low"] for c in history_1d)

    hit_1d = today_low_1d <= atl_1d
    print(f"[ATL] {symbol} 1D — today_low={today_low_1d}, ATL={atl_1d}, hit={hit_1d}")

    if not hit_1d:
        return False

    # --- 3D check (confirm) ---
    candles_3d = fetch_candles(pair, "3d", limit=500)
    if len(candles_3d) < 2:
        print(f"[ATL] {symbol}: not enough 3D candles, skipping 3D confirm")
        # If no 3D data available, pass on 1D alone (optional: change to return False)
        return True

    history_3d   = candles_3d[:-1]
    today_3d     = candles_3d[-1]
    today_low_3d = today_3d["low"]
    atl_3d       = min(c["low"] for c in history_3d)

    hit_3d = today_low_3d <= atl_3d
    print(f"[ATL] {symbol} 3D — today_low={today_low_3d}, ATL={atl_3d}, hit={hit_3d}")

    return hit_3d


# =====================================================
# SHEET HELPERS
# =====================================================

def delete_tp_completed_rows():
    rows = sheet.get_all_values()
    for i in range(len(rows) - 1, -1, -1):
        col_b = str(rows[i][1]).strip().upper() if len(rows[i]) > 1 else ""
        if col_b == "TP COMPLETED":
            sheet.delete_rows(i + 1)
            print(f"[SHEET] Deleted row {i+1} ({rows[i][0]}) — TP COMPLETED")
            time.sleep(0.3)


def get_existing_atl_symbols():
    rows = sheet.get_all_values()
    return set(
        str(row[0]).strip().upper()
        for row in rows
        if len(row) > 1 and str(row[1]).strip().upper() == "ATL HIT"
    )


def add_atl_to_sheet(symbol, today_low, atl):
    sheet.append_row([symbol, "ATL HIT", str(today_low), str(atl)])
    print(f"[SHEET] ➕ Added ATL HIT: {symbol}")
    time.sleep(0.3)


# =====================================================
# MAIN BOT
# =====================================================

def run_bot(cycle):
    print("=" * 50)
    print(f"🤖 ATL BOT — CYCLE #{cycle}")
    print("=" * 50)

    symbols = get_all_symbols()
    if not symbols:
        print("No symbols fetched.")
        return

    # Every 10th cycle: clean TP COMPLETED rows
    if cycle % 10 == 0:
        print("\n--- Cleaning TP COMPLETED rows ---")
        delete_tp_completed_rows()
    else:
        next_clean = ((cycle // 10) + 1) * 10
        print(f"--- Skipping TP cleanup (next at cycle {next_clean}) ---")

    # Get already-logged ATL symbols to avoid duplicates
    existing_atl = get_existing_atl_symbols()
    print(f"[SHEET] Already logged ATL symbols: {len(existing_atl)}")

    atl_hits = []

    for symbol in symbols:
        if symbol.upper() in existing_atl:
            print(f"[ATL] {symbol}: already logged, skipping")
            continue

        try:
            pair = symbol_to_pair(symbol)
            candles_1d = fetch_candles(pair, "1d", limit=1000)
            if len(candles_1d) < 2:
                continue

            today_low = candles_1d[-1]["low"]
            atl       = min(c["low"] for c in candles_1d[:-1])

            hit = check_atl(symbol)
            if hit:
                atl_hits.append((symbol, today_low, atl))
                add_atl_to_sheet(symbol, today_low, atl)

        except Exception as e:
            print(f"[ERROR] {symbol}: {e}")

        time.sleep(0.2)   # gentle rate limit

    print("\n" + "=" * 50)
    print(f"✅ DONE — {len(atl_hits)} ATL hits found")
    for s, lo, atl in atl_hits:
        print(f"   🔴 {s}  low={lo}  ATL={atl}")
    print("=" * 50)


# =====================================================
# INFINITE LOOP — EVERY HOUR
# =====================================================

cycle = 1

while True:
    try:
        print(f"\n{'='*50}")
        print(f"🔁 CYCLE #{cycle}  |  {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*50}")

        run_bot(cycle)

        cycle += 1
        print(f"\n⏳ Sleeping 1 hour... next at {time.strftime('%H:%M:%S', time.localtime(time.time() + 3600))}")
        time.sleep(3600)

    except Exception as e:
        print(f"\n❌ BOT ERROR: {e}")
        print("⏳ Retrying in 60 seconds...")
        time.sleep(60)