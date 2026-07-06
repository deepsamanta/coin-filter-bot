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


def symbol_to_pair(symbol):
    if symbol.endswith("USDT"):
        base = symbol[:-4]
        return f"B-{base}_USDT"
    return symbol


# =====================================================
# CANDLE FETCH
# =====================================================

CANDLE_URL = "https://public.coindcx.com/market_data/candles"

INTERVAL_MS = {
    "1d": 24 * 60 * 60 * 1000,
    "3d": 3 * 24 * 60 * 60 * 1000,
}

def fetch_candles(pair, interval, limit=500):
    try:
        params = {"pair": pair, "interval": interval, "limit": limit}
        r = requests.get(CANDLE_URL, params=params, timeout=15)
        data = r.json()
        if not isinstance(data, list):
            return []
        normalised = []
        for c in data:
            if isinstance(c, dict):
                ts  = int(c.get("time") or c.get("t") or c.get("timestamp") or 0)
                low = float(c.get("low") or c.get("l") or 0)
            elif isinstance(c, (list, tuple)) and len(c) >= 4:
                ts  = int(c[0])
                low = float(c[3])
            else:
                print(f"[CANDLES] Unknown format: {c}")
                continue
            normalised.append({"ts": ts, "low": low})
        normalised.sort(key=lambda x: x["ts"])
        return normalised
    except Exception as e:
        print(f"[CANDLES] Error fetching {pair} {interval}: {e}")
        return []


def split_candles(candles, interval):
    if not candles:
        return [], None
    now_ms      = int(time.time() * 1000)
    interval_ms = INTERVAL_MS[interval]
    closed  = [c for c in candles if c["ts"] + interval_ms <= now_ms]
    forming = [c for c in candles if c["ts"] + interval_ms >  now_ms]
    current = forming[-1] if forming else candles[-1]
    return closed, current


# =====================================================
# TICKER
# =====================================================

def get_all_current_prices():
    try:
        r = requests.get("https://api.coindcx.com/exchange/ticker", timeout=15)
        prices = {}
        for t in r.json():
            market = str(t.get("market", "")).upper()
            lp = t.get("last_price")
            if market and lp:
                prices[market] = float(lp)
        return prices
    except Exception as e:
        print(f"[TICKER] Error: {e}")
        return {}


# =====================================================
# ATL DETECTION
# =====================================================

def check_atl(symbol):
    pair = symbol_to_pair(symbol)

    # --- 1D ---
    candles_1d = fetch_candles(pair, "1d", limit=1000)
    closed_1d, current_1d = split_candles(candles_1d, "1d")

    if not closed_1d or current_1d is None:
        print(f"[ATL] {symbol}: no 1D candle data")
        return False, 0, 0

    if len(closed_1d) < 150:
        print(f"[ATL] {symbol}: only {len(closed_1d)} days old — skipping (< 150 days)")
        return False, 0, 0

    today_low_1d = current_1d["low"]
    atl_1d       = min(c["low"] for c in closed_1d)
    hit_1d       = today_low_1d <= atl_1d
    print(f"[ATL] {symbol} 1D — current_low={today_low_1d}, ATL={atl_1d}, hit={hit_1d}")

    if not hit_1d:
        return False, today_low_1d, atl_1d

    # --- 3D confirm ---
    candles_3d = fetch_candles(pair, "3d", limit=500)
    if len(candles_3d) < 2:
        print(f"[ATL] {symbol}: not enough 3D candles — passing on 1D only")
        return True, today_low_1d, atl_1d

    closed_3d, current_3d = split_candles(candles_3d, "3d")
    if len(closed_3d) < 1:
        print(f"[ATL] {symbol}: no closed 3D candles — passing on 1D only")
        return True, today_low_1d, atl_1d

    today_low_3d = current_3d["low"]
    atl_3d       = min(c["low"] for c in closed_3d)
    hit_3d       = today_low_3d <= atl_3d
    print(f"[ATL] {symbol} 3D — current_low={today_low_3d}, ATL={atl_3d}, hit={hit_3d}")

    return hit_3d, today_low_1d, atl_1d


# =====================================================
# SHEET HELPERS
# =====================================================

def remove_bounced_stale_rows(prices):
    """
    Delete rows where col B == 'ATL HIT' (trade never taken)
    and current price is 16%+ above today_low in col C (the real ATL price).
    """
    rows = sheet.get_all_values()
    to_delete = []

    for i, row in enumerate(rows):
        if len(row) < 3:
            continue
        symbol  = str(row[0]).strip().upper()
        col_b   = str(row[1]).strip().upper()
        atl_str = str(row[2]).strip()   # col C — today_low = real ATL

        if col_b != "ATL HIT":
            continue

        try:
            atl_price = float(atl_str)
        except ValueError:
            continue

        if atl_price <= 0:
            continue

        current = prices.get(symbol)
        if current is None:
            continue

        bounce_pct = (current - atl_price) / atl_price * 100
        if bounce_pct >= 16:
            to_delete.append(i + 1)
            print(f"[SHEET] 🗑️ {symbol} bounced {bounce_pct:.1f}% from ATL — queued for removal")

    for row_num in sorted(to_delete, reverse=True):
        sheet.delete_rows(row_num)
        print(f"[SHEET] Deleted stale row {row_num}")
        time.sleep(0.3)


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

    prices = get_all_current_prices()
    remove_bounced_stale_rows(prices)

    symbols = get_all_symbols()
    if not symbols:
        print("No symbols fetched.")
        return

    if cycle % 10 == 0:
        print("\n--- Cleaning TP COMPLETED rows ---")
        delete_tp_completed_rows()
    else:
        next_clean = ((cycle // 10) + 1) * 10
        print(f"--- Skipping TP cleanup (next at cycle {next_clean}) ---")

    existing_atl = get_existing_atl_symbols()
    print(f"[SHEET] Already logged ATL symbols: {len(existing_atl)}")

    atl_hits = []

    for symbol in symbols:
        if symbol.upper() in existing_atl:
            print(f"[ATL] {symbol}: already logged, skipping")
            continue

        try:
            hit, today_low, atl = check_atl(symbol)
            if hit:
                atl_hits.append((symbol, today_low, atl))
                add_atl_to_sheet(symbol, today_low, atl)
        except Exception as e:
            print(f"[ERROR] {symbol}: {e}")

        time.sleep(0.2)

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