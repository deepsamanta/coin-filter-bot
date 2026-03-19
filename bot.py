import requests
import time
import gspread
from google.oauth2.service_account import Credentials

from config import SHEET_ID 


# =====================================================
# CONFIG
# =====================================================

#SHEET_ID       = "your_sheet_id_here"
GAIN_THRESHOLD = 15.0

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
    return requests.get(url).json()


def pair_to_symbol(pair):
    return pair.replace("B-", "").replace("_", "")


def get_daily_change_pct(pair):
    url = "https://public.coindcx.com/market_data/candlesticks"
    now = int(time.time())
    params = {
        "pair":       pair,
        "from":       now - (24 * 3600),
        "to":         now,
        "resolution": "60",
        "pcode":      "f",
    }
    try:
        candles = sorted(
            requests.get(url, params=params, timeout=10).json()["data"],
            key=lambda x: x["time"]
        )
        if len(candles) < 2:
            return None
        open_24h  = float(candles[0]["open"])
        close_now = float(candles[-1]["close"])
        return round(((close_now - open_24h) / open_24h) * 100, 2)
    except Exception:
        return None


# =====================================================
# STEP 1: SCAN ALL COINS → FILTER > 15% GAIN
# =====================================================

def get_gainers():
    pairs   = get_all_pairs()
    gainers = []

    print(f"Scanning {len(pairs)} pairs for >{GAIN_THRESHOLD}% gain...\n")

    for i, pair in enumerate(pairs):
        symbol = pair_to_symbol(pair)
        pct    = get_daily_change_pct(pair)

        if pct is None:
            print(f"[{i+1}/{len(pairs)}] {symbol:20s} → no data")
            continue

        print(f"[{i+1}/{len(pairs)}] {symbol:20s} → {pct:+.2f}%")

        if pct >= GAIN_THRESHOLD:
            gainers.append(symbol)

        time.sleep(0.2)

    print(f"\n✅ Found {len(gainers)} gainers above {GAIN_THRESHOLD}%: {gainers}\n")
    return gainers


# =====================================================
# STEP 2: DELETE ROWS WHERE COLUMN B = "TP COMPLETED"
# =====================================================

def delete_tp_completed_rows():
    rows = sheet.get_all_values()

    for i in range(len(rows) - 1, -1, -1):
        col_b = str(rows[i][1]).strip().upper() if len(rows[i]) > 1 else ""
        if col_b == "TP COMPLETED":
            sheet.delete_rows(i + 1)
            print(f"[SHEET] Deleted row {i+1} ({rows[i][0]}) — TP COMPLETED")
            time.sleep(0.3)


# =====================================================
# STEP 3: ADD NEW COINS NOT ALREADY IN COLUMN A
# =====================================================

def add_new_gainers(gainers):
    rows = sheet.get_all_values()

    existing_symbols = set(
        str(row[0]).strip().upper()
        for row in rows if row and row[0]
    )

    print(f"[SHEET] Existing symbols: {existing_symbols}\n")

    added = []
    for symbol in gainers:
        if symbol.upper() not in existing_symbols:
            sheet.append_row([symbol, ""])
            print(f"[SHEET] ➕ Added new coin: {symbol}")
            added.append(symbol)
            time.sleep(0.3)
        else:
            print(f"[SHEET] ⏭️  Already exists: {symbol}")

    return added


# =====================================================
# MAIN BOT
# =====================================================

def run_bot():
    print("=" * 50)
    print("🤖 BOT STARTED")
    print("=" * 50)

    gainers = get_gainers()

    if not gainers:
        print("No gainers found above threshold.")
        return

    print("\n--- Cleaning TP COMPLETED rows ---")
    delete_tp_completed_rows()

    print("\n--- Updating sheet with new gainers ---")
    added = add_new_gainers(gainers)

    print("\n" + "=" * 50)
    print(f"✅ DONE — {len(added)} new coins added to sheet")
    for s in added:
        print(f"   🟢 {s}")
    print("=" * 50)


# =====================================================
# INFINITE LOOP — RUNS EVERY HOUR
# =====================================================

cycle = 1

while True:
    try:
        print(f"\n{'='*50}")
        print(f"🔁 CYCLE #{cycle}  |  {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*50}")

        run_bot()

        cycle += 1
        print(f"\n⏳ Sleeping 1 hour... next run at {time.strftime('%H:%M:%S', time.localtime(time.time() + 3600))}")
        time.sleep(3600)   # wait 1 hour

    except Exception as e:
        print(f"\n❌ BOT ERROR: {e}")
        print("⏳ Retrying in 60 seconds...")
        time.sleep(60)     # on error wait 1 min and retry
