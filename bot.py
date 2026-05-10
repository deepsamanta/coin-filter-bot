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
    return requests.get(url).json()


def pair_to_symbol(pair):
    return pair.replace("B-", "").replace("_", "")


# =====================================================
# 200 EMA (4H) FILTER HELPERS
# =====================================================

def get_4h_candles(pair, limit=300):
    url = f"https://public.coindcx.com/market_data/candles?pair={pair}&interval=4h&limit={limit}"
    try:
        return requests.get(url, timeout=10).json()
    except Exception as e:
        print(f"[CANDLES] {pair} fetch failed: {e}")
        return []


def ema(values, period):
    k = 2 / (period + 1)
    ema_val = sum(values[:period]) / period   # SMA seed
    for v in values[period:]:
        ema_val = v * k + ema_val * (1 - k)
    return ema_val


def is_below_200ema_4h(pair):
    candles = get_4h_candles(pair, limit=300)
    if not candles or len(candles) < 200:
        return False

    # CoinDCX returns newest first → reverse to oldest→newest
    candles = list(reversed(candles))
    closes = [float(c["close"]) for c in candles]

    ema200 = ema(closes, 200)
    last_close = closes[-1]

    below = last_close < ema200
    print(f"[EMA] {pair}  close={last_close:.6f}  ema200={ema200:.6f}  {'⬇ BELOW' if below else '⬆ above'}")
    return below


# =====================================================
# STEP 1: GET COINS BELOW 200 EMA ON 4H
# =====================================================

def get_losers():
    pairs  = get_all_pairs()
    losers = []

    print(f"Scanning {len(pairs)} pairs against 200 EMA (4h)...\n")

    for p in pairs:
        pair_name = p if isinstance(p, str) else p.get("pair")
        if not pair_name:
            continue

        if is_below_200ema_4h(pair_name):
            losers.append(pair_to_symbol(pair_name))

        time.sleep(0.15)  # avoid rate limits

    print(f"\nFound {len(losers)} coins below 200 EMA on 4h\n")
    return losers


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

def add_new_losers(losers):
    rows = sheet.get_all_values()

    existing_symbols = set(
        str(row[0]).strip().upper()
        for row in rows if row and row[0]
    )

    print(f"[SHEET] Existing symbols: {len(existing_symbols)}\n")

    added = []
    for symbol in losers:
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

def run_bot(cycle):
    print("=" * 50)
    print("🤖 BOT STARTED")
    print("=" * 50)

    losers = get_losers()

    if not losers:
        print("No coins fetched.")
        return

    # Every 10th cycle: delete TP COMPLETED rows
    if cycle % 10 == 0:
        print("\n--- Cleaning TP COMPLETED rows (every 10th cycle) ---")
        delete_tp_completed_rows()
    else:
        next_clean = ((cycle // 10) + 1) * 10
        print(f"\n--- Skipping TP cleanup (next cleanup at cycle {next_clean}) ---")

    print("\n--- Updating sheet with new coins ---")
    added = add_new_losers(losers)

    print("\n" + "=" * 50)
    print(f"✅ DONE — {len(added)} new coins added to sheet")
    for s in added:
        print(f"   🔴 {s}")
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

        run_bot(cycle)

        cycle += 1
        print(f"\n⏳ Sleeping 1 hour... next run at {time.strftime('%H:%M:%S', time.localtime(time.time() + 3600))}")
        time.sleep(3600)

    except Exception as e:
        print(f"\n❌ BOT ERROR: {e}")
        print("⏳ Retrying in 60 seconds...")
        time.sleep(60)