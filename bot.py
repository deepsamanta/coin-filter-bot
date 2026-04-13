import requests
import time
import gspread
from google.oauth2.service_account import Credentials

from config import SHEET_ID 


# =====================================================
# CONFIG
# =====================================================

EMA_LEN        = 21
FILTER_LOOK    = 70      # last 70 x 4H candles
MIN_ABOVE_PERC = 65.0    # % of bars whose close must be ABOVE the 21 EMA

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
# EMA HELPER
# =====================================================

def calc_ema(closes, length):
    k = 2 / (length + 1)
    ema_vals = [None] * len(closes)

    if len(closes) < length:
        return ema_vals

    ema_vals[length - 1] = sum(closes[:length]) / length

    for i in range(length, len(closes)):
        ema_vals[i] = closes[i] * k + ema_vals[i - 1] * (1 - k)

    return ema_vals


# =====================================================
# COIN FILTER — 70% of last 50 x 4H candles ABOVE 21 EMA
#               + current price must also be ABOVE 21 EMA
# =====================================================

def passes_ema_filter(pair):
    candles_needed = EMA_LEN + FILTER_LOOK
    now = int(time.time())
    url = "https://public.coindcx.com/market_data/candlesticks"
    params = {
        "pair":       pair,
        "from":       now - (candles_needed * 4 * 3600),
        "to":         now,
        "resolution": "240",
        "pcode":      "f",
    }
    try:
        candles = sorted(
            requests.get(url, params=params, timeout=10).json()["data"],
            key=lambda x: x["time"]
        )
        if len(candles) < candles_needed:
            return False

        closes   = [float(c["close"]) for c in candles]
        ema_vals = calc_ema(closes, EMA_LEN)

        bars_above = 0
        checked    = 0
        for i in range(len(closes) - FILTER_LOOK, len(closes)):
            if ema_vals[i] is None:
                continue
            checked += 1
            if closes[i] > ema_vals[i]:
                bars_above += 1

        if checked == 0:
            return False

        pct_above = (bars_above / checked) * 100
        if pct_above < MIN_ABOVE_PERC:
            return False

        current_close = closes[-1]
        current_ema   = ema_vals[-1]
        if current_ema is None or current_close <= current_ema:
            return False

        return True

    except Exception:
        return False


# =====================================================
# STEP 1: SCAN ALL COINS — returns (winners, failed_symbols)
# =====================================================

def get_winners():
    pairs   = get_all_pairs()
    winners = []
    failed  = []

    print(f"Scanning {len(pairs)} pairs — 70% of last 50 x 4H candles ABOVE 21 EMA + current price above EMA...\n")

    for i, pair in enumerate(pairs):
        symbol = pair_to_symbol(pair)

        if passes_ema_filter(pair):
            print(f"[{i+1}/{len(pairs)}] {symbol:20s} → ✅ passed — added!")
            winners.append(symbol)
        else:
            print(f"[{i+1}/{len(pairs)}] {symbol:20s} → ❌ failed EMA filter")
            failed.append(symbol)

        time.sleep(0.2)

    print(f"\n✅ Found {len(winners)} coins: {winners}\n")
    return winners, failed


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
# STEP 3: REMOVE FAILED COINS WITH NO ACTIVE TRADE
#         Runs every 5th cycle
#         Removes coin if: failed EMA filter AND column B is blank
# =====================================================

def remove_failed_coins(failed_symbols):
    rows = sheet.get_all_values()
    failed_upper = set(s.upper() for s in failed_symbols)

    print("\n--- Checking sheet for failed coins with no active trade ---")

    for i in range(len(rows) - 1, -1, -1):
        symbol = str(rows[i][0]).strip().upper() if rows[i] else ""
        col_b  = str(rows[i][1]).strip() if len(rows[i]) > 1 else ""

        if symbol in failed_upper and col_b == "":
            sheet.delete_rows(i + 1)
            print(f"[SHEET] 🗑️  Removed {symbol} — failed EMA + no active trade")
            time.sleep(0.3)
        elif symbol in failed_upper and col_b != "":
            print(f"[SHEET] ⚠️  Skipped {symbol} — failed EMA but trade is active ({col_b})")


# =====================================================
# STEP 4: ADD NEW COINS NOT ALREADY IN COLUMN A
# =====================================================

def add_new_winners(winners):
    rows = sheet.get_all_values()

    existing_symbols = set(
        str(row[0]).strip().upper()
        for row in rows if row and row[0]
    )

    print(f"[SHEET] Existing symbols: {existing_symbols}\n")

    added = []
    for symbol in winners:
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

    winners, failed = get_winners()

    if not winners:
        print("No coins passed the EMA filter.")
        return

    # Every 10th cycle: delete TP COMPLETED rows
    if cycle % 1 == 0:
        print("\n--- Cleaning TP COMPLETED rows (every 10th cycle) ---")
        delete_tp_completed_rows()
    else:
        print(f"\n--- Skipping TP cleanup (next cleanup at cycle {((cycle // 1) + 1) * 1}) ---")
    # Every 5th cycle: remove failed coins with no active trade
    if cycle % 5 == 0:
        print("\n--- Removing failed coins with no active trade (every 5th cycle) ---")
        remove_failed_coins(failed)
    else:
        print(f"\n--- Skipping failed coin cleanup (next cleanup at cycle {((cycle // 5) + 1) * 5}) ---")

    print("\n--- Updating sheet with new coins ---")
    added = add_new_winners(winners)

    print("\n" + "=" * 50)
    print(f"✅ DONE — {len(added)} new coins added to sheet")
    for s in added:
        print(f"   🟢 {s}")
    print("=" * 50)


# =====================================================
# INFINITE LOOP — RUNS EVERY 2 HOURS
# =====================================================

cycle = 1

while True:
    try:
        print(f"\n{'='*50}")
        print(f"🔁 CYCLE #{cycle}  |  {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*50}")

        run_bot(cycle)

        cycle += 1
        print(f"\n⏳ Sleeping 2 hours... next run at {time.strftime('%H:%M:%S', time.localtime(time.time() + 3600))}")
        time.sleep(3600)

    except Exception as e:
        print(f"\n❌ BOT ERROR: {e}")
        print("⏳ Retrying in 60 seconds...")
        time.sleep(60)