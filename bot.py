
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
# MAIN BOT
# =====================================================

def run_bot(cycle):
    print("=" * 50)
    print(f"🤖 COIN LIST BOT — CYCLE #{cycle}")
    print("=" * 50)

    symbols = get_all_symbols()
    if not symbols:
        print("No symbols fetched from CoinDCX.")
        return

    # Get all existing symbols from Column A to avoid duplicates
    existing_rows = sheet.get_all_values()
    existing_symbols = set(str(row[0]).strip().upper() for row in existing_rows if row)
    print(f"[SHEET] Already logged symbols: {len(existing_symbols)}")

    # Collect all coins that aren't in the sheet yet (Only adding the symbol)
    new_rows = []
    for symbol in symbols:
        if symbol.upper() not in existing_symbols:
            new_rows.append([symbol])  # This writes ONLY to Column A

    # Bulk add to Google Sheets
    if new_rows:
        print(f"[SHEET] Adding {len(new_rows)} new coins to the sheet...")
        sheet.append_rows(new_rows)
        for row in new_rows:
            print(f"   ➕ Added: {row[0]}")
    else:
        print("[SHEET] No new coins to add.")

    print("\n" + "=" * 50)
    print(f"✅ DONE — Tracked {len(symbols)} total coins.")
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
