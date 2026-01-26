import os
import time
from binance.client import Client

# =========================
# SETTINGS FROM ENV
# =========================
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

SYMBOLS_RAW = os.getenv("SYMBOLS", "SOLUSDC,AVAXUSDC")  # comma-separated
TARGET_PNL = float(os.getenv("TARGET_PNL", "100"))      # +USDT
STOP_PNL = float(os.getenv("STOP_PNL", "-10000"))       # -USDT
CHECK_SEC = int(os.getenv("CHECK_SEC", "2"))

if not API_KEY or not API_SECRET:
    raise SystemExit("Missing BINANCE_API_KEY or BINANCE_API_SECRET")

SYMBOLS = [s.strip().upper() for s in SYMBOLS_RAW.split(",") if s.strip()]
if len(SYMBOLS) < 1:
    raise SystemExit("SYMBOLS must contain at least 1 symbol (e.g. SOLUSDC,AVAXUSDC)")

client = Client(API_KEY, API_SECRET)

def get_position_info(symbol: str):
    """
    Returns:
      positionAmt (float): + long, - short, 0 no position
      unRealizedProfit (float): USDT
    """
    data = client.futures_position_information(symbol=symbol)
    pos = data[0]
    amt = float(pos["positionAmt"])
    pnl = float(pos["unRealizedProfit"])
    return amt, pnl

def close_position_market(symbol: str, position_amt: float):
    """
    Closes position using reduceOnly MARKET order.
    """
    if position_amt == 0:
        print(f"[{symbol}] No position to close.")
        return

    side = "SELL" if position_amt > 0 else "BUY"
    qty = abs(position_amt)

    print(f"[{symbol}] Closing: side={side}, qty={qty}")
    client.futures_create_order(
        symbol=symbol,
        side=side,
        type="MARKET",
        quantity=qty,
        reduceOnly=True
    )

def main():
    print("✅ PNL Watcher Bot started")
    print(f"Symbols: {SYMBOLS}")
    print(f"TARGET_PNL={TARGET_PNL} | STOP_PNL={STOP_PNL} | CHECK_SEC={CHECK_SEC}")

    while True:
        try:
            total_pnl = 0.0
            positions = {}  # {symbol: positionAmt}
            parts = []

            for sym in SYMBOLS:
                amt, pnl = get_position_info(sym)
                positions[sym] = amt
                total_pnl += pnl
                parts.append(f"{sym}: {pnl:.2f} (amt={amt})")

            print("PNL | " + " | ".join(parts) + f" | TOTAL: {total_pnl:.2f}")

            if total_pnl >= TARGET_PNL:
                print("✅ TARGET HIT -> closing ALL positions...")
                for sym, amt in positions.items():
                    close_position_market(sym, amt)
                print("✅ Done. Exiting.")
                break

            if total_pnl <= STOP_PNL:
                print("🛑 STOP HIT -> closing ALL positions...")
                for sym, amt in positions.items():
                    close_position_market(sym, amt)
                print("🛑 Done. Exiting.")
                break

            time.sleep(CHECK_SEC)

        except Exception as e:
            print(f"ERROR: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
