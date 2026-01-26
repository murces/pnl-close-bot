import os
import time
from dataclasses import dataclass
from typing import Dict, List, Tuple
from binance.client import Client

# =========================
# ENV SETTINGS
# =========================
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

PAIR_CONFIG = os.getenv("PAIR_CONFIG", "SOLUSDC|AVAXUSDC|100|-10000")
CHECK_SEC = int(os.getenv("CHECK_SEC", "2"))
POST_CLOSE_SLEEP_SEC = int(os.getenv("POST_CLOSE_SLEEP_SEC", "2"))

# Timestamp problems fix
RECV_WINDOW = int(os.getenv("RECV_WINDOW", "60000"))  # 60 seconds

if not API_KEY or not API_SECRET:
    raise SystemExit("Missing BINANCE_API_KEY or BINANCE_API_SECRET")

client = Client(API_KEY, API_SECRET)


# =========================
# DATA STRUCTURES
# =========================
@dataclass
class PairRule:
    sym1: str
    sym2: str
    target_pnl: float
    stop_pnl: float


@dataclass
class PairState:
    disabled: bool = False  # no-cycle: once closed, pair is disabled


# =========================
# HELPERS
# =========================
def parse_pair_config(raw: str) -> List[PairRule]:
    """
    Format:
      PAIR_CONFIG = "SYM1|SYM2|TARGET|STOP;SYM3|SYM4|TARGET|STOP;..."
    Example:
      "SOLUSDC|AVAXUSDC|100|-10000;BNBUSDT|ETHUSDT|50|-500"
    """
    rules: List[PairRule] = []
    chunks = [c.strip() for c in raw.split(";") if c.strip()]
    for c in chunks:
        parts = [p.strip() for p in c.split("|")]
        if len(parts) != 4:
            raise SystemExit(
                f"PAIR_CONFIG parsing error. Each pair must be: SYM1|SYM2|TARGET|STOP. Got: {c}"
            )

        sym1, sym2 = parts[0].upper(), parts[1].upper()
        target = float(parts[2])
        stop = float(parts[3])  # stop should be negative (e.g. -200)

        rules.append(PairRule(sym1=sym1, sym2=sym2, target_pnl=target, stop_pnl=stop))

    return rules


def sync_time() -> None:
    """
    Fix Binance -1021 error by syncing local time to server time.
    """
    try:
        server_time = client.futures_time()["serverTime"]
        local_time = int(time.time() * 1000)
        client.timestamp_offset = server_time - local_time
        print(f"✅ Time synced. offset(ms)={client.timestamp_offset}")
    except Exception as e:
        print(f"⚠️ Time sync failed: {e}")


def get_position_info(symbol: str) -> Tuple[float, float]:
    """
    Returns:
      positionAmt: + long, - short, 0 no position
      unRealizedProfit: PnL (USDT/USDC)
    """
    data = client.futures_position_information(symbol=symbol, recvWindow=RECV_WINDOW)
    pos = data[0]
    amt = float(pos["positionAmt"])
    pnl = float(pos["unRealizedProfit"])
    return amt, pnl


def close_position_market(symbol: str, position_amt: float) -> None:
    """
    Close open position using reduceOnly MARKET order.
    """
    if position_amt == 0:
        print(f"    [{symbol}] No position to close.")
        return

    side = "SELL" if position_amt > 0 else "BUY"
    qty = abs(position_amt)

    print(f"    [{symbol}] Closing: side={side}, qty={qty}")

    client.futures_create_order(
        symbol=symbol,
        side=side,
        type="MARKET",
        quantity=qty,
        reduceOnly=True,
        recvWindow=RECV_WINDOW
    )


# =========================
# MAIN LOOP
# =========================
def main():
    rules = parse_pair_config(PAIR_CONFIG)

    states: Dict[str, PairState] = {}
    for r in rules:
        key = f"{r.sym1}/{r.sym2}"
        states[key] = PairState(disabled=False)

    print("✅ Multi-Pair PNL Watcher started (NO CYCLE per pair)")
    print("Pairs:")
    for r in rules:
        print(f"  - {r.sym1}/{r.sym2} | TP={r.target_pnl} | SL={r.stop_pnl}")
    print(f"CHECK_SEC={CHECK_SEC} | POST_CLOSE_SLEEP_SEC={POST_CLOSE_SLEEP_SEC} | RECV_WINDOW={RECV_WINDOW}")

    # Initial time sync
    sync_time()

    while True:
        try:
            for r in rules:
                key = f"{r.sym1}/{r.sym2}"
                st = states[key]

                # ✅ this pair is already closed once -> no cycle -> ignore
                if st.disabled:
                    continue

                amt1, pnl1 = get_position_info(r.sym1)
                amt2, pnl2 = get_position_info(r.sym2)
                total = pnl1 + pnl2

                print(
                    f"PAIR {key} | "
                    f"{r.sym1}: {pnl1:.2f} (amt={amt1}) | "
                    f"{r.sym2}: {pnl2:.2f} (amt={amt2}) | "
                    f"TOTAL: {total:.2f} | TP={r.target_pnl} SL={r.stop_pnl}"
                )

                # no position on this pair
                if amt1 == 0 and amt2 == 0:
                    continue

                # ✅ Take Profit: close this pair only, bot continues
                if total >= r.target_pnl:
                    print(f"✅ PAIR {key} TARGET HIT -> closing both legs (bot continues)...")
                    close_position_market(r.sym1, amt1)
                    close_position_market(r.sym2, amt2)
                    time.sleep(POST_CLOSE_SLEEP_SEC)
                    st.disabled = True
                    print(f"✅ PAIR {key} CLOSED and DISABLED (no cycle).")
                    continue

                # ✅ Stop Loss: close this pair only, bot continues
                if total <= r.stop_pnl:
                    print(f"🛑 PAIR {key} STOP HIT -> closing both legs (bot continues)...")
                    close_position_market(r.sym1, amt1)
                    close_position_market(r.sym2, amt2)
                    time.sleep(POST_CLOSE_SLEEP_SEC)
                    st.disabled = True
                    print(f"🛑 PAIR {key} CLOSED and DISABLED (no cycle).")
                    continue

            time.sleep(CHECK_SEC)

        except Exception as e:
            msg = str(e)
            print(f"ERROR: {e}")

            # Binance timestamp error fix
            if "code=-1021" in msg:
                print("🔄 Timestamp issue detected (-1021). Re-syncing time...")
                sync_time()

            time.sleep(2)


if __name__ == "__main__":
    main()
