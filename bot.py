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

PAIR_CONFIG = os.getenv(
    "PAIR_CONFIG",
    "SOLUSDC|AVAXUSDC|100|-10000"
)
CHECK_SEC = int(os.getenv("CHECK_SEC", "2"))

# Kapanıştan sonra borsanın pozisyonu güncellemesi için kısa bekleme
POST_CLOSE_SLEEP_SEC = int(os.getenv("POST_CLOSE_SLEEP_SEC", "2"))

if not API_KEY or not API_SECRET:
    raise SystemExit("Missing BINANCE_API_KEY or BINANCE_API_SECRET")

client = Client(API_KEY, API_SECRET)


@dataclass
class PairRule:
    sym1: str
    sym2: str
    target_pnl: float
    stop_pnl: float


@dataclass
class PairState:
    disabled: bool = False  # ✅ cycle yok: kapandıktan sonra bu pair devre dışı kalır


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
        stop = float(parts[3])  # STOP negatif olmalı (örn -200)
        rules.append(PairRule(sym1, sym2, target, stop))
    return rules


def get_position_info(symbol: str) -> Tuple[float, float]:
    """
    Returns:
      positionAmt: + long, - short, 0 no position
      unRealizedProfit: PnL (USDT/USDC)
    """
    data = client.futures_position_information(symbol=symbol)
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
        reduceOnly=True
    )


def main():
    rules = parse_pair_config(PAIR_CONFIG)

    # state per pair
    states: Dict[str, PairState] = {}
    for r in rules:
        key = f"{r.sym1}/{r.sym2}"
        states[key] = PairState(disabled=False)

    print("✅ Multi-Pair PNL Watcher started (NO CYCLE per pair)")
    print("Pairs:")
    for r in rules:
        print(f"  - {r.sym1}/{r.sym2} | TP={r.target_pnl} | SL={r.stop_pnl}")
    print(f"CHECK_SEC={CHECK_SEC} | POST_CLOSE_SLEEP_SEC={POST_CLOSE_SLEEP_SEC}")

    while True:
        try:
            for r in rules:
                key = f"{r.sym1}/{r.sym2}"
                st = states[key]

                # ✅ Bu pair bir kez kapandıysa artık takip etmiyoruz (cycle yok)
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

                # Bu pairde pozisyon yoksa geç
                if amt1 == 0 and amt2 == 0:
                    continue

                # ✅ TP hit -> bu pair'i kapat + devre dışı bırak
                if total >= r.target_pnl:
                    print(f"✅ PAIR {key} TARGET HIT -> closing both legs (bot continues for others)...")
                    close_position_market(r.sym1, amt1)
                    close_position_market(r.sym2, amt2)
                    time.sleep(POST_CLOSE_SLEEP_SEC)
                    st.disabled = True
                    print(f"✅ PAIR {key} CLOSED and DISABLED (no cycle).")
                    continue

                # ✅ SL hit -> bu pair'i kapat + devre dışı bırak
                if total <= r.stop_pnl:
                    print(f"🛑 PAIR {key} STOP HIT -> closing both legs (bot continues for others)...")
                    close_position_market(r.sym1, amt1)
                    close_position_market(r.sym2, amt2)
                    time.sleep(POST_CLOSE_SLEEP_SEC)
                    st.disabled = True
                    print(f"🛑 PAIR {key} CLOSED and DISABLED (no cycle).")
                    continue

            time.sleep(CHECK_SEC)

        except Exception as e:
            print(f"ERROR: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
