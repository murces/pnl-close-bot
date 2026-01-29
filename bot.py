import os
import time
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from binance.client import Client

# =========================
# ENV
# =========================
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

PAIR_CONFIG = os.getenv("PAIR_CONFIG", "GMXUSDT|INJUSDT|200|-10000|2000|0.001")
CHECK_SEC = int(os.getenv("CHECK_SEC", "2"))
POST_CLOSE_SLEEP_SEC = int(os.getenv("POST_CLOSE_SLEEP_SEC", "2"))

RECV_WINDOW = int(os.getenv("RECV_WINDOW", "60000"))
REENTRY_COOLDOWN_SEC = int(os.getenv("REENTRY_COOLDOWN_SEC", "30"))

DEFAULT_MAX_USD_PER_LEG = float(os.getenv("DEFAULT_MAX_USD_PER_LEG", "2000"))
DEFAULT_REENTRY_BAND_PCT = float(os.getenv("DEFAULT_REENTRY_BAND_PCT", "0.001"))

if not API_KEY or not API_SECRET:
    raise SystemExit("Missing BINANCE_API_KEY or BINANCE_API_SECRET")

client = Client(API_KEY, API_SECRET)

# caches
_LOT_CACHE: Dict[str, Tuple[float, float]] = {}
_SYMBOL_SET: Optional[set] = None


# =========================
# DATA
# =========================
@dataclass
class PairRule:
    sym1: str
    sym2: str
    target_pnl: float
    stop_pnl: float
    max_usd_per_leg: float
    reentry_band_pct: float


@dataclass
class PairState:
    baseline_ratio: Optional[float] = None  # set ON RESTART from mark ratio and stays until next restart
    dir1: Optional[int] = None              # +1 long, -1 short  (learned from OPEN positions on restart or later)
    dir2: Optional[int] = None
    last_close_time: float = 0.0
    armed: bool = False                     # re-entry only after at least one close event
    disabled: bool = False                  # invalid symbol etc.


# =========================
# HELPERS
# =========================
def sync_time() -> None:
    try:
        server_time = client.futures_time()["serverTime"]
        local_time = int(time.time() * 1000)
        client.timestamp_offset = server_time - local_time
        print(f"✅ Time synced. offset(ms)={client.timestamp_offset}")
    except Exception as e:
        print(f"⚠️ Time sync failed: {e}")


def load_symbol_set() -> set:
    global _SYMBOL_SET
    if _SYMBOL_SET is None:
        info = client.futures_exchange_info()
        _SYMBOL_SET = set(s["symbol"] for s in info["symbols"])
    return _SYMBOL_SET


def is_valid_symbol(symbol: str) -> bool:
    try:
        return symbol in load_symbol_set()
    except Exception:
        # If exchangeInfo fails, runtime calls will reveal invalid symbols.
        return True


def parse_pair_config(raw: str) -> List[PairRule]:
    """
    Supports:
      SYM1|SYM2|TP|SL
      SYM1|SYM2|TP|SL|MAXUSD|BAND
    Multiple pairs separated by ';'
    """
    rules: List[PairRule] = []
    chunks = [c.strip() for c in raw.split(";") if c.strip()]
    for c in chunks:
        parts = [p.strip() for p in c.split("|")]
        if len(parts) not in (4, 6):
            raise SystemExit(
                f"PAIR_CONFIG error. Use 4 or 6 parts: SYM1|SYM2|TP|SL[|MAXUSD|BAND]. Got: {c}"
            )
        sym1, sym2 = parts[0].upper(), parts[1].upper()
        tp = float(parts[2])
        sl = float(parts[3])
        if len(parts) == 6:
            maxusd = float(parts[4])
            band = float(parts[5])
        else:
            maxusd = DEFAULT_MAX_USD_PER_LEG
            band = DEFAULT_REENTRY_BAND_PCT
        rules.append(PairRule(sym1, sym2, tp, sl, maxusd, band))
    return rules


def get_position_info(symbol: str) -> Tuple[float, float]:
    data = client.futures_position_information(symbol=symbol, recvWindow=RECV_WINDOW)
    pos = data[0]
    amt = float(pos["positionAmt"])
    pnl = float(pos["unRealizedProfit"])
    return amt, pnl


def get_mark_price(symbol: str) -> float:
    mp = client.futures_mark_price(symbol=symbol)
    return float(mp["markPrice"])


def current_ratio(sym1: str, sym2: str) -> float:
    return get_mark_price(sym1) / get_mark_price(sym2)


def within_band(cur: float, base: float, band_pct: float) -> bool:
    if base == 0:
        return False
    return abs(cur - base) / base <= band_pct


def get_step_and_min_qty(symbol: str) -> Tuple[float, float]:
    if symbol not in _LOT_CACHE:
        info = client.futures_exchange_info()
        for s in info["symbols"]:
            if s["symbol"] == symbol:
                step = None
                minq = None
                for f in s["filters"]:
                    if f["filterType"] == "LOT_SIZE":
                        step = float(f["stepSize"])
                        minq = float(f["minQty"])
                if step is None or minq is None:
                    raise RuntimeError(f"LOT_SIZE not found for {symbol}")
                _LOT_CACHE[symbol] = (step, minq)
                break
        if symbol not in _LOT_CACHE:
            raise RuntimeError(f"Symbol not found in exchangeInfo: {symbol}")
    return _LOT_CACHE[symbol]


def round_qty(symbol: str, qty: float) -> float:
    step, minq = get_step_and_min_qty(symbol)
    if qty <= 0:
        return 0.0
    rounded = math.floor(qty / step) * step
    if rounded < minq:
        return 0.0
    return float(f"{rounded:.12f}")


def open_position_market(symbol: str, direction: int, usd: float) -> None:
    """
    direction: +1 => BUY (long), -1 => SELL (short)
    usd: notional target per leg (max_usd_per_leg)
    """
    price = get_mark_price(symbol)
    qty = usd / price
    qty = round_qty(symbol, qty)
    if qty <= 0:
        raise RuntimeError(f"[{symbol}] qty too small. usd={usd}, price={price}")

    side = "BUY" if direction > 0 else "SELL"
    print(f"    [{symbol}] OPEN: side={side}, usd≈{usd}, qty={qty}")

    client.futures_create_order(
        symbol=symbol,
        side=side,
        type="MARKET",
        quantity=qty,
        recvWindow=RECV_WINDOW
    )


def close_position_market(symbol: str, position_amt: float) -> None:
    if position_amt == 0:
        print(f"    [{symbol}] No position to close.")
        return
    side = "SELL" if position_amt > 0 else "BUY"
    qty = abs(position_amt)
    print(f"    [{symbol}] CLOSE: side={side}, qty={qty}")
    client.futures_create_order(
        symbol=symbol,
        side=side,
        type="MARKET",
        quantity=qty,
        reduceOnly=True,
        recvWindow=RECV_WINDOW
    )


# =========================
# MAIN
# =========================
def main():
    rules = parse_pair_config(PAIR_CONFIG)
    states: Dict[str, PairState] = {f"{r.sym1}/{r.sym2}": PairState() for r in rules}

    print("✅ Multi-Pair PNL + Restart-Baseline ReEntry bot started (DIR=auto)")
    print("Pairs:")
    for r in rules:
        print(f"  - {r.sym1}/{r.sym2} | TP={r.target_pnl} | SL={r.stop_pnl} | MAXUSD/leg={r.max_usd_per_leg} | band={r.reentry_band_pct}")
    print(f"CHECK_SEC={CHECK_SEC} | COOLDOWN={REENTRY_COOLDOWN_SEC}s | RECV_WINDOW={RECV_WINDOW}")

    sync_time()

    # --- On restart: set baseline_ratio for each pair from current mark ratio ---
    for r in rules:
        key = f"{r.sym1}/{r.sym2}"
        st = states[key]

        if not is_valid_symbol(r.sym1) or not is_valid_symbol(r.sym2):
            st.disabled = True
            print(f"❌ {key} disabled: invalid symbol in exchangeInfo (check futures availability).")
            continue

        try:
            st.baseline_ratio = current_ratio(r.sym1, r.sym2)
            print(f"📌 STARTUP BASELINE for {key}: ratio={st.baseline_ratio:.8f}")
        except Exception as e:
            st.disabled = True
            print(f"❌ {key} disabled at startup (error fetching prices): {e}")

    while True:
        try:
            for r in rules:
                key = f"{r.sym1}/{r.sym2}"
                st = states[key]
                if st.disabled:
                    continue

                amt1, pnl1 = get_position_info(r.sym1)
                amt2, pnl2 = get_position_info(r.sym2)
                total = pnl1 + pnl2

                # If both legs are open and direction is not locked yet -> learn and lock until next restart
                if st.dir1 is None and st.dir2 is None and amt1 != 0 and amt2 != 0:
                    st.dir1 = 1 if amt1 > 0 else -1
                    st.dir2 = 1 if amt2 > 0 else -1
                    print(f"🧭 DIR LOCKED for {key}: dir=({st.dir1},{st.dir2}) [locked until next restart]")

                base_txt = f"{st.baseline_ratio:.8f}" if st.baseline_ratio is not None else "None"
                dir_txt = f"({st.dir1},{st.dir2})" if st.dir1 is not None else "None"
                print(
                    f"PAIR {key} | "
                    f"{r.sym1}: {pnl1:.2f} (amt={amt1}) | "
                    f"{r.sym2}: {pnl2:.2f} (amt={amt2}) | "
                    f"TOTAL: {total:.2f} | TP={r.target_pnl} SL={r.stop_pnl} | BASE={base_txt} | DIR={dir_txt}"
                )

                # Safety: one leg open other closed -> do nothing
                if (amt1 == 0) != (amt2 == 0):
                    print(f"⚠️ WARNING {key}: One leg open, other closed. Skipping actions for safety.")
                    continue

                # -------- OPEN -> manage TP/SL --------
                if amt1 != 0 and amt2 != 0:
                    if total >= r.target_pnl:
                        print(f"✅ {key} TARGET HIT -> closing both legs")
                        close_position_market(r.sym1, amt1)
                        close_position_market(r.sym2, amt2)
                        time.sleep(POST_CLOSE_SLEEP_SEC)
                        st.last_close_time = time.time()
                        st.armed = True
                        print(f"✅ {key} CLOSED. Armed for re-entry at BASELINE.")
                        continue

                    if total <= r.stop_pnl:
                        print(f"🛑 {key} STOP HIT -> closing both legs")
                        close_position_market(r.sym1, amt1)
                        close_position_market(r.sym2, amt2)
                        time.sleep(POST_CLOSE_SLEEP_SEC)
                        st.last_close_time = time.time()
                        st.armed = True
                        print(f"🛑 {key} CLOSED by STOP. Armed for re-entry at BASELINE.")
                        continue

                    continue  # keep monitoring open positions

                # -------- FLAT -> re-entry when ratio returns to startup baseline --------
                # Re-entry only after at least one close, AND directions must be known (locked from an open position)
                if not st.armed:
                    continue
                if st.baseline_ratio is None or st.dir1 is None or st.dir2 is None:
                    continue

                if st.last_close_time and (time.time() - st.last_close_time) < REENTRY_COOLDOWN_SEC:
                    continue

                cur = current_ratio(r.sym1, r.sym2)
                if within_band(cur, st.baseline_ratio, r.reentry_band_pct):
                    print(f"🚀 RE-ENTRY {key}: cur_ratio={cur:.8f} near BASE={st.baseline_ratio:.8f} (band={r.reentry_band_pct})")
                    open_position_market(r.sym1, st.dir1, r.max_usd_per_leg)
                    open_position_market(r.sym2, st.dir2, r.max_usd_per_leg)
                    print(f"🚀 {key} RE-ENTERED. Monitoring for TP/SL.")

            time.sleep(CHECK_SEC)

        except Exception as e:
            msg = str(e)
            print(f"ERROR: {e}")

            if "code=-1021" in msg:
                print("🔄 Timestamp issue detected (-1021). Re-syncing time...")
                sync_time()

            if "code=-1121" in msg:
                print("❌ Invalid symbol detected at runtime. Check PAIR_CONFIG symbols (USDT/USDC availability).")

            time.sleep(2)


if __name__ == "__main__":
    main()
