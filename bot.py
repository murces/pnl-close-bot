import os
import time
import math
import statistics
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from binance.client import Client

# =========================
# ENV
# =========================
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
if not API_KEY or not API_SECRET:
    raise SystemExit("Missing BINANCE_API_KEY / BINANCE_API_SECRET")

UNIVERSE_QUOTE = os.getenv("UNIVERSE_QUOTE", "USDT").upper()  # USDT or USDC
TOP_N_COINS = int(os.getenv("TOP_N_COINS", "40"))
SELECT_TOP_PAIRS = int(os.getenv("SELECT_TOP_PAIRS", "5"))

BAR_INTERVAL = os.getenv("BAR_INTERVAL", "1m")
LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "1440"))  # 1 day default

ENTRY_Z = float(os.getenv("ENTRY_Z", "2.0"))
EXIT_Z = float(os.getenv("EXIT_Z", "0.3"))

MAX_USD_PER_LEG = float(os.getenv("MAX_USD_PER_LEG", "500"))
TP_USD = float(os.getenv("TP_USD", "25"))
SL_USD = float(os.getenv("SL_USD", "-80"))
MAX_HOLD_MIN = int(os.getenv("MAX_HOLD_MIN", "180"))
MAX_OPEN_PAIRS = int(os.getenv("MAX_OPEN_PAIRS", "3"))
COOLDOWN_MIN = int(os.getenv("COOLDOWN_MIN", "10"))

CHECK_SEC = int(os.getenv("CHECK_SEC", "5"))
RECV_WINDOW = int(os.getenv("RECV_WINDOW", "60000"))

client = Client(API_KEY, API_SECRET)

# Caches
_SYMBOL_SET: Optional[set] = None
_LOT_CACHE: Dict[str, Tuple[float, float]] = {}  # symbol -> (stepSize, minQty)

# =========================
# DATA
# =========================
@dataclass
class Pair:
    a: str
    b: str

@dataclass
class PairState:
    open: bool = False
    entry_time: float = 0.0
    last_close_time: float = 0.0
    dir_a: int = 0   # +1 long, -1 short
    dir_b: int = 0

# =========================
# BINANCE HELPERS
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
        return True

def get_mark(symbol: str) -> float:
    mp = client.futures_mark_price(symbol=symbol)
    return float(mp["markPrice"])

def get_position(symbol: str) -> Tuple[float, float]:
    """
    returns (positionAmt, unrealizedProfit)
    """
    data = client.futures_position_information(symbol=symbol, recvWindow=RECV_WINDOW)
    pos = data[0]
    return float(pos["positionAmt"]), float(pos["unRealizedProfit"])

def get_step_min(symbol: str) -> Tuple[float, float]:
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
            raise RuntimeError(f"Symbol not found: {symbol}")
    return _LOT_CACHE[symbol]

def round_qty(symbol: str, qty: float) -> float:
    step, minq = get_step_min(symbol)
    if qty <= 0:
        return 0.0
    rounded = math.floor(qty / step) * step
    if rounded < minq:
        return 0.0
    return float(f"{rounded:.12f}")

def open_market(symbol: str, direction: int, usd: float) -> None:
    price = get_mark(symbol)
    qty = round_qty(symbol, usd / price)
    if qty <= 0:
        raise RuntimeError(f"[{symbol}] qty too small for usd={usd}")
    side = "BUY" if direction > 0 else "SELL"
    client.futures_create_order(
        symbol=symbol, side=side, type="MARKET", quantity=qty, recvWindow=RECV_WINDOW
    )

def close_market_reduce_only(symbol: str, position_amt: float) -> None:
    if position_amt == 0:
        return
    side = "SELL" if position_amt > 0 else "BUY"
    qty = abs(position_amt)
    client.futures_create_order(
        symbol=symbol, side=side, type="MARKET", quantity=qty, reduceOnly=True, recvWindow=RECV_WINDOW
    )

# =========================
# DATA / STATS
# =========================
def fetch_closes(symbol: str, interval: str, lookback_minutes: int) -> List[float]:
    # Binance futures klines: limit max 1500. For 1m and 1440 minutes, it's ok.
    limit = min(1500, max(50, lookback_minutes))
    kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    closes = [float(k[4]) for k in kl]
    return closes

def returns(series: List[float]) -> List[float]:
    out = []
    for i in range(1, len(series)):
        if series[i-1] == 0:
            out.append(0.0)
        else:
            out.append((series[i] / series[i-1]) - 1.0)
    return out

def corr(x: List[float], y: List[float]) -> float:
    n = min(len(x), len(y))
    if n < 30:
        return 0.0
    x = x[-n:]
    y = y[-n:]
    mx = statistics.mean(x)
    my = statistics.mean(y)
    sx = statistics.pstdev(x)
    sy = statistics.pstdev(y)
    if sx == 0 or sy == 0:
        return 0.0
    cov = sum((x[i]-mx)*(y[i]-my) for i in range(n)) / n
    return cov / (sx*sy)

def zscore(series: List[float]) -> float:
    if len(series) < 50:
        return 0.0
    m = statistics.mean(series)
    s = statistics.pstdev(series)
    if s == 0:
        return 0.0
    return (series[-1] - m) / s

def mean_reversion_score(ratio_series: List[float]) -> float:
    """
    Simple score:
      - higher when ratio zscore frequently crosses 0 (mean)
    """
    if len(ratio_series) < 100:
        return 0.0
    m = statistics.mean(ratio_series)
    # count sign changes around mean
    crossings = 0
    prev = ratio_series[0] - m
    for v in ratio_series[1:]:
        cur = v - m
        if (prev <= 0 < cur) or (prev >= 0 > cur):
            crossings += 1
        prev = cur
    return crossings / len(ratio_series)

# =========================
# PAIR SELECTION
# =========================
def get_universe_symbols(quote: str, top_n: int) -> List[str]:
    """
    Picks top_n symbols by 24h quoteVolume from futures tickers.
    """
    tickers = client.futures_ticker()  # list of tickers
    candidates = []
    for t in tickers:
        sym = t.get("symbol", "")
        if not sym.endswith(quote):
            continue
        if not is_valid_symbol(sym):
            continue
        # exclude very weird symbols if needed
        try:
            qv = float(t.get("quoteVolume", 0.0))
        except Exception:
            qv = 0.0
        candidates.append((qv, sym))
    candidates.sort(reverse=True, key=lambda x: x[0])
    return [s for _, s in candidates[:top_n]]

def select_pairs(symbols: List[str], k: int) -> List[Pair]:
    """
    For each pair (i,j), compute:
      score = 0.7*abs(corr(returns)) + 0.3*mean_reversion_score(ratio)
    Pick top k.
    """
    scored = []
    # limit combinations to keep runtime reasonable
    max_syms = min(len(symbols), 25)  # keep MVP light
    base = symbols[:max_syms]

    # fetch closes once
    closes_map: Dict[str, List[float]] = {}
    for s in base:
        try:
            closes_map[s] = fetch_closes(s, BAR_INTERVAL, LOOKBACK_MINUTES)
        except Exception:
            continue

    keys = list(closes_map.keys())
    for i in range(len(keys)):
        for j in range(i+1, len(keys)):
            a, b = keys[i], keys[j]
            ca, cb = closes_map[a], closes_map[b]
            n = min(len(ca), len(cb))
            if n < 200:
                continue
            ca, cb = ca[-n:], cb[-n:]
            ra, rb = returns(ca), returns(cb)
            c = abs(corr(ra, rb))
            ratio_series = [ca[x] / cb[x] if cb[x] != 0 else 0.0 for x in range(n)]
            mr = mean_reversion_score(ratio_series)
            score = 0.7 * c + 0.3 * mr
            scored.append((score, a, b))

    scored.sort(reverse=True, key=lambda x: x[0])
    out = []
    for s, a, b in scored[:k]:
        out.append(Pair(a=a, b=b))
    return out

# =========================
# TRADING LOGIC
# =========================
def compute_pair_z(pair: Pair) -> float:
    ca = fetch_closes(pair.a, BAR_INTERVAL, LOOKBACK_MINUTES)
    cb = fetch_closes(pair.b, BAR_INTERVAL, LOOKBACK_MINUTES)
    n = min(len(ca), len(cb))
    ca, cb = ca[-n:], cb[-n:]
    ratio_series = [ca[i] / cb[i] if cb[i] != 0 else 0.0 for i in range(n)]
    return zscore(ratio_series)

def open_pair_market(pair: Pair, st: PairState, z: float) -> None:
    # If z positive: ratio high -> short A, long B (expect ratio to fall)
    if z >= 0:
        st.dir_a, st.dir_b = -1, +1
    else:
        st.dir_a, st.dir_b = +1, -1

    print(f"🚀 ENTRY {pair.a}/{pair.b} | z={z:.2f} | dirA={st.dir_a} dirB={st.dir_b} | usd/leg={MAX_USD_PER_LEG}")
    # taker-only, send both
    open_market(pair.a, st.dir_a, MAX_USD_PER_LEG)
    open_market(pair.b, st.dir_b, MAX_USD_PER_LEG)

    st.open = True
    st.entry_time = time.time()

def close_pair(pair: Pair) -> None:
    amt_a, _ = get_position(pair.a)
    amt_b, _ = get_position(pair.b)
    print(f"🧯 EXIT {pair.a}/{pair.b} | closing | amtA={amt_a} amtB={amt_b}")
    close_market_reduce_only(pair.a, amt_a)
    close_market_reduce_only(pair.b, amt_b)

def main():
    sync_time()

    print("✅ Auto Pair Trading Bot (Taker-only, Unrealized PnL TP/SL)")
    print(f"Universe quote={UNIVERSE_QUOTE}, TOP_N_COINS={TOP_N_COINS}, SELECT_TOP_PAIRS={SELECT_TOP_PAIRS}")
    print(f"ENTRY_Z={ENTRY_Z}, EXIT_Z={EXIT_Z}, TP_USD={TP_USD}, SL_USD={SL_USD}, MAX_HOLD_MIN={MAX_HOLD_MIN}")
    print(f"MAX_USD_PER_LEG={MAX_USD_PER_LEG}, MAX_OPEN_PAIRS={MAX_OPEN_PAIRS}, COOLDOWN_MIN={COOLDOWN_MIN}, CHECK_SEC={CHECK_SEC}")

    # 1) select pairs on startup (MVP)
    symbols = get_universe_symbols(UNIVERSE_QUOTE, TOP_N_COINS)
    pairs = select_pairs(symbols, SELECT_TOP_PAIRS)

    if not pairs:
        raise SystemExit("No pairs selected. Try increasing TOP_N_COINS or LOOKBACK_MINUTES.")

    print("✅ Selected pairs:")
    for p in pairs:
        print(f"  - {p.a}/{p.b}")

    states: Dict[str, PairState] = {f"{p.a}/{p.b}": PairState() for p in pairs}

    while True:
        try:
            open_count = 0
            for p in pairs:
                key = f"{p.a}/{p.b}"
                st = states[key]

                # cooldown
                if st.last_close_time and (time.time() - st.last_close_time) < (COOLDOWN_MIN * 60):
                    continue

                # read positions & pnl
                amt_a, pnl_a = get_position(p.a)
                amt_b, pnl_b = get_position(p.b)

                # safety: one leg open
                if (amt_a == 0) != (amt_b == 0):
                    print(f"⚠️ ONE-LEG RISK {key} | amtA={amt_a} amtB={amt_b} -> forcing close remaining leg")
                    close_pair(p)
                    st.open = False
                    st.last_close_time = time.time()
                    continue

                total_pnl = pnl_a + pnl_b

                # infer open state from exchange if needed
                if amt_a != 0 and amt_b != 0:
                    st.open = True
                    open_count += 1
                else:
                    st.open = False

                # If open -> exit checks
                if st.open:
                    held_min = (time.time() - st.entry_time) / 60 if st.entry_time else 0.0
                    # z for exit (mean reversion)
                    z = compute_pair_z(p)

                    print(f"PAIR {key} | PNL={total_pnl:.2f} | z={z:.2f} | held={held_min:.1f}m")

                    if total_pnl >= TP_USD:
                        print(f"✅ TP HIT {key} -> close")
                        close_pair(p)
                        st.open = False
                        st.last_close_time = time.time()
                        time.sleep(2)
                        continue

                    if total_pnl <= SL_USD:
                        print(f"🛑 SL HIT {key} -> close")
                        close_pair(p)
                        st.open = False
                        st.last_close_time = time.time()
                        time.sleep(2)
                        continue

                    if abs(z) <= EXIT_Z:
                        print(f"✅ EXIT_Z HIT {key} -> close")
                        close_pair(p)
                        st.open = False
                        st.last_close_time = time.time()
                        time.sleep(2)
                        continue

                    if held_min >= MAX_HOLD_MIN:
                        print(f"⏳ TIME STOP {key} -> close")
                        close_pair(p)
                        st.open = False
                        st.last_close_time = time.time()
                        time.sleep(2)
                        continue

                    continue

                # If flat -> entry checks (respect max open pairs)
                if open_count >= MAX_OPEN_PAIRS:
                    continue

                z = compute_pair_z(p)
                print(f"SCAN {key} | z={z:.2f}")

                if abs(z) >= ENTRY_Z:
                    open_pair_market(p, st, z)
                    time.sleep(1)  # small spacing
                    continue

            time.sleep(CHECK_SEC)

        except Exception as e:
            msg = str(e)
            print(f"ERROR: {e}")
            if "code=-1021" in msg:
                print("🔄 Timestamp issue (-1021) -> resync")
                sync_time()
            time.sleep(2)

if __name__ == "__main__":
    main()
