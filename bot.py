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

TRADING_ENABLED = int(os.getenv("TRADING_ENABLED", "0"))  # 0=dry-run, 1=live

UNIVERSE_QUOTE = os.getenv("UNIVERSE_QUOTE", "USDT").upper()
TOP_N_COINS = int(os.getenv("TOP_N_COINS", "40"))
SELECT_TOP_PAIRS = int(os.getenv("SELECT_TOP_PAIRS", "5"))

BAR_INTERVAL = os.getenv("BAR_INTERVAL", "1m")
LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "1440"))

ENTRY_Z = float(os.getenv("ENTRY_Z", "2.0"))
EXIT_Z = float(os.getenv("EXIT_Z", "0.3"))

BASE_USD_PER_LEG = float(os.getenv("BASE_USD_PER_LEG", "500"))
TP_USD = float(os.getenv("TP_USD", "25"))
SL_USD = float(os.getenv("SL_USD", "-80"))
MAX_HOLD_MIN = int(os.getenv("MAX_HOLD_MIN", "180"))
MAX_OPEN_PAIRS = int(os.getenv("MAX_OPEN_PAIRS", "3"))
COOLDOWN_MIN = int(os.getenv("COOLDOWN_MIN", "10"))

CHECK_SEC = int(os.getenv("CHECK_SEC", "10"))
RECV_WINDOW = int(os.getenv("RECV_WINDOW", "60000"))

# Futures settings (live)
HEDGE_MODE = int(os.getenv("HEDGE_MODE", "0"))
LEVERAGE = int(os.getenv("LEVERAGE", "10"))
MARGIN_TYPE = os.getenv("MARGIN_TYPE", "ISOLATED").upper()

# Pair refresh
PAIR_REFRESH_MIN = int(os.getenv("PAIR_REFRESH_MIN", "60"))

# Signal
SIGNAL_MODE = os.getenv("SIGNAL_MODE", "RATIO_Z").upper()  # RATIO_Z or SPREAD_OLS
BETA_LOOKBACK = int(os.getenv("BETA_LOOKBACK", "720"))
ZSCORE_LOOKBACK = int(os.getenv("ZSCORE_LOOKBACK", "720"))

# Half-life filter (minutes)
HL_LOOKBACK = int(os.getenv("HL_LOOKBACK", "720"))
HL_MIN_MIN = int(os.getenv("HL_MIN_MIN", "5"))
HL_MAX_MIN = int(os.getenv("HL_MAX_MIN", "180"))

client = Client(API_KEY, API_SECRET)

# =========================
# CACHES
# =========================
_SYMBOL_SET: Optional[set] = None
_LOT_CACHE: Dict[str, Tuple[float, float]] = {}
_KLINE_CACHE: Dict[Tuple[str, str, int], Tuple[int, List[float]]] = {}

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
    dir_a: int = 0
    dir_b: int = 0
    beta: float = 1.0
    alpha: float = 0.0

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

def floor_to_step(symbol: str, qty: float) -> float:
    step, minq = get_step_min(symbol)
    if qty <= 0:
        return 0.0
    q = math.floor(qty / step) * step
    q = float(f"{q:.12f}")
    if q < minq:
        return 0.0
    return q

def ensure_symbol_settings(symbol: str) -> None:
    if TRADING_ENABLED == 0:
        return
    try:
        client.futures_change_leverage(symbol=symbol, leverage=LEVERAGE, recvWindow=RECV_WINDOW)
    except Exception as e:
        print(f"⚠️ leverage set failed {symbol}: {e}")
    try:
        client.futures_change_margin_type(symbol=symbol, marginType=MARGIN_TYPE, recvWindow=RECV_WINDOW)
    except Exception as e:
        msg = str(e).lower()
        if "no need to change margin type" not in msg:
            print(f"⚠️ margin type set failed {symbol}: {e}")

def open_market(symbol: str, direction: int, usd: float) -> None:
    if TRADING_ENABLED == 0:
        return
    price = get_mark(symbol)
    qty = floor_to_step(symbol, usd / price)
    if qty <= 0:
        raise RuntimeError(f"[{symbol}] qty too small for usd={usd}")
    side = "BUY" if direction > 0 else "SELL"
    params = dict(symbol=symbol, side=side, type="MARKET", quantity=qty, recvWindow=RECV_WINDOW)
    if HEDGE_MODE == 1:
        params["positionSide"] = "LONG" if direction > 0 else "SHORT"
    client.futures_create_order(**params)

def close_market_reduce_only(symbol: str, position_amt: float) -> None:
    if TRADING_ENABLED == 0:
        return
    if position_amt == 0:
        return
    side = "SELL" if position_amt > 0 else "BUY"
    qty = floor_to_step(symbol, abs(position_amt))
    if qty <= 0:
        return
    params = dict(symbol=symbol, side=side, type="MARKET", quantity=qty, reduceOnly=True, recvWindow=RECV_WINDOW)
    if HEDGE_MODE == 1:
        params["positionSide"] = "LONG" if position_amt > 0 else "SHORT"
    client.futures_create_order(**params)

# =========================
# DATA / STATS
# =========================
def interval_to_minutes(interval: str) -> int:
    interval = interval.strip().lower()
    if interval.endswith("m"):
        return int(interval[:-1])
    if interval.endswith("h"):
        return int(interval[:-1]) * 60
    if interval.endswith("d"):
        return int(interval[:-1]) * 1440
    raise ValueError(f"Unsupported interval: {interval}")

def fetch_closes(symbol: str, interval: str, lookback_minutes: int) -> List[float]:
    mins = interval_to_minutes(interval)
    bars = max(50, lookback_minutes // mins)
    limit = min(1500, bars)
    cache_key = (symbol, interval, limit)

    try:
        kl1 = client.futures_klines(symbol=symbol, interval=interval, limit=1)
        last_open_time = int(kl1[-1][0])
        cached = _KLINE_CACHE.get(cache_key)
        if cached and cached[0] == last_open_time:
            return cached[1]
    except Exception:
        pass

    kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    last_open_time = int(kl[-1][0])
    closes = [float(k[4]) for k in kl]
    _KLINE_CACHE[cache_key] = (last_open_time, closes)
    return closes

def returns(series: List[float]) -> List[float]:
    out = []
    for i in range(1, len(series)):
        if series[i - 1] == 0:
            out.append(0.0)
        else:
            out.append((series[i] / series[i - 1]) - 1.0)
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
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n)) / n
    return cov / (sx * sy)

def zscore_last(series: List[float], lookback: int) -> float:
    if len(series) < max(50, lookback):
        return 0.0
    w = series[-lookback:]
    m = statistics.mean(w)
    s = statistics.pstdev(w)
    if s == 0:
        return 0.0
    return (w[-1] - m) / s

def mean_reversion_score(ratio_series: List[float]) -> float:
    if len(ratio_series) < 100:
        return 0.0
    m = statistics.mean(ratio_series)
    crossings = 0
    prev = ratio_series[0] - m
    for v in ratio_series[1:]:
        cur = v - m
        if (prev <= 0 < cur) or (prev >= 0 > cur):
            crossings += 1
        prev = cur
    return crossings / len(ratio_series)

def log_prices(closes: List[float]) -> List[float]:
    out = []
    for c in closes:
        out.append(math.log(c) if c > 0 else 0.0)
    return out

def ols_beta_alpha(x: List[float], y: List[float]) -> Tuple[float, float]:
    n = min(len(x), len(y))
    if n < 50:
        return 1.0, 0.0
    x = x[-n:]
    y = y[-n:]
    mx = statistics.mean(x)
    my = statistics.mean(y)
    vy = sum((yi - my) ** 2 for yi in y) / n
    if vy == 0:
        return 1.0, mx - my
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n)) / n
    beta = cov / vy
    alpha = mx - beta * my
    if not math.isfinite(beta):
        beta = 1.0
    beta = max(0.0, min(beta, 5.0))
    return beta, alpha

def spread_series_from_prices(a_closes: List[float], b_closes: List[float], beta: float, alpha: float) -> List[float]:
    xa = log_prices(a_closes)
    yb = log_prices(b_closes)
    n = min(len(xa), len(yb))
    xa = xa[-n:]
    yb = yb[-n:]
    return [xa[i] - (alpha + beta * yb[i]) for i in range(n)]

def half_life_minutes(spread: List[float], interval: str, lookback: int) -> Optional[float]:
    """
    Estimate mean reversion half-life via AR(1):
      Δs_t = a + b * s_{t-1} + e_t
    Half-life (bars) = -ln(2) / ln(1+b)  (for -1<b<0)
    Convert to minutes using interval.
    """
    if len(spread) < max(60, lookback):
        return None

    mins_per_bar = interval_to_minutes(interval)
    w = spread[-lookback:]
    s_lag = w[:-1]
    ds = [w[i] - w[i-1] for i in range(1, len(w))]  # Δs_t aligned with s_{t-1}

    n = len(ds)
    if n < 50:
        return None

    mx = statistics.mean(s_lag)
    my = statistics.mean(ds)

    vx = sum((x - mx) ** 2 for x in s_lag) / n
    if vx == 0:
        return None
    cov = sum((s_lag[i] - mx) * (ds[i] - my) for i in range(n)) / n
    b = cov / vx  # slope

    # need mean reversion: b negative and (1+b) between (0,1)
    if not math.isfinite(b):
        return None
    if b >= 0:
        return None
    one_plus_b = 1.0 + b
    if one_plus_b <= 0 or one_plus_b >= 1:
        return None

    hl_bars = -math.log(2.0) / math.log(one_plus_b)
    if not math.isfinite(hl_bars) or hl_bars <= 0:
        return None

    return hl_bars * mins_per_bar

# =========================
# PAIR SELECTION
# =========================
def get_universe_symbols(quote: str, top_n: int) -> List[str]:
    tickers = client.futures_ticker()
    candidates = []
    for t in tickers:
        sym = t.get("symbol", "")
        if not sym.endswith(quote):
            continue
        if not is_valid_symbol(sym):
            continue
        try:
            qv = float(t.get("quoteVolume", 0.0))
        except Exception:
            qv = 0.0
        candidates.append((qv, sym))
    candidates.sort(reverse=True, key=lambda x: x[0])
    return [s for _, s in candidates[:top_n]]

def select_pairs(symbols: List[str], k: int) -> List[Pair]:
    scored = []
    max_syms = min(len(symbols), 25)
    base = symbols[:max_syms]

    closes_map: Dict[str, List[float]] = {}
    for s in base:
        try:
            closes_map[s] = fetch_closes(s, BAR_INTERVAL, LOOKBACK_MINUTES)
        except Exception:
            continue

    keys = list(closes_map.keys())
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
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
    return [Pair(a=a, b=b) for _, a, b in scored[:k]]

# =========================
# SIGNAL
# =========================
def compute_signal(pair: Pair) -> Tuple[float, float, float, Optional[float]]:
    """
    Returns (z, beta, alpha, half_life_min)
    """
    ca = fetch_closes(pair.a, BAR_INTERVAL, LOOKBACK_MINUTES)
    cb = fetch_closes(pair.b, BAR_INTERVAL, LOOKBACK_MINUTES)
    n = min(len(ca), len(cb))
    ca, cb = ca[-n:], cb[-n:]

    if SIGNAL_MODE == "SPREAD_OLS":
        beta_lb = min(BETA_LOOKBACK, n)
        z_lb = min(ZSCORE_LOOKBACK, n)
        hl_lb = min(HL_LOOKBACK, n)

        xa = log_prices(ca[-beta_lb:])
        yb = log_prices(cb[-beta_lb:])
        beta, alpha = ols_beta_alpha(xa, yb)

        spread = spread_series_from_prices(ca, cb, beta, alpha)
        z = zscore_last(spread, lookback=max(50, z_lb))
        hl = half_life_minutes(spread, BAR_INTERVAL, lookback=max(60, hl_lb))
        return z, beta, alpha, hl

    ratio = [ca[i] / cb[i] if cb[i] != 0 else 0.0 for i in range(n)]
    z = zscore_last(ratio, lookback=max(50, min(ZSCORE_LOOKBACK, n)))
    return z, 1.0, 0.0, None

# =========================
# TRADING
# =========================
def open_pair_market(pair: Pair, st: PairState, z: float, beta: float, alpha: float) -> None:
    if z >= 0:
        dir_a, dir_b = -1, +1
    else:
        dir_a, dir_b = +1, -1

    beta_eff = max(0.25, min(beta, 4.0))
    usd_a = BASE_USD_PER_LEG
    usd_b = BASE_USD_PER_LEG * beta_eff

    st.dir_a, st.dir_b = dir_a, dir_b
    st.beta, st.alpha = beta_eff, alpha
    st.open = True
    st.entry_time = time.time()

    if TRADING_ENABLED == 0:
        print(f"[DRY] ENTRY {pair.a}/{pair.b} | z={z:.2f} | beta={beta_eff:.3f} alpha={alpha:.3f} | "
              f"dirA={dir_a} usdA={usd_a:.2f} | dirB={dir_b} usdB={usd_b:.2f}")
        return

    print(f"🚀 ENTRY {pair.a}/{pair.b} | z={z:.2f} | beta={beta_eff:.3f} alpha={alpha:.3f} | "
          f"dirA={dir_a} usdA={usd_a:.2f} | dirB={dir_b} usdB={usd_b:.2f}")
    open_market(pair.a, dir_a, usd_a)
    open_market(pair.b, dir_b, usd_b)

def close_pair(pair: Pair, st: PairState, reason: str) -> None:
    st.open = False
    st.last_close_time = time.time()

    if TRADING_ENABLED == 0:
        print(f"[DRY] EXIT {pair.a}/{pair.b} | reason={reason}")
        return

    amt_a, _ = get_position(pair.a)
    amt_b, _ = get_position(pair.b)
    print(f"🧯 EXIT {pair.a}/{pair.b} | reason={reason} | amtA={amt_a} amtB={amt_b}")
    close_market_reduce_only(pair.a, amt_a)
    close_market_reduce_only(pair.b, amt_b)

def refresh_pairs() -> List[Pair]:
    symbols = get_universe_symbols(UNIVERSE_QUOTE, TOP_N_COINS)
    new_pairs = select_pairs(symbols, SELECT_TOP_PAIRS)
    return new_pairs if new_pairs else []

# =========================
# MAIN
# =========================
def main():
    sync_time()

    print("✅ Auto Pair Trading Bot (OLS Spread Z + Half-Life Filter + DRY State)")
    print(f"TRADING_ENABLED={TRADING_ENABLED} | HEDGE_MODE={HEDGE_MODE} | LEVERAGE={LEVERAGE} | MARGIN_TYPE={MARGIN_TYPE}")
    print(f"Universe quote={UNIVERSE_QUOTE}, TOP_N_COINS={TOP_N_COINS}, SELECT_TOP_PAIRS={SELECT_TOP_PAIRS}")
    print(f"BAR_INTERVAL={BAR_INTERVAL}, LOOKBACK_MINUTES={LOOKBACK_MINUTES}, CHECK_SEC={CHECK_SEC}")
    print(f"ENTRY_Z={ENTRY_Z}, EXIT_Z={EXIT_Z}, TP_USD={TP_USD}, SL_USD={SL_USD}, MAX_HOLD_MIN={MAX_HOLD_MIN}")
    print(f"BASE_USD_PER_LEG={BASE_USD_PER_LEG}, MAX_OPEN_PAIRS={MAX_OPEN_PAIRS}, COOLDOWN_MIN={COOLDOWN_MIN}")
    print(f"PAIR_REFRESH_MIN={PAIR_REFRESH_MIN} | SIGNAL_MODE={SIGNAL_MODE} | BETA_LOOKBACK={BETA_LOOKBACK} | ZSCORE_LOOKBACK={ZSCORE_LOOKBACK}")
    print(f"HL_LOOKBACK={HL_LOOKBACK} | HL_MIN_MIN={HL_MIN_MIN} | HL_MAX_MIN={HL_MAX_MIN}")

    pairs = refresh_pairs()
    if not pairs:
        raise SystemExit("No pairs selected. Try increasing TOP_N_COINS or LOOKBACK_MINUTES.")

    print("✅ Selected pairs:")
    for p in pairs:
        print(f"  - {p.a}/{p.b}")

    if TRADING_ENABLED == 1:
        uniq = set()
        for p in pairs:
            uniq.add(p.a); uniq.add(p.b)
        for sym in sorted(uniq):
            ensure_symbol_settings(sym)

    states: Dict[str, PairState] = {f"{p.a}/{p.b}": PairState() for p in pairs}
    last_refresh_ts = time.time()

    while True:
        try:
            open_count = sum(1 for s in states.values() if s.open)
            any_open_state = any(s.open for s in states.values())

            if (time.time() - last_refresh_ts) >= (PAIR_REFRESH_MIN * 60) and not any_open_state:
                print("🔄 REFRESH: selecting new pairs...")
                new_pairs = refresh_pairs()
                if new_pairs:
                    pairs = new_pairs
                    states = {f"{p.a}/{p.b}": PairState() for p in pairs}
                    print("✅ REFRESH done. New pairs:")
                    for p in pairs:
                        print(f"  - {p.a}/{p.b}")
                    if TRADING_ENABLED == 1:
                        uniq = set()
                        for p in pairs:
                            uniq.add(p.a); uniq.add(p.b)
                        for sym in sorted(uniq):
                            ensure_symbol_settings(sym)
                else:
                    print("⚠️ REFRESH failed (no pairs). Keeping current list.")
                last_refresh_ts = time.time()

            for p in pairs:
                key = f"{p.a}/{p.b}"
                st = states.setdefault(key, PairState())

                if st.last_close_time and (time.time() - st.last_close_time) < (COOLDOWN_MIN * 60):
                    continue

                # LIVE: exchange truth
                if TRADING_ENABLED == 1:
                    amt_a, pnl_a = get_position(p.a)
                    amt_b, pnl_b = get_position(p.b)

                    if (amt_a == 0) != (amt_b == 0):
                        print(f"⚠️ ONE-LEG RISK {key} | amtA={amt_a} amtB={amt_b} -> forcing close")
                        close_pair(p, st, reason="ONE_LEG_RISK")
                        continue

                    total_pnl = pnl_a + pnl_b
                    st.open = (amt_a != 0 and amt_b != 0)

                    if st.open:
                        held_min = (time.time() - st.entry_time) / 60 if st.entry_time else 0.0
                        z, beta, alpha, hl = compute_signal(p)
                        hl_txt = f"{hl:.1f}m" if hl is not None else "na"
                        print(f"PAIR {key} | PNL={total_pnl:.2f} | z={z:.2f} | beta={beta:.3f} | hl={hl_txt} | held={held_min:.1f}m")

                        if total_pnl >= TP_USD:
                            close_pair(p, st, reason="TP_USD"); time.sleep(2); continue
                        if total_pnl <= SL_USD:
                            close_pair(p, st, reason="SL_USD"); time.sleep(2); continue
                        if abs(z) <= EXIT_Z:
                            close_pair(p, st, reason="EXIT_Z"); time.sleep(2); continue
                        if held_min >= MAX_HOLD_MIN:
                            close_pair(p, st, reason="TIME_STOP"); time.sleep(2); continue
                        continue

                    if open_count >= MAX_OPEN_PAIRS:
                        continue

                    z, beta, alpha, hl = compute_signal(p)
                    hl_txt = f"{hl:.1f}m" if hl is not None else "na"
                    print(f"SCAN {key} | z={z:.2f} | beta={beta:.3f} | hl={hl_txt}")

                    if abs(z) >= ENTRY_Z:
                        if hl is None or hl < HL_MIN_MIN or hl > HL_MAX_MIN:
                            print(f"⛔ HL_FILTER {key} | hl={hl_txt} (min={HL_MIN_MIN} max={HL_MAX_MIN}) -> skip entry")
                            continue
                        open_pair_market(p, st, z, beta, alpha)
                        open_count += 1
                        time.sleep(1)
                    continue

                # DRY: simulated engine
                if st.open:
                    held_min = (time.time() - st.entry_time) / 60 if st.entry_time else 0.0
                    z, beta, alpha, hl = compute_signal(p)
                    hl_txt = f"{hl:.1f}m" if hl is not None else "na"
                    print(f"[DRY] PAIR {key} | z={z:.2f} | beta={beta:.3f} | hl={hl_txt} | held={held_min:.1f}m")

                    if abs(z) <= EXIT_Z:
                        close_pair(p, st, reason="EXIT_Z"); time.sleep(1); continue
                    if held_min >= MAX_HOLD_MIN:
                        close_pair(p, st, reason="TIME_STOP"); time.sleep(1); continue
                    continue

                if open_count >= MAX_OPEN_PAIRS:
                    continue

                z, beta, alpha, hl = compute_signal(p)
                hl_txt = f"{hl:.1f}m" if hl is not None else "na"
                print(f"SCAN {key} | z={z:.2f} | beta={beta:.3f} | hl={hl_txt}")

                if abs(z) >= ENTRY_Z:
                    if hl is None or hl < HL_MIN_MIN or hl > HL_MAX_MIN:
                        print(f"⛔ HL_FILTER {key} | hl={hl_txt} (min={HL_MIN_MIN} max={HL_MAX_MIN}) -> skip entry")
                        continue
                    open_pair_market(p, st, z, beta, alpha)
                    open_count += 1
                    time.sleep(1)

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
