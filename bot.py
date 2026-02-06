import os
import time
import math
import json
import statistics
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from binance.client import Client

# =========================================================
# ENV / BINANCE
# =========================================================
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
if not API_KEY or not API_SECRET:
    raise SystemExit("Missing BINANCE_API_KEY / BINANCE_API_SECRET")

TRADING_ENABLED = int(os.getenv("TRADING_ENABLED", "0"))  # 0=dry-run, 1=live
RECV_WINDOW = int(os.getenv("RECV_WINDOW", "60000"))
CHECK_SEC = int(os.getenv("CHECK_SEC", "5"))
DEBUG_SCAN = int(os.getenv("DEBUG_SCAN", "0"))
DEBUG_TOPK = int(os.getenv("DEBUG_TOPK", "5"))

# Futures settings (FORCED)
HEDGE_MODE = 0  # <- FORCED one-way mode (no hedge)
LEVERAGE = int(os.getenv("LEVERAGE", "10"))
MARGIN_TYPE = os.getenv("MARGIN_TYPE", "ISOLATED").upper()

# =========================================================
# STRATEGY: DCA SCALP (ONLY) + AUTO SYMBOL + AUTO SIDE
# =========================================================
UNIVERSE_QUOTE = os.getenv("UNIVERSE_QUOTE", "USDT").upper()
TOP_N_COINS = int(os.getenv("TOP_N_COINS", "150"))
COOLDOWN_MIN = int(os.getenv("COOLDOWN_MIN", "10"))
RESELECT_MIN = int(os.getenv("RESELECT_MIN", "5"))  # re-scan frequency when flat

# Timeframes
TREND_INTERVAL = os.getenv("TREND_INTERVAL", "15m")
ENTRY_INTERVAL = os.getenv("ENTRY_INTERVAL", "1m")

# Trend / pullback / reversal
TREND_EMA = int(os.getenv("TREND_EMA", "200"))
FAST_EMA = int(os.getenv("FAST_EMA", "20"))
SLOW_EMA = int(os.getenv("SLOW_EMA", "50"))

PULLBACK_LOOKBACK_BARS = int(os.getenv("PULLBACK_LOOKBACK_BARS", "90"))
PULLBACK_MIN_PCT = float(os.getenv("PULLBACK_MIN_PCT", "0.20")) / 100.0
PULLBACK_MAX_PCT = float(os.getenv("PULLBACK_MAX_PCT", "2.20")) / 100.0

LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "2880"))  # 2 days

VOL_LOOKBACK_BARS = int(os.getenv("VOL_LOOKBACK_BARS", "180"))
VOL_MIN = float(os.getenv("VOL_MIN", "0.0004"))
VOL_MAX = float(os.getenv("VOL_MAX", "0.0060"))

# DCA
DCA_BASE_USD = float(os.getenv("DCA_BASE_USD", "50"))
DCA_STEP_PCT = float(os.getenv("DCA_STEP_PCT", "0.40")) / 100.0
DCA_MULT = float(os.getenv("DCA_MULT", "1.5"))
DCA_MAX_LEVELS = int(os.getenv("DCA_MAX_LEVELS", "5"))

# Exits
USE_PNL_EXIT = int(os.getenv("USE_PNL_EXIT", "1"))  # (live)
TP_PNL_USD = float(os.getenv("TP_PNL_USD", "8"))
SL_PNL_USD = float(os.getenv("SL_PNL_USD", "-25"))

USE_PRICE_EXIT = int(os.getenv("USE_PRICE_EXIT", "1"))
TP_PCT = float(os.getenv("TP_PCT", "0.45")) / 100.0
HARD_STOP_PCT = float(os.getenv("HARD_STOP_PCT", "2.00")) / 100.0
TIME_STOP_MIN = int(os.getenv("TIME_STOP_MIN", "45"))

# Exposure guard
MAX_TOTAL_USD_EXPOSURE = float(os.getenv("MAX_TOTAL_USD_EXPOSURE", "600"))
EXPOSURE_ACTION = os.getenv("EXPOSURE_ACTION", "STOP_ADD").upper()  # STOP_ADD or FORCE_CLOSE

# Symbol filters
EXCLUDE_KEYWORDS = os.getenv(
    "EXCLUDE_KEYWORDS",
    "BUSD,USDC,TUSD,FDUSD,DAI,PAX,EUR,GBP,TRY"
).upper().split(",")

ALLOWLIST = [s.strip().upper() for s in os.getenv("SYMBOL_ALLOWLIST", "").split(",") if s.strip()]
DENYLIST = [s.strip().upper() for s in os.getenv("SYMBOL_DENYLIST", "").split(",") if s.strip()]

# State persistence
STATE_PATH = os.getenv("STATE_PATH", "dca_state.json")
RECOVER_OPEN_POS = int(os.getenv("RECOVER_OPEN_POS", "1"))

client = Client(API_KEY, API_SECRET)

# =========================================================
# CACHES
# =========================================================
_SYMBOL_SET: Optional[set] = None
_LOT_CACHE: Dict[str, Tuple[float, float]] = {}
_KLINE_CACHE: Dict[Tuple[str, str, int], Tuple[int, List[float]]] = {}

# =========================================================
# DATA
# =========================================================
@dataclass
class DCAState:
    symbol: str = ""
    side: int = 0  # +1 long, -1 short
    open: bool = False

    entry_time: float = 0.0
    last_close_time: float = 0.0
    last_scan_ts: float = 0.0

    level: int = 0
    next_add_price: float = 0.0

    avg_entry_est: float = 0.0
    pos_qty_est: float = 0.0
    total_usd_est: float = 0.0

# =========================================================
# BINANCE HELPERS
# =========================================================
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

def get_any_open_position_symbol() -> Optional[str]:
    try:
        positions = client.futures_position_information(recvWindow=RECV_WINDOW)
        for p in positions:
            amt = float(p.get("positionAmt", "0") or 0.0)
            sym = p.get("symbol", "")
            if amt != 0.0 and sym and sym.endswith(UNIVERSE_QUOTE):
                return sym
    except Exception:
        return None
    return None

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
    """
    One-way mode only. Prevents crossed/flip by requiring existing position direction to match.
    """
    if TRADING_ENABLED == 0:
        return

    # Crossed-position guard (exchange truth)
    amt, _ = get_position(symbol)
    if amt != 0.0:
        existing_dir = 1 if amt > 0 else -1
        if existing_dir != direction:
            raise RuntimeError(f"[CROSSED_GUARD] Existing dir={existing_dir} but attempted dir={direction} on {symbol}")

    price = get_mark(symbol)
    qty = floor_to_step(symbol, usd / price)
    if qty <= 0:
        raise RuntimeError(f"[{symbol}] qty too small for usd={usd}")

    side = "BUY" if direction > 0 else "SELL"
    params = dict(symbol=symbol, side=side, type="MARKET", quantity=qty, recvWindow=RECV_WINDOW)
    # HEDGE_MODE forced off: no positionSide
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
    client.futures_create_order(**params)

# =========================================================
# DATA / STATS
# =========================================================
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
    bars = max(120, lookback_minutes // mins)
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

def ema(series: List[float], period: int) -> float:
    if len(series) < period + 5:
        return series[-1] if series else 0.0
    k = 2.0 / (period + 1.0)
    e = series[0]
    for v in series[1:]:
        e = v * k + e * (1 - k)
    return e

def returns(series: List[float]) -> List[float]:
    out = []
    for i in range(1, len(series)):
        if series[i - 1] == 0:
            out.append(0.0)
        else:
            out.append((series[i] / series[i - 1]) - 1.0)
    return out

def stdev_returns(closes: List[float], lookback: int) -> float:
    if len(closes) < lookback + 5:
        return 0.0
    r = returns(closes[-(lookback + 1):])
    if len(r) < 30:
        return 0.0
    s = statistics.pstdev(r)
    if not math.isfinite(s):
        return 0.0
    return s

# =========================================================
# STATE
# =========================================================
def save_state(st: DCAState) -> None:
    try:
        data = {
            "symbol": st.symbol,
            "side": st.side,
            "open": st.open,
            "entry_time": st.entry_time,
            "last_close_time": st.last_close_time,
            "last_scan_ts": st.last_scan_ts,
            "level": st.level,
            "next_add_price": st.next_add_price,
            "avg_entry_est": st.avg_entry_est,
            "pos_qty_est": st.pos_qty_est,
            "total_usd_est": st.total_usd_est,
        }
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"⚠️ state save failed: {e}")

def load_state() -> DCAState:
    try:
        if not os.path.exists(STATE_PATH):
            return DCAState()
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        st = DCAState()
        st.symbol = str(data.get("symbol", "")).upper()
        st.side = int(data.get("side", 0))
        st.open = bool(data.get("open", False))
        st.entry_time = float(data.get("entry_time", 0.0))
        st.last_close_time = float(data.get("last_close_time", 0.0))
        st.last_scan_ts = float(data.get("last_scan_ts", 0.0))
        st.level = int(data.get("level", 0))
        st.next_add_price = float(data.get("next_add_price", 0.0))
        st.avg_entry_est = float(data.get("avg_entry_est", 0.0))
        st.pos_qty_est = float(data.get("pos_qty_est", 0.0))
        st.total_usd_est = float(data.get("total_usd_est", 0.0))
        return st
    except Exception as e:
        print(f"⚠️ state load failed: {e}")
        return DCAState()

def reset_position_state(st: DCAState) -> None:
    st.open = False
    st.entry_time = 0.0
    st.level = 0
    st.next_add_price = 0.0
    st.avg_entry_est = 0.0
    st.pos_qty_est = 0.0
    st.total_usd_est = 0.0

# =========================================================
# DCA PRICING
# =========================================================
def dca_next_add_price(avg_entry: float, side: int, step_pct: float, level: int) -> float:
    if avg_entry <= 0:
        return 0.0
    if side > 0:
        return avg_entry * (1.0 - step_pct * level)
    else:
        return avg_entry * (1.0 + step_pct * level)

def price_tp(avg_entry: float, side: int, tp_pct: float) -> float:
    if avg_entry <= 0:
        return 0.0
    return avg_entry * (1.0 + tp_pct) if side > 0 else avg_entry * (1.0 - tp_pct)

def price_stop(avg_entry: float, side: int, stop_pct: float) -> float:
    if avg_entry <= 0:
        return 0.0
    return avg_entry * (1.0 - stop_pct) if side > 0 else avg_entry * (1.0 + stop_pct)

# =========================================================
# SYMBOL FILTERS / CANDIDATES
# =========================================================
def excluded_symbol(sym: str) -> bool:
    s = sym.upper()
    if not s.endswith(UNIVERSE_QUOTE):
        return True
    if ALLOWLIST and s not in set(ALLOWLIST):
        return True
    if s in set(DENYLIST):
        return True
    for kw in EXCLUDE_KEYWORDS:
        kw = kw.strip()
        if not kw:
            continue
        if s.startswith(kw):
            return True
    return False

def get_top_symbols_by_volume(quote: str, top_n: int) -> List[str]:
    tickers = client.futures_ticker()
    cand = []
    for t in tickers:
        sym = t.get("symbol", "")
        if not sym.endswith(quote):
            continue
        if excluded_symbol(sym):
            continue
        if not is_valid_symbol(sym):
            continue
        try:
            qv = float(t.get("quoteVolume", 0.0))
        except Exception:
            qv = 0.0
        cand.append((qv, sym))
    cand.sort(reverse=True, key=lambda x: x[0])
    return [s for _, s in cand[:top_n]]

# =========================================================
# TREND + PULLBACK + REVERSAL (simple)
# =========================================================
def trend_direction(symbol: str) -> Optional[int]:
    closes = fetch_closes(symbol, TREND_INTERVAL, LOOKBACK_MINUTES)
    if len(closes) < max(TREND_EMA + 20, 250):
        return None
    w = closes[-max(TREND_EMA * 3, 450):]
    et = ema(w, TREND_EMA)
    px = w[-1]
    if et <= 0:
        return None
    if px > et:
        return +1
    if px < et:
        return -1
    return None

def pullback_pct(closes: List[float], trend: int) -> float:
    if len(closes) < PULLBACK_LOOKBACK_BARS + 5:
        return 0.0
    recent = closes[-PULLBACK_LOOKBACK_BARS:]
    px = recent[-1]
    hi = max(recent)
    lo = min(recent)
    if trend > 0:
        return (hi - px) / hi if hi > 0 else 0.0
    else:
        return (px - lo) / px if px > 0 else 0.0

def reversal_ok_simple(entry_closes: List[float], trend: int) -> bool:
    if len(entry_closes) < max(SLOW_EMA + 50, 200):
        return False
    w = entry_closes[-max(SLOW_EMA * 6, 300):]
    ef = ema(w, FAST_EMA)
    es = ema(w, SLOW_EMA)
    px = w[-1]
    if ef <= 0 or es <= 0:
        return False
    if trend > 0:
        return (ef > es) and (px > ef)
    else:
        return (ef < es) and (px < ef)

# =========================================================
# SCAN (with optional debug)
# =========================================================
def scan_for_entry_candidate() -> Optional[Tuple[str, int, float, float, float]]:
    tops = get_top_symbols_by_volume(UNIVERSE_QUOTE, TOP_N_COINS)

    seen = 0
    trend_fail = 0
    data_fail = 0
    pullback_fail = 0
    reversal_fail = 0
    vol_fail = 0
    ok = 0

    best = None  # (score, sym, side, pb, vol)
    candidates: List[Tuple[float, str, int, float, float]] = []

    for idx, sym in enumerate(tops):
        seen += 1
        try:
            tr = trend_direction(sym)
            if tr is None:
                trend_fail += 1
                continue

            entry_closes = fetch_closes(sym, ENTRY_INTERVAL, LOOKBACK_MINUTES)
            if len(entry_closes) < max(SLOW_EMA + 120, 250):
                data_fail += 1
                continue

            pb = pullback_pct(entry_closes, tr)
            if pb < PULLBACK_MIN_PCT or pb > PULLBACK_MAX_PCT:
                pullback_fail += 1
                continue

            if not reversal_ok_simple(entry_closes, tr):
                reversal_fail += 1
                continue

            vol = stdev_returns(entry_closes, VOL_LOOKBACK_BARS)
            if vol <= 0 or (vol < VOL_MIN or vol > VOL_MAX):
                vol_fail += 1
                continue

            ok += 1

            rank_bonus = (len(tops) - idx) / max(1, len(tops))
            pb_mid = (PULLBACK_MIN_PCT + PULLBACK_MAX_PCT) / 2.0
            pb_closeness = 1.0 - min(1.0, abs(pb - pb_mid) / (pb_mid if pb_mid > 0 else 1.0))
            vol_mid = (VOL_MIN + VOL_MAX) / 2.0
            vol_closeness = 1.0 - min(1.0, abs(vol - vol_mid) / (vol_mid if vol_mid > 0 else 1.0))

            score = 0.55 * rank_bonus + 0.25 * pb_closeness + 0.20 * vol_closeness

            candidates.append((score, sym, tr, pb, vol))
            if best is None or score > best[0]:
                best = (score, sym, tr, pb, vol)

        except Exception:
            data_fail += 1
            continue

    if DEBUG_SCAN == 1:
        print(
            f"SCAN_DEBUG | seen={seen} ok={ok} | "
            f"trend_fail={trend_fail} data_fail={data_fail} "
            f"pullback_fail={pullback_fail} reversal_fail={reversal_fail} vol_fail={vol_fail}"
        )
        if candidates:
            candidates.sort(reverse=True, key=lambda x: x[0])
            print("SCAN_DEBUG_TOP:")
            for sc, sym, tr, pb, vol in candidates[:max(1, DEBUG_TOPK)]:
                side_txt = "LONG" if tr > 0 else "SHORT"
                print(f"  - {sym} {side_txt} | score={sc:.3f} pb={pb*100:.2f}% vol={vol:.5f}")

    if best is None:
        return None

    return best[1], best[2], best[0], best[3], best[4]

# =========================================================
# MAIN
# =========================================================
def main():
    sync_time()
    st = load_state()

    print("✅ DCA SCALP Bot — AUTO SYMBOL + AUTO SIDE + PnL Exit + Exposure Guard + CROSSED-POSITION GUARD")
    print(f"TRADING_ENABLED={TRADING_ENABLED} | HEDGE_MODE(FORCED)={HEDGE_MODE} | LEVERAGE={LEVERAGE} | MARGIN_TYPE={MARGIN_TYPE}")
    print(f"Universe={UNIVERSE_QUOTE} TOP_N_COINS={TOP_N_COINS} | TREND={TREND_INTERVAL} EMA{TREND_EMA} | ENTRY={ENTRY_INTERVAL} EMA{FAST_EMA}/{SLOW_EMA}")
    print(f"Pullback: lookback={PULLBACK_LOOKBACK_BARS} min={PULLBACK_MIN_PCT*100:.2f}% max={PULLBACK_MAX_PCT*100:.2f}%")
    print(f"Vol: lookbackBars={VOL_LOOKBACK_BARS} min={VOL_MIN} max={VOL_MAX}")
    print(f"DCA: base={DCA_BASE_USD} step={DCA_STEP_PCT*100:.2f}% mult={DCA_MULT} maxLv={DCA_MAX_LEVELS}")
    print(f"PnL Exit: use={USE_PNL_EXIT} TP={TP_PNL_USD} SL={SL_PNL_USD} | Price Exit: use={USE_PRICE_EXIT} TP%={TP_PCT*100:.2f} Stop%={HARD_STOP_PCT*100:.2f} TimeStop={TIME_STOP_MIN}m")
    print(f"Exposure: max={MAX_TOTAL_USD_EXPOSURE} action={EXPOSURE_ACTION} | Cooldown={COOLDOWN_MIN}m Reselect={RESELECT_MIN}m")
    print(f"LOOKBACK_MINUTES={LOOKBACK_MINUTES} | CHECK_SEC={CHECK_SEC} | DEBUG_SCAN={DEBUG_SCAN}")

    # Recover open position on restart (live)
    if TRADING_ENABLED == 1 and RECOVER_OPEN_POS == 1:
        sym = get_any_open_position_symbol()
        if sym:
            st.symbol = sym
            amt, pnl = get_position(sym)
            st.open = (amt != 0.0)
            st.side = +1 if amt > 0 else -1
            if st.entry_time == 0.0:
                st.entry_time = time.time()
            if st.level == 0:
                st.level = 1
            if st.total_usd_est == 0.0:
                st.total_usd_est = DCA_BASE_USD
            if st.avg_entry_est == 0.0:
                st.avg_entry_est = get_mark(sym)
            st.next_add_price = dca_next_add_price(st.avg_entry_est, st.side, DCA_STEP_PCT, st.level)
            ensure_symbol_settings(sym)
            save_state(st)
            print(f"⚠️ RECOVERED open position: {sym} amt={amt} pnl={pnl:.2f} side={'LONG' if st.side>0 else 'SHORT'}")

    while True:
        try:
            now = time.time()

            # Cooldown after close
            if st.last_close_time and (now - st.last_close_time) < (COOLDOWN_MIN * 60):
                time.sleep(CHECK_SEC)
                continue

            # =========================
            # LIVE: always trust exchange + crossed guard
            # =========================
            if TRADING_ENABLED == 1 and st.symbol:
                amt, pnl = get_position(st.symbol)

                if amt != 0.0:
                    exch_side = +1 if amt > 0 else -1

                    # Crossed/flip guard:
                    # If state says one direction but exchange is opposite -> close immediately (no flip opens)
                    if st.side != 0 and exch_side != st.side:
                        print(f"🛑 CROSSED DETECTED {st.symbol} | stateSide={st.side} exchSide={exch_side} -> FORCE CLOSE")
                        close_market_reduce_only(st.symbol, amt)
                        reset_position_state(st)
                        st.last_close_time = time.time()
                        save_state(st)
                        time.sleep(2)
                        continue

                    st.open = True
                    st.side = exch_side  # keep aligned with exchange truth
                else:
                    # Exchange flat
                    if st.open:
                        reset_position_state(st)
                        st.last_close_time = now
                        save_state(st)
                    st.open = False

            # =========================
            # FLAT: scan + enter
            # =========================
            if not st.open:
                if st.last_scan_ts and (now - st.last_scan_ts) < (RESELECT_MIN * 60):
                    time.sleep(CHECK_SEC)
                    continue

                st.last_scan_ts = now
                cand = scan_for_entry_candidate()
                if not cand:
                    print("SCAN: no entry candidates now.")
                    save_state(st)
                    time.sleep(CHECK_SEC)
                    continue

                sym, side, score, pb, vol = cand
                st.symbol = sym
                st.side = side
                ensure_symbol_settings(sym)

                side_txt = "LONG" if side > 0 else "SHORT"
                mark = get_mark(sym)

                st.open = True
                st.entry_time = now
                st.level = 1
                st.avg_entry_est = mark
                st.pos_qty_est = 0.0
                st.total_usd_est = DCA_BASE_USD
                st.next_add_price = dca_next_add_price(st.avg_entry_est, st.side, DCA_STEP_PCT, st.level)

                if TRADING_ENABLED == 0:
                    print(f"[DRY] ENTRY {sym} side={side_txt} score={score:.3f} pb={pb*100:.2f}% vol={vol:.5f} mark={mark:.6f} usd={DCA_BASE_USD:.2f} nextAdd={st.next_add_price:.6f}")
                else:
                    print(f"🚀 ENTRY {sym} side={side_txt} score={score:.3f} pb={pb*100:.2f}% vol={vol:.5f} mark={mark:.6f} usd={DCA_BASE_USD:.2f} nextAdd={st.next_add_price:.6f}")
                    open_market(sym, st.side, DCA_BASE_USD)

                save_state(st)
                time.sleep(1)
                continue

            # =========================
            # OPEN: manage
            # =========================
            sym = st.symbol
            if not sym:
                reset_position_state(st)
                save_state(st)
                time.sleep(CHECK_SEC)
                continue

            mark = get_mark(sym)
            held_min = (now - st.entry_time) / 60.0 if st.entry_time else 0.0
            side_txt = "LONG" if st.side > 0 else "SHORT"

            # Live PnL exits
            if TRADING_ENABLED == 1 and USE_PNL_EXIT == 1:
                amt, pnl = get_position(sym)
                if amt == 0.0:
                    print("⚠️ Exchange flat while state open -> reset")
                    reset_position_state(st)
                    st.last_close_time = time.time()
                    save_state(st)
                    time.sleep(2)
                    continue

                exch_side = +1 if amt > 0 else -1
                if exch_side != st.side:
                    print(f"🛑 CROSSED DETECTED mid-run {sym} | stateSide={st.side} exchSide={exch_side} -> FORCE CLOSE")
                    close_market_reduce_only(sym, amt)
                    reset_position_state(st)
                    st.last_close_time = time.time()
                    save_state(st)
                    time.sleep(2)
                    continue

                print(f"POS {sym} {side_txt} | mark={mark:.6f} | pnl={pnl:.2f} | lv={st.level}/{DCA_MAX_LEVELS} | estExp={st.total_usd_est:.2f} | nextAdd={st.next_add_price:.6f} | held={held_min:.1f}m")

                if pnl >= TP_PNL_USD:
                    print(f"✅ TP_PNL hit pnl={pnl:.2f} -> close")
                    close_market_reduce_only(sym, amt)
                    reset_position_state(st)
                    st.last_close_time = time.time()
                    save_state(st)
                    time.sleep(2)
                    continue

                if pnl <= SL_PNL_USD:
                    print(f"🛑 SL_PNL hit pnl={pnl:.2f} -> close")
                    close_market_reduce_only(sym, amt)
                    reset_position_state(st)
                    st.last_close_time = time.time()
                    save_state(st)
                    time.sleep(2)
                    continue
            else:
                print(f"POS {sym} {side_txt} | mark={mark:.6f} | lv={st.level}/{DCA_MAX_LEVELS} | estExp={st.total_usd_est:.2f} | nextAdd={st.next_add_price:.6f} | held={held_min:.1f}m")

            # Price exits
            if USE_PRICE_EXIT == 1 and st.avg_entry_est > 0:
                tp_px = price_tp(st.avg_entry_est, st.side, TP_PCT)
                sl_px = price_stop(st.avg_entry_est, st.side, HARD_STOP_PCT)

                if (st.side > 0 and mark >= tp_px) or (st.side < 0 and mark <= tp_px):
                    print("✅ PRICE_TP -> close")
                    if TRADING_ENABLED == 1:
                        amt, _ = get_position(sym)
                        close_market_reduce_only(sym, amt)
                    reset_position_state(st)
                    st.last_close_time = time.time()
                    save_state(st)
                    time.sleep(2)
                    continue

                if (st.side > 0 and mark <= sl_px) or (st.side < 0 and mark >= sl_px):
                    print("🛑 PRICE_STOP -> close")
                    if TRADING_ENABLED == 1:
                        amt, _ = get_position(sym)
                        close_market_reduce_only(sym, amt)
                    reset_position_state(st)
                    st.last_close_time = time.time()
                    save_state(st)
                    time.sleep(2)
                    continue

            if held_min >= TIME_STOP_MIN:
                print("⏳ TIME_STOP -> close")
                if TRADING_ENABLED == 1:
                    amt, _ = get_position(sym)
                    close_market_reduce_only(sym, amt)
                reset_position_state(st)
                st.last_close_time = time.time()
                save_state(st)
                time.sleep(2)
                continue

            # DCA add
            can_add = (st.level < DCA_MAX_LEVELS)
            hit_add = (st.side > 0 and mark <= st.next_add_price) or (st.side < 0 and mark >= st.next_add_price)

            if can_add and hit_add:
                usd_add = DCA_BASE_USD * (DCA_MULT ** (st.level - 1))
                projected = st.total_usd_est + usd_add

                if projected > MAX_TOTAL_USD_EXPOSURE:
                    if EXPOSURE_ACTION == "FORCE_CLOSE":
                        print(f"🧯 EXPOSURE LIMIT -> FORCE_CLOSE | est={st.total_usd_est:.2f} + add={usd_add:.2f} > max={MAX_TOTAL_USD_EXPOSURE:.2f}")
                        if TRADING_ENABLED == 1:
                            amt, _ = get_position(sym)
                            close_market_reduce_only(sym, amt)
                        reset_position_state(st)
                        st.last_close_time = time.time()
                        save_state(st)
                        time.sleep(2)
                        continue
                    else:
                        print(f"⛔ EXPOSURE LIMIT -> STOP_ADD | est={st.total_usd_est:.2f} + add={usd_add:.2f} > max={MAX_TOTAL_USD_EXPOSURE:.2f}")
                        time.sleep(CHECK_SEC)
                        continue

                # Live extra safety: ensure still same direction
                if TRADING_ENABLED == 1:
                    amt, _ = get_position(sym)
                    if amt != 0.0:
                        exch_side = +1 if amt > 0 else -1
                        if exch_side != st.side:
                            print(f"🛑 CROSSED DETECTED before add {sym} -> FORCE CLOSE")
                            close_market_reduce_only(sym, amt)
                            reset_position_state(st)
                            st.last_close_time = time.time()
                            save_state(st)
                            time.sleep(2)
                            continue

                st.level += 1
                st.total_usd_est = projected

                add_qty = (usd_add / mark) if mark > 0 else 0.0
                total_cost = st.avg_entry_est * st.pos_qty_est + mark * add_qty
                st.pos_qty_est += add_qty
                if st.pos_qty_est > 0:
                    st.avg_entry_est = total_cost / st.pos_qty_est

                st.next_add_price = dca_next_add_price(st.avg_entry_est, st.side, DCA_STEP_PCT, st.level)

                if TRADING_ENABLED == 0:
                    print(f"[DRY] DCA ADD {sym} {side_txt} | lv={st.level} usd={usd_add:.2f} mark={mark:.6f} newAvg={st.avg_entry_est:.6f} nextAdd={st.next_add_price:.6f} estExp={st.total_usd_est:.2f}")
                else:
                    print(f"➕ DCA ADD {sym} {side_txt} | lv={st.level} usd={usd_add:.2f} mark={mark:.6f} newAvg={st.avg_entry_est:.6f} nextAdd={st.next_add_price:.6f} estExp={st.total_usd_est:.2f}")
                    open_market(sym, st.side, usd_add)

                save_state(st)
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
