import os
import time
import math
import json
import statistics
from dataclasses import dataclass, asdict
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

# Forced one-way mode
HEDGE_MODE = 0  # DO NOT CHANGE
LEVERAGE = int(os.getenv("LEVERAGE", "10"))
MARGIN_TYPE = os.getenv("MARGIN_TYPE", "ISOLATED").upper()

client = Client(API_KEY, API_SECRET)

# =========================================================
# STRATEGY / PORTFOLIO
# =========================================================
UNIVERSE_QUOTE = os.getenv("UNIVERSE_QUOTE", "USDT").upper()
TOP_N_COINS = int(os.getenv("TOP_N_COINS", "150"))
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "5"))

# Cooldown only for closed symbol
COOLDOWN_MIN = int(os.getenv("COOLDOWN_MIN", "10"))

# How often to rescan (when we still have capacity)
RESELECT_MIN = int(os.getenv("RESELECT_MIN", "1"))

# Timeframes
TREND_INTERVAL = os.getenv("TREND_INTERVAL", "15m")
ENTRY_INTERVAL = os.getenv("ENTRY_INTERVAL", "1m")

# Trend / entry
TREND_EMA = int(os.getenv("TREND_EMA", "200"))
FAST_EMA = int(os.getenv("FAST_EMA", "20"))
SLOW_EMA = int(os.getenv("SLOW_EMA", "50"))

LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "12000"))  # big enough for EMA200 on 15m

# Pullback
PULLBACK_LOOKBACK_BARS = int(os.getenv("PULLBACK_LOOKBACK_BARS", "90"))
PULLBACK_MIN_PCT = float(os.getenv("PULLBACK_MIN_PCT", "0.20")) / 100.0
PULLBACK_MAX_PCT = float(os.getenv("PULLBACK_MAX_PCT", "2.20")) / 100.0

# Vol filter
VOL_LOOKBACK_BARS = int(os.getenv("VOL_LOOKBACK_BARS", "180"))
VOL_MIN = float(os.getenv("VOL_MIN", "0.0004"))
VOL_MAX = float(os.getenv("VOL_MAX", "0.0060"))

# DCA
DCA_BASE_USD = float(os.getenv("DCA_BASE_USD", "50"))
DCA_STEP_PCT = float(os.getenv("DCA_STEP_PCT", "0.40")) / 100.0
DCA_MULT = float(os.getenv("DCA_MULT", "1.5"))
DCA_MAX_LEVELS = int(os.getenv("DCA_MAX_LEVELS", "5"))

# Exits
USE_PNL_EXIT = int(os.getenv("USE_PNL_EXIT", "1"))
TP_PNL_USD = float(os.getenv("TP_PNL_USD", "8"))
SL_PNL_USD = float(os.getenv("SL_PNL_USD", "-25"))

USE_PRICE_EXIT = int(os.getenv("USE_PRICE_EXIT", "1"))
TP_PCT = float(os.getenv("TP_PCT", "0.45")) / 100.0
HARD_STOP_PCT = float(os.getenv("HARD_STOP_PCT", "2.00")) / 100.0
TIME_STOP_MIN = int(os.getenv("TIME_STOP_MIN", "45"))

# Exposure (per trade and global)
MAX_USD_PER_TRADE = float(os.getenv("MAX_USD_PER_TRADE", "600"))
MAX_TOTAL_USD_EXPOSURE = float(os.getenv("MAX_TOTAL_USD_EXPOSURE", "2500"))
EXPOSURE_ACTION = os.getenv("EXPOSURE_ACTION", "STOP_ADD").upper()  # STOP_ADD or FORCE_CLOSE

# Filters
EXCLUDE_KEYWORDS = os.getenv(
    "EXCLUDE_KEYWORDS",
    "BUSD,USDC,TUSD,FDUSD,DAI,PAX,EUR,GBP,TRY"
).upper().split(",")
ALLOWLIST = [s.strip().upper() for s in os.getenv("SYMBOL_ALLOWLIST", "").split(",") if s.strip()]
DENYLIST = [s.strip().upper() for s in os.getenv("SYMBOL_DENYLIST", "").split(",") if s.strip()]

# Debug
DEBUG_SCAN = int(os.getenv("DEBUG_SCAN", "0"))
DEBUG_TOPK = int(os.getenv("DEBUG_TOPK", "5"))
DEBUG_EVERY_SEC = int(os.getenv("DEBUG_EVERY_SEC", "30"))

# State
STATE_PATH = os.getenv("STATE_PATH", "dca_portfolio_state.json")
RECOVER_OPEN_POS = int(os.getenv("RECOVER_OPEN_POS", "1"))

# =========================================================
# CACHES
# =========================================================
_SYMBOL_SET: Optional[set] = None
_LOT_CACHE: Dict[str, Tuple[float, float]] = {}
_KLINE_CACHE: Dict[Tuple[str, str, int], Tuple[int, List[float]]] = {}

# =========================================================
# DATA STRUCTURES
# =========================================================
@dataclass
class TradeState:
    symbol: str
    side: int  # +1 long, -1 short
    open: bool = True
    entry_time: float = 0.0
    level: int = 1
    next_add_price: float = 0.0

    # Estimated tracking (DRY / convenience)
    avg_entry_est: float = 0.0
    pos_qty_est: float = 0.0
    usd_est: float = 0.0

@dataclass
class PortfolioState:
    trades: Dict[str, TradeState]  # key=symbol
    cooldowns: Dict[str, float]    # symbol -> cooldown_until_ts
    last_scan_ts: float = 0.0

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
    client.futures_create_order(
        symbol=symbol, side=side, type="MARKET", quantity=qty, recvWindow=RECV_WINDOW
    )

def close_market_reduce_only(symbol: str, position_amt: float) -> None:
    if TRADING_ENABLED == 0:
        return
    if position_amt == 0:
        return
    side = "SELL" if position_amt > 0 else "BUY"
    qty = floor_to_step(symbol, abs(position_amt))
    if qty <= 0:
        return
    client.futures_create_order(
        symbol=symbol, side=side, type="MARKET", quantity=qty, reduceOnly=True, recvWindow=RECV_WINDOW
    )

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
    return s if math.isfinite(s) else 0.0

# =========================================================
# PRICE HELPERS
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
# FILTERS / UNIVERSE
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
# SIGNAL: trend + pullback + reversal + vol
# =========================================================
def trend_direction(symbol: str) -> Optional[int]:
    closes = fetch_closes(symbol, TREND_INTERVAL, LOOKBACK_MINUTES)
    need = max(TREND_EMA + 20, 250)
    if len(closes) < need:
        return None
    w = closes[-max(TREND_EMA * 3, 450):]
    et = ema(w, TREND_EMA)
    px = w[-1]
    if et <= 0:
        return None
    return +1 if px > et else (-1 if px < et else None)

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
# PORTFOLIO STATE I/O
# =========================================================
def save_state(ps: PortfolioState) -> None:
    try:
        out = {
            "trades": {k: asdict(v) for k, v in ps.trades.items()},
            "cooldowns": ps.cooldowns,
            "last_scan_ts": ps.last_scan_ts,
        }
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(out, f)
    except Exception as e:
        print(f"⚠️ state save failed: {e}")

def load_state() -> PortfolioState:
    try:
        if not os.path.exists(STATE_PATH):
            return PortfolioState(trades={}, cooldowns={}, last_scan_ts=0.0)
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        trades_raw = data.get("trades", {}) or {}
        trades: Dict[str, TradeState] = {}
        for sym, td in trades_raw.items():
            try:
                trades[sym] = TradeState(**td)
            except Exception:
                continue
        cooldowns = data.get("cooldowns", {}) or {}
        last_scan_ts = float(data.get("last_scan_ts", 0.0))
        return PortfolioState(trades=trades, cooldowns=cooldowns, last_scan_ts=last_scan_ts)
    except Exception as e:
        print(f"⚠️ state load failed: {e}")
        return PortfolioState(trades={}, cooldowns={}, last_scan_ts=0.0)

# =========================================================
# SCAN: return top candidates (not just one)
# =========================================================
def scan_candidates(ps: PortfolioState, need_slots: int) -> List[Tuple[float, str, int, float, float]]:
    """
    Returns list of candidates sorted by score desc:
      (score, symbol, side(+1/-1), pullback, vol)
    Skips:
      - already open trades
      - cooldown symbols
    """
    now = time.time()
    tops = get_top_symbols_by_volume(UNIVERSE_QUOTE, TOP_N_COINS)

    seen = 0
    trend_fail = 0
    data_fail = 0
    pullback_fail = 0
    reversal_fail = 0
    vol_fail = 0
    skipped_open = 0
    skipped_cd = 0
    ok = 0

    candidates: List[Tuple[float, str, int, float, float]] = []

    open_syms = set(ps.trades.keys())

    for idx, sym in enumerate(tops):
        seen += 1

        if sym in open_syms:
            skipped_open += 1
            continue

        cd_until = float(ps.cooldowns.get(sym, 0.0) or 0.0)
        if cd_until and now < cd_until:
            skipped_cd += 1
            continue

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

            # score
            rank_bonus = (len(tops) - idx) / max(1, len(tops))
            pb_mid = (PULLBACK_MIN_PCT + PULLBACK_MAX_PCT) / 2.0
            pb_closeness = 1.0 - min(1.0, abs(pb - pb_mid) / (pb_mid if pb_mid > 0 else 1.0))
            vol_mid = (VOL_MIN + VOL_MAX) / 2.0
            vol_closeness = 1.0 - min(1.0, abs(vol - vol_mid) / (vol_mid if vol_mid > 0 else 1.0))
            score = 0.55 * rank_bonus + 0.25 * pb_closeness + 0.20 * vol_closeness

            candidates.append((score, sym, tr, pb, vol))

        except Exception:
            data_fail += 1
            continue

    candidates.sort(reverse=True, key=lambda x: x[0])

    if DEBUG_SCAN == 1:
        print(
            f"SCAN_DEBUG | seen={seen} ok={ok} needSlots={need_slots} | "
            f"skip_open={skipped_open} skip_cd={skipped_cd} | "
            f"trend_fail={trend_fail} data_fail={data_fail} pullback_fail={pullback_fail} "
            f"reversal_fail={reversal_fail} vol_fail={vol_fail}"
        )
        if candidates:
            print("SCAN_DEBUG_TOP:")
            for sc, sym, tr, pb, vol in candidates[:max(1, DEBUG_TOPK)]:
                side_txt = "LONG" if tr > 0 else "SHORT"
                print(f"  - {sym} {side_txt} | score={sc:.3f} pb={pb*100:.2f}% vol={vol:.5f}")

    return candidates[:max(0, need_slots)]

# =========================================================
# GLOBAL EXPOSURE (estimate only, for guard)
# =========================================================
def portfolio_est_exposure(ps: PortfolioState) -> float:
    return sum(t.usd_est for t in ps.trades.values() if t.open)

# =========================================================
# RECOVER OPEN POSITIONS (LIVE)
# =========================================================
def recover_live_positions(ps: PortfolioState) -> None:
    if TRADING_ENABLED == 0 or RECOVER_OPEN_POS != 1:
        return
    try:
        positions = client.futures_position_information(recvWindow=RECV_WINDOW)
        for p in positions:
            sym = p.get("symbol", "")
            if not sym or not sym.endswith(UNIVERSE_QUOTE):
                continue
            amt = float(p.get("positionAmt", "0") or 0.0)
            if amt == 0.0:
                continue
            side = +1 if amt > 0 else -1
            if sym in ps.trades:
                continue
            mark = get_mark(sym)
            ensure_symbol_settings(sym)
            ts = TradeState(
                symbol=sym,
                side=side,
                open=True,
                entry_time=time.time(),
                level=1,
                next_add_price=dca_next_add_price(mark, side, DCA_STEP_PCT, 1),
                avg_entry_est=mark,
                pos_qty_est=0.0,
                usd_est=DCA_BASE_USD,
            )
            ps.trades[sym] = ts
            print(f"⚠️ RECOVERED {sym} {'LONG' if side>0 else 'SHORT'} amt={amt} mark={mark:.6f}")
    except Exception as e:
        print(f"⚠️ recover failed: {e}")

# =========================================================
# TRADE MANAGEMENT
# =========================================================
def close_trade(ps: PortfolioState, sym: str, reason: str) -> None:
    ts = ps.trades.get(sym)
    if not ts:
        return
    print(f"🧯 CLOSE {sym} reason={reason}")
    if TRADING_ENABLED == 1:
        amt, _ = get_position(sym)
        if amt != 0.0:
            close_market_reduce_only(sym, amt)
    # cooldown only for this symbol
    ps.cooldowns[sym] = time.time() + COOLDOWN_MIN * 60
    # remove from open trades
    ps.trades.pop(sym, None)

def force_close_if_crossed(ps: PortfolioState, sym: str, expected_side: int) -> bool:
    """
    Returns True if force-closed due to crossed direction.
    """
    if TRADING_ENABLED == 0:
        return False
    amt, _ = get_position(sym)
    if amt == 0.0:
        return False
    exch_side = +1 if amt > 0 else -1
    if exch_side != expected_side:
        print(f"🛑 CROSSED DETECTED {sym} stateSide={expected_side} exchSide={exch_side} -> FORCE CLOSE")
        close_market_reduce_only(sym, amt)
        ps.cooldowns[sym] = time.time() + COOLDOWN_MIN * 60
        ps.trades.pop(sym, None)
        return True
    return False

# =========================================================
# MAIN
# =========================================================
def main():
    sync_time()
    ps = load_state()
    if ps.trades is None:
        ps.trades = {}
    if ps.cooldowns is None:
        ps.cooldowns = {}

    print("✅ DCA SCALP Bot — MULTI-TRADE + AUTO SYMBOL + AUTO SIDE + Per-Symbol Cooldown + MAX_OPEN_TRADES")
    print(f"TRADING_ENABLED={TRADING_ENABLED} | HEDGE_MODE(FORCED)={HEDGE_MODE} | LEVERAGE={LEVERAGE} | MARGIN_TYPE={MARGIN_TYPE}")
    print(f"Universe={UNIVERSE_QUOTE} TOP_N_COINS={TOP_N_COINS} | MAX_OPEN_TRADES={MAX_OPEN_TRADES}")
    print(f"TREND={TREND_INTERVAL} EMA{TREND_EMA} | ENTRY={ENTRY_INTERVAL} EMA{FAST_EMA}/{SLOW_EMA} | LOOKBACK_MINUTES={LOOKBACK_MINUTES}")
    print(f"Pullback: lookback={PULLBACK_LOOKBACK_BARS} min={PULLBACK_MIN_PCT*100:.2f}% max={PULLBACK_MAX_PCT*100:.2f}%")
    print(f"Vol: lookbackBars={VOL_LOOKBACK_BARS} min={VOL_MIN} max={VOL_MAX}")
    print(f"DCA: base={DCA_BASE_USD} step={DCA_STEP_PCT*100:.2f}% mult={DCA_MULT} maxLv={DCA_MAX_LEVELS}")
    print(f"PnL Exit: use={USE_PNL_EXIT} TP={TP_PNL_USD} SL={SL_PNL_USD} | Price Exit: use={USE_PRICE_EXIT} TP%={TP_PCT*100:.2f} Stop%={HARD_STOP_PCT*100:.2f} TimeStop={TIME_STOP_MIN}m")
    print(f"Exposure: perTradeMax={MAX_USD_PER_TRADE} totalMax={MAX_TOTAL_USD_EXPOSURE} action={EXPOSURE_ACTION}")
    print(f"COOLDOWN_MIN(per symbol)={COOLDOWN_MIN} | RESELECT_MIN={RESELECT_MIN} | CHECK_SEC={CHECK_SEC} | DEBUG_SCAN={DEBUG_SCAN}")

    # Recover (live)
    recover_live_positions(ps)
    save_state(ps)

    last_heartbeat = 0.0

    while True:
        try:
            now = time.time()

            # Heartbeat
            if DEBUG_EVERY_SEC > 0 and now - last_heartbeat >= DEBUG_EVERY_SEC:
                last_heartbeat = now
                active_cd = sum(1 for _, t in ps.cooldowns.items() if now < float(t))
                print(f"HEARTBEAT | openTrades={len(ps.trades)}/{MAX_OPEN_TRADES} | cooldownActive={active_cd} | estExposure={portfolio_est_exposure(ps):.2f}")

            # =========================
            # 1) MANAGE ALL OPEN TRADES
            # =========================
            to_close: List[Tuple[str, str]] = []

            for sym, ts in list(ps.trades.items()):
                # safety: crossed guard
                if force_close_if_crossed(ps, sym, ts.side):
                    continue

                mark = get_mark(sym)
                held_min = (now - ts.entry_time) / 60.0 if ts.entry_time else 0.0
                side_txt = "LONG" if ts.side > 0 else "SHORT"

                if TRADING_ENABLED == 1 and USE_PNL_EXIT == 1:
                    amt, pnl = get_position(sym)
                    if amt == 0.0:
                        # exchange flat unexpectedly
                        to_close.append((sym, "EXCHANGE_FLAT"))
                        continue

                    print(f"POS {sym} {side_txt} | mark={mark:.6f} | pnl={pnl:.2f} | lv={ts.level}/{DCA_MAX_LEVELS} | estExp={ts.usd_est:.2f} | nextAdd={ts.next_add_price:.6f} | held={held_min:.1f}m")

                    if pnl >= TP_PNL_USD:
                        to_close.append((sym, "TP_PNL"))
                        continue
                    if pnl <= SL_PNL_USD:
                        to_close.append((sym, "SL_PNL"))
                        continue
                else:
                    print(f"POS {sym} {side_txt} | mark={mark:.6f} | lv={ts.level}/{DCA_MAX_LEVELS} | estExp={ts.usd_est:.2f} | nextAdd={ts.next_add_price:.6f} | held={held_min:.1f}m")

                # Price exits (based on estimated avg)
                if USE_PRICE_EXIT == 1 and ts.avg_entry_est > 0:
                    tp_px = price_tp(ts.avg_entry_est, ts.side, TP_PCT)
                    sl_px = price_stop(ts.avg_entry_est, ts.side, HARD_STOP_PCT)

                    if (ts.side > 0 and mark >= tp_px) or (ts.side < 0 and mark <= tp_px):
                        to_close.append((sym, "PRICE_TP"))
                        continue

                    if (ts.side > 0 and mark <= sl_px) or (ts.side < 0 and mark >= sl_px):
                        to_close.append((sym, "PRICE_STOP"))
                        continue

                if held_min >= TIME_STOP_MIN:
                    to_close.append((sym, "TIME_STOP"))
                    continue

                # DCA add
                can_add = (ts.level < DCA_MAX_LEVELS)
                hit_add = (ts.side > 0 and mark <= ts.next_add_price) or (ts.side < 0 and mark >= ts.next_add_price)
                if can_add and hit_add:
                    usd_add = DCA_BASE_USD * (DCA_MULT ** (ts.level - 1))
                    projected_trade = ts.usd_est + usd_add
                    projected_total = portfolio_est_exposure(ps) + usd_add

                    if projected_trade > MAX_USD_PER_TRADE or projected_total > MAX_TOTAL_USD_EXPOSURE:
                        if EXPOSURE_ACTION == "FORCE_CLOSE":
                            to_close.append((sym, "EXPOSURE_FORCE_CLOSE"))
                        else:
                            print(f"⛔ EXPOSURE STOP_ADD {sym} | tradeProj={projected_trade:.2f}/{MAX_USD_PER_TRADE:.2f} totalProj={projected_total:.2f}/{MAX_TOTAL_USD_EXPOSURE:.2f}")
                        continue

                    # live pre-check crossed again before add
                    if force_close_if_crossed(ps, sym, ts.side):
                        continue

                    ts.level += 1
                    ts.usd_est = projected_trade

                    # update estimated avg (for price exits and next add)
                    add_qty = (usd_add / mark) if mark > 0 else 0.0
                    total_cost = ts.avg_entry_est * ts.pos_qty_est + mark * add_qty
                    ts.pos_qty_est += add_qty
                    if ts.pos_qty_est > 0:
                        ts.avg_entry_est = total_cost / ts.pos_qty_est

                    ts.next_add_price = dca_next_add_price(ts.avg_entry_est, ts.side, DCA_STEP_PCT, ts.level)

                    if TRADING_ENABLED == 0:
                        print(f"[DRY] DCA ADD {sym} {side_txt} | lv={ts.level} usd={usd_add:.2f} mark={mark:.6f} newAvg={ts.avg_entry_est:.6f} nextAdd={ts.next_add_price:.6f} estExp={ts.usd_est:.2f}")
                    else:
                        print(f"➕ DCA ADD {sym} {side_txt} | lv={ts.level} usd={usd_add:.2f} mark={mark:.6f} newAvg={ts.avg_entry_est:.6f} nextAdd={ts.next_add_price:.6f} estExp={ts.usd_est:.2f}")
                        open_market(sym, ts.side, usd_add)

            # Apply closes
            for sym, reason in to_close:
                close_trade(ps, sym, reason)

            # =========================
            # 2) FILL EMPTY SLOTS (SCAN & OPEN)
            # =========================
            open_count = len(ps.trades)
            slots = max(0, MAX_OPEN_TRADES - open_count)

            if slots > 0:
                if ps.last_scan_ts and (now - ps.last_scan_ts) < (RESELECT_MIN * 60):
                    save_state(ps)
                    time.sleep(CHECK_SEC)
                    continue

                ps.last_scan_ts = now
                cands = scan_candidates(ps, need_slots=slots)

                if not cands:
                    print("SCAN: no entry candidates now.")
                else:
                    for score, sym, side, pb, vol in cands:
                        if len(ps.trades) >= MAX_OPEN_TRADES:
                            break

                        # re-check cooldown & open set (race safety)
                        if sym in ps.trades:
                            continue
                        cd_until = float(ps.cooldowns.get(sym, 0.0) or 0.0)
                        if cd_until and time.time() < cd_until:
                            continue

                        ensure_symbol_settings(sym)
                        mark = get_mark(sym)
                        side_txt = "LONG" if side > 0 else "SHORT"

                        ts = TradeState(
                            symbol=sym,
                            side=side,
                            open=True,
                            entry_time=time.time(),
                            level=1,
                            next_add_price=dca_next_add_price(mark, side, DCA_STEP_PCT, 1),
                            avg_entry_est=mark,
                            pos_qty_est=0.0,
                            usd_est=DCA_BASE_USD,
                        )

                        # exposure guards before entry
                        projected_total = portfolio_est_exposure(ps) + ts.usd_est
                        if ts.usd_est > MAX_USD_PER_TRADE or projected_total > MAX_TOTAL_USD_EXPOSURE:
                            print(f"⛔ ENTRY EXPOSURE SKIP {sym} | trade={ts.usd_est:.2f}/{MAX_USD_PER_TRADE:.2f} totalProj={projected_total:.2f}/{MAX_TOTAL_USD_EXPOSURE:.2f}")
                            continue

                        if TRADING_ENABLED == 0:
                            print(f"[DRY] ENTRY {sym} side={side_txt} score={score:.3f} pb={pb*100:.2f}% vol={vol:.5f} mark={mark:.6f} usd={DCA_BASE_USD:.2f} nextAdd={ts.next_add_price:.6f}")
                        else:
                            print(f"🚀 ENTRY {sym} side={side_txt} score={score:.3f} pb={pb*100:.2f}% vol={vol:.5f} mark={mark:.6f} usd={DCA_BASE_USD:.2f} nextAdd={ts.next_add_price:.6f}")
                            open_market(sym, side, DCA_BASE_USD)

                        ps.trades[sym] = ts
                        time.sleep(0.5)

            save_state(ps)
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
