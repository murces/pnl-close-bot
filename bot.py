import os
import time
import math
import json
import statistics
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

from binance.client import Client

# =========================
# ENV
# =========================
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
if not API_KEY or not API_SECRET:
    raise SystemExit("Missing BINANCE_API_KEY / BINANCE_API_SECRET")

TRADING_ENABLED = int(os.getenv("TRADING_ENABLED", "0"))

LEVERAGE = int(os.getenv("LEVERAGE", "10"))
MARGIN_TYPE = os.getenv("MARGIN_TYPE", "CROSSED").upper()
RECV_WINDOW = int(os.getenv("RECV_WINDOW", "60000"))
HEDGE_MODE = int(os.getenv("HEDGE_MODE", "0"))  # 0 = one-way (no positionSide)

UNIVERSE_QUOTE = os.getenv("UNIVERSE_QUOTE", "USDT").upper()
TOP_N_COINS = int(os.getenv("TOP_N_COINS", "80"))
CHECK_SEC = int(os.getenv("CHECK_SEC", "5"))
LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "6000"))
RESELECT_MIN = int(os.getenv("RESELECT_MIN", "2"))
COOLDOWN_MIN = int(os.getenv("COOLDOWN_MIN", "10"))

TREND_INTERVAL = os.getenv("TREND_INTERVAL", "1h")
TREND_EMA = int(os.getenv("TREND_EMA", "200"))
ENTRY_INTERVAL = os.getenv("ENTRY_INTERVAL", "5m")
FAST_EMA = int(os.getenv("FAST_EMA", "20"))
SLOW_EMA = int(os.getenv("SLOW_EMA", "50"))
EMA_SLOPE_BARS = int(os.getenv("EMA_SLOPE_BARS", "3"))
TREND_SLOPE_BARS = int(os.getenv("TREND_SLOPE_BARS", "10"))

PULLBACK_LOOKBACK_BARS = int(os.getenv("PULLBACK_LOOKBACK_BARS", "90"))
EMA50_TOL_PCT = float(os.getenv("EMA50_TOL_PCT", "0.30")) / 100.0  # 0.30% -> 0.003

USE_VOL_FILTER = int(os.getenv("USE_VOL_FILTER", "1"))
VOL_LOOKBACK_BARS = int(os.getenv("VOL_LOOKBACK_BARS", "180"))
VOL_MIN = float(os.getenv("VOL_MIN", "0.0005"))
VOL_MAX = float(os.getenv("VOL_MAX", "0.0055"))

RISK_USD_PER_TRADE = float(os.getenv("RISK_USD_PER_TRADE", "12"))
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
ATR_MULT = float(os.getenv("ATR_MULT", "1.20"))
RR = float(os.getenv("RR", "1.50"))
MAX_NOTIONAL_PER_TRADE = float(os.getenv("MAX_NOTIONAL_PER_TRADE", "1200"))

TIME_STOP_MIN = int(os.getenv("TIME_STOP_MIN", "120"))

MAX_OPEN_LONG = int(os.getenv("MAX_OPEN_LONG", "3"))
MAX_OPEN_SHORT = int(os.getenv("MAX_OPEN_SHORT", "3"))
MAX_TOTAL_USD_EXPOSURE = float(os.getenv("MAX_TOTAL_USD_EXPOSURE", "2000"))
EXPOSURE_ACTION = os.getenv("EXPOSURE_ACTION", "STOP_ADD").upper()  # STOP_ADD or FORCE_CLOSE

STATE_PATH = os.getenv("STATE_PATH", "pro_state.json")
RECOVER_OPEN_POS = int(os.getenv("RECOVER_OPEN_POS", "1"))

DEBUG_SCAN = int(os.getenv("DEBUG_SCAN", "1"))
DEBUG_TOPK = int(os.getenv("DEBUG_TOPK", "5"))

# Telegram (only CLOSE reason+pnl)
LOG_TELEGRAM = int(os.getenv("LOG_TELEGRAM", "0"))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

import urllib.request
import urllib.parse

def tg_send(text: str) -> None:
    if LOG_TELEGRAM != 1 or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": "true"}
    data = urllib.parse.urlencode(payload).encode("utf-8")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")

# =========================
# Binance Client
# =========================
client = Client(API_KEY, API_SECRET)

# =========================
# CACHES
# =========================
_SYMBOL_SET: Optional[set] = None
_LOT_CACHE: Dict[str, Tuple[float, float]] = {}
_KLINE_CACHE: Dict[Tuple[str, str, int], Tuple[int, List[float], List[float], List[float]]] = {}  # (t, closes, highs, lows)

# =========================
# STATE
# =========================
@dataclass
class Trade:
    symbol: str
    side: str  # "LONG" or "SHORT"
    entry: float
    stop: float
    tp: float
    qty: float
    notional: float
    opened_ts: float

@dataclass
class State:
    trades: Dict[str, Trade]
    cooldowns: Dict[str, float]
    last_select_ts: float

def load_state() -> State:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            d = json.load(f)
        trades = {k: Trade(**v) for k, v in d.get("trades", {}).items()}
        cooldowns = {k: float(v) for k, v in d.get("cooldowns", {}).items()}
        last_select_ts = float(d.get("last_select_ts", 0.0))
        return State(trades=trades, cooldowns=cooldowns, last_select_ts=last_select_ts)
    except Exception:
        return State(trades={}, cooldowns={}, last_select_ts=0.0)

def save_state(st: State) -> None:
    try:
        d = {
            "trades": {k: asdict(v) for k, v in st.trades.items()},
            "cooldowns": st.cooldowns,
            "last_select_ts": st.last_select_ts,
        }
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2)
    except Exception as e:
        print(f"⚠️ State save failed: {e}")

# =========================
# Helpers
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

def get_position(symbol: str) -> Tuple[float, float, float]:
    data = client.futures_position_information(symbol=symbol, recvWindow=RECV_WINDOW)
    pos = data[0]
    amt = float(pos["positionAmt"])
    upl = float(pos["unRealizedProfit"])
    entry_price = float(pos.get("entryPrice", 0.0) or 0.0)
    return amt, upl, entry_price

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

def open_market(symbol: str, side: str, qty: float) -> None:
    if TRADING_ENABLED == 0:
        return
    if qty <= 0:
        raise RuntimeError("qty <= 0")
    order_side = "BUY" if side == "LONG" else "SELL"
    params = dict(symbol=symbol, side=order_side, type="MARKET", quantity=qty, recvWindow=RECV_WINDOW)
    # HEDGE_MODE=0 => no positionSide
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

def interval_to_minutes(interval: str) -> int:
    interval = interval.strip().lower()
    if interval.endswith("m"):
        return int(interval[:-1])
    if interval.endswith("h"):
        return int(interval[:-1]) * 60
    if interval.endswith("d"):
        return int(interval[:-1]) * 1440
    raise ValueError(f"Unsupported interval: {interval}")

def fetch_ohlc(symbol: str, interval: str, lookback_minutes: int) -> Tuple[List[float], List[float], List[float]]:
    mins = interval_to_minutes(interval)
    bars = max(200, lookback_minutes // mins)
    limit = min(1500, bars)
    cache_key = (symbol, interval, limit)

    try:
        kl1 = client.futures_klines(symbol=symbol, interval=interval, limit=1)
        last_open_time = int(kl1[-1][0])
        cached = _KLINE_CACHE.get(cache_key)
        if cached and cached[0] == last_open_time:
            return cached[1], cached[2], cached[3]
    except Exception:
        pass

    kl = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    last_open_time = int(kl[-1][0])
    closes = [float(k[4]) for k in kl]
    highs = [float(k[2]) for k in kl]
    lows  = [float(k[3]) for k in kl]
    _KLINE_CACHE[cache_key] = (last_open_time, closes, highs, lows)
    return closes, highs, lows

def ema(series: List[float], length: int) -> List[float]:
    if len(series) < length + 5:
        return []
    k = 2.0 / (length + 1.0)
    out = []
    e = series[0]
    for v in series:
        e = (v * k) + (e * (1 - k))
        out.append(e)
    return out

def atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> Optional[float]:
    n = min(len(highs), len(lows), len(closes))
    if n < period + 5:
        return None
    trs = []
    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1]),
        )
        trs.append(tr)
    if len(trs) < period:
        return None
    return statistics.mean(trs[-period:])

def vol_std(returns: List[float], lookback: int) -> Optional[float]:
    if len(returns) < lookback + 5:
        return None
    w = returns[-lookback:]
    if len(w) < 10:
        return None
    return statistics.pstdev(w)

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

def portfolio_exposure_usd(st: State) -> float:
    return sum(abs(t.notional) for t in st.trades.values())

def count_sides(st: State) -> Tuple[int, int]:
    longs = sum(1 for t in st.trades.values() if t.side == "LONG")
    shorts = sum(1 for t in st.trades.values() if t.side == "SHORT")
    return longs, shorts

def in_cooldown(st: State, symbol: str) -> bool:
    until = st.cooldowns.get(symbol, 0.0)
    return time.time() < until

def set_cooldown(st: State, symbol: str) -> None:
    st.cooldowns[symbol] = time.time() + COOLDOWN_MIN * 60

def close_trade(st: State, symbol: str, reason: str) -> None:
    # Send CLOSE telegram with reason + pnl (only)
    pnl_txt = "na"
    if TRADING_ENABLED == 1:
        try:
            _, pnl, _ = get_position(symbol)
            pnl_txt = f"{pnl:.2f}"
        except Exception:
            pnl_txt = "na"
    tg_send(f"CLOSE {symbol} | reason={reason} | pnl={pnl_txt}")

    if TRADING_ENABLED == 1:
        amt, _, _ = get_position(symbol)
        close_market_reduce_only(symbol, amt)

    if symbol in st.trades:
        del st.trades[symbol]
    set_cooldown(st, symbol)

def compute_candidate(symbol: str) -> Optional[Dict]:
    # Trend (1h)
    closes1, highs1, lows1 = fetch_ohlc(symbol, TREND_INTERVAL, LOOKBACK_MINUTES)
    if len(closes1) < TREND_EMA + TREND_SLOPE_BARS + 20:
        return None
    ema200 = ema(closes1, TREND_EMA)
    if not ema200:
        return None
    trend_now = ema200[-1]
    trend_prev = ema200[-1 - TREND_SLOPE_BARS]
    slope = trend_now - trend_prev
    price_1h = closes1[-1]

    if price_1h > trend_now and slope > 0:
        trend_side = "LONG"
    elif price_1h < trend_now and slope < 0:
        trend_side = "SHORT"
    else:
        return None

    # Entry (5m)
    closes, highs, lows = fetch_ohlc(symbol, ENTRY_INTERVAL, LOOKBACK_MINUTES)
    if len(closes) < max(SLOW_EMA, FAST_EMA) + PULLBACK_LOOKBACK_BARS + 20:
        return None

    efast = ema(closes, FAST_EMA)
    eslow = ema(closes, SLOW_EMA)
    if not efast or not eslow:
        return None

    price = closes[-1]
    e20 = efast[-1]
    e50 = eslow[-1]
    e20_prev = efast[-1 - EMA_SLOPE_BARS]
    e20_slope = e20 - e20_prev

    # Pullback touch condition (EMA20 touched in last lookback)
    lb = PULLBACK_LOOKBACK_BARS
    recent_closes = closes[-lb:]
    recent_e20 = efast[-lb:]
    if len(recent_e20) != len(recent_closes):
        # fallback if ema arrays slightly shorter
        m = min(len(recent_closes), len(recent_e20))
        recent_closes = recent_closes[-m:]
        recent_e20 = recent_e20[-m:]

    touched = False
    for c, e in zip(recent_closes, recent_e20):
        if trend_side == "LONG":
            if c < e:  # dipped below EMA20 at least once
                touched = True
                break
        else:
            if c > e:
                touched = True
                break
    if not touched:
        return None

    # Healthy pullback: avoid being too far beyond EMA50
    if trend_side == "LONG":
        if price < e50 * (1.0 - EMA50_TOL_PCT):
            return None
    else:
        if price > e50 * (1.0 + EMA50_TOL_PCT):
            return None

    # Re-entry trigger (EMA structure + slope + price confirmation)
    if trend_side == "LONG":
        if not (e20 > e50 and e20_slope > 0 and price > e20):
            return None
    else:
        if not (e20 < e50 and e20_slope < 0 and price < e20):
            return None

    # Vol filter (optional)
    if USE_VOL_FILTER == 1:
        rets = []
        for i in range(1, len(closes)):
            if closes[i-1] == 0:
                rets.append(0.0)
            else:
                rets.append((closes[i] / closes[i-1]) - 1.0)
        v = vol_std(rets, VOL_LOOKBACK_BARS)
        if v is None:
            return None
        if not (VOL_MIN <= v <= VOL_MAX):
            return None
    else:
        v = None

    # ATR for stop sizing
    a = atr(highs, lows, closes, ATR_PERIOD)
    if a is None or a <= 0:
        return None

    # Score: trend slope strength + "clean re-entry" proximity
    # (keep simple & stable)
    slope_score = min(1.0, abs(slope) / max(1e-9, abs(trend_now) * 0.01))  # normalized-ish
    reentry_score = 1.0 - min(1.0, abs(price - e20) / max(1e-9, price * 0.005))
    score = 0.65 * slope_score + 0.35 * reentry_score

    return {
        "symbol": symbol,
        "side": trend_side,
        "price": price,
        "atr": a,
        "score": score,
        "vol": v,
        "trend_slope": slope,
    }

def size_trade(symbol: str, side: str, entry: float, atr_val: float) -> Optional[Tuple[float, float, float, float]]:
    # stop distance via ATR
    stop_dist = ATR_MULT * atr_val
    if stop_dist <= 0:
        return None

    if side == "LONG":
        stop = entry - stop_dist
        tp = entry + RR * (entry - stop)
    else:
        stop = entry + stop_dist
        tp = entry - RR * (stop - entry)

    if stop <= 0 or tp <= 0:
        return None

    risk_perc = abs(entry - stop) / entry
    if risk_perc <= 0:
        return None

    notional = RISK_USD_PER_TRADE / risk_perc
    notional = min(notional, MAX_NOTIONAL_PER_TRADE)
    qty = floor_to_step(symbol, notional / entry)
    if qty <= 0:
        return None

    notional_eff = qty * entry
    return qty, notional_eff, stop, tp

def maybe_recover_positions(st: State) -> None:
    if TRADING_ENABLED != 1 or RECOVER_OPEN_POS != 1:
        return
    try:
        # we only recover symbols that are in universe (top coins), to avoid junk
        universe = set(get_universe_symbols(UNIVERSE_QUOTE, TOP_N_COINS))
        for sym in universe:
            try:
                amt, _, entry_price = get_position(sym)
                if amt == 0:
                    continue
                if sym in st.trades:
                    continue

                side = "LONG" if amt > 0 else "SHORT"
                mark = get_mark(sym)
                entry = entry_price if entry_price > 0 else mark

                # Build stop/tp from current ATR (best effort)
                closes, highs, lows = fetch_ohlc(sym, ENTRY_INTERVAL, LOOKBACK_MINUTES)
                a = atr(highs, lows, closes, ATR_PERIOD)
                if a is None:
                    continue
                sized = size_trade(sym, side, entry, a)
                if not sized:
                    continue
                qty_est, notional, stop, tp = sized

                # qty should be the actual open position qty (abs amt)
                qty_real = floor_to_step(sym, abs(amt))
                if qty_real <= 0:
                    continue

                st.trades[sym] = Trade(
                    symbol=sym, side=side, entry=entry, stop=stop, tp=tp,
                    qty=qty_real, notional=qty_real * entry, opened_ts=time.time()
                )
                print(f"⚠️ RECOVERED {sym} {side} amt={amt} entry={entry:.6f} mark={mark:.6f}")
            except Exception:
                continue
    except Exception as e:
        print(f"⚠️ Recover scan failed: {e}")

# =========================
# MAIN
# =========================
def main():
    sync_time()

    print("✅ PRO EMA Bot — NO DCA | Trend(1h EMA200+slope) + Re-entry(5m EMA20/50+slope+confirm) | RR exits | Per-symbol cooldown | Long/Short caps")
    print(f"TRADING_ENABLED={TRADING_ENABLED} | HEDGE_MODE={HEDGE_MODE} | LEVERAGE={LEVERAGE} | MARGIN_TYPE={MARGIN_TYPE}")
    print(f"Universe={UNIVERSE_QUOTE} TOP_N_COINS={TOP_N_COINS} CHECK_SEC={CHECK_SEC} RESELECT_MIN={RESELECT_MIN} COOLDOWN_MIN={COOLDOWN_MIN}")
    print(f"Trend: {TREND_INTERVAL} EMA{TREND_EMA} slopeBars={TREND_SLOPE_BARS} | Entry: {ENTRY_INTERVAL} EMA{FAST_EMA}/{SLOW_EMA} emaSlopeBars={EMA_SLOPE_BARS}")
    print(f"Risk: RISK_USD_PER_TRADE={RISK_USD_PER_TRADE} ATR({ATR_PERIOD})*{ATR_MULT} RR={RR} MAX_NOTIONAL_PER_TRADE={MAX_NOTIONAL_PER_TRADE}")
    print(f"Limits: MAX_OPEN_LONG={MAX_OPEN_LONG} MAX_OPEN_SHORT={MAX_OPEN_SHORT} MAX_TOTAL_USD_EXPOSURE={MAX_TOTAL_USD_EXPOSURE} action={EXPOSURE_ACTION}")
    print(f"Exits: TIME_STOP_MIN={TIME_STOP_MIN} | VolFilter={USE_VOL_FILTER} VOL[{VOL_MIN},{VOL_MAX}] lb={VOL_LOOKBACK_BARS}")
    print(f"State: {STATE_PATH} | Recover={RECOVER_OPEN_POS} | TelegramClose={LOG_TELEGRAM}")

    st = load_state()
    if TRADING_ENABLED == 1:
        maybe_recover_positions(st)

    last_loop_save = 0.0

    while True:
        try:
            # 1) Manage open trades
            now = time.time()
            to_close: List[Tuple[str, str]] = []

            for sym, tr in list(st.trades.items()):
                mark = get_mark(sym)

                # time stop
                held_min = (now - tr.opened_ts) / 60.0
                if held_min >= TIME_STOP_MIN:
                    to_close.append((sym, "TIME_STOP"))
                    continue

                # stop/tp hit
                if tr.side == "LONG":
                    if mark <= tr.stop:
                        to_close.append((sym, "PRICE_STOP"))
                        continue
                    if mark >= tr.tp:
                        to_close.append((sym, "PRICE_TP"))
                        continue
                else:
                    if mark >= tr.stop:
                        to_close.append((sym, "PRICE_STOP"))
                        continue
                    if mark <= tr.tp:
                        to_close.append((sym, "PRICE_TP"))
                        continue

            for sym, reason in to_close:
                print(f"✅ {reason} -> close {sym}")
                close_trade(st, sym, reason)
                time.sleep(0.5)

            # 2) Fill slots if needed
            longs, shorts = count_sides(st)
            open_total = len(st.trades)
            exposure = portfolio_exposure_usd(st)

            # optional periodic debug heartbeat
            if DEBUG_SCAN == 1:
                print(f"HEARTBEAT | openTrades={open_total} (L={longs}/S={shorts}) | cooldownActive={sum(1 for v in st.cooldowns.values() if now < v)} | estExposure={exposure:.2f}")

            # selection throttle
            if (now - st.last_select_ts) < (RESELECT_MIN * 60):
                if (now - last_loop_save) > 10:
                    save_state(st); last_loop_save = now
                time.sleep(CHECK_SEC)
                continue

            # determine needed side(s)
            need_long = longs < MAX_OPEN_LONG
            need_short = shorts < MAX_OPEN_SHORT
            if not need_long and not need_short:
                st.last_select_ts = now
                if (now - last_loop_save) > 10:
                    save_state(st); last_loop_save = now
                time.sleep(CHECK_SEC)
                continue

            if exposure >= MAX_TOTAL_USD_EXPOSURE and EXPOSURE_ACTION == "STOP_ADD":
                if DEBUG_SCAN == 1:
                    print("⛔ Exposure cap reached -> skip new entries")
                st.last_select_ts = now
                if (now - last_loop_save) > 10:
                    save_state(st); last_loop_save = now
                time.sleep(CHECK_SEC)
                continue

            universe = get_universe_symbols(UNIVERSE_QUOTE, TOP_N_COINS)

            # scan candidates
            candidates = []
            counters = {"seen": 0, "ok": 0, "cooldown": 0, "already_open": 0, "cap_side": 0, "data_fail": 0}

            for sym in universe:
                counters["seen"] += 1

                if sym in st.trades:
                    counters["already_open"] += 1
                    continue
                if in_cooldown(st, sym):
                    counters["cooldown"] += 1
                    continue

                cand = compute_candidate(sym)
                if not cand:
                    counters["data_fail"] += 1
                    continue

                side = cand["side"]
                if side == "LONG" and not need_long:
                    counters["cap_side"] += 1
                    continue
                if side == "SHORT" and not need_short:
                    counters["cap_side"] += 1
                    continue

                counters["ok"] += 1
                candidates.append(cand)

            if DEBUG_SCAN == 1:
                print(f"SCAN_DEBUG | seen={counters['seen']} ok={counters['ok']} already_open={counters['already_open']} cooldown={counters['cooldown']} cap_side={counters['cap_side']} data_fail={counters['data_fail']}")

            if not candidates:
                print("SCAN: no entry candidates now.")
                st.last_select_ts = now
                if (now - last_loop_save) > 10:
                    save_state(st); last_loop_save = now
                time.sleep(CHECK_SEC)
                continue

            candidates.sort(key=lambda x: x["score"], reverse=True)

            if DEBUG_SCAN == 1:
                print("SCAN_DEBUG_TOP:")
                for c in candidates[:max(1, DEBUG_TOPK)]:
                    vtxt = "na" if c["vol"] is None else f"{c['vol']:.5f}"
                    print(f"  - {c['symbol']} {c['side']} | score={c['score']:.3f} atr={c['atr']:.6f} vol={vtxt}")

            # try to open top candidate(s) until slots filled (but usually 1 per cycle)
            opened_any = False
            for c in candidates[:max(1, DEBUG_TOPK)]:
                sym = c["symbol"]
                side = c["side"]
                mark = get_mark(sym)

                sized = size_trade(sym, side, mark, c["atr"])
                if not sized:
                    continue
                qty, notional, stop, tp = sized

                # portfolio exposure guard
                if (portfolio_exposure_usd(st) + abs(notional)) > MAX_TOTAL_USD_EXPOSURE:
                    if EXPOSURE_ACTION == "STOP_ADD":
                        continue
                    elif EXPOSURE_ACTION == "FORCE_CLOSE":
                        # force close the worst pnl position? keep simple: skip
                        continue

                if TRADING_ENABLED == 1:
                    ensure_symbol_settings(sym)
                    open_market(sym, side, qty)

                st.trades[sym] = Trade(
                    symbol=sym, side=side, entry=mark, stop=stop, tp=tp,
                    qty=qty, notional=notional, opened_ts=time.time()
                )

                print(f"{'[DRY]' if TRADING_ENABLED==0 else '🚀'} ENTRY {sym} {side} | qty={qty} entry={mark:.6f} stop={stop:.6f} tp={tp:.6f} notional={notional:.2f} score={c['score']:.3f}")
                opened_any = True

                # update slot needs
                longs, shorts = count_sides(st)
                need_long = longs < MAX_OPEN_LONG
                need_short = shorts < MAX_OPEN_SHORT
                if not need_long and not need_short:
                    break

                time.sleep(0.3)

            st.last_select_ts = now
            save_state(st)
            last_loop_save = now

            if not opened_any:
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
