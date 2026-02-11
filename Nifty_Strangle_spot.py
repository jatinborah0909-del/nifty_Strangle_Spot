#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY SPOT – THREAT-BASED AUTO ADJUSTING SHORT STRANGLE
Railway Deployable | DB only | NO STATE TABLE

FIXED LOGIC
-----------
• Adjust ONLY when spot comes within MIN_THRESHOLD of a sold leg
• After adjustment, do NOT adjust again unless spot moves another LOSS_PUSH_DIST
  in SAME direction
• Opposite direction adjustments are allowed immediately

UPGRADES (Option B)
-------------------
✅ LIVE Stocko order placement (ENTRY/EXIT/OPEN_NEW/TIME_EXIT/FLAG_EXIT)
✅ Kill-switch (READ ONLY): trade_flag.live_strangle_nifty_spot
   - If FALSE while OPEN -> square-off + HALT
   - Resume when TRUE again (and no open position)
✅ Order response JSON logged to DB (extra JSONB)
✅ Per-leg safe execution + rollback attempts
✅ Forced square-off at 15:25
✅ Optional PROFIT_TARGET and CIRCUIT_STOP_LOSS (env)
✅ Keeps PnL FIX: EXIT rows do not double-count unrealized
"""

# =====================================================
# IMPORTS
# =====================================================

import os, time, math, requests
from datetime import datetime, time as dtime, date
import pytz
import pandas as pd
import psycopg2
from psycopg2.extras import Json
from kiteconnect import KiteConnect

# =====================================================
# CONFIG (ENV)
# =====================================================

BOT_NAME = os.getenv("BOT_NAME", "NIFTY_INTRADAY_STRANGLE_SPOT")

UNDERLYING = "NIFTY"
EXCH_OPT   = "NFO"
SPOT_KEY   = "NSE:NIFTY 50"

STRIKE_STEP = int(os.getenv("STRIKE_STEP", "50"))
QTY_PER_LEG = int(os.getenv("QTY_PER_LEG", "65"))

INITIAL_OFFSET     = int(os.getenv("INITIAL_OFFSET", "100"))
MIN_THRESHOLD      = int(os.getenv("MIN_THRESHOLD", "25"))
LOSS_PUSH_DIST     = int(os.getenv("LOSS_PUSH_DIST", "50"))
MAX_STRANGLE_WIDTH = int(os.getenv("MAX_STRANGLE_WIDTH", "150"))

ENTRY_TOLERANCE   = int(os.getenv("ENTRY_TOLERANCE", "4"))
ROLL_COOLDOWN_SEC = int(os.getenv("ROLL_COOLDOWN_SEC", "3"))

TICK_INTERVAL     = float(os.getenv("TICK_INTERVAL", "1"))
SNAPSHOT_INTERVAL = int(os.getenv("SNAPSHOT_INTERVAL", "60"))

MARKET_TZ = pytz.timezone(os.getenv("MARKET_TZ", "Asia/Kolkata"))
MARKET_START = dtime(int(os.getenv("MARKET_START_HH", "9")), int(os.getenv("MARKET_START_MM", "20")))
SQUARE_OFF_TIME = dtime(int(os.getenv("SQUARE_OFF_HH", "15")), int(os.getenv("SQUARE_OFF_MM", "25")))

# Optional risk controls
PROFIT_TARGET     = float(os.getenv("PROFIT_TARGET", "0"))      # 0 disables
CIRCUIT_STOP_LOSS = float(os.getenv("CIRCUIT_STOP_LOSS", "0"))  # 0 disables; e.g. 4000 => exit if pnl <= -4000

API_KEY      = os.getenv("KITE_API_KEY", "").strip()
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

TABLE = os.getenv("TABLE", "nifty_intraday_strangle_spot").strip()

# Kill-switch (READ ONLY)
FLAG_TABLE = os.getenv("FLAG_TABLE", "trade_flag").strip()
FLAG_COL   = os.getenv("FLAG_COL", "live_strangle_nifty_spot").strip()

# LIVE MODE + STOCKO
LIVE_MODE = os.getenv("LIVE_MODE", "false").lower() in ("1", "true", "yes", "y", "on")
STOCKO_BASE_URL     = os.getenv("STOCKO_BASE_URL", "https://api.stocko.in").strip()
STOCKO_ACCESS_TOKEN = os.getenv("STOCKO_ACCESS_TOKEN", "").strip()
STOCKO_CLIENT_ID    = os.getenv("STOCKO_CLIENT_ID", "").strip()

# =====================================================
# SAFETY
# =====================================================

if not API_KEY or not ACCESS_TOKEN:
    raise SystemExit("❌ Missing Kite credentials (KITE_API_KEY / KITE_ACCESS_TOKEN)")

if not DATABASE_URL:
    raise SystemExit("❌ Missing DATABASE_URL")

if LIVE_MODE and (not STOCKO_ACCESS_TOKEN or not STOCKO_CLIENT_ID):
    raise SystemExit("❌ LIVE_MODE=True but missing STOCKO_ACCESS_TOKEN / STOCKO_CLIENT_ID")

# =====================================================
# KITE
# =====================================================

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# =====================================================
# DB
# =====================================================

def db():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def init_db():
    with db() as con:
        with con.cursor() as cur:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id BIGSERIAL PRIMARY KEY,
                ts TIMESTAMPTZ DEFAULT NOW(),
                bot_name TEXT,

                row_type TEXT,        -- EVENT / SNAPSHOT
                status TEXT,
                event TEXT,
                reason TEXT,

                spot NUMERIC,

                ce_strike INT,
                pe_strike INT,
                ce_symbol TEXT,
                pe_symbol TEXT,

                ce_entry NUMERIC,
                pe_entry NUMERIC,
                ce_ltp NUMERIC,
                pe_ltp NUMERIC,

                ce_exit NUMERIC,
                pe_exit NUMERIC,

                qty_per_leg INT,
                unreal_pnl NUMERIC,
                realized_pnl NUMERIC,
                total_pnl NUMERIC,

                adjust_dir TEXT,
                last_adjust_spot NUMERIC,

                extra JSONB
            );
            """)
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_ts ON {TABLE}(ts);")

def insert_row(data: dict):
    cols = ",".join(data.keys())
    vals = list(data.values())
    ph   = ",".join(["%s"] * len(vals))
    with db() as con:
        with con.cursor() as cur:
            cur.execute(f"INSERT INTO {TABLE} ({cols}) VALUES ({ph})", vals)

def read_trade_flag() -> bool:
    """
    Read-only kill switch.
    Safer default: if table/column missing or error -> treat as FALSE (no trading).
    """
    try:
        with db() as con:
            with con.cursor() as cur:
                cur.execute(f"SELECT {FLAG_COL} FROM {FLAG_TABLE} LIMIT 1;")
                r = cur.fetchone()
                return bool(r[0]) if r else False
    except Exception:
        return False

# =====================================================
# TIME / HELPERS
# =====================================================

def now():
    return datetime.now(MARKET_TZ)

def after_start():
    return now().time() >= MARKET_START

def nearest_strike(x):
    return int(round(x / STRIKE_STEP) * STRIKE_STEP)

def on_strike_entry(x):
    return abs(x - nearest_strike(x)) <= ENTRY_TOLERANCE

def safe_quote(keys):
    while True:
        try:
            return kite.quote(keys)
        except Exception:
            time.sleep(1)

def get_spot():
    return float(safe_quote([SPOT_KEY])[SPOT_KEY]["last_price"])

def get_ltp(sym):
    k = f"{EXCH_OPT}:{sym}"
    return float(safe_quote([k])[k]["last_price"])

# =====================================================
# STOCKO LIVE ORDER MODULE (safe + logged)
# =====================================================

def _stocko_headers():
    return {"Authorization": f"Bearer {STOCKO_ACCESS_TOKEN}", "Content-Type": "application/json"}

def _gen_user_order_id(offset=0) -> str:
    base = int(time.time() * 1000) + int(offset)
    return str(base)[-15:]

def stocko_search_token(tradingsymbol: str) -> int:
    url = f"{STOCKO_BASE_URL}/api/v1/search"
    r = requests.get(url, params={"key": tradingsymbol}, headers=_stocko_headers(), timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"Stocko search failed: {r.status_code} {r.text}")
    data = r.json()
    result = data.get("result") or data.get("data", {}).get("result", [])
    for rec in result:
        if rec.get("exchange") == "NFO":
            return int(rec["token"])
    raise RuntimeError(f"Stocko search: no NFO token found for {tradingsymbol}")

def stocko_place(tradingsymbol: str, side: str, qty: int, offset=0, retries=2):
    """
    Places a MARKET order on Stocko. Returns response JSON or {"simulated": True}.
    """
    if not LIVE_MODE:
        return {"simulated": True, "tradingsymbol": tradingsymbol, "side": side, "qty": qty}

    token = stocko_search_token(tradingsymbol)
    url = f"{STOCKO_BASE_URL}/api/v1/orders"

    payload = {
        "exchange": "NFO",
        "order_type": "MARKET",
        "instrument_token": int(token),
        "quantity": int(qty),
        "disclosed_quantity": 0,
        "order_side": side.upper(),
        "price": 0,
        "trigger_price": 0,
        "validity": "DAY",
        "product": "NRML",
        "client_id": STOCKO_CLIENT_ID,
        "user_order_id": _gen_user_order_id(offset),
        "market_protection_percentage": 0,
        "device": "WEB",
    }

    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.post(url, json=payload, headers=_stocko_headers(), timeout=10)
            if r.status_code != 200:
                raise RuntimeError(f"Stocko order failed: {r.status_code} {r.text}")
            return r.json()
        except Exception as e:
            last_err = str(e)
            time.sleep(0.4 + 0.3 * attempt)

    raise RuntimeError(last_err or "Stocko order failed (unknown)")

# =====================================================
# OPTION MAP
# =====================================================

def load_weekly_option_map():
    df = pd.DataFrame(kite.instruments(EXCH_OPT))
    df = df[(df["name"] == UNDERLYING) & (df["instrument_type"].isin(["CE","PE"]))].copy()
    df["expiry"] = pd.to_datetime(df["expiry"]).dt.normalize()

    exp = df[df["expiry"] >= pd.Timestamp.now().normalize()]["expiry"].min()
    df = df[df["expiry"] == exp]

    print(f"[CONFIG] Weekly expiry: {exp.date()}")
    return {(int(r.strike), r.instrument_type): r.tradingsymbol for _, r in df.iterrows()}

# =====================================================
# STATE (IN-MEMORY ONLY)
# =====================================================

state = {
    "has_position": False,
    "spot": None,

    "ce": None,
    "pe": None,
    "ce_sym": None,
    "pe_sym": None,

    "ce_entry": 0.0,
    "pe_entry": 0.0,

    "realized_pnl": 0.0,

    "last_adjust_spot": None,
    "last_adjust_dir": None,
    "last_roll_ts": 0.0,

    "halted": False,              # kill-switch halt
    "flag_cached": False,         # last read flag
}

# =====================================================
# LOGGING / PNL
# =====================================================

def calc_pnl(ce_ltp=None, pe_ltp=None):
    """
    Returns (unreal, realized, total) for CURRENT OPEN position.
    If no open position, unreal=None and total=realized.
    """
    if not state["has_position"]:
        return None, state["realized_pnl"], state["realized_pnl"]

    ce_ltp = ce_ltp if ce_ltp is not None else get_ltp(state["ce_sym"])
    pe_ltp = pe_ltp if pe_ltp is not None else get_ltp(state["pe_sym"])

    unreal = ((state["ce_entry"] - ce_ltp) + (state["pe_entry"] - pe_ltp)) * QTY_PER_LEG
    total  = unreal + state["realized_pnl"]
    return unreal, state["realized_pnl"], total

def log_event(event, reason, status=None, ce_exit=None, pe_exit=None, extra=None, is_exit=False):
    """
    ✅ FIX preserved:
    - On EXIT rows, do NOT compute unreal PnL from old legs (avoids double counting).
      For EXIT rows: unreal=0, total=realized (after realized has been updated).
    - For non-EXIT rows: total = realized + unreal (if open).
    """
    if is_exit:
        unreal = 0
        realized = state["realized_pnl"]
        total = realized
    else:
        unreal, realized, total = calc_pnl()

    insert_row({
        "bot_name": BOT_NAME,
        "row_type": "EVENT",
        "status": status,
        "event": event,
        "reason": reason,
        "spot": state["spot"],

        "ce_strike": state["ce"],
        "pe_strike": state["pe"],
        "ce_symbol": state["ce_sym"],
        "pe_symbol": state["pe_sym"],

        "ce_entry": state["ce_entry"],
        "pe_entry": state["pe_entry"],

        "ce_exit": ce_exit,
        "pe_exit": pe_exit,

        "qty_per_leg": QTY_PER_LEG,
        "unreal_pnl": unreal,
        "realized_pnl": realized,
        "total_pnl": total,

        "adjust_dir": state["last_adjust_dir"],
        "last_adjust_spot": state["last_adjust_spot"],

        "extra": Json(extra or {})
    })

def log_snapshot():
    ce_ltp = get_ltp(state["ce_sym"]) if state["has_position"] else None
    pe_ltp = get_ltp(state["pe_sym"]) if state["has_position"] else None

    unreal, realized, total = calc_pnl(ce_ltp, pe_ltp)

    insert_row({
        "bot_name": BOT_NAME,
        "row_type": "SNAPSHOT",
        "status": "RUNNING",
        "event": "SNAPSHOT",
        "reason": f"FLAG={state['flag_cached']},HALT={state['halted']}",
        "spot": state["spot"],

        "ce_strike": state["ce"],
        "pe_strike": state["pe"],
        "ce_symbol": state["ce_sym"],
        "pe_symbol": state["pe_sym"],

        "ce_entry": state["ce_entry"],
        "pe_entry": state["pe_entry"],
        "ce_ltp": ce_ltp,
        "pe_ltp": pe_ltp,

        "qty_per_leg": QTY_PER_LEG,
        "unreal_pnl": unreal,
        "realized_pnl": realized,
        "total_pnl": total,

        "adjust_dir": state["last_adjust_dir"],
        "last_adjust_spot": state["last_adjust_spot"],

        "extra": Json({})
    })

# =====================================================
# THREAT LOGIC
# =====================================================

def threat_dir(spot):
    # Threat on CE side: spot approaching CE strike from below
    if (state["ce"] - spot) <= MIN_THRESHOLD:
        return "UP"
    # Threat on PE side: spot approaching PE strike from above
    if (spot - state["pe"]) <= MIN_THRESHOLD:
        return "DOWN"
    return None

def can_adjust(spot, d):
    if state["last_adjust_spot"] is None:
        return True
    if d != state["last_adjust_dir"]:
        return True
    if d == "UP":
        return spot >= state["last_adjust_spot"] + LOSS_PUSH_DIST
    return spot <= state["last_adjust_spot"] - LOSS_PUSH_DIST

# =====================================================
# LIVE-SAFE EXECUTION HELPERS
# =====================================================

def safe_exit_all(reason: str, event_name: str):
    """
    Exits both legs (BUY to close) + logs EXIT with exit prices + clears position.
    Keeps your realized/unreal fix by using is_exit=True.
    """
    if not state["has_position"]:
        return

    orders = {"exit": {}, "errors": []}

    # Place exit orders first (best-effort)
    for leg, sym, off in [("CE", state["ce_sym"], 1001), ("PE", state["pe_sym"], 1002)]:
        try:
            orders["exit"][leg] = stocko_place(sym, "BUY", QTY_PER_LEG, offset=off)
        except Exception as e:
            orders["errors"].append({leg: str(e)})

    # Now fetch exit LTPs for realized calc
    ce_exit = get_ltp(state["ce_sym"])
    pe_exit = get_ltp(state["pe_sym"])

    delta = ((state["ce_entry"] - ce_exit) +
             (state["pe_entry"] - pe_exit)) * QTY_PER_LEG
    state["realized_pnl"] += delta

    log_event(
        "EXIT",
        reason,
        "EXITING",
        ce_exit=ce_exit,
        pe_exit=pe_exit,
        extra={"delta": delta, "orders": orders, "event": event_name},
        is_exit=True
    )

    # Clear position
    state.update({
        "has_position": False,
        "ce": None, "pe": None,
        "ce_sym": None, "pe_sym": None,
        "ce_entry": 0.0, "pe_entry": 0.0,
    })

def safe_enter_pair(ce_sym: str, pe_sym: str, reason: str, offsets=(1, 2)):
    """
    Place SELL orders for both legs.
    If one succeeds and the other fails, attempt rollback (BUY the successful one).
    """
    orders = {"entry": {}, "rollback": {}, "errors": []}
    placed = {"CE": False, "PE": False}

    # Try CE SELL
    try:
        orders["entry"]["CE"] = stocko_place(ce_sym, "SELL", QTY_PER_LEG, offset=offsets[0])
        placed["CE"] = True
    except Exception as e:
        orders["errors"].append({"CE_SELL": str(e)})

    # Try PE SELL
    try:
        orders["entry"]["PE"] = stocko_place(pe_sym, "SELL", QTY_PER_LEG, offset=offsets[1])
        placed["PE"] = True
    except Exception as e:
        orders["errors"].append({"PE_SELL": str(e)})

    # If one leg failed, rollback the other (best-effort) and fail the entry
    if not (placed["CE"] and placed["PE"]):
        if placed["CE"] and not placed["PE"]:
            try:
                orders["rollback"]["CE_BUY"] = stocko_place(ce_sym, "BUY", QTY_PER_LEG, offset=9001)
            except Exception as e:
                orders["errors"].append({"CE_ROLLBACK_FAIL": str(e)})

        if placed["PE"] and not placed["CE"]:
            try:
                orders["rollback"]["PE_BUY"] = stocko_place(pe_sym, "BUY", QTY_PER_LEG, offset=9002)
            except Exception as e:
                orders["errors"].append({"PE_ROLLBACK_FAIL": str(e)})

        raise RuntimeError(f"ENTRY_FAILED_PARTIAL. Details: {orders}")

    # If both placed, return order bundle for logging
    return orders

# =====================================================
# MAIN
# =====================================================

def main():
    init_db()
    opt_map = load_weekly_option_map()

    last_snap = 0
    print("[START] Threat-based NIFTY short strangle (NO STATE TABLE)")
    print(f"[MODE] LIVE_MODE={LIVE_MODE} | FLAG={FLAG_TABLE}.{FLAG_COL} | SQUARE_OFF={SQUARE_OFF_TIME.strftime('%H:%M')}")
    print(f"[RISK] PROFIT_TARGET={PROFIT_TARGET} | CIRCUIT_STOP_LOSS={CIRCUIT_STOP_LOSS}")

    while True:
        try:
            spot = get_spot()
            state["spot"] = spot

            # -------------------------------------------------
            # Forced square-off at 15:25
            # -------------------------------------------------
            if now().time() >= SQUARE_OFF_TIME:
                if state["has_position"]:
                    safe_exit_all("TIME_1525", "TIME_EXIT")
                log_event("TIME_EXIT", "STOP_PROCESS", "DONE", extra={"time": SQUARE_OFF_TIME.strftime("%H:%M")})
                print("[STOP] 15:25 square-off executed. Exiting.")
                return

            # -------------------------------------------------
            # Snapshot + Kill-switch check
            # -------------------------------------------------
            if time.time() - last_snap >= SNAPSHOT_INTERVAL:
                state["flag_cached"] = read_trade_flag()

                # Snapshot row (includes flag+halt in reason)
                log_snapshot()
                last_snap = time.time()

                # Enforce kill-switch: if false & position open -> exit + halt
                if (not state["flag_cached"]) and state["has_position"]:
                    safe_exit_all("FLAG_FALSE", "FLAG_EXIT")
                    state["halted"] = True
                    log_event("HALT", "FLAG_FALSE_HALT", "HALTED", extra={})
                # Resume: flag true, halted, and no position
                if state["flag_cached"] and state["halted"] and (not state["has_position"]):
                    state["halted"] = False
                    log_event("RESUME", "FLAG_TRUE_RESUME", "RUNNING", extra={})

            # If halted, do nothing else
            if state["halted"]:
                time.sleep(TICK_INTERVAL)
                continue

            # -------------------------------------------------
            # Optional risk checks (only if position open)
            # -------------------------------------------------
            if state["has_position"]:
                unreal, realized, total = calc_pnl()
                # circuit SL
                if CIRCUIT_STOP_LOSS > 0 and total is not None and total <= -abs(CIRCUIT_STOP_LOSS):
                    safe_exit_all("CIRCUIT_SL", "CIRCUIT_SL")
                    state["halted"] = True
                    log_event("HALT", "CIRCUIT_SL_HALT", "HALTED", extra={"total_pnl": total})
                    time.sleep(TICK_INTERVAL)
                    continue
                # profit target
                if PROFIT_TARGET > 0 and total is not None and total >= PROFIT_TARGET:
                    safe_exit_all("PROFIT_TARGET", "PROFIT_EXIT")
                    state["halted"] = True
                    log_event("HALT", "PROFIT_HALT", "HALTED", extra={"total_pnl": total})
                    time.sleep(TICK_INTERVAL)
                    continue

            # -------------------------------------------------
            # ENTRY
            # -------------------------------------------------
            if (not state["has_position"]) and after_start() and on_strike_entry(spot):
                # Block entry if flag is false (safer)
                if not state["flag_cached"]:
                    log_event("SKIP_ENTRY", "FLAG_FALSE", "SKIPPED", extra={})
                    time.sleep(TICK_INTERVAL)
                    continue

                atm = nearest_strike(spot)
                ce = atm + INITIAL_OFFSET
                pe = atm - INITIAL_OFFSET

                ce_sym = opt_map.get((ce, "CE"))
                pe_sym = opt_map.get((pe, "PE"))
                if not ce_sym or not pe_sym:
                    raise RuntimeError(f"Option symbol not found for strikes: CE={ce}, PE={pe}")

                # LIVE entry (SELL both) with rollback safety
                orders = safe_enter_pair(ce_sym, pe_sym, "ENTRY_ATM±OFFSET", offsets=(11, 12))

                state.update({
                    "has_position": True,
                    "ce": ce,
                    "pe": pe,
                    "ce_sym": ce_sym,
                    "pe_sym": pe_sym,
                    "ce_entry": get_ltp(ce_sym),
                    "pe_entry": get_ltp(pe_sym),
                    "last_roll_ts": time.time()
                })

                log_event("ENTRY", "ENTRY_ATM±OFFSET", "OPEN", extra={"orders": orders})

            # -------------------------------------------------
            # ADJUST
            # -------------------------------------------------
            if state["has_position"] and time.time() - state["last_roll_ts"] >= ROLL_COOLDOWN_SEC:
                d = threat_dir(spot)
                if d and can_adjust(spot, d):
                    # 1) Exit old legs (BUY to close) and log EXIT properly
                    safe_exit_all(f"THREAT_{d}", "ADJ_EXIT")

                    # If kill-switch turned false after exit, do not re-enter
                    if not state["flag_cached"]:
                        state["halted"] = True
                        log_event("HALT", "FLAG_FALSE_POST_EXIT", "HALTED", extra={})
                        time.sleep(TICK_INTERVAL)
                        continue

                    # 2) Compute new strikes
                    if d == "UP":
                        new_ce = (nearest_strike(spot) + INITIAL_OFFSET)  # keep consistent behavior: shift up logic via LOSS_PUSH_DIST below
                        # Original adjust logic:
                        new_ce = (new_ce if state["ce"] is None else (state["ce"] + LOSS_PUSH_DIST))
                        new_pe = new_ce - MAX_STRANGLE_WIDTH
                    else:
                        new_pe = (nearest_strike(spot) - INITIAL_OFFSET)
                        new_pe = (new_pe if state["pe"] is None else (state["pe"] - LOSS_PUSH_DIST))
                        new_ce = new_pe + MAX_STRANGLE_WIDTH

                    new_ce_sym = opt_map.get((new_ce, "CE"))
                    new_pe_sym = opt_map.get((new_pe, "PE"))
                    if not new_ce_sym or not new_pe_sym:
                        raise RuntimeError(f"Option symbol not found for new strikes: CE={new_ce}, PE={new_pe}")

                    # 3) Open new legs (SELL both) with rollback safety
                    orders = safe_enter_pair(new_ce_sym, new_pe_sym, f"ADJ_{d}", offsets=(21, 22))

                    # 4) Update state + log OPEN_NEW
                    state.update({
                        "has_position": True,
                        "ce": new_ce,
                        "pe": new_pe,
                        "ce_sym": new_ce_sym,
                        "pe_sym": new_pe_sym,
                        "ce_entry": get_ltp(new_ce_sym),
                        "pe_entry": get_ltp(new_pe_sym),
                        "last_adjust_spot": spot,
                        "last_adjust_dir": d,
                        "last_roll_ts": time.time()
                    })

                    log_event("OPEN_NEW", f"ADJ_{d}", "OPEN", extra={"orders": orders})

            time.sleep(TICK_INTERVAL)

        except Exception as e:
            try:
                log_event("ERROR", "EXCEPTION", "ERROR", extra={"error": str(e)})
            except Exception:
                pass
            print("[ERROR]", e)
            time.sleep(2)

if __name__ == "__main__":
    main()
