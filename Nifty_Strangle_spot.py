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

DB
--
• Single table: nifty_intraday_strangle_spot
• Stores ENTRY / EXIT / OPEN_NEW / SNAPSHOT / ERROR
• EXIT rows store CE & PE exit prices (FIXED)
"""

# =====================================================
# IMPORTS
# =====================================================

import os, time
from datetime import datetime, time as dtime
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

STRIKE_STEP = 50
QTY_PER_LEG = 65

INITIAL_OFFSET     = 100
MIN_THRESHOLD      = 25
LOSS_PUSH_DIST     = 50
MAX_STRANGLE_WIDTH = 150

ENTRY_TOLERANCE   = 4
ROLL_COOLDOWN_SEC = 3

TICK_INTERVAL     = 1
SNAPSHOT_INTERVAL = 60

MARKET_TZ = pytz.timezone("Asia/Kolkata")
MARKET_START = dtime(9, 20)

API_KEY      = os.getenv("KITE_API_KEY", "")
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

TABLE = "nifty_intraday_strangle_spot"

# =====================================================
# SAFETY
# =====================================================

if not API_KEY or not ACCESS_TOKEN:
    raise SystemExit("❌ Missing Kite credentials")

if not DATABASE_URL:
    raise SystemExit("❌ Missing DATABASE_URL")

# =====================================================
# KITE
# =====================================================

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

# =====================================================
# DB
# =====================================================

def db():
    return psycopg2.connect(DATABASE_URL)

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
            cur.execute(
                f"INSERT INTO {TABLE} ({cols}) VALUES ({ph})",
                vals
            )

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
# OPTION MAP
# =====================================================

def load_weekly_option_map():
    df = pd.DataFrame(kite.instruments(EXCH_OPT))
    df = df[(df["name"] == UNDERLYING) & (df["instrument_type"].isin(["CE","PE"]))]
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
}

# =====================================================
# LOGGING
# =====================================================

def calc_pnl(ce_ltp=None, pe_ltp=None):
    if not state["has_position"]:
        return None, state["realized_pnl"], state["realized_pnl"]

    ce_ltp = ce_ltp if ce_ltp is not None else get_ltp(state["ce_sym"])
    pe_ltp = pe_ltp if pe_ltp is not None else get_ltp(state["pe_sym"])

    unreal = ((state["ce_entry"] - ce_ltp) + (state["pe_entry"] - pe_ltp)) * QTY_PER_LEG
    total  = unreal + state["realized_pnl"]
    return unreal, state["realized_pnl"], total

def log_event(event, reason, status=None, ce_exit=None, pe_exit=None, extra=None):
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
        "reason": "INTERVAL",
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

        "extra": Json({})
    })

# =====================================================
# THREAT LOGIC
# =====================================================

def threat_dir(spot):
    if (state["ce"] - spot) <= MIN_THRESHOLD:
        return "UP"
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
# MAIN
# =====================================================

def main():
    init_db()
    opt_map = load_weekly_option_map()

    last_snap = 0
    print("[START] Threat-based NIFTY short strangle (NO STATE TABLE)")

    while True:
        try:
            spot = get_spot()
            state["spot"] = spot

            # ENTRY
            if not state["has_position"] and after_start() and on_strike_entry(spot):
                atm = nearest_strike(spot)
                ce = atm + INITIAL_OFFSET
                pe = atm - INITIAL_OFFSET

                state.update({
                    "has_position": True,
                    "ce": ce,
                    "pe": pe,
                    "ce_sym": opt_map[(ce, "CE")],
                    "pe_sym": opt_map[(pe, "PE")],
                    "ce_entry": get_ltp(opt_map[(ce, "CE")]),
                    "pe_entry": get_ltp(opt_map[(pe, "PE")]),
                    "last_roll_ts": time.time()
                })

                log_event("ENTRY", "ENTRY_ATM±OFFSET", "OPEN")

            # ADJUST
            if state["has_position"] and time.time() - state["last_roll_ts"] >= ROLL_COOLDOWN_SEC:
                d = threat_dir(spot)
                if d and can_adjust(spot, d):
                    ce_exit = get_ltp(state["ce_sym"])
                    pe_exit = get_ltp(state["pe_sym"])

                    delta = ((state["ce_entry"] - ce_exit) +
                             (state["pe_entry"] - pe_exit)) * QTY_PER_LEG
                    state["realized_pnl"] += delta

                    log_event("EXIT", f"THREAT_{d}", "EXITING",
                              ce_exit=ce_exit, pe_exit=pe_exit,
                              extra={"delta": delta})

                    if d == "UP":
                        new_ce = state["ce"] + LOSS_PUSH_DIST
                        new_pe = new_ce - MAX_STRANGLE_WIDTH
                    else:
                        new_pe = state["pe"] - LOSS_PUSH_DIST
                        new_ce = new_pe + MAX_STRANGLE_WIDTH

                    state.update({
                        "ce": new_ce,
                        "pe": new_pe,
                        "ce_sym": opt_map[(new_ce, "CE")],
                        "pe_sym": opt_map[(new_pe, "PE")],
                        "ce_entry": get_ltp(opt_map[(new_ce, "CE")]),
                        "pe_entry": get_ltp(opt_map[(new_pe, "PE")]),
                        "last_adjust_spot": spot,
                        "last_adjust_dir": d,
                        "last_roll_ts": time.time()
                    })

                    log_event("OPEN_NEW", f"ADJ_{d}", "OPEN")

            # SNAPSHOT
            if time.time() - last_snap >= SNAPSHOT_INTERVAL:
                log_snapshot()
                last_snap = time.time()

            time.sleep(TICK_INTERVAL)

        except Exception as e:
            log_event("ERROR", "EXCEPTION", "ERROR", extra={"error": str(e)})
            print("[ERROR]", e)
            time.sleep(2)

if __name__ == "__main__":
    main()
