"""
STATELESS PAXGUSD ORB monitor, designed to run as a GitHub Actions job on a
schedule (see .github/workflows/live-monitor.yml). No API key needed - all
data used here is Delta Exchange's public market data.

*** Settings below are the COMEX-gold-validated ORB parameters, carried over *
*** as a provisional starting point. They have NOT yet been separately     *
*** validated on PAXGUSD's own history - update them once that backtest is *
*** done.                                                                  *

WHY "STATELESS": every GitHub Actions run starts a brand-new, empty machine
with no memory of the last run. So this script:
  1. loads state.json (committed in the repo) to see where it left off
  2. re-fetches every 1-minute PAXGUSD candle from today's session start up
     to now, from Delta's server (the authoritative, unchanging record)
  3. replays only the candles it hasn't processed yet through the EXACT same
     bar-by-bar logic as the original backtest/live-monitor script
  4. saves its new position back to state.json
This makes the result identical to what a continuously-running version would
have computed for the same closed candles - see the chat explanation for why.
The one real difference is notification timing (checked every few minutes
here, not instantly), not the correctness of what gets logged.

FILES this writes/updates in the repo (the workflow commits them every run):
  state.json       - internal bookkeeping, not meant to be read by a person
  live_log.txt     - human-readable running narrative (open this to just read)
  alerts_log.csv   - every early-warning alert: FIRED, then RESOLVED with
                     whether a real breakout followed
  trades_log.csv   - every paper trade, with entry/exit/result/R, AND the
                     dollar effect on three simulated account sizes
  SUMMARY.md       - always-current headline stats, rewritten every run
"""

import csv
import json
import os
import time
import datetime as dt
from collections import deque

import requests
import pytz

# ============================== settings ===================================
BASE_URL = "https://cdn.india.deltaex.org"
SYMBOL = "PAXGUSD"

RANGE_START_H, RANGE_START_M = 8, 30      # New York time, provisional
RANGE_MINUTES = 10
TRADE_END_H, TRADE_END_M = 11, 0
FLAT_H, FLAT_M = 15, 0

ATR_STOP_MULT = 1.0
RR = 3.5
MAX_RANGE_ATR_MULT = 2.0
ALERT_ATR_FRAC = 0.20
MOMENTUM_BARS = 3

NY_TZ = pytz.timezone("America/New_York")

BALANCES = [100.0, 1000.0, 10000.0]   # the three simulated account sizes
RISK_PERCENT = 1.0                    # fixed % risk per trade, same as TEST_fixed.py

STATE_FILE = "state.json"
LIVE_LOG = "live_log.txt"
ALERTS_LOG = "alerts_log.csv"
TRADES_LOG = "trades_log.csv"
SUMMARY_FILE = "SUMMARY.md"

TRADES_HEADER = (
    ["entry_time", "side", "entry", "sl", "tp", "exit_time", "exit", "result", "R", "had_alert"]
    + [f"{int(b)}_before" for b in BALANCES]
    + [f"{int(b)}_pnl" for b in BALANCES]
    + [f"{int(b)}_after" for b in BALANCES]
)
ALERTS_HEADER = ["event", "alert_id", "time", "side", "price", "range_high", "range_low", "atr", "outcome"]


# ------------------------------ small helpers -------------------------------
def log_line(msg):
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LIVE_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def ensure_csv(path, header):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(header)


def append_csv(path, header, row):
    ensure_csv(path, header)
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)


def fetch_candles(session, resolution, start, end):
    r = session.get(f"{BASE_URL}/v2/history/candles",
                     params=dict(symbol=SYMBOL, resolution=resolution, start=start, end=end),
                     timeout=20)
    r.raise_for_status()
    data = r.json()
    rows = data.get("result", []) if data.get("success") else []
    return sorted(rows, key=lambda r: r["time"])


def atr14_m15(session, before_ts):
    rows = fetch_candles(session, "15m", before_ts - 15 * 60 * 30, before_ts - 15 * 60)
    if len(rows) < 15:
        return None
    trs = []
    for i in range(1, len(rows)):
        h, l, pc = rows[i]["high"], rows[i]["low"], rows[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-14:]) / 14


def ny_time_of(ts_utc):
    d = dt.datetime.fromtimestamp(ts_utc, tz=pytz.utc).astimezone(NY_TZ)
    return d, d.hour * 60 + d.minute


def default_tier():
    return dict(balance=None, peak=None, max_dd_pct=0.0, trades=0, wins=0, losses=0)


def default_day_state(day_key):
    return dict(day=day_key, range_high=None, range_low=None, day_atr=None, range_ok=False,
                traded_today=False, alerted_up=False, alerted_dn=False,
                closes_before=[], open_trade=None)


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    s = dict(last_bar_time=None,
              tiers={str(int(b)): {**default_tier(), "balance": b, "peak": b} for b in BALANCES},
              alerts_total=0, alerts_followed=0, alerts_not_followed=0,
              trades_total=0, trades_wins=0, trades_losses=0, sum_R=0.0,
              trades_with_alert=0, wins_with_alert=0, trades_without_alert=0, wins_without_alert=0)
    s.update(default_day_state(None))
    return s


def save_state(s):
    with open(STATE_FILE, "w") as f:
        json.dump(s, f, indent=2)


# ------------------------------- main engine --------------------------------
def close_trade(state, ot, result, exit_px, exit_iso):
    risk = abs(ot["entry"] - ot["sl"])
    R = ((exit_px - ot["entry"]) / risk if ot["side"] == 1 else (ot["entry"] - exit_px) / risk)
    state["trades_total"] += 1
    state["sum_R"] += R
    if R > 0:
        state["trades_wins"] += 1
    else:
        state["trades_losses"] += 1
    if ot["had_alert"]:
        state["trades_with_alert"] += 1
        if R > 0:
            state["wins_with_alert"] += 1
    else:
        state["trades_without_alert"] += 1
        if R > 0:
            state["wins_without_alert"] += 1

    befores, pnls, afters = [], [], []
    for b in BALANCES:
        key = str(int(b))
        tier = state["tiers"][key]
        before = tier["balance"]
        risk_dollars = before * (RISK_PERCENT / 100.0)
        pnl = R * risk_dollars
        after = before + pnl
        tier["balance"] = after
        tier["peak"] = max(tier["peak"], after)
        dd = (after - tier["peak"]) / tier["peak"] * 100.0 if tier["peak"] > 0 else 0.0
        tier["max_dd_pct"] = min(tier["max_dd_pct"], dd)
        tier["trades"] += 1
        if R > 0:
            tier["wins"] += 1
        else:
            tier["losses"] += 1
        befores.append(round(before, 2))
        pnls.append(round(pnl, 2))
        afters.append(round(after, 2))

    log_line(f"PAPER TRADE CLOSED  {('BUY' if ot['side']==1 else 'SELL')} "
             f"entry {ot['entry']:.2f} -> exit {exit_px:.2f}  result={result}  R={R:.2f}  "
             f"(had_alert={ot['had_alert']})")
    append_csv(TRADES_LOG, TRADES_HEADER,
               [ot["entry_time"], "BUY" if ot["side"] == 1 else "SELL", ot["entry"], ot["sl"],
                ot["tp"], exit_iso, exit_px, result, round(R, 3), ot["had_alert"]]
               + befores + pnls + afters)


def process_bar(state, bar, ny_dt, tod):
    day_key = ny_dt.date().isoformat()
    if state.get("day") != day_key:
        state.update(default_day_state(day_key))

    range_start = RANGE_START_H * 60 + RANGE_START_M
    range_end = range_start + RANGE_MINUTES
    trade_end = TRADE_END_H * 60 + TRADE_END_M
    flat_time = FLAT_H * 60 + FLAT_M

    o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]

    # --- manage an open paper trade first ---
    ot = state["open_trade"]
    if ot is not None:
        hit_sl = (h >= ot["sl"]) if ot["side"] == -1 else (l <= ot["sl"])
        hit_tp = (l <= ot["tp"]) if ot["side"] == -1 else (h >= ot["tp"])
        is_flat_time = tod >= flat_time
        if hit_sl or hit_tp or is_flat_time:
            if hit_sl:
                result, exit_px = "SL", ot["sl"]
            elif hit_tp:
                result, exit_px = "TP", ot["tp"]
            else:
                result, exit_px = "FLAT", c
            close_trade(state, ot, result, exit_px, ny_dt.isoformat())
            state["open_trade"] = None

    # --- opening range build ---
    if range_start <= tod < range_end:
        state["range_high"] = h if state["range_high"] is None else max(state["range_high"], h)
        state["range_low"] = l if state["range_low"] is None else min(state["range_low"], l)
        state["closes_before"] = []

    if tod == range_end and state["range_high"] is not None and state["day_atr"] is None:
        a = atr14_m15(SESSION, bar["time"])
        state["day_atr"] = a
        if a:
            rng_w = state["range_high"] - state["range_low"]
            state["range_ok"] = rng_w <= MAX_RANGE_ATR_MULT * a
            verdict = "TRADEABLE" if state["range_ok"] else "SKIPPED (range too wide vs ATR)"
            log_line(f"Range set: {state['range_low']:.2f}-{state['range_high']:.2f} "
                     f"(width {rng_w:.2f}, ATR15 {a:.2f}) -> {verdict}")

    closes_before = deque(state["closes_before"], maxlen=MOMENTUM_BARS)

    # --- alerts + real signal ---
    if (range_end <= tod < trade_end and state["range_ok"] and not state["traded_today"]
            and state["open_trade"] is None):
        rh, rl, a = state["range_high"], state["range_low"], state["day_atr"]
        alert_zone = ALERT_ATR_FRAC * a

        if len(closes_before) == MOMENTUM_BARS:
            prior = list(closes_before)
            rising = all(prior[k] > prior[k - 1] for k in range(1, len(prior)))
            falling = all(prior[k] < prior[k - 1] for k in range(1, len(prior)))

            if not state["alerted_up"] and c < rh and (rh - h) <= alert_zone and rising:
                state["alerted_up"] = True
                aid = f"{day_key}-UP"
                log_line(f"\U0001F514 ALERT FIRED - WATCH BUY  approaching {rh:.2f}  [alert_id={aid}]")
                state["alerts_total"] += 1
                append_csv(ALERTS_LOG, ALERTS_HEADER,
                           ["FIRED", aid, ny_dt.isoformat(), "BUY", c, rh, rl, a, ""])

            if not state["alerted_dn"] and c > rl and (l - rl) <= alert_zone and falling:
                state["alerted_dn"] = True
                aid = f"{day_key}-DN"
                log_line(f"\U0001F514 ALERT FIRED - WATCH SELL  approaching {rl:.2f}  [alert_id={aid}]")
                state["alerts_total"] += 1
                append_csv(ALERTS_LOG, ALERTS_HEADER,
                           ["FIRED", aid, ny_dt.isoformat(), "SELL", c, rh, rl, a, ""])

        side = 1 if c > rh else (-1 if c < rl else 0)
        if side != 0:
            had_alert = state["alerted_up"] if side == 1 else state["alerted_dn"]
            aid = f"{day_key}-{'UP' if side == 1 else 'DN'}"
            if had_alert:
                state["alerts_followed"] += 1
                append_csv(ALERTS_LOG, ALERTS_HEADER,
                           ["RESOLVED", aid, ny_dt.isoformat(), "BUY" if side == 1 else "SELL",
                            c, rh, rl, a, "BREAKOUT_FOLLOWED"])
            sl = c - ATR_STOP_MULT * a if side == 1 else c + ATR_STOP_MULT * a
            tp = c + RR * ATR_STOP_MULT * a if side == 1 else c - RR * ATR_STOP_MULT * a
            log_line(f">>> {'BUY' if side==1 else 'SELL'} SIGNAL  entry~{c:.2f}  "
                     f"SL {sl:.2f}  TP {tp:.2f}  (had_alert={had_alert})")
            state["traded_today"] = True
            state["open_trade"] = dict(side=side, entry=c, sl=sl, tp=tp,
                                        entry_time=ny_dt.isoformat(), had_alert=had_alert)

    closes_before.append(c)
    state["closes_before"] = list(closes_before)

    # unresolved alerts once the trade window is about to end
    if tod == trade_end - 1:
        for flag, key in ((state["alerted_up"], "UP"), (state["alerted_dn"], "DN")):
            if flag and not state["traded_today"]:
                aid = f"{day_key}-{key}"
                state["alerts_not_followed"] += 1
                append_csv(ALERTS_LOG, ALERTS_HEADER,
                           ["RESOLVED", aid, ny_dt.isoformat(), "BUY" if key == "UP" else "SELL",
                            c, state["range_high"], state["range_low"], state["day_atr"], "NOT_FOLLOWED"])


def write_summary(state):
    lines = []
    now_utc = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines.append("# PAXGUSD ORB - live forward-test summary\n")
    lines.append(f"Last updated: {now_utc}\n")
    lines.append("**Provisional settings copied from COMEX-gold backtest - not yet separately "
                  "validated on PAXGUSD. Paper trading only, no real money involved. "
                  "Checked on a schedule (see workflow) - notification lag applies.**\n")

    at, af, anf = state["alerts_total"], state["alerts_followed"], state["alerts_not_followed"]
    resolved = af + anf
    lines.append("## Early-warning indicator accuracy\n")
    lines.append(f"- Alerts fired: {at}\n")
    lines.append(f"- Resolved so far: {resolved} (followed by real breakout: {af}, not followed: {anf})\n")
    if resolved:
        lines.append(f"- Follow-through rate: {af/resolved*100:.1f}%\n")

    tt, tw, tl, sr = state["trades_total"], state["trades_wins"], state["trades_losses"], state["sum_R"]
    lines.append("\n## Trades (paper)\n")
    lines.append(f"- Total: {tt}  |  Wins: {tw}  |  Losses: {tl}\n")
    if tt:
        lines.append(f"- Win rate: {tw/tt*100:.1f}%\n")
        lines.append(f"- Total R: {sr:.2f}  |  Avg R/trade: {sr/tt:.3f}\n")
    twa, wwa, twoa, wwoa = (state["trades_with_alert"], state["wins_with_alert"],
                            state["trades_without_alert"], state["wins_without_alert"])
    if twa:
        lines.append(f"- Trades WITH a prior alert: {twa}, win rate {wwa/twa*100:.1f}%\n")
    if twoa:
        lines.append(f"- Trades WITHOUT a prior alert: {twoa}, win rate {wwoa/twoa*100:.1f}%\n")

    lines.append("\n## Simulated account balances (1% risk per trade, compounding)\n")
    lines.append("| Starting balance | Current balance | Return | Max drawdown | Trades | Win rate |\n")
    lines.append("|---|---|---|---|---|---|\n")
    for b in BALANCES:
        tier = state["tiers"][str(int(b))]
        ret_pct = (tier["balance"] / b - 1) * 100
        wr = (tier["wins"] / tier["trades"] * 100) if tier["trades"] else 0.0
        lines.append(f"| ${b:,.0f} | ${tier['balance']:,.2f} | {ret_pct:+.2f}% | "
                      f"{tier['max_dd_pct']:.2f}% | {tier['trades']} | {wr:.1f}% |\n")

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.writelines(lines)


SESSION = requests.Session()


def main():
    state = load_state()
    now = int(time.time())

    ny_now, _ = ny_time_of(now)
    range_start_min = RANGE_START_H * 60 + RANGE_START_M
    window_start_ny = NY_TZ.localize(dt.datetime.combine(
        ny_now.date(), dt.time(0, 0))) + dt.timedelta(minutes=max(range_start_min - 20, 0))
    fetch_from = int(window_start_ny.timestamp())
    if state.get("last_bar_time"):
        fetch_from = min(fetch_from, state["last_bar_time"] + 60)

    rows = fetch_candles(SESSION, "1m", fetch_from, now)
    last_bar_time = state.get("last_bar_time")
    new_rows = [r for r in rows
                if (last_bar_time is None or r["time"] > last_bar_time) and r["time"] + 60 <= now]

    if not new_rows:
        log_line("No new closed 1-min bars since last run - nothing to do.")
    else:
        for bar in new_rows:
            ny_dt, tod = ny_time_of(bar["time"])
            process_bar(state, bar, ny_dt, tod)
            state["last_bar_time"] = bar["time"]

    write_summary(state)
    save_state(state)


if __name__ == "__main__":
    main()
