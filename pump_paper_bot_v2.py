#!/usr/bin/env python3
"""
pump.fun PAPER-TRADE bot v2 — momentum + anti-rug strategy (dry-run).

Key difference vs v1: we do NOT buy at token birth. New tokens go to a
WATCHLIST; we only enter when a token has actually SURVIVED and shown real
buy momentum, so we skip the dev-buy-then-rug pattern that killed v1.

Entry (all must pass):
  1. sane name/symbol (no spam)
  2. survived on DexScreener >= MIN_DEX_AGE_SEC (wasn't rugged instantly)
  3. momentum: current mcap >= (1+MOMENTUM_MIN) x first-observed mcap
     and <= (1+MOMENTUM_MAX) x  (up, but not so late we're exit liquidity)
  4. real liquidity >= MIN_LIQUIDITY_USD (pool you can actually sell into)
  5. real 24h volume >= MIN_VOLUME_USD (people actually trading it)

Exit: TP +40% / SL -30% / timeout 45min.
Prices from DexScreener (free). NO real transactions.
"""

import asyncio, json, time, csv, os, re
import urllib.request
import websockets

# ---- config ----
VIRTUAL_WALLET_SOL = 10.0
POSITION_PCT = 0.10
MAX_POSITIONS = 5
MAX_WATCHLIST = 30

MIN_DEX_AGE_SEC = 180          # survive at least 3 min
MOMENTUM_MIN = 0.15            # +15% from first dex observation
MOMENTUM_MAX = 2.0             # skip if already +200% (too late)
MIN_LIQUIDITY_USD = 25000.0
MIN_VOLUME_USD = 15000.0

ENTRY_MCAP_SOL_MIN = 15.0      # ignore dust / pre-pumped giants
ENTRY_MCAP_SOL_MAX = 120.0

TAKE_PROFIT = 0.40
STOP_LOSS = -0.30
TIMEOUT_SEC = 45 * 60

POLL_SEC = 30

LOG_DIR = "/home/ubuntu/pumpbot"
LOG_CSV = os.path.join(LOG_DIR, "paper_log_v2.csv")
STATE_JSON = os.path.join(LOG_DIR, "paper_state_v2.json")

UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def is_sane(name, symbol):
    if not name or not symbol:
        return False
    if len(name) < 3 or len(name) > 24:
        return False
    if len(symbol) < 2 or len(symbol) > 10:
        return False
    if re.search(r"[^\x20-\x7e]", name):
        return False
    if re.search(r"(.)\1{4,}", name.upper()):
        return False
    if name.strip().upper() == symbol.strip().upper():
        return False
    return True


def get_info(mint):
    """DexScreener lookup -> dict(mcap_usd, price_usd, liquidity, volume) or None."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        req = urllib.request.Request(url, headers=UA)
        data = json.load(urllib.request.urlopen(req, timeout=10))
        pairs = data.get("pairs") or []
        if not pairs:
            return None
        p = None
        for cand in pairs:
            if cand.get("dexId") == "pump.fun" or cand.get("chainId") == "solana":
                p = cand
                break
        if p is None:
            p = pairs[0]
        mcap = p.get("marketCap") or p.get("fdv")
        if not mcap:
            return None
        liq = p.get("liquidity", {}) or {}
        vol = p.get("volume", {}) or {}
        return {
            "mcap_usd": float(mcap),
            "price_usd": p.get("priceUsd"),
            "liquidity": liq.get("usd") or 0.0,
            "volume": vol.get("h24") or 0.0,
        }
    except Exception:
        return None


def load_state():
    if os.path.exists(STATE_JSON):
        try:
            return json.load(open(STATE_JSON))
        except Exception:
            pass
    return {
        "wallet_sol": VIRTUAL_WALLET_SOL,
        "watchlist": {},      # mint -> {name,symbol,create_ts,create_mcap_sol, first_dex_mcap, first_dex_ts}
        "positions": {},      # mint -> position
        "closed": [],
    }


def save_state(state):
    json.dump(state, open(STATE_JSON, "w"), indent=1)


def log_csv(row):
    new = not os.path.exists(LOG_CSV)
    with open(LOG_CSV, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "mint", "name", "symbol", "first_mcap_usd",
                        "entry_mcap_usd", "exit_mcap_usd", "pnl_pct",
                        "exit_reason", "duration_sec"])
        w.writerow(row)


async def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    state = load_state()
    print(f"[v2] paper-trade start | wallet {state['wallet_sol']} SOL")
    print(f"[v2] rule: survive>{MIN_DEX_AGE_SEC}s & momentum "
          f"+{int(MOMENTUM_MIN*100)}..{int(MOMENTUM_MAX*100)}% & "
          f"liq>${MIN_LIQUIDITY_USD/1e3:.0f}k & vol>${MIN_VOLUME_USD/1e3:.0f}k")
    print(f"[v2] TP +{int(TAKE_PROFIT*100)}% SL {int(STOP_LOSS*100)}% "
          f"timeout {TIMEOUT_SEC//60}m | poll {POLL_SEC}s")

    uri = "wss://pumpportal.fun/api/data"
    entries = 0

    async def watcher():
        """Poll watchlist + positions every POLL_SEC; apply momentum-entry
        and exit rules."""
        while True:
            await asyncio.sleep(POLL_SEC)
            now = time.time()

            # 1) promote watchlist -> position if momentum criteria pass
            for mint, w in list(state["watchlist"].items()):
                if len(state["positions"]) >= MAX_POSITIONS:
                    break
                info = await asyncio.to_thread(get_info, mint)
                if not info:
                    continue
                if w.get("first_dex_mcap") is None:
                    # first time visible on dex -> baseline
                    w["first_dex_mcap"] = info["mcap_usd"]
                    w["first_dex_ts"] = now
                    continue
                age = now - w["first_dex_ts"]
                first = w["first_dex_mcap"]
                if first <= 0:
                    continue
                momentum = info["mcap_usd"] / first - 1.0
                if (age >= MIN_DEX_AGE_SEC
                        and MOMENTUM_MIN <= momentum <= MOMENTUM_MAX
                        and info["liquidity"] >= MIN_LIQUIDITY_USD
                        and info["volume"] >= MIN_VOLUME_USD):
                    size = state["wallet_sol"] * POSITION_PCT
                    if size <= 0.0:
                        break
                    state["wallet_sol"] -= size
                    state["positions"][mint] = {
                        "mint": mint, "name": w["name"], "symbol": w["symbol"],
                        "entry_mcap_usd": info["mcap_usd"],
                        "entry_ts": now, "size_sol": size,
                        "momentum_at_entry": round(momentum, 3)}
                    state["watchlist"].pop(mint, None)
                    entries += 1
                    print(f"[entry] {w['symbol']} ({w['name']}) "
                          f"mom={momentum*100:+.1f}% liq=${info['liquidity']/1e3:.0f}k "
                          f"vol=${info['volume']/1e3:.0f}k size={size:.3f} SOL | "
                          f"open={len(state['positions'])} wallet={state['wallet_sol']:.3f}")
                    save_state(state)

            # 2) exit positions on TP/SL/timeout
            for mint, pos in list(state["positions"].items()):
                dur = now - pos["entry_ts"]
                info = await asyncio.to_thread(get_info, mint)
                if not info:
                    if dur >= TIMEOUT_SEC:
                        # price vanished -> treat as total loss (rug)
                        size = pos["size_sol"]
                        state["positions"].pop(mint, None)
                        state["closed"].append({
                            "mint": mint, "name": pos["name"], "symbol": pos["symbol"],
                            "pnl_pct": -1.0, "reason": "rug_vanished", "dur": int(dur)})
                        log_csv([int(now), mint, pos["name"], pos["symbol"],
                                 round(pos["entry_mcap_usd"], 2),
                                 round(pos["entry_mcap_usd"], 2), 0.0,
                                 -1.0, "rug_vanished", int(dur)])
                        print(f"[close] {pos['symbol']} rug_vanished (-100%) "
                              f"wallet={state['wallet_sol']:.3f}")
                        save_state(state)
                    continue
                entry = pos["entry_mcap_usd"]
                if entry <= 0:
                    continue
                pnl = info["mcap_usd"] / entry - 1.0
                reason = None
                if pnl >= TAKE_PROFIT:
                    reason = "take_profit"
                elif pnl <= STOP_LOSS:
                    reason = "stop_loss"
                elif dur >= TIMEOUT_SEC:
                    reason = "timeout"
                if reason:
                    size = pos["size_sol"]
                    pnl_sol = size * pnl
                    state["wallet_sol"] += size + pnl_sol
                    state["positions"].pop(mint, None)
                    state["closed"].append({
                        "mint": mint, "name": pos["name"], "symbol": pos["symbol"],
                        "pnl_pct": round(pnl, 4), "reason": reason, "dur": int(dur)})
                    log_csv([int(now), mint, pos["name"], pos["symbol"],
                             round(pos.get("first_mcap_usd", entry), 2),
                             round(entry, 2), round(info["mcap_usd"], 2),
                             round(pnl, 4), reason, int(dur)])
                    print(f"[close] {pos['symbol']} {reason} pnl={pnl*100:+.1f}% "
                          f"({size:.3f}->{size+pnl_sol:.3f} SOL) "
                          f"wallet={state['wallet_sol']:.3f}")
                    save_state(state)

            # 3) prune watchlist: drop anything never seen on dex after 10 min
            for mint, w in list(state["watchlist"].items()):
                if w.get("first_dex_mcap") is None and now - w["create_ts"] > 600:
                    state["watchlist"].pop(mint, None)

    poller = asyncio.create_task(watcher())

    try:
        async with websockets.connect(uri, open_timeout=15) as ws:
            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            print("[v2] subscribed to new-token stream (watchlist mode)")
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=60)
                except asyncio.TimeoutError:
                    continue
                try:
                    ev = json.loads(raw)
                except Exception:
                    continue
                if ev.get("txType") != "create":
                    continue
                name = ev.get("name", "")
                symbol = ev.get("symbol", "")
                mcap = float(ev.get("marketCapSol") or 0)
                mint = ev.get("mint", "")
                if not is_sane(name, symbol):
                    continue
                if not (ENTRY_MCAP_SOL_MIN <= mcap <= ENTRY_MCAP_SOL_MAX):
                    continue
                if mint in state["watchlist"] or mint in state["positions"]:
                    continue
                if len(state["watchlist"]) >= MAX_WATCHLIST:
                    # evict oldest watchlist entry
                    oldest = min(state["watchlist"].values(),
                                 key=lambda w: w["create_ts"])
                    state["watchlist"].pop(oldest["mint"], None)
                state["watchlist"][mint] = {
                    "name": name, "symbol": symbol,
                    "create_ts": time.time(), "create_mcap_sol": mcap,
                    "first_dex_mcap": None, "first_dex_ts": None,
                }
                print(f"[watch] {symbol} ({name}) mcap={mcap:.1f} SOL "
                      f"| wl={len(state['watchlist'])}")
                save_state(state)
    except Exception as e:
        print(f"[v2] stream error: {type(e).__name__} {str(e)[:120]}")
    finally:
        poller.cancel()
        save_state(state)
        print(f"[v2] stopped | entries={entries} open={len(state['positions'])} "
              f"closed={len(state['closed'])} watchlist={len(state['watchlist'])} "
              f"wallet={state['wallet_sol']:.3f} SOL")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
