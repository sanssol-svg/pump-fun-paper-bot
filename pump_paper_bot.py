#!/usr/bin/env python3
"""
pump.fun PAPER-TRADE bot (dry-run, no real money).

Strategy (simple + honest):
  - Stream NEW tokens from pumpportal.fun (free WebSocket).
  - Entry filter: sane name/symbol, marketCapSol 25..70 (fresh, not pumped).
  - Open a paper position of 10% of the virtual wallet (like the tweet).
  - Poll DexScreener for current market cap; exit on TP/SL/timeout.
  - Log everything to CSV. NO real transactions.

P&L approximation: uses market-cap ratio as a proxy for price change
(market cap scales ~linearly with price on the pump.fun bonding curve).
"""

import asyncio, json, time, csv, os, re, sys
import urllib.request
import websockets

# ---- config ----
VIRTUAL_WALLET_SOL = 10.0
POSITION_PCT = 0.10          # 10% of wallet per trade
MAX_POSITIONS = 5
ENTRY_MCAP_MIN = 25.0        # SOL
ENTRY_MCAP_MAX = 70.0
TAKE_PROFIT = 0.40           # +40%
STOP_LOSS = -0.30            # -30%
TIMEOUT_SEC = 45 * 60        # 45 min

LOG_DIR = "/home/ubuntu/pumpbot"
LOG_CSV = os.path.join(LOG_DIR, "paper_log.csv")
STATE_JSON = os.path.join(LOG_DIR, "paper_state.json")

UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def is_sane(name, symbol):
    """Reject obvious spam/rug names."""
    if not name or not symbol:
        return False
    if len(name) < 3 or len(name) > 24:
        return False
    if len(symbol) < 2 or len(symbol) > 10:
        return False
    # reject non-ascii-heavy / all-symbol garbage
    if re.search(r"[^\x20-\x7e]", name):
        return False
    # reject repeated-char spam like "AAAAAAAA"
    if re.search(r"(.)\1{4,}", name.upper()):
        return False
    # reject name == symbol (classic spam)
    if name.strip().upper() == symbol.strip().upper():
        return False
    return True


def get_price(mint):
    """Poll DexScreener for current market cap (USD) of a token."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        req = urllib.request.Request(url, headers=UA)
        data = json.load(urllib.request.urlopen(req, timeout=10))
        pairs = data.get("pairs") or []
        if not pairs:
            return None
        # prefer the pump.fun pair
        p = None
        for cand in pairs:
            if cand.get("dexId") == "pump.fun" or cand.get("chainId") == "solana":
                p = cand
                break
        if p is None:
            p = pairs[0]
        mcap = p.get("marketCap") or p.get("fdv")
        price = p.get("priceUsd")
        return {"mcap_usd": mcap, "price_usd": price,
                "liquidity": p.get("liquidity", {}).get("usd"),
                "volume": p.get("volume", {}).get("h24")}
    except Exception:
        return None


def load_state():
    if os.path.exists(STATE_JSON):
        try:
            return json.load(open(STATE_JSON))
        except Exception:
            pass
    return {"wallet_sol": VIRTUAL_WALLET_SOL, "positions": {}, "closed": []}


def save_state(state):
    json.dump(state, open(STATE_JSON, "w"), indent=1)


def log_csv(row):
    new = not os.path.exists(LOG_CSV)
    with open(LOG_CSV, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["ts", "mint", "name", "symbol", "entry_mcap_sol",
                        "exit_mcap_usd", "entry_mcap_usd", "pnl_pct",
                        "exit_reason", "duration_sec"])
        w.writerow(row)


async def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    state = load_state()
    print(f"[bot] paper-trade start | virtual wallet {state['wallet_sol']} SOL")
    print(f"[bot] entry mcap {ENTRY_MCAP_MIN}-{ENTRY_MCAP_MAX} SOL | "
          f"TP +{int(TAKE_PROFIT*100)}% SL {int(STOP_LOSS*100)}% timeout {TIMEOUT_SEC//60}m")

    uri = "wss://pumpportal.fun/api/data"
    entries = 0
    start = time.time()

    async def price_poller():
        """Every 30s, check open positions and apply exit rules."""
        while True:
            await asyncio.sleep(30)
            now = time.time()
            for mint, pos in list(state["positions"].items()):
                dur = now - pos["entry_ts"]
                info = await asyncio.to_thread(get_price, mint)
                if info and info.get("mcap_usd"):
                    entry_mcap_usd = pos.get("entry_mcap_usd")
                    cur = info["mcap_usd"]
                    pnl = (cur - entry_mcap_usd) / entry_mcap_usd if entry_mcap_usd else 0.0
                    reason = None
                    if pnl >= TAKE_PROFIT:
                        reason = "take_profit"
                    elif pnl <= STOP_LOSS:
                        reason = "stop_loss"
                    elif dur >= TIMEOUT_SEC:
                        reason = "timeout"
                    if reason:
                        # close position
                        size = pos["size_sol"]
                        pnl_sol = size * pnl
                        state["wallet_sol"] += size + pnl_sol
                        state["positions"].pop(mint, None)
                        state["closed"].append({
                            "mint": mint, "name": pos["name"], "symbol": pos["symbol"],
                            "pnl_pct": round(pnl, 4), "reason": reason,
                            "dur": int(dur)})
                        log_csv([int(now), mint, pos["name"], pos["symbol"],
                                 round(pos["entry_mcap_sol"], 4),
                                 round(cur, 2), round(entry_mcap_usd, 2),
                                 round(pnl, 4), reason, int(dur)])
                        print(f"[close] {pos['symbol']} {reason} pnl={pnl*100:+.1f}% "
                              f"({size:.3f}->{size+pnl_sol:.3f} SOL) wallet={state['wallet_sol']:.3f}")
                        save_state(state)

    poller = asyncio.create_task(price_poller())

    try:
        async with websockets.connect(uri, open_timeout=15) as ws:
            await ws.send(json.dumps({"method": "subscribeNewToken"}))
            print("[bot] subscribed to new-token stream")
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
                if not (ENTRY_MCAP_MIN <= mcap <= ENTRY_MCAP_MAX):
                    continue
                if len(state["positions"]) >= MAX_POSITIONS:
                    continue
                size = state["wallet_sol"] * POSITION_PCT
                if size <= 0:
                    print("[bot] wallet empty, stop taking entries")
                    continue
                # approximate entry mcap in USD: 1 SOL ~= 220 USD (rough)
                entry_mcap_usd = mcap * 220.0
                state["positions"][mint] = {
                    "name": name, "symbol": symbol,
                    "entry_mcap_sol": mcap, "entry_mcap_usd": entry_mcap_usd,
                    "entry_ts": time.time(), "size_sol": size,
                    "mint": mint}
                state["wallet_sol"] -= size
                entries += 1
                print(f"[entry] {symbol} ({name}) mcap={mcap:.1f} SOL "
                      f"size={size:.3f} SOL | open={len(state['positions'])} "
                      f"wallet={state['wallet_sol']:.3f}")
                save_state(state)
    except Exception as e:
        print(f"[bot] stream error: {type(e).__name__} {str(e)[:120]}")
    finally:
        poller.cancel()
        save_state(state)
        print(f"[bot] stopped | entries={entries} open={len(state['positions'])} "
              f"closed={len(state['closed'])} wallet={state['wallet_sol']:.3f} SOL")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
