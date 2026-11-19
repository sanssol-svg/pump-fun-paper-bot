# pump.fun paper-trade bot

A dry-run (paper) trading bot for pump.fun tokens. It streams **new token
events** from pumpportal.fun and simulates a momentum + anti-rug strategy
using a **virtual** wallet — no real money, no real transactions.

> ⚠️ Educational only. Naive "buy every new token at birth" loses money to
> rug-pulls; the whole point of this repo is to measure that honestly before
> anyone risks real funds.

## Strategy (v2 — current)

New tokens go to a **watchlist**; we only take a paper entry when ALL pass:

1. sane name/symbol (no spam)
2. survived on DexScreener ≥ 3 min (wasn't rugged instantly)
3. momentum `+15%..+200%` from first observed market cap (real buyers, not
   just the dev seeding liquidity)
4. liquidity ≥ $25k (a pool you could actually sell into)
5. 24h volume ≥ $15k (people actually trading)

Exit: take-profit `+40%`, stop-loss `-30%`, timeout `45 min`.

## Why paper-trade

v1 bought every token the moment it launched and got rug-pulled (-69%, -66%
on the first two trades). v2 waits for a token to *prove* it has momentum.

## Run

```bash
python -m venv .venv && .venv/bin/pip install websockets
.venv/bin/python pump_paper_bot_v2.py
```

State/logs: `paper_state_v2.json`, `paper_log_v2.csv`.

## Data sources

- new-token stream: `wss://pumpportal.fun/api/data` (free)
- price/market-cap: DexScreener `latest/dex/tokens/{mint}` (free, polled)
