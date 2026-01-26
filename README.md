# poly-gocopy

This repo contains a set of Go CLIs for Polymarket/Polygon automation, including:

- `cmd/copytrade` (deprecated; retained for reference only)
- `cmd/arbitrage`: two-token range bot (banded entries + configurable exit target; default 3%)
- `cmd/claim`: auto-claim redeemable (expired) wins

Benchmarking/latency tools live in:

- `cmd/benchmark/README.md`
- `cmd/latency/README.md`

For local development, the Go programs will automatically load a `.env` file from the current working directory (if present) before reading environment variables.

## Requirements

- Go 1.22+
- A Polygon WebSocket RPC endpoint (e.g. Alchemy/Infura/your own node)

## Future TODO

- [ ] Add a safer fill reconciliation path using trade history and/or the user WebSocket channel when `GET /data/order/<id>` returns missing.
- [ ] Add background inventory sync for arbitrage so manual trades are reflected.

## Deprecated: copytrade (taker-only, confirmed fills)

`cmd/copytrade` is deprecated and no longer supported. It remains in the repo
for reference only and may be removed in a future cleanup. If you still need to
inspect it, run `go run ./cmd/copytrade --help` locally; the deploy scripts do
not include copytrade anymore.

## Claim redeemable positions

`cmd/claim` checks the Polymarket Data API for redeemable positions tied to your
funding wallet and submits `redeemPositions` on-chain. By default it runs once
and exits; set `CLAIM_EVERY` to poll on an interval.

Dry-run (default):

```bash
go run ./cmd/claim
```

Enable claims:

```bash
export ENABLE_CLAIMS=true
go run ./cmd/claim
```

Common configuration lives in `.env.example`:

- `DATA_API_URL` (default `https://data-api.polymarket.com`)
- `CLAIM_EVERY` (e.g. `30m`)
- `CLAIM_SIZE_THRESHOLD` (set to `0` to include dust)
- `CLAIM_LIMIT` (max `500`)

When using a Safe (`CLOB_SIGNATURE_TYPE=2`), claims are sent one at a time and
the CLI waits for each transaction to confirm before sending the next, avoiding
Safe nonce race failures.

Relayer claims run alongside the weighted arbitrage bot by default. To override:

- `ARBITRAGE_WEIGHTED_CLAIM_ENABLE=false` (disable)
- `ARBITRAGE_WEIGHTED_CLAIM_EVERY=5m`
- `ARBITRAGE_WEIGHTED_CLAIM_CMD=uv run scripts/claim_relayer.py`

### Proxy/Safe wallets via Polymarket Relayer (recommended)

If your funding wallet is a Polymarket proxy (Safe or custom proxy wallet),
use the relayer client script instead of direct on-chain transactions:

```bash
uv run scripts/claim_relayer.py
```

This uses Polymarket's relayer for gasless transactions. It requires **builder
relayer access** and builder API credentials from your Polymarket Builder Profile:

- `POLY_BUILDER_API_KEY`
- `POLY_BUILDER_SECRET`
- `POLY_BUILDER_PASSPHRASE`

Set `RELAYER_TX_TYPE=SAFE` for browser wallets (Gnosis Safe). The Python relayer
client used by `scripts/claim_relayer.py` currently supports **Safe wallets only**;
for Magic/email proxy wallets, use the JS relayer client instead. If you do not
have builder credentials yet, request relayer access via the Builder Program.

## Range arbitrage (two-token price bands)

This repo includes a range arbitrage bot (`cmd/arbitrage`) that watches two complementary outcome tokens (e.g. UP/DOWN for the same market) and buys inside configured price bands (see `.env.example`).

### What it does (plain English)

- You give it two Polymarket outcome token IDs (token A and token B) that are complements.
- It subscribes to Polymarket RTDS and receives real-time aggregated order book snapshots (`clob_market` / `agg_orderbook`) for both tokens (asks/bids).
- It classifies the current best asks into win/lose bands using your configured ranges.
- It buys the winning side first (2/3 of the per-entry budget), then waits for the losing side to enter its band and buys that side (1/3).
- It respects per-side caps (`--cap-a` and `--cap-b`) so it can’t over-spend on either side.
- It sells both sides once mark-to-market PnL reaches your configured target (default 3%, set via `ARBITRAGE_RANGE_PROFIT_TARGET`; optional stop-loss adds after the configured delay).

### Token IDs: what are “token A” and “token B”?

They must be the **two complementary ERC1155 outcome token IDs for the same binary market** (e.g. YES/NO or UP/DOWN).

Practical ways to find them:

1. Open the market on polymarket.com.
2. Open browser DevTools → Network.
3. Look for either:
   - A WebSocket connection to `ws-live-data.polymarket.com` with a subscription to `clob_market` / `agg_orderbook`, where the `filters` array contains the token IDs, or
   - HTTP requests to `clob.polymarket.com` that include `token_id=...` query params.
4. Copy the two token IDs and pass them as `--token-a` and `--token-b`.

Example (placeholder IDs; replace with real ones):

```bash
go run ./cmd/arbitrage --token-a=123456 --token-b=789012
```

### Auto context switching for rolling markets (15m / hourly / 4h)

Some markets “roll” on a fixed cadence and encode the window start time in the event slug, for example:

- Full slug: `btc-updown-15m-1765791900`
- Slug stem (recommended input): `btc-updown-15m`

The trailing number is a Unix timestamp and increments by the window interval (e.g. 15m, 4h). If you pass `--event-slug`, the bot will:

1. Compute the *current* window start timestamp based on `--event-tz` (default `America/New_York`).
2. Fetch the corresponding event from the Gamma API and extract the `clobTokenIds`.
3. Use those two token IDs as `--token-a/--token-b`.
4. Automatically switch to the next window each time the window rolls.

Example:

```bash
go run ./cmd/arbitrage \
  --event-slug=btc-updown-15m \
  --cap-a=50 --cap-b=50
```

Hourly “up-or-down” markets use a different slug style on polymarket.com (human-readable ET), for example:

- Full slug: `bitcoin-up-or-down-december-15-3pm-et`
- Slug stem (recommended input): `bitcoin-up-or-down`

The bot will compute the correct `month-day-<hour><am|pm>-et` suffix for the current ET hour and auto-switch every hour.

Notes:

- Do not combine `--event-slug` with `--token-a/--token-b` (the slug mode derives token IDs automatically).
- Override Gamma or timezone if needed: `--gamma-url=...`, `--event-tz=...`.
- Multiple slugs are supported via a comma-separated list:
  - `--event-slug=btc-updown-15m,eth-updown-15m,sol-updown-15m,xrp-updown-15m`
  - or `EVENT_SLUGS=...`
- Optional: track a future window by adding a `+<n>` offset (in number of windows):
  - `--event-slug=btc-updown-15m+1` (next window)
  - `--event-slug=btc-updown-15m,btc-updown-15m+1` (current + next)
  - Note: listing `+1` as a separate slug trades that future window immediately; if you want “only start next when current is almost done”, use `ARBITRAGE_NEXT_SLUG_LAST` instead.
- Default: start trading the next window late in the current window:
  - `ARBITRAGE_NEXT_SLUG_LAST=3m` (start trading next window when current has <= 3 minutes remaining)
  - Set `ARBITRAGE_NEXT_SLUG_LAST=0` to disable lookahead.
- For an easily editable list, you can also use a file (one slug stem per line; supports `#` comments):
  - `--event-slugs-file=./slugs.txt`
  - or `EVENT_SLUGS_FILE=...` (reloadable via `SIGHUP`)
  - Reload example: `kill -HUP <pid>` (or `systemctl kill -s HUP <service>`)
- If you don’t pass any slug/token flags and a `slugs.txt` exists in your current directory (or a parent directory up to the module root), the bot will automatically use it.

### Run (dry-run)

Dry-run is the default (no orders are posted; it prints what it would do). No private key is required for dry-run.
`--cap-a` / `--cap-b` can also be set via `.env` using `ARBITRAGE_CAP_A` / `ARBITRAGE_CAP_B` (or `CAP_A` / `CAP_B`).
Range strategy entries use `ARBITRAGE_RANGE_ENTRY_USD` (default $3), split 2/3 win and 1/3 lose.
Range stop-loss/add uses `ARBITRAGE_RANGE_STOP_LOSS_AFTER` (default 30s), `ARBITRAGE_RANGE_STOP_LOSS_BUDGET` (default $10 per side), `ARBITRAGE_RANGE_STOP_LOSS_MAX_PRICE` (default $0.96), and `ARBITRAGE_STOP_LOSS_MARKET` (default true; market orders for stop-loss adds) to add to the winning side if PnL stays below your target (`ARBITRAGE_RANGE_PROFIT_TARGET`, default 3%).
Buy size is clipped by default using `ARBITRAGE_BUY_USD_MIN` / `ARBITRAGE_BUY_USD_MAX` (both default to $1 unless overridden).
`ARBITRAGE_CAP_SPLIT_BALANCE=true` (split 50/50 from on-chain USDC) is ignored in range-only mode.
To sanity-check the balance used for this, run `go run ./cmd/balance` (reads `.env`).

```bash
go run ./cmd/arbitrage \
  --token-a=<TOKEN_ID_A> \
  --token-b=<TOKEN_ID_B> \
  --cap-a=50 --cap-b=50 \
  --out=./out/arbitrage_trades.jsonl
```

If you want the older REST polling behavior (instead of RTDS), use `--source=poll`:

```bash
go run ./cmd/arbitrage \
  --source=poll \
  --poll=500ms \
  --token-a=<TOKEN_ID_A> \
  --token-b=<TOKEN_ID_B> \
  --cap-a=50 --cap-b=50
```

See all flags:

```bash
go run ./cmd/arbitrage --help
```

### Equal-shares strategy

The equal-shares strategy is a separate binary (`cmd/arbitrage-equal`). It waits for the
start delay, then targets equal shares per side based on a per-side budget and a target
average price (default `ARBITRAGE_EQUAL_SUM_MAX / 2`). It buys with FAK orders in small
increments, using a max acceptable price derived from the remaining total budget and
remaining target shares. The combined average (YES_avg + NO_avg) must be ≤ `SUM_MAX`
by completion; temporary overshoot is allowed if later cheaper buys can bring the sum
back under the cap. Within the last 3 minutes of the market window it enforces zero
share drift (equal shares). This strategy does not run stop-loss adds or profit-taking
sells; it holds until market resolution.

Configure via `.env` (preferred):

```
ARBITRAGE_EQUAL_ASSIGNED_USD=20
ARBITRAGE_EQUAL_START_DELAY=2m
ARBITRAGE_EQUAL_TARGET_AVG=0.485
ARBITRAGE_EQUAL_SUM_MAX=0.97
ARBITRAGE_EQUAL_SUM_ABORT=0.97
```

Note: total budget is `2x ARBITRAGE_EQUAL_ASSIGNED_USD`, spending can be uneven across sides,
and the bot waits if the best ask would violate the remaining-budget cap or if there is
insufficient size to meet the $1 minimum order.

Run (dry-run):

```bash
go run ./cmd/arbitrage-equal \
  --event-slug=btc-updown-15m \
  --cap-a=50 --cap-b=50
```

### Weighted-average strategy

The weighted-average strategy is a separate binary (`cmd/arbitrage-weighted`). It scans
order book depth each loop, uses a **total** budget and entry percentage, and only
buys matched shares when the combined average cost (A_avg + B_avg) stays under the
configured cap. If one side fills without the other, it pauses new entries and places
a recovery limit order on the missing side at the target price (rounded down to cents).
This strategy holds until resolution (no stop-loss adds or profit-taking sells).

Configure via `.env` (preferred):

```
ARBITRAGE_WEIGHTED_TOTAL_USD=40
ARBITRAGE_WEIGHTED_ENTRY_PCT=0.10
ARBITRAGE_WEIGHTED_SUM_MAX=0.97
ARBITRAGE_WEIGHTED_LOOP_INTERVAL=1s
```

Run (dry-run):

```bash
go run ./cmd/arbitrage-weighted \
  --event-slug=btc-updown-15m \
  --cap-a=50 --cap-b=50
```

### Sell / exit logic (default)

For the range strategy, the bot’s mark-to-market sell logic is always enabled. It will:

- Estimate your **liquidation value** if you sold both sides right now (using current bids).
- Compute PnL as a percentage of total invested (`(value - invested) / invested`).
- When PnL meets the range target (default 3.00%, `ARBITRAGE_RANGE_PROFIT_TARGET`), submit marketable **SELL** orders for both tokens.

Sell cadence and order type:

- Evaluate every `SELL_EVERY` interval (default `1.3s`).
- Use `SELL_ORDER_TYPE` for sells (default `FAK`).
- Optional: `ARBITRAGE_SELL_SLIPPAGE_BPS` to widen sell limits (default `0`).

Legacy rolling-window flags (`SELL_LAST`, `SELL_FORCE_LAST`, `SELL_MIN_PROFIT_EARLY/LATE`, `SELL_DISABLE_BUYS_IN_LAST`) are ignored in range-only mode.

All sell flags can also be set via `.env` (loaded automatically at startup). CLI flags override env. Env vars:
- `SELL_EVERY`, `SELL_ORDER_TYPE`, `ARBITRAGE_SELL_SLIPPAGE_BPS`

Example (`.env`):

```
SELL_EVERY=5s
SELL_ORDER_TYPE=FAK
ARBITRAGE_SELL_SLIPPAGE_BPS=25
```

### Run (live trading)

Live trading is gated behind `--enable-trading`. This will actually post orders, so start with small caps.

The bot uses the existing Polymarket CLOB auth flow: provide API creds explicitly or it will derive them.

```bash
export PRIVATE_KEY=0xYourPrivateKey
export FUNDER=0xYourFundingWallet
export CLOB_SIGNATURE_TYPE=2 # 2=POLY_GNOSIS_SAFE, 1=POLY_PROXY, 0=EOA

go run ./cmd/arbitrage \
  --token-a=<TOKEN_ID_A> \
  --token-b=<TOKEN_ID_B> \
  --cap-a=50 --cap-b=50 \
  --enable-trading
```

## Deploy to a VPS (systemd over SSH)

This repo includes two helper scripts that deploy a statically-linked Linux binary to a remote host and run it as a `systemd` service:

- `scripts/deploy_ssh.sh` — deploy one CLI (build + upload + install/enable service)
- `scripts/restart_services.sh` — restart/reload (or redeploy) arbitrage services together

### 1) Set up your VPS

Requirements on the VPS:

- Linux with `systemd`
- Outbound network access (Polygon WS RPC + Polymarket HTTP/WS)
- Either:
  - SSH access as `root`, or
  - SSH access as a non-root user with **passwordless sudo** (`sudo -n`)

### 2) Configure `.env`

Create a local `.env` in the repo root (see `.env.example`) and at minimum set:

- `SSH_SERVER` — `user@host`
- `SSH_PASSWORD` **or** `SSH_KEY_PATH`
- `SSH_USE_SUDO=1` if your SSH user is not root
- Runtime config required by the command(s) you deploy (RPC URLs, private key, etc.)

### 3) Deploy / redeploy

Deploy a single command:

```bash
./scripts/deploy_ssh.sh arbitrage -- --event-slug=btc-updown-15m --cap-a=50 --cap-b=50
./scripts/deploy_ssh.sh arbitrage-equal -- --event-slug=btc-updown-15m --cap-a=50 --cap-b=50
./scripts/deploy_ssh.sh arbitrage-weighted -- --event-slug=btc-updown-15m --cap-a=50 --cap-b=50
```

Redeploy all services + restart + show logs:

```bash
./scripts/restart_services.sh --deploy --logs
```

Reload-or-restart all services (uses `ExecReload` if supported; `arbitrage` reloads its slugs file via `SIGHUP` when running with `--event-slugs-file=...` or `EVENT_SLUGS_FILE=...`):

```bash
./scripts/restart_services.sh --reload --logs
```

Status only:

```bash
./scripts/restart_services.sh --status --logs
```

Follow logs:

```bash
./scripts/restart_services.sh --follow
```
