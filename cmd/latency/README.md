# `cmd/latency` (CLOB + RTDS latency probe)

This command measures:

- CLOB HTTP latency via `GET /time` (and optionally `GET /book?token_id=...`)
- RTDS WebSocket dial time + `ping`/`pong` RTT
- RTDS stream message delay for `clob_market` / `agg_orderbook` (requires token IDs)

> Repo preference: configure via `.env` where possible. CLI flags still work and override env vars.

## Configure (`.env`)

For local development, the Go programs will automatically load a `.env` file from the current working directory (if present) before reading environment variables.

Optional env vars:

- `LATENCY_LABEL` — label printed + included in JSONL rows
- `LATENCY_OUT_FILE` — output file path (if `--out` not provided)
- `CLOB_URL` — defaults to `https://clob.polymarket.com`
- `CLOB_TOKEN_ID` — enables `GET /book` probing
- `RTDS_URL` — defaults to `wss://ws-live-data.polymarket.com`
- `RTDS_FILTERS` — comma-separated token IDs for RTDS subscribe (if not using `--rtds-filters`)

## Run

Example:

```bash
go run ./cmd/latency \
  --label=vpn-nyc \
  --duration=30s \
  --print-format=table \
  --clob-no-keepalive \
  --token-id=<TOKEN_ID_FOR_BOOK> \
  --rtds-filters=<TOKEN_ID_A>,<TOKEN_ID_B> \
  --out=./out/clob_rtds_latency.jsonl
```

The default console output is a small human-readable table printed every `--print-every`, while `--out` writes raw JSONL samples for later analysis.

Single-line summary instead of the table:

```bash
go run ./cmd/latency --print-format=compact
```

Ping-only (skip RTDS subscribe / message latency):

```bash
go run ./cmd/latency --duration=20s --rtds-no-subscribe
```
