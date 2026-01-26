# `cmd/benchmark` (Polygon log latency benchmark)

This command subscribes to Polygon `OrderFilled` logs over a WebSocket RPC endpoint, filters for a leader proxy address, and periodically prints latency stats plus per‑tx samples.

`benchmark.ts` is kept as a reference of the pre‑migration behavior.

> Repo preference: configure via `.env` where possible. CLI flags still work and override env vars.

## Requirements

- Go 1.22+
- A Polygon WebSocket RPC endpoint (`wss://...`)

## Configure (`.env`)

For local development, the Go programs will automatically load a `.env` file from the current working directory (if present) before reading environment variables.

Create a `.env` in the repo root (or export env vars) with:

- `LEADER_PROXY` — leader proxy address(es) (single `0x...` or comma-separated)
- `LEADER_PROXIES` — optional alias for `LEADER_PROXY` (same format)
- `RPC_WS_URL` — preferred Polygon WSS endpoint
- `RPC_URL` / `POLYGON_WS_URL` — fallbacks if `RPC_WS_URL` not set
- `LATENCY_OUT_FILE` — output file path (JSONL) if `--out` not provided

The program refuses to start if the WS URL is not `wss://...` or still contains a `YOUR_KEY` placeholder.

## Run

Minimal (prefer `.env`):

```bash
go run ./cmd/benchmark
```

Fully explicit (flags override env vars):

```bash
go run ./cmd/benchmark \
  --leader=0xYourLeaderProxy \
  --polygon-ws=wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY \
  --dump-ms=10000 \
  --out=./out/latency.jsonl
```

## Notes (performance)

- The Go version avoids ABI unpacking for speed by reading `maker`/`taker` directly from indexed topics.
- Block timestamps are cached per block number to reduce RPC calls.

