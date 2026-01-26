# poly-gocopy — VPS operations (VSCode Remote SSH)

This doc is written for you (the operator of the VPS). It walks through:

1) copying `.env.example` → `.env`,
2) editing config safely on the server, and
3) rebuilding + deploying updated binaries,

using **VSCode Remote - SSH**.

## What runs where (this setup)

In our setup, **the source tree and the runtime directory are the same folder**:

- `DEPLOY_REMOTE_DIR=/opt/poly-gocopy`

That means `/opt/poly-gocopy` contains:

- Source code (this repo, including `.env.example` and `./cmd/...`)
- Runtime config at `/opt/poly-gocopy/.env` (loaded by the Go programs at startup)
- Binaries at `/opt/poly-gocopy/bin/arbitrage`, `/opt/poly-gocopy/bin/arbitrage-equal`, `/opt/poly-gocopy/bin/arbitrage-weighted`, and `/opt/poly-gocopy/bin/balance`
- Logs/output at `/opt/poly-gocopy/out/` (JSONL, etc.)

Note: `cmd/copytrade` is deprecated and no longer supported; the deploy/restart
scripts target arbitrage services only.

If you’re not sure what your service is using, run:

```bash
systemctl cat poly-gocopy-arbitrage | sed -n 's/^[[:space:]]*\\(WorkingDirectory\\|EnvironmentFile\\|ExecStart\\)=/\\1=/p'
```

## 0) Create or update your runtime `.env`

### 0.1 Where `.env` must live

The systemd services created by the deploy scripts typically use:

- `WorkingDirectory=/opt/poly-gocopy`
- `EnvironmentFile=/opt/poly-gocopy/.env`

So the `.env` you edit should usually be:

```bash
/opt/poly-gocopy/.env
```

### 0.2 Edit `/opt/poly-gocopy/.env`

Edit the file directly in VSCode or use a terminal editor:

```bash
nano /opt/poly-gocopy/.env
```

### 0.3 Key settings you will commonly edit

Examples (do not copy secrets from here; use your real values):

```bash
# RPC endpoints
RPC_WS_URL=wss://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY

# Polymarket CLOB
CLOB_URL=https://clob.polymarket.com
PRIVATE_KEY=0xYOUR_PRIVATE_KEY
FUNDER=0xYOUR_FUNDING_WALLET
CLOB_SIGNATURE_TYPE=2

# Arbitrage slugs file
EVENT_SLUGS_FILE=/opt/poly-gocopy/slugs.txt

# Arbitrage edge threshold (strict: priceA + priceB < threshold)
ARBITRAGE_MAX_PAIR_COST=0.96

# Enable live trading (default is dry-run)
ENABLE_TRADING=true
```

Important:

- After changing `.env`, you must **restart** the service for changes to take effect.

### 0.4 Create the file from `.env.example` (first time only)

If `.env` doesn't exist yet:

```bash
cd /opt/poly-gocopy
mkdir -p /opt/poly-gocopy/bin /opt/poly-gocopy/out
cp -n /opt/poly-gocopy/.env.example /opt/poly-gocopy/.env
chmod 600 /opt/poly-gocopy/.env
```

Notes:

- `cp -n` will **not** overwrite an existing `/opt/poly-gocopy/.env`.
- `.env.example` contains both "deploy settings" and "runtime settings". On the VPS you can delete the deploy section (`SSH_*`, `DEPLOY_*`) if you want; the bots only care about runtime keys.
- Never commit `.env` to git.

## 1) Restart after `.env` changes

Restart arbitrage:

```bash
systemctl restart poly-gocopy-arbitrage
systemctl status poly-gocopy-arbitrage --no-pager
```

Restart arbitrage-equal:

```bash
systemctl restart poly-gocopy-arbitrage-equal
systemctl status poly-gocopy-arbitrage-equal --no-pager
```

Restart arbitrage-weighted:

```bash
systemctl restart poly-gocopy-arbitrage-weighted
systemctl status poly-gocopy-arbitrage-weighted --no-pager
```

View logs:

```bash
journalctl -u poly-gocopy-arbitrage -n 200 --no-pager
journalctl -u poly-gocopy-arbitrage-equal -n 200 --no-pager
journalctl -u poly-gocopy-arbitrage-weighted -n 200 --no-pager
```

Follow logs live:

```bash
journalctl -u poly-gocopy-arbitrage -f --no-pager
journalctl -u poly-gocopy-arbitrage-equal -f --no-pager
journalctl -u poly-gocopy-arbitrage-weighted -f --no-pager
```

## 2) Rebuild and deploy binaries

Run the build script:

```bash
/opt/poly-gocopy/scripts/build_and_deploy.sh
```

This script pulls the latest code, runs tests, builds the binaries, installs them, and restarts the services.

## 3) (Optional) Update `slugs.txt` and hot-reload arbitrage

Edit the slugs file:

```bash
nano /opt/poly-gocopy/slugs.txt
```

Then reload the arbitrage service (this triggers `SIGHUP` when configured with `ExecReload`):

```bash
systemctl reload poly-gocopy-arbitrage
```

If reload is not configured on your unit, just restart:

```bash
systemctl restart poly-gocopy-arbitrage
```
