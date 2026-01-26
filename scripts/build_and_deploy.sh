#!/bin/bash
set -euo pipefail

cd /opt/poly-gocopy

echo "Pulling latest changes..."
git pull

echo "Running tests..."
go test ./...

echo "Building binaries..."
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tmp/arbitrage ./cmd/arbitrage
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tmp/arbitrage-equal ./cmd/arbitrage-equal
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tmp/arbitrage-weighted ./cmd/arbitrage-weighted
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tmp/balance ./cmd/balance

echo "Installing binaries..."
install -m 700 /tmp/arbitrage /opt/poly-gocopy/bin/arbitrage
install -m 700 /tmp/arbitrage-equal /opt/poly-gocopy/bin/arbitrage-equal
install -m 700 /tmp/arbitrage-weighted /opt/poly-gocopy/bin/arbitrage-weighted
install -m 700 /tmp/balance /opt/poly-gocopy/bin/balance

echo "Restarting services..."
systemctl restart poly-gocopy-arbitrage
systemctl restart poly-gocopy-arbitrage-equal
systemctl restart poly-gocopy-arbitrage-weighted

echo "Done. Service status:"
systemctl status poly-gocopy-arbitrage --no-pager || true
systemctl status poly-gocopy-arbitrage-equal --no-pager || true
systemctl status poly-gocopy-arbitrage-weighted --no-pager || true
