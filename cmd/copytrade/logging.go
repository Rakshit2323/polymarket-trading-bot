package main

import (
	"log"

	"poly-gocopy/internal/jsonl"
)

type copyLogEvent struct {
	TsMs  int64  `json:"ts_ms"`
	Event string `json:"event"`

	Mode string `json:"mode,omitempty"` // dry | live

	Leader        string   `json:"leader,omitempty"`
	Leaders       []string `json:"leaders,omitempty"`
	CopyBps       int64    `json:"copy_bps,omitempty"`
	ExactAsLeader bool     `json:"exact_as_leader,omitempty"`
	Backfill      bool     `json:"backfill,omitempty"`
	Confirmations uint64   `json:"confirmations,omitempty"`

	// Leader fill reference.
	TxHash   string `json:"tx_hash,omitempty"`
	LogIndex uint64 `json:"log_index,omitempty"`

	// Trade decision.
	TokenID     string `json:"token_id,omitempty"`
	Side        string `json:"side,omitempty"`
	AmountUnits string `json:"amount_units,omitempty"`

	Price    string `json:"price,omitempty"`
	TickSize string `json:"tick_size,omitempty"`

	BlockMs int64 `json:"block_ms,omitempty"`
	RxMs    int64 `json:"rx_ms,omitempty"`
	RxLagMs int64 `json:"rx_lag_ms,omitempty"`

	OrderType     string         `json:"order_type,omitempty"`
	EnableTrading bool           `json:"enable_trading,omitempty"`
	Ok            bool           `json:"ok,omitempty"`
	Resp          map[string]any `json:"resp,omitempty"`
	Err           string         `json:"err,omitempty"`

	UptimeMs int64 `json:"uptime_ms,omitempty"`
}

func copyMode(enableTrading bool) string {
	if enableTrading {
		return "live"
	}
	return "dry"
}

func logCopyEvent(w *jsonl.Writer, ev copyLogEvent) {
	if w == nil {
		return
	}
	if err := w.Write(ev); err != nil {
		log.Printf("[warn] trade log write failed: %v", err)
	}
}
