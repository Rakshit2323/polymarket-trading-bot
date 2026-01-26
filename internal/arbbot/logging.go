package arbbot

import (
	"log"
	"time"

	"poly-gocopy/internal/jsonl"
)

type arbLogEvent struct {
	TsMs  int64  `json:"ts_ms"`
	Event string `json:"event"`

	Mode       string   `json:"mode,omitempty"`   // dry | live
	Source     string   `json:"source,omitempty"` // rtds | poll
	EventSlug  string   `json:"event_slug,omitempty"`
	EventSlugs []string `json:"event_slugs,omitempty"`

	// Auto-switch sessions: full rolling slug (stem-<timestamp>).
	WindowSlug string `json:"window_slug,omitempty"`

	TokenA string `json:"token_a,omitempty"`
	TokenB string `json:"token_b,omitempty"`

	// Per-order fields.
	TokenID      string         `json:"token_id,omitempty"`
	OrderType    string         `json:"order_type,omitempty"`
	Price        string         `json:"price,omitempty"`
	TickSize     string         `json:"tick_size,omitempty"`
	MakerMicros  uint64         `json:"maker_micros,omitempty"`
	SharesMicros uint64         `json:"shares_micros,omitempty"`
	Resp         map[string]any `json:"resp,omitempty"`
	Reason       string         `json:"reason,omitempty"`

	// Sell evaluation fields.
	TotalInvestedMicros uint64 `json:"total_invested_micros,omitempty"`
	MarkGrossMicros     uint64 `json:"mark_gross_micros,omitempty"`
	MarkNetMicros       uint64 `json:"mark_net_micros,omitempty"`
	PnLBps              int64  `json:"pnl_bps,omitempty"`
	MinProfitBps        int    `json:"min_profit_bps,omitempty"`
	TimeRemainingMs     int64  `json:"time_remaining_ms,omitempty"`

	CapAMicros   uint64 `json:"cap_a_micros,omitempty"`
	CapBMicros   uint64 `json:"cap_b_micros,omitempty"`
	SpentAMicros uint64 `json:"spent_a_micros,omitempty"`
	SpentBMicros uint64 `json:"spent_b_micros,omitempty"`

	EnableTrading bool `json:"enable_trading,omitempty"`

	// Per-order status.
	Ok bool `json:"ok,omitempty"`

	Err string `json:"err,omitempty"`

	UptimeMs int64 `json:"uptime_ms,omitempty"`
}

func arbMode(enableTrading bool) string {
	if enableTrading {
		return "live"
	}
	return "dry"
}

func logArbEvent(w *jsonl.Writer, ev arbLogEvent) {
	if w == nil {
		return
	}
	if err := w.Write(ev); err != nil {
		log.Printf("[warn] trade log write failed: %v", err)
	}
}

func safeTokenID(s *sideState) string {
	if s == nil {
		return ""
	}
	return s.tokenID
}

func uptimeMs(startedAt time.Time) int64 {
	return time.Since(startedAt).Milliseconds()
}
