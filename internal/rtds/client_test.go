package rtds

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSubscribeRequest_JSONShape(t *testing.T) {
	req := subscribeRequest{
		Action: "subscribe",
		Subscriptions: []Subscription{
			{
				Topic:   "clob_market",
				Type:    "agg_orderbook",
				Filters: `["100","200"]`,
			},
		},
	}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got, ok := m["action"].(string); !ok || got != "subscribe" {
		t.Fatalf("action mismatch: %#v", m["action"])
	}
	subs, ok := m["subscriptions"].([]any)
	if !ok || len(subs) != 1 {
		t.Fatalf("subscriptions mismatch: %#v", m["subscriptions"])
	}
	sub0, ok := subs[0].(map[string]any)
	if !ok {
		t.Fatalf("subscription[0] type mismatch: %#v", subs[0])
	}
	if got := sub0["filters"]; got != `["100","200"]` {
		t.Fatalf("filters mismatch: got=%#v want=%q", got, `["100","200"]`)
	}
}

func TestOptions_WithDefaults(t *testing.T) {
	o := (Options{}).withDefaults()
	if o.PingInterval != DefaultPingInterval {
		t.Fatalf("PingInterval: got=%s want=%s", o.PingInterval, DefaultPingInterval)
	}
	if o.BackoffMin <= 0 || o.BackoffMax <= 0 {
		t.Fatalf("backoff defaults missing: %#v", o)
	}
	if o.OutBuffer <= 0 {
		t.Fatalf("OutBuffer default missing: %#v", o)
	}
}

func TestNextBackoff_CapsAtMax(t *testing.T) {
	if got := nextBackoff(2*time.Second, 3*time.Second); got != 3*time.Second {
		t.Fatalf("got=%s want=%s", got, 3*time.Second)
	}
	if got := nextBackoff(250*time.Millisecond, 3*time.Second); got != 500*time.Millisecond {
		t.Fatalf("got=%s want=%s", got, 500*time.Millisecond)
	}
}

