package gamma

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestResolveMarketBySlug_ParsesStringifiedArray(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events" {
			http.NotFound(w, r)
			return
		}
		if got := r.URL.Query().Get("slug"); got != "btc-updown-15m-1765791900" {
			http.Error(w, "bad slug", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
  {
    "slug": "btc-updown-15m-1765791900",
    "markets": [
      {
        "slug": "btc-updown-15m-1765791900",
        "outcomes": "[\"Up\",\"Down\"]",
        "clobTokenIds": "[\"1\",\"2\"]"
      }
    ]
  }
]`))
	}))
	defer srv.Close()

	c, err := NewClient(srv.URL)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := c.ResolveMarketBySlug(ctx, "btc-updown-15m-1765791900")
	if err != nil {
		t.Fatalf("ResolveMarketBySlug: %v", err)
	}
	if len(res.TokenIDs) != 2 || res.TokenIDs[0] != "1" || res.TokenIDs[1] != "2" {
		t.Fatalf("unexpected TokenIDs: %#v", res.TokenIDs)
	}
	if len(res.Outcomes) != 2 || res.Outcomes[0] != "Up" || res.Outcomes[1] != "Down" {
		t.Fatalf("unexpected Outcomes: %#v", res.Outcomes)
	}
}

func TestResolveMarketBySlug_ParsesArray(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/events" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
  {
    "slug": "x",
    "markets": [
      {
        "slug": "x",
        "outcomes": ["YES","NO"],
        "clobTokenIds": ["10","20"]
      }
    ]
  }
]`))
	}))
	defer srv.Close()

	c, err := NewClient(srv.URL)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := c.ResolveMarketBySlug(ctx, "x")
	if err != nil {
		t.Fatalf("ResolveMarketBySlug: %v", err)
	}
	if len(res.TokenIDs) != 2 || res.TokenIDs[0] != "10" || res.TokenIDs[1] != "20" {
		t.Fatalf("unexpected TokenIDs: %#v", res.TokenIDs)
	}
}
