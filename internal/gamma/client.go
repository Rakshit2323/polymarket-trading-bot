package gamma

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const DefaultURL = "https://gamma-api.polymarket.com"

// DefaultUserAgent mimics a browser UA to avoid Cloudflare 403s.
const DefaultUserAgent = "Mozilla/5.0"

type Client struct {
	host       string
	httpClient *http.Client
	userAgent  string
}

func NewClient(host string) (*Client, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		host = DefaultURL
	}
	host = strings.TrimRight(host, "/")

	u, err := url.Parse(host)
	if err != nil {
		return nil, fmt.Errorf("gamma url parse %q: %w", host, err)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return nil, fmt.Errorf("gamma url must be http(s), got %q", host)
	}

	return &Client{
		host: host,
		httpClient: &http.Client{
			Timeout: 12 * time.Second,
		},
		userAgent: DefaultUserAgent,
	}, nil
}

type clobTokenIDs []string

func (c *clobTokenIDs) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		*c = nil
		return nil
	}

	// Gamma commonly returns clobTokenIds as a JSON string that itself contains a JSON array.
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		s = strings.TrimSpace(s)
		if s == "" {
			*c = nil
			return nil
		}
		var ids []string
		if err := json.Unmarshal([]byte(s), &ids); err != nil {
			return err
		}
		*c = ids
		return nil
	}

	// Some endpoints may return it directly as an array.
	var ids []string
	if err := json.Unmarshal(b, &ids); err != nil {
		return err
	}
	*c = ids
	return nil
}

type stringList []string

func (s *stringList) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		*s = nil
		return nil
	}

	// Gamma sometimes returns lists as a JSON string that itself contains a JSON array.
	if b[0] == '"' {
		var raw string
		if err := json.Unmarshal(b, &raw); err != nil {
			return err
		}
		raw = strings.TrimSpace(raw)
		if raw == "" {
			*s = nil
			return nil
		}
		var vals []string
		if err := json.Unmarshal([]byte(raw), &vals); err != nil {
			return err
		}
		*s = vals
		return nil
	}

	var vals []string
	if err := json.Unmarshal(b, &vals); err != nil {
		return err
	}
	*s = vals
	return nil
}

type event struct {
	Slug    string   `json:"slug"`
	Markets []market `json:"markets"`
}

type market struct {
	Slug         string       `json:"slug"`
	Outcomes     stringList   `json:"outcomes"`
	ClobTokenIDs clobTokenIDs `json:"clobTokenIds"`
}

type ResolvedMarket struct {
	EventSlug string
	Outcomes  []string
	TokenIDs  []string
}

func (c *Client) ResolveMarketBySlug(ctx context.Context, eventSlug string) (ResolvedMarket, error) {
	if c == nil {
		return ResolvedMarket{}, fmt.Errorf("gamma client nil")
	}
	eventSlug = strings.TrimSpace(eventSlug)
	if eventSlug == "" {
		return ResolvedMarket{}, fmt.Errorf("event slug required")
	}

	q := url.Values{}
	q.Set("slug", eventSlug)
	endpoint := c.host + "/events?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return ResolvedMarket{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return ResolvedMarket{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body := readBodyLimit(resp.Body, 8<<10)
		return ResolvedMarket{}, fmt.Errorf("gamma %s: status=%d body=%q", endpoint, resp.StatusCode, body)
	}

	var events []event
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&events); err != nil {
		return ResolvedMarket{}, fmt.Errorf("gamma decode: %w", err)
	}
	if len(events) == 0 {
		return ResolvedMarket{}, fmt.Errorf("gamma: no event for slug %q", eventSlug)
	}

	// Prefer a market with an exact matching slug, else fallback to the first market.
	var chosen *market
	for i := range events {
		ev := &events[i]
		for j := range ev.Markets {
			m := &ev.Markets[j]
			if strings.TrimSpace(m.Slug) == eventSlug {
				chosen = m
				break
			}
		}
		if chosen != nil {
			break
		}
	}
	if chosen == nil {
		if len(events[0].Markets) == 0 {
			return ResolvedMarket{}, fmt.Errorf("gamma: event %q has no markets", eventSlug)
		}
		chosen = &events[0].Markets[0]
	}

	ids := make([]string, 0, len(chosen.ClobTokenIDs))
	for _, id := range chosen.ClobTokenIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) != 2 {
		return ResolvedMarket{}, fmt.Errorf("gamma: expected 2 clobTokenIds for %q, got %d", eventSlug, len(ids))
	}

	return ResolvedMarket{
		EventSlug: eventSlug,
		Outcomes: append([]string(nil), chosen.Outcomes...),
		TokenIDs: ids,
	}, nil
}

func readBodyLimit(r io.Reader, max int64) string {
	if r == nil || max <= 0 {
		return ""
	}
	lr := &io.LimitedReader{R: r, N: max}
	b, _ := io.ReadAll(lr)
	return strings.TrimSpace(string(b))
}
