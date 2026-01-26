package dataapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const DefaultURL = "https://data-api.polymarket.com"

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
		return nil, fmt.Errorf("data api url parse %q: %w", host, err)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return nil, fmt.Errorf("data api url must be http(s), got %q", host)
	}

	return &Client{
		host: host,
		httpClient: &http.Client{
			Timeout: 12 * time.Second,
		},
		userAgent: DefaultUserAgent,
	}, nil
}

type PositionsParams struct {
	User          string
	Market        []string
	EventID       []int
	SizeThreshold *float64
	Redeemable    *bool
	Mergeable     *bool
	Limit         int
	Offset        int
	SortBy        string
	SortDirection string
	Title         string
}

type Position struct {
	ProxyWallet        string  `json:"proxyWallet"`
	Asset              string  `json:"asset"`
	ConditionID        string  `json:"conditionId"`
	Size               float64 `json:"size"`
	AvgPrice           float64 `json:"avgPrice"`
	InitialValue       float64 `json:"initialValue"`
	CurrentValue       float64 `json:"currentValue"`
	CashPnl            float64 `json:"cashPnl"`
	PercentPnl         float64 `json:"percentPnl"`
	TotalBought        float64 `json:"totalBought"`
	RealizedPnl        float64 `json:"realizedPnl"`
	PercentRealizedPnl float64 `json:"percentRealizedPnl"`
	CurPrice           float64 `json:"curPrice"`
	Redeemable         bool    `json:"redeemable"`
	Mergeable          bool    `json:"mergeable"`
	Title              string  `json:"title"`
	Slug               string  `json:"slug"`
	Icon               string  `json:"icon"`
	EventSlug          string  `json:"eventSlug"`
	Outcome            string  `json:"outcome"`
	OutcomeIndex       int     `json:"outcomeIndex"`
	OppositeOutcome    string  `json:"oppositeOutcome"`
	OppositeAsset      string  `json:"oppositeAsset"`
	EndDate            string  `json:"endDate"`
	NegativeRisk       bool    `json:"negativeRisk"`
}

func (c *Client) GetPositions(ctx context.Context, params PositionsParams) ([]Position, error) {
	if c == nil {
		return nil, fmt.Errorf("data api client nil")
	}
	if strings.TrimSpace(params.User) == "" {
		return nil, fmt.Errorf("positions user required")
	}

	q := url.Values{}
	q.Set("user", strings.TrimSpace(params.User))
	if len(params.Market) > 0 {
		q.Set("market", strings.Join(params.Market, ","))
	}
	if len(params.EventID) > 0 {
		parts := make([]string, 0, len(params.EventID))
		for _, id := range params.EventID {
			parts = append(parts, strconv.Itoa(id))
		}
		q.Set("eventId", strings.Join(parts, ","))
	}
	if params.SizeThreshold != nil {
		q.Set("sizeThreshold", strconv.FormatFloat(*params.SizeThreshold, 'f', -1, 64))
	}
	if params.Redeemable != nil {
		q.Set("redeemable", strconv.FormatBool(*params.Redeemable))
	}
	if params.Mergeable != nil {
		q.Set("mergeable", strconv.FormatBool(*params.Mergeable))
	}
	if params.Limit > 0 {
		q.Set("limit", strconv.Itoa(params.Limit))
	}
	if params.Offset > 0 {
		q.Set("offset", strconv.Itoa(params.Offset))
	}
	if strings.TrimSpace(params.SortBy) != "" {
		q.Set("sortBy", strings.TrimSpace(params.SortBy))
	}
	if strings.TrimSpace(params.SortDirection) != "" {
		q.Set("sortDirection", strings.TrimSpace(params.SortDirection))
	}
	if strings.TrimSpace(params.Title) != "" {
		q.Set("title", strings.TrimSpace(params.Title))
	}

	endpoint := c.host + "/positions?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body := readBodyLimit(resp.Body, 8<<10)
		return nil, fmt.Errorf("data api %s: status=%d body=%q", endpoint, resp.StatusCode, body)
	}

	var out []Position
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("data api decode: %w", err)
	}
	return out, nil
}

func readBodyLimit(r io.Reader, limit int64) string {
	if r == nil {
		return ""
	}
	if limit <= 0 {
		limit = 8 << 10
	}
	b, _ := io.ReadAll(io.LimitReader(r, limit))
	return string(b)
}
