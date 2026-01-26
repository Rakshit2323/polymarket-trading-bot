package clob

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// OrderInfo mirrors the /data/order/<order_hash> response payload.
type OrderInfo struct {
	ID               string   `json:"id"`
	Status           string   `json:"status"`
	Market           string   `json:"market"`
	AssetID          string   `json:"asset_id"`
	Side             string   `json:"side"`
	Price            string   `json:"price"`
	OriginalSize     string   `json:"original_size"`
	SizeMatched      string   `json:"size_matched"`
	AssociatedTrades []string `json:"associate_trades"`
	Type             string   `json:"type"`
	OrderType        string   `json:"order_type"`
}

type orderInfoResp struct {
	Order *OrderInfo `json:"order"`
}

type cancelOrderReq struct {
	OrderID string `json:"orderID"`
}

// CancelOrder submits a cancel request for a single order ID/hash.
func (c *Client) CancelOrder(ctx context.Context, orderID string, useServerTime bool) (map[string]any, error) {
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return nil, fmt.Errorf("order id required")
	}
	if !c.HasApiCreds() {
		return nil, fmt.Errorf("api creds not configured")
	}

	body, err := json.Marshal(cancelOrderReq{OrderID: orderID})
	if err != nil {
		return nil, fmt.Errorf("marshal cancel order: %w", err)
	}

	path := "/order"
	ts, err := c.timestampForAuth(ctx, useServerTime)
	if err != nil {
		return nil, err
	}
	headers, err := c.l2Headers(ts, http.MethodDelete, path, body)
	if err != nil {
		return nil, err
	}

	var resp map[string]any
	if err := c.doJSONBody(ctx, http.MethodDelete, path, nil, headers, body, &resp); err != nil {
		return resp, err
	}
	return resp, nil
}

// MakerOrder represents a maker order fragment in a trade.
type MakerOrder struct {
	OrderID       string `json:"order_id"`
	MatchedAmount string `json:"matched_amount"`
	Price         string `json:"price"`
	AssetID       string `json:"asset_id"`
	Side          string `json:"side"`
}

// Trade mirrors the /data/trades response payload.
type Trade struct {
	ID           string       `json:"id"`
	Market       string       `json:"market"`
	AssetID      string       `json:"asset_id"`
	Side         string       `json:"side"`
	Size         string       `json:"size"`
	Price        string       `json:"price"`
	Status       string       `json:"status"`
	Type         string       `json:"type"`
	TakerOrderID string       `json:"taker_order_id"`
	MakerOrders  []MakerOrder `json:"maker_orders"`
}

// TradeParams holds optional filters for GetTrades.
type TradeParams struct {
	ID     string
	Taker  string
	Maker  string
	Market string
	Before string
	After  string
}

// GetOrder fetches a single order by ID/hash.
func (c *Client) GetOrder(ctx context.Context, orderID string, useServerTime bool) (*OrderInfo, error) {
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return nil, fmt.Errorf("order id required")
	}
	if !c.HasApiCreds() {
		return nil, fmt.Errorf("api creds not configured")
	}

	path := "/data/order/" + orderID
	ts, err := c.timestampForAuth(ctx, useServerTime)
	if err != nil {
		return nil, err
	}
	headers, err := c.l2Headers(ts, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	var resp orderInfoResp
	if err := c.doJSON(ctx, http.MethodGet, path, nil, headers, &resp); err != nil {
		return nil, err
	}
	if resp.Order == nil {
		return nil, fmt.Errorf("order missing in response")
	}
	return resp.Order, nil
}

// GetTrades fetches trades filtered by the provided parameters.
func (c *Client) GetTrades(ctx context.Context, params TradeParams, useServerTime bool) ([]Trade, error) {
	if !c.HasApiCreds() {
		return nil, fmt.Errorf("api creds not configured")
	}

	q := url.Values{}
	if params.ID != "" {
		q.Set("id", params.ID)
	}
	if params.Taker != "" {
		q.Set("taker", params.Taker)
	}
	if params.Maker != "" {
		q.Set("maker", params.Maker)
	}
	if params.Market != "" {
		q.Set("market", params.Market)
	}
	if params.Before != "" {
		q.Set("before", params.Before)
	}
	if params.After != "" {
		q.Set("after", params.After)
	}

	path := "/data/trades"
	signedPath := path
	if len(q) > 0 {
		signedPath = path + "?" + q.Encode()
	}

	ts, err := c.timestampForAuth(ctx, useServerTime)
	if err != nil {
		return nil, err
	}
	headers, err := c.l2Headers(ts, http.MethodGet, signedPath, nil)
	if err != nil {
		return nil, err
	}

	var resp []Trade
	if err := c.doJSON(ctx, http.MethodGet, signedPath, nil, headers, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}
